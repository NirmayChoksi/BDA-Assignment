"""Streamlit dashboard for the Hadoop/Hive UPI transaction analysis.

All number crunching happens in Hadoop (see sql/analysis.sql, run via
scripts/run_pipeline.sh). This file only loads the small result CSVs
from data/processed/ and draws them.
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

RESULTS_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

st.set_page_config(page_title="UPI Transaction Analysis", layout="wide")


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

@st.cache_data
def read(name: str) -> pd.DataFrame:
    """Load one result CSV, e.g. read('q3_category') -> data/processed/q3_category.csv"""
    return pd.read_csv(RESULTS_DIR / f"{name}.csv")


def rupees(amount: float) -> str:
    """Format an INR amount: 327939009 -> '₹327.94 Cr'"""
    if amount >= 1e7:
        return f"₹{amount / 1e7:,.2f} Cr"
    return f"₹{amount / 1e5:,.2f} L"


def table(df: pd.DataFrame, renames: dict) -> None:
    """Show a result CSV as a table with readable column headers."""
    st.dataframe(df.rename(columns=renames), hide_index=True, width="stretch")


def chart(fig) -> None:
    """Show a plotly figure full width."""
    st.plotly_chart(fig, width="stretch")


# Common column renames, reused by several tables
HEADERS = {
    "txn_count": "Txns",
    "total_value_inr": "Value (INR)",
    "avg_ticket_inr": "Avg ₹",
}

# Fail early with a clear message if the pipeline has not run yet
try:
    kpi = read("q0_kpis").iloc[0]
except FileNotFoundError:
    st.error("No results found. Run `./scripts/run_pipeline.sh` first.")
    st.stop()


# ----------------------------------------------------------------------------
# Page title + headline numbers
# ----------------------------------------------------------------------------

st.title("Indian Consumer UPI Transaction Analysis")
st.caption(
    "250,000 transactions (FY2024) stored in HDFS, aggregated with Hive on Tez, visualised here."
)

headline = {
    "Transactions": f"{kpi.total_transactions:,.0f}",
    "Total value": rupees(kpi.total_value_inr),
    "Avg ticket": f"₹{kpi.avg_ticket_inr:,.0f}",
    "Success rate": f"{kpi.success_pct}%",
    "Fraud rate": f"{kpi.fraud_pct}%",
    "States / Banks": f"{kpi.states:.0f} / {kpi.sender_banks:.0f}",
}
for col, (label, value) in zip(st.columns(len(headline)), headline.items()):
    col.metric(label, value)


# ----------------------------------------------------------------------------
# Tabs
# ----------------------------------------------------------------------------

overview, trends, categories, geography, behaviour, risk = st.tabs(
    ["Overview", "Spending Trends", "Categories", "Geography", "Customer Behaviour", "Risk & Fraud"]
)


# --- Overview: payment modes, success/failure, banks -------------------------

with overview:
    modes = read("q5_type")
    status = read("q8_status")
    banks = read("q7_bank")

    left, right = st.columns(2)

    with left:
        st.subheader("Payment mode split")
        fig = px.pie(modes, names="txn_type", values="txn_count", hole=0.45)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        chart(fig)
        table(modes, {"txn_type": "Mode", **HEADERS})

    with right:
        st.subheader("Success vs failed by payment mode")
        pivot = status.pivot(index="txn_type", columns="txn_status", values="txn_count").fillna(0)
        pivot["fail_%"] = (100 * pivot["FAILED"] / pivot.sum(axis=1)).round(2)
        fig = px.bar(
            pivot.reset_index(),
            x="txn_type",
            y=["SUCCESS", "FAILED"],
            barmode="stack",
            labels={"value": "Transactions", "variable": "Status"},
        )
        chart(fig)
        st.dataframe(
            pivot["fail_%"].rename("Failure %").sort_values(ascending=False),
            width="stretch",
        )

    st.subheader("Bank market share (senders)")
    chart(
        px.bar(
            banks.sort_values("txn_count"),
            y="sender_bank",
            x="txn_count",
            orientation="h",
            color="total_value_inr",
            color_continuous_scale="Blues",
            labels={"sender_bank": "Bank", "txn_count": "Transactions"},
            hover_data={"total_value_inr": ":,"},
        )
    )


# --- Spending Trends: month by month ----------------------------------------

with trends:
    monthly = read("q1_monthly")

    st.subheader("Monthly transaction volume & value")
    fig = px.bar(
        monthly,
        x="month",
        y="txn_count",
        labels={"month": "Month", "txn_count": "Transactions"},
    )
    fig.add_scatter(
        x=monthly.month,
        y=monthly.total_value_inr,
        yaxis="y2",
        name="Value (INR)",
        line=dict(color="firebrick", width=2),
    )
    fig.update_layout(
        yaxis2=dict(overlaying="y", side="right", title="Value (INR)"),
        legend=dict(orientation="h", y=1.12),
    )
    chart(fig)

    best = monthly.loc[monthly.txn_count.idxmax()]
    worst = monthly.loc[monthly.txn_count.idxmin()]
    st.caption(
        f"Busiest month: {best.month} ({best.txn_count:,} txns), quietest: "
        f"{worst.month} ({worst.txn_count:,} txns)."
    )


# --- Categories: where the money goes ---------------------------------------

with categories:
    cat = read("q3_category")

    left, right = st.columns(2)

    with left:
        st.subheader("Value by merchant category")
        chart(
            px.bar(
                cat.sort_values("total_value_inr"),
                orientation="h",
                y="merchant_category",
                x="total_value_inr",
                labels={"merchant_category": "Category", "total_value_inr": "Value (INR)"},
                hover_data={"txn_count": ",", "avg_ticket_inr": ":,.0f"},
            )
        )

    with right:
        st.subheader("Average ticket size")
        chart(
            px.bar(
                cat.sort_values("avg_ticket_inr"),
                orientation="h",
                y="merchant_category",
                x="avg_ticket_inr",
                labels={"merchant_category": "Category", "avg_ticket_inr": "Avg ₹"},
            )
        )

    table(cat, {"merchant_category": "Category", **HEADERS, "max_ticket_inr": "Max ₹"})


# --- Geography: spend by state ----------------------------------------------

with geography:
    states = read("q4_state")

    left, right = st.columns(2)

    with left:
        st.subheader("Value by sender state")
        chart(
            px.bar(
                states.sort_values("total_value_inr"),
                orientation="h",
                y="sender_state",
                x="total_value_inr",
                labels={"sender_state": "State", "total_value_inr": "Value (INR)"},
            )
        )

    with right:
        st.subheader("Transaction count by sender state")
        chart(
            px.bar(
                states.sort_values("txn_count"),
                orientation="h",
                y="sender_state",
                x="txn_count",
                color="avg_ticket_inr",
                color_continuous_scale="Greens",
                labels={"sender_state": "State", "txn_count": "Txns", "avg_ticket_inr": "Avg ₹"},
            )
        )

    top = states.iloc[0]
    st.caption(
        f"Top state: {top.sender_state} — {top.txn_count:,} txns worth ₹{top.total_value_inr:,.0f}."
    )


# --- Customer Behaviour: who spends, on what device -------------------------

with behaviour:
    ages = read("q5b_age")
    devices = read("q6_device")
    networks = read("q6_network")

    st.subheader("Spending by sender age group")
    chart(
        px.bar(
            ages,
            x="sender_age_group",
            y="total_value_inr",
            color="avg_ticket_inr",
            color_continuous_scale="Teal",
            labels={
                "sender_age_group": "Age group",
                "total_value_inr": "Value (INR)",
                "avg_ticket_inr": "Avg ₹",
            },
            hover_data={"txn_count": ","},
        )
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Device used")
        chart(px.pie(devices, names="device_type", values="txn_count", hole=0.5))
    with right:
        st.subheader("Network used")
        chart(px.pie(networks, names="network_type", values="txn_count", hole=0.5))


# --- Risk & Fraud ------------------------------------------------------------

with risk:
    fraud_cat = read("q9_fraud_category")
    fraud_hour = read("q9_fraud_hour")
    fraud_state = read("q9_fraud_state")

    st.subheader("Fraud rate by merchant category")
    chart(
        px.bar(
            fraud_cat,
            x="merchant_category",
            y="fraud_pct",
            labels={"merchant_category": "Category", "fraud_pct": "Fraud %"},
            hover_data={"txn_count": ",", "fraud_count": ","},
        )
    )

    left, right = st.columns(2)

    with left:
        st.subheader("Fraud rate by hour of day")
        chart(
            px.line(
                fraud_hour,
                x="hour_of_day",
                y="fraud_pct",
                markers=True,
                labels={"hour_of_day": "Hour", "fraud_pct": "Fraud %"},
            )
        )

    with right:
        st.subheader("Fraud rate by sender state")
        chart(
            px.bar(
                fraud_state.sort_values("fraud_pct"),
                orientation="h",
                y="sender_state",
                x="fraud_pct",
                labels={"sender_state": "State", "fraud_pct": "Fraud %"},
                hover_data={"txn_count": ",", "fraud_count": ","},
            )
        )

    st.caption(
        f"Overall fraud rate is {kpi.fraud_pct}% — "
        f"{int(fraud_cat.fraud_count.sum())} flagged transactions across all categories."
    )
