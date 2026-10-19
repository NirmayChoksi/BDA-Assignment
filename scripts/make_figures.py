"""Regenerate every figure used in REPORT.md from data/processed/*.csv.

Run:  .venv/bin/python scripts/make_figures.py     (outputs docs/figures/*.png)
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "processed"
OUT = ROOT / "docs" / "figures"
OUT.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({"figure.dpi": 150, "font.size": 9, "axes.grid": True,
                     "grid.alpha": 0.3, "axes.spines.top": False, "axes.spines.right": False})
BLUE, ORANGE, GREEN, RED = "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"


def read(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA / f"{name}.csv")


def save(fig, name: str) -> None:
    fig.savefig(OUT / name, bbox_inches="tight")
    plt.close(fig)
    print("wrote docs/figures/" + name)


# --------------------------------------------------------------------------
# Fig. 1 - end-to-end pipeline
# --------------------------------------------------------------------------

def fig_pipeline() -> None:
    fig, ax = plt.subplots(figsize=(11, 5.2))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.grid(False)

    def box(x, y, text, fc="#dbe9f6"):
        ax.add_patch(FancyBboxPatch((x, y), 0.21, 0.17, boxstyle="round,pad=0.012",
                                    fc=fc, ec="#444", lw=1.1, mutation_aspect=0.35))
        ax.text(x + 0.105, y + 0.085, text, ha="center", va="center", fontsize=7.6)

    def arrow(x1, y1, x2, y2):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=13,
                                     color="#333", lw=1.2))

    def label(x1, x2, y, text):
        # sit in the gap *above* the boxes so it never overlaps box text
        ax.text((x1 + x2) / 2, y, text, ha="center", va="bottom", fontsize=7,
                style="italic", color="#333",
                bbox=dict(fc="white", ec="none", pad=1.2))

    y1, y2 = 0.66, 0.16          # row heights
    xs = [0.02, 0.27, 0.52, 0.77]

    box(xs[0], y1, "data/raw/\ntransactions.csv\n(28.4 MB, 250k rows)")
    box(xs[1], y1, "HDFS /data/raw\nraw CSV")
    box(xs[2], y1, "spark/etl.py\nPySpark local[*]\nenrich + partition", "#e2f0d9")
    box(xs[3], y1, "HDFS /data/curated\nupi_enriched/\nParquet 6.2 MB, 12 parts")

    box(xs[0], y2, "sql/ddl.sql\nexternal tables\n+ MSCK REPAIR", "#e2f0d9")
    box(xs[1], y2, "Hive on Tez (YARN)\n15 aggregation\nqueries", "#e2f0d9")
    box(xs[2], y2, "HDFS /output/\nresults/q0..q10")
    box(xs[3], y2, "dashboard/app.py\nStreamlit :8501", "#ffe8cc")

    for i in range(3):
        arrow(xs[i] + 0.21, y1 + 0.085, xs[i + 1], y1 + 0.085)
        arrow(xs[i] + 0.21, y2 + 0.085, xs[i + 1], y2 + 0.085)
    label(xs[0] + 0.21, xs[1], y1 + 0.18, "hdfs dfs -put")
    label(xs[1] + 0.21, xs[2], y1 + 0.18, "spark-submit")
    label(xs[2] + 0.21, xs[3], y1 + 0.18, "partitionBy(year, month)")
    label(xs[0] + 0.21, xs[1], y2 + 0.18, "beeline -f")
    label(xs[1] + 0.21, xs[2], y2 + 0.18, "INSERT OVERWRITE")
    label(xs[2] + 0.21, xs[3], y2 + 0.18, "hdfs dfs -getmerge")

    # wrap-around: end of row 1 -> start of row 2 (routed below row 2)
    x_end, x_start = xs[3] + 0.21, xs[0] + 0.105
    ax.plot([x_end, 0.995], [y1 + 0.085, y1 + 0.085], color="#333", lw=1.2)
    ax.plot([0.995, 0.995], [y1 + 0.085, 0.05], color="#333", lw=1.2)
    ax.plot([0.995, x_start], [0.05, 0.05], color="#333", lw=1.2)
    arrow(x_start, 0.05, x_start, y2)
    ax.text(0.5, 0.062, "external table over Parquet + 15 Tez queries", ha="center",
            va="bottom", fontsize=7, style="italic", color="#333",
            bbox=dict(fc="white", ec="none", pad=1.2))
    save(fig, "fig1_pipeline.png")


# --------------------------------------------------------------------------
# Fig. 2 - monthly trend
# --------------------------------------------------------------------------

def fig_monthly() -> None:
    df = read("q1_monthly")
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.bar(df.month, df.txn_count, color=BLUE, label="Transactions")
    ax.set_ylabel("Transactions")
    ax.set_xlabel("Month (2024)")
    ax2 = ax.twinx()
    ax2.plot(df.month, df.total_value_inr / 1e7, color=RED, marker="o", ms=4, lw=1.8,
             label="Value")
    ax2.set_ylabel("Value (Rs crore)")
    ax2.grid(False)
    ax.tick_params(axis="x", rotation=45)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="lower center", ncol=2, frameon=False,
              bbox_to_anchor=(0.5, 1.0))
    save(fig, "fig2_monthly.png")


# --------------------------------------------------------------------------
# Fig. 3 - merchant categories
# --------------------------------------------------------------------------

def fig_categories() -> None:
    df = read("q3_category").sort_values("total_value_inr")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 3.6))
    ax1.barh(df.merchant_category, df.total_value_inr / 1e7, color=BLUE)
    ax1.set_xlabel("Value (Rs crore)")
    ax1.set_title("Total value", fontsize=10)
    ax2.barh(df.merchant_category, df.avg_ticket_inr, color=ORANGE)
    ax2.set_xlabel("Avg ticket (Rs)")
    ax2.set_title("Average ticket size", fontsize=10)
    fig.tight_layout()
    save(fig, "fig3_categories.png")


# --------------------------------------------------------------------------
# Fig. 4 - geography
# --------------------------------------------------------------------------

def fig_states() -> None:
    df = read("q4_state").sort_values("txn_count")
    fig, ax = plt.subplots(figsize=(7, 3.4))
    bars = ax.barh(df.sender_state, df.txn_count, color=BLUE)
    for bar, value in zip(bars, df.total_value_inr):
        ax.text(bar.get_width() + 400, bar.get_y() + bar.get_height() / 2,
                f"Rs {value/1e7:,.1f} Cr", va="center", fontsize=7.5, color="#333")
    ax.set_xlabel("Transactions (label = total value)")
    ax.set_xlim(0, df.txn_count.max() * 1.25)
    fig.tight_layout()
    save(fig, "fig4_states.png")


# --------------------------------------------------------------------------
# Fig. 5 - customer behaviour (age / device / network)
# --------------------------------------------------------------------------

def fig_behaviour() -> None:
    ages, dev, net = read("q5b_age"), read("q6_device"), read("q6_network")
    fig, (a1, a2, a3) = plt.subplots(1, 3, figsize=(12, 3.3))

    a1.bar(ages.sender_age_group, ages.total_value_inr / 1e7, color=BLUE)
    a1.set_ylabel("Value (Rs crore)")
    a1.set_title("Spend by sender age group", fontsize=10)

    a2.pie(dev.txn_count, labels=dev.device_type, autopct="%1.0f%%",
           colors=[BLUE, ORANGE, GREEN], startangle=90, textprops={"fontsize": 8})
    a2.set_title("Device share", fontsize=10)

    a3.pie(net.txn_count, labels=net.network_type, autopct="%1.0f%%",
           colors=[BLUE, ORANGE, GREEN, RED], startangle=90, textprops={"fontsize": 8})
    a3.set_title("Network share", fontsize=10)
    fig.tight_layout()
    save(fig, "fig5_behaviour.png")


# --------------------------------------------------------------------------
# Fig. 6 - fraud rate by hour
# --------------------------------------------------------------------------

def fig_fraud_hour() -> None:
    df = read("q9_fraud_hour")
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(df.hour_of_day, df.fraud_pct, color=RED, marker="o", ms=4, lw=1.6)
    peak = df.loc[df.fraud_pct.idxmax()]
    ax.annotate(f"peak {peak.fraud_pct}% at {int(peak.hour_of_day)}:00",
                xy=(peak.hour_of_day, peak.fraud_pct),
                xytext=(peak.hour_of_day + 1.5, peak.fraud_pct + 0.02),
                fontsize=8, arrowprops={"arrowstyle": "->", "lw": 0.9})
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Fraud rate (%)")
    ax.set_xticks(range(0, 24, 2))
    fig.tight_layout()
    save(fig, "fig6_fraud_hour.png")


if __name__ == "__main__":
    fig_pipeline()
    fig_monthly()
    fig_categories()
    fig_states()
    fig_behaviour()
    fig_fraud_hour()
