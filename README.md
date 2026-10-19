# Analyzing Indian Consumer UPI Transactions using Hadoop

Big Data Analysis project — HDFS for storage, **PySpark** for the ETL/enrichment step,
**Hive 4.1 on Tez (Hadoop 3.5.0)** for distributed SQL aggregation, and a **Streamlit**
dashboard for visualisation.

Dataset: `data/raw/transactions.csv` — 250,000 UPI transactions from FY2024 (Jan–Dec),
17 columns (timestamp, payment mode, merchant category, amount, status, age group,
state, bank, device, network, fraud flag, …).

## Architecture

```
data/raw/transactions.csv
        │  hdfs dfs -put
        ▼
┌───────────────────────── HDFS ──────────────────────────┐
│  /data/raw/transactions.csv          (raw CSV, 28.4 MB) │
│  /data/curated/upi_enriched/         (Spark Parquet,    │
│       year=YYYY/month=MM/            6.2 MB, 12 parts)  │
│  /output/results/q0..q10/            (aggregates)       │
└─────────────────────────────────────────────────────────┘
        ▲ Parquet            ▲ LOCATION          │ getmerge
        │                    │                    ▼
  spark/etl.py         Hive external table   data/processed/*.csv
  (PySpark ETL,        upi.transactions_         │
   local[*])           enriched (Parquet)         ▼
                       (sql/ddl.sql)        dashboard/app.py → Streamlit :8501
                            ▲
                            │
                      sql/analysis.sql ── 15 aggregation queries, each
                                          INSERT OVERWRITE DIRECTORY
                                          (executed as Tez DAGs on YARN)
```

Everything is driven by one script: `scripts/run_pipeline.sh`.

## Project structure

```
code/
├── data/
│   ├── raw/transactions.csv        # source dataset
│   └── processed/*.csv             # Hive query results (q0..q10)
├── spark/
│   └── etl.py                      # PySpark job: CSV -> enriched Parquet
├── sql/
│   ├── ddl.sql                     # external tables (raw CSV + enriched Parquet)
│   └── analysis.sql                # 15 aggregation queries
├── scripts/
│   ├── run_pipeline.sh             # end-to-end pipeline
│   └── make_figures.py             # regenerate the paper's figures
├── docs/
│   └── figures/*.png               # Fig. 1-6 used by REPORT.md
├── dashboard/
│   └── app.py                      # Streamlit dashboard
├── requirements.txt
├── README.md
└── REPORT.md                       # IEEE-style research paper
```

## Prerequisites

| Component | Version | Path                                      |
| --------- | ------- | ----------------------------------------- |
| Java      | 17      | `/usr/lib/jvm` / homebrew openjdk         |
| Hadoop    | 3.5.0   | `~/Nirma/Sem-3/BDA/hadoop-3.5.0`          |
| Hive      | 4.1.0   | `~/Nirma/Sem-3/BDA/apache-hive-4.1.0-bin` |
| Tez       | 0.10.5  | `~/Nirma/Sem-3/BDA/apache-tez-0.10.5-bin` |
| PySpark   | 4.2.0   | in `.venv` (pip)                          |
| Python    | 3.14    | venv at `.venv`                           |

`~/.zshrc` already exports `HADOOP_HOME`, `HIVE_HOME`, `TEZ_CONF_DIR` and
`HADOOP_CLASSPATH` (needed so Tez jars are on the Hadoop classpath).

Tez must be staged in HDFS once per cluster:

```bash
hdfs dfs -mkdir -p /apps/tez
hdfs dfs -put -f ~/Nirma/Sem-3/BDA/tez-minimal.tar.gz /apps/tez/
```

## How to run

```bash
# 1. Python environment (once)
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. Full pipeline: start cluster -> upload -> Spark ETL -> Hive DDL -> 15 Tez queries -> pull CSVs
./scripts/run_pipeline.sh

# 3. Dashboard
.venv/bin/streamlit run dashboard/app.py      # http://localhost:8501

# 4. (optional) regenerate the REPORT.md figures from data/processed
.venv/bin/python scripts/make_figures.py      # -> docs/figures/*.png
```

`run_pipeline.sh` is idempotent: already-running daemons are detected and skipped.

## Pipeline stages

1. **Services** — starts `start-dfs.sh`, `start-yarn.sh` and `hiveserver2` if not already up.
2. **Ingest** — `hdfs dfs -put -f data/raw/transactions.csv /data/raw/`.
3. **Spark ETL** — `spark-submit --master 'local[*]' spark/etl.py` enriches the CSV
   (date parts, `day_type`, `amount_category`, `is_peak_hour`, `is_successful`) and writes
   Parquet partitioned by `year`/`month` to `/data/curated/upi_enriched` — 28.4 MB CSV
   compresses to 6.2 MB.
4. **Schema** — `sql/ddl.sql` creates two external tables: `upi.transactions` (raw CSV)
   and `upi.transactions_enriched` (the Parquet, `PARTITIONED BY (year, month)`,
   discovered with `MSCK REPAIR TABLE`). No data is copied; Hive only reads metadata.
5. **Analysis** — `sql/analysis.sql` runs 15 aggregation queries as Tez DAGs against the
   enriched table; each writes to `hdfs:///output/results/<qN>`.
6. **Export** — `hdfs dfs -getmerge` pulls every result set into `data/processed/<qN>.csv`
   with a header line added.

## Result sets

| File                                                   | Contents                                                     |
| ------------------------------------------------------ | ------------------------------------------------------------ |
| `q0_kpis`                                              | headline KPIs (count, value, avg ticket, success %, fraud %) |
| `q1_monthly`                                           | monthly volume/value trend                                   |
| `q3_category`                                          | merchant category count/value/avg/max                        |
| `q4_state`                                             | sender-state volume/value                                    |
| `q5_type`                                              | payment mode split (P2P / P2M / Bill / Recharge)             |
| `q5b_age`                                              | age-group spending                                           |
| `q6_device`, `q6_network`                              | device and network splits                                    |
| `q7_bank`                                              | sender-bank market share                                     |
| `q8_status`                                            | success/failure counts per payment mode                      |
| `q9_fraud_category`, `q9_fraud_hour`, `q9_fraud_state` | fraud rates                                                  |
| `q10_amount`, `q10_daytype`                            | Spark-derived amount brackets and weekday/weekend split      |

## Troubleshooting

- **`hive` CLI says "No current connection"** — Hive 4 removed the old CLI; use
  `beeline -u jdbc:hive2://localhost:10000 -f file.sql`, which needs `hiveserver2` running.
- **Tez job fails with class-not-found** — `export HADOOP_CLASSPATH=$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*`.
- **Port 10000 not up** — `nohup hiveserver2 >/tmp/hiveserver2.log 2>&1 &`.
- **Query results empty** — check `/tmp/analysis.log` (the pipeline prints the tail on failure).
- **`spark-submit: Could not find valid SPARK_HOME`** — pip-installed PySpark needs
  `export SPARK_HOME=.venv/lib/python3.14/site-packages/pyspark`; `run_pipeline.sh` sets this.
