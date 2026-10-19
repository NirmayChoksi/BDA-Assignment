#!/usr/bin/env bash
# ============================================================
# UPI analysis pipeline: HDFS -> Spark ETL -> Hive (Tez) -> CSV results
#   1. start HDFS + YARN + HiveServer2 (if not running)
#   2. upload raw CSV to /data/raw
#   3. Spark ETL (spark/etl.py) -> partitioned Parquet in /data/curated
#   4. create Hive tables over raw CSV + Parquet (sql/ddl.sql)
#   5. run aggregations (sql/analysis.sql) -> hdfs:///output/results/<q>
#   6. pull results back to data/processed/<q>.csv (with headers)
# Usage: ./scripts/run_pipeline.sh
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/Nirma/Sem-3/BDA/hadoop-3.5.0}"
export HIVE_HOME="${HIVE_HOME:-$HOME/Nirma/Sem-3/BDA/apache-hive-4.1.0-bin}"
export TEZ_HOME="${TEZ_HOME:-$HOME/Nirma/Sem-3/BDA/apache-tez-0.10.5-bin}"
export TEZ_CONF_DIR="${TEZ_CONF_DIR:-$HOME/Nirma/Sem-3/BDA/tez-conf}"
export HADOOP_CLASSPATH="$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*:${HADOOP_CLASSPATH:-}"
export JAVA_HOME="${JAVA_HOME:-/home/linuxbrew/.linuxbrew/opt/openjdk@17}"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH"
# pip-installed PySpark needs to be told where it lives
export SPARK_HOME="${SPARK_HOME:-$(echo "$ROOT"/.venv/lib/python*/site-packages/pyspark)}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$ROOT/.venv/bin/python}"

BEELINE="beeline -u jdbc:hive2://localhost:10000 -n $(whoami) -f"
OUT_DIR="$ROOT/data/processed"

echo "==> [1/6] starting services (skips ones already running)"
jps | grep -qw NameNode || { "$HADOOP_HOME/sbin/start-dfs.sh" >/dev/null; sleep 5; }
jps | grep -q ResourceManager || "$HADOOP_HOME/sbin/start-yarn.sh" >/dev/null
ss -ltn | grep -q ':10000' || { setsid nohup hiveserver2 >/tmp/hiveserver2.log 2>&1 < /dev/null & }
for i in $(seq 1 60); do ss -ltn | grep -q ':10000' && break; sleep 2; done
ss -ltn | grep -q ':10000' || { echo "HiveServer2 failed to start (see /tmp/hiveserver2.log)"; exit 1; }

echo "==> [2/6] uploading raw CSV to HDFS /data/raw"
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put -f "$ROOT/data/raw/transactions.csv" /data/raw/

echo "==> [3/6] Spark ETL: enriching CSV -> partitioned Parquet in HDFS"
"$ROOT/.venv/bin/spark-submit" --master 'local[*]' "$ROOT/spark/etl.py" 2>&1 \
  | grep -E "^wrote|Error|Exception|Traceback" || true
hdfs dfs -ls /data/curated/upi_enriched > /dev/null 2>&1 || {
  echo "Spark ETL produced no output"; exit 1; }

echo "==> [4/6] creating Hive tables (raw + enriched)"
$BEELINE "$ROOT/sql/ddl.sql" > /tmp/ddl.log 2>&1 || {
  echo "DDL failed, tail of /tmp/ddl.log:"; tail -30 /tmp/ddl.log; exit 1; }

echo "==> [5/6] running analysis queries on Tez"
hdfs dfs -rm -r -f /output/results
$BEELINE "$ROOT/sql/analysis.sql" > /tmp/analysis.log 2>&1 || {
  echo "query failed, tail of /tmp/analysis.log:"; tail -30 /tmp/analysis.log; exit 1; }
echo "    $(hdfs dfs -ls /output/results 2>/dev/null | grep -c '^drwx') result sets in HDFS"

echo "==> [6/6] pulling results to $OUT_DIR"
rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"
# header line for each result set
header() {
  case "$1" in
    q0_kpis)          echo "total_transactions,total_value_inr,avg_ticket_inr,success_pct,fraud_pct,states,sender_banks";;
    q1_monthly)       echo "month,month_name,txn_count,total_value_inr,avg_ticket_inr";;
    q3_category)      echo "merchant_category,txn_count,total_value_inr,avg_ticket_inr,max_ticket_inr";;
    q4_state)         echo "sender_state,txn_count,total_value_inr,avg_ticket_inr";;
    q5_type)          echo "txn_type,txn_count,total_value_inr,avg_ticket_inr";;
    q5b_age)          echo "sender_age_group,txn_count,total_value_inr,avg_ticket_inr";;
    q6_device)        echo "device_type,txn_count,total_value_inr";;
    q6_network)       echo "network_type,txn_count,total_value_inr";;
    q7_bank)          echo "sender_bank,txn_count,total_value_inr";;
    q8_status)        echo "txn_type,txn_status,txn_count";;
    q9_fraud_category) echo "merchant_category,txn_count,fraud_count,fraud_pct";;
    q9_fraud_hour)    echo "hour_of_day,txn_count,fraud_count,fraud_pct";;
    q9_fraud_state)   echo "sender_state,txn_count,fraud_count,fraud_pct";;
    q10_amount)       echo "amount_category,txn_count,total_value_inr,avg_ticket_inr";;
    q10_daytype)      echo "day_type,txn_count,total_value_inr,avg_ticket_inr";;
    *)                echo "";;
  esac
}

for q in q0_kpis q1_monthly q3_category q4_state q5_type q5b_age \
         q6_device q6_network q7_bank q8_status q9_fraud_category \
         q9_fraud_hour q9_fraud_state q10_amount q10_daytype; do
  hdfs dfs -getmerge "/output/results/$q" /tmp/_merge.csv 2>/dev/null
  header "$q" > "$OUT_DIR/$q.csv"
  grep -v '^$' /tmp/_merge.csv >> "$OUT_DIR/$q.csv" || true
  printf "    %-20s %4s rows\n" "$q" "$(($(wc -l < "$OUT_DIR/$q.csv") - 1))"
done
rm -f /tmp/_merge.csv "$ROOT/derby.log"   # derby.log is spurious client noise
echo "==> done. Run: .venv/bin/streamlit run dashboard/app.py"
