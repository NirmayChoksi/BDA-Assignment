INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q0_kpis'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT COUNT(*)                              AS total_transactions,
       SUM(amount)                           AS total_value_inr,
       ROUND(AVG(amount), 2)                 AS avg_ticket_inr,
       ROUND(100.0 * SUM(IF(txn_status = 'SUCCESS', 1, 0)) / COUNT(*), 2) AS success_pct,
       ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 2)             AS fraud_pct,
       COUNT(DISTINCT sender_state)          AS states,
       COUNT(DISTINCT sender_bank)           AS sender_banks
FROM upi.transactions_enriched;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q1_monthly'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT concat(year, '-', lpad(CAST(month AS STRING), 2, '0')) AS month,
       month_name,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY year, month, month_name
ORDER BY year, month;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q3_category'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT merchant_category,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr,
       MAX(amount)                     AS max_ticket_inr
FROM upi.transactions_enriched
GROUP BY merchant_category
ORDER BY total_value_inr DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q4_state'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT sender_state,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY sender_state
ORDER BY total_value_inr DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q5_type'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT txn_type,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY txn_type
ORDER BY txn_count DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q5b_age'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT sender_age_group,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY sender_age_group
ORDER BY sender_age_group;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q6_device'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT device_type, COUNT(*) AS txn_count, SUM(amount) AS total_value_inr
FROM upi.transactions_enriched GROUP BY device_type ORDER BY txn_count DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q6_network'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT network_type, COUNT(*) AS txn_count, SUM(amount) AS total_value_inr
FROM upi.transactions_enriched GROUP BY network_type ORDER BY txn_count DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q7_bank'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT sender_bank,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr
FROM upi.transactions_enriched
GROUP BY sender_bank
ORDER BY txn_count DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q8_status'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT txn_type,
       txn_status,
       COUNT(*)                        AS txn_count
FROM upi.transactions_enriched
GROUP BY txn_type, txn_status
ORDER BY txn_type, txn_status;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q9_fraud_category'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT merchant_category,
       COUNT(*)                                              AS txn_count,
       SUM(fraud_flag)                                       AS fraud_count,
       ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 2)          AS fraud_pct
FROM upi.transactions_enriched
GROUP BY merchant_category
ORDER BY fraud_pct DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q9_fraud_hour'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT hour_of_day,
       COUNT(*)                                              AS txn_count,
       SUM(fraud_flag)                                       AS fraud_count,
       ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 2)          AS fraud_pct
FROM upi.transactions_enriched
GROUP BY hour_of_day
ORDER BY hour_of_day;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q9_fraud_state'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT sender_state,
       COUNT(*)                                              AS txn_count,
       SUM(fraud_flag)                                       AS fraud_count,
       ROUND(100.0 * SUM(fraud_flag) / COUNT(*), 2)          AS fraud_pct
FROM upi.transactions_enriched
GROUP BY sender_state
ORDER BY fraud_pct DESC;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q10_amount'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT amount_category,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY amount_category
ORDER BY amount_category;

INSERT OVERWRITE DIRECTORY 'hdfs:///output/results/q10_daytype'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT day_type,
       COUNT(*)                        AS txn_count,
       SUM(amount)                     AS total_value_inr,
       ROUND(AVG(amount), 2)           AS avg_ticket_inr
FROM upi.transactions_enriched
GROUP BY day_type
ORDER BY day_type;
