CREATE DATABASE IF NOT EXISTS upi;

DROP TABLE IF EXISTS upi.transactions;
CREATE EXTERNAL TABLE upi.transactions (
    txn_id            STRING,
    ts                TIMESTAMP,
    txn_type          STRING,      
    merchant_category STRING,
    amount            BIGINT,      
    txn_status        STRING,      
    sender_age_group  STRING,
    receiver_age_group STRING,
    sender_state      STRING,
    sender_bank       STRING,
    receiver_bank     STRING,
    device_type       STRING,
    network_type      STRING,
    fraud_flag        INT,
    hour_of_day       INT,
    day_of_week       STRING,
    is_weekend        INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Enriched table written by the Spark ETL (spark/etl.py) as Parquet
-- partitioned by year/month under /data/curated/upi_enriched
DROP TABLE IF EXISTS upi.transactions_enriched;
CREATE EXTERNAL TABLE upi.transactions_enriched (
    txn_id             STRING,
    ts                 TIMESTAMP,
    txn_type           STRING,
    merchant_category  STRING,
    amount             BIGINT,
    txn_status         STRING,
    sender_age_group   STRING,
    receiver_age_group STRING,
    sender_state       STRING,
    sender_bank        STRING,
    receiver_bank      STRING,
    device_type        STRING,
    network_type       STRING,
    fraud_flag         INT,
    hour_of_day        INT,
    day_of_week        STRING,
    is_weekend         INT,
    transaction_date   DATE,
    month_name         STRING,
    quarter            INT,
    week_of_year       INT,
    day_type           STRING,
    is_peak_hour       INT,
    amount_category    STRING,
    is_successful      INT
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/curated/upi_enriched';

-- discover the year=/month= partitions written by Spark
MSCK REPAIR TABLE upi.transactions_enriched;
