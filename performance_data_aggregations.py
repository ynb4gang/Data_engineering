"""
This script performs a series of Spark SQL queries and writes aggregation results to Ozone in Parquet format.

Purpose:
    - Generate analytical datasets and aggregations required for benchmarking and performance testing of the data lake infrastructure.
    - Evaluate the performance of a Spark + Hive + Ozone-based cluster under real-world workload conditions.

Data Sources:
    - `transaction_part`, `document_part`, `contractor`, `agreement`, `account`, and related reference tables.

Output:
    - Aggregated datasets saved in Parquet format to `ozone://bucket/...` for further testing and performance measurement.

Requirements:
    - Apache Spark with Hive support.
    - Hive-compatible metastore.
    - Ozone-compatible storage configuration.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, cast, date_add, date_sub, expr, substring, when
from pyspark.sql.types import DecimalType, DateType

spark = SparkSession.builder \
    .appName("DataGeneration") \
    .config("spark.hadoop.fs.defaultFS", "ofs://ozone-service") \
    .config("spark.sql.parquet.compression.codec", "zstd") \
    .config("spark.sql.shuffle.partitions", "16") \
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("CREATE DATABASE IF NOT EXISTS data_nt")


test_generate_data = [(str(i),) for i in range(10)]
test_generate_df = spark.createDataFrame(test_generate_data, ["c"])
test_generate_df.write.mode("overwrite").format("parquet").saveAsTable("data_nt.test_generate")


spark.range(0, 1000000000).createOrReplaceTempView("temp_gen9")
spark.sql("""
CREATE TABLE data_nt.test_gen9str USING parquet
AS SELECT concat(cast(id as string), cast(id as string), cast(id as string)) as c 
FROM temp_gen9
""")

spark.sql("""
CREATE TABLE data_nt.test_gen9 USING parquet
AS SELECT cast(c as bigint) as c FROM data_nt.test_gen9str
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.document_type (
  document_type_id_int bigint, 
  document_type_id_char string, 
  name string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.document_type
SELECT
  id AS document_type_id_int,
  concat(cast(id as string), '_varchar_key_value_', cast(id as string), '_test_', cast(id as string)) AS document_type_id_char,
  concat('document_type_', cast(id as string)) AS name  
FROM (
  SELECT 10000000 + cast(c as bigint) AS id
  FROM data_nt.test_gen9
  WHERE c < 1000
) AS t
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.document_part (
  document_id_int bigint, 
  document_id_char string, 
  payer_contractor_id_int bigint, 
  payer_contractor_id_char string, 
  payee_contractor_id_int bigint, 
  payee_contractor_id_char string, 
  document_nbr string, 
  document_date date, 
  document_type_id_int bigint, 
  document_type_id_char string,
  document_month string
) USING parquet
PARTITIONED BY (document_month)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.transaction (
  transaction_id_int bigint, 
  transaction_id_char string, 
  account_id_int bigint, 
  account_id_char string, 
  document_id_int bigint, 
  document_id_char string, 
  transaction_type_id_int bigint, 
  transaction_type_id_char string, 
  transaction_date date, 
  transaction_amount decimal(25,6), 
  transaction_sign int
) USING parquet
""")

for i in range(2, 45):
    start_id = i * 1000000000
    spark.sql(f"""
    INSERT INTO data_nt.transaction
    SELECT
      transaction_id_int,
      concat(cast(transaction_id_int as string), '_varchar_key_value_', cast(transaction_id_int as string), '_test_XX') AS transaction_id_char,
      account_id_int,
      concat(cast(account_id_int as string), '_varchar_key_value_', cast(account_id_int as string), '_test_XX') AS account_id_char,
      document_id_int,
      concat(cast(document_id_int as string), '_varchar_key_value_', cast(document_id_int as string), '_test_XX') AS document_id_char,
      transaction_type_id_int,
      concat(cast(transaction_type_id_int as string), '_varchar_key_value_', 
             cast(transaction_type_id_int as string), '_test_', cast(transaction_type_id_int as string)) AS transaction_type_id_char,
      cast(date_add('2000-01-01', cast(document_id_int % 6200 as int)) as date) AS transaction_date,
      cast((transaction_id_int div 2)/ 50000.0 as decimal(25,6)) AS transaction_amount,
      CASE WHEN transaction_id_int % 2 = 0 THEN 1 ELSE -1 END AS transaction_sign
    FROM (
      SELECT
        id AS transaction_id_int,
        10000000 + id % 500000000 AS account_id_int,
        id div 2 AS document_id_int,
        10000000 + id % 500 AS transaction_type_id_int
      FROM (
        SELECT {start_id}+cast(c as bigint) AS id
        FROM data_nt.test_gen9
      ) AS t
    ) AS t
    """)

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.transaction_part (
  transaction_id_int bigint, 
  transaction_id_char string, 
  account_id_int bigint, 
  account_id_char string, 
  document_id_int bigint, 
  document_id_char string, 
  transaction_type_id_int bigint, 
  transaction_type_id_char string, 
  transaction_date date, 
  transaction_amount decimal(25,6), 
  transaction_sign int,
  transaction_month string
) USING parquet
PARTITIONED BY (transaction_month)
""")

spark.sql("""
INSERT INTO data_nt.transaction_part
SELECT 
  transaction_id_int, transaction_id_char, account_id_int, account_id_char,
  document_id_int, document_id_char, transaction_type_id_int, transaction_type_id_char,
  transaction_date, transaction_amount, transaction_sign,
  substr(cast(transaction_date as string), 1, 7) AS transaction_month
FROM data_nt.transaction
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.account (
  account_id_int bigint, 
  account_id_char string, 
  agreement_id_int bigint, 
  agreement_id_char string, 
  start_date date, 
  final_date date, 
  account_nbr string, 
  name string, 
  account_type_id_int bigint, 
  account_type_id_char string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.account
SELECT
  account_id_int,
  concat(cast(account_id_int as string), '_varchar_key_value_', cast(account_id_int as string), '_test_XX') AS account_id_char,
  agreement_id_int,
  concat(cast(agreement_id_int as string), '_varchar_key_value_', cast(agreement_id_int as string), '_test_XX') AS agreement_id_char,
  cast(date_add('2000-01-01', cast(agreement_id_int % 6200 as int)) as date) AS start_date,
  cast('2100-01-01' as date) AS final_date,
  substr(concat(cast(id as string), cast(id as string), cast(id as string), cast(id as string)), 1, 32) AS account_nbr,
  concat(account_nbr, '_account_name_test_value_', account_nbr) AS name,
  account_type_id_int,
  concat(cast(account_type_id_int as string), '_varchar_key_value_', 
         cast(account_type_id_int as string), '_test_', cast(account_type_id_int as string)) AS account_type_id_char
FROM (
  SELECT
    10000000+cast(c as bigint) AS id,
    10000000 + cast(c as bigint) % 1000 AS account_type_id_int,
    CASE 
      WHEN cast(c as bigint) >= 0 AND cast(c as bigint) < 60000000 THEN 10000000+cast(c as bigint)
      WHEN cast(c as bigint) >= 60000000 AND cast(c as bigint) < 110000000 THEN 10000000+cast(c as bigint) - 150000000
      WHEN cast(c as bigint) >= 110000000 AND cast(c as bigint) < 135000000 THEN 10000000+cast(c as bigint) - 300000000
      ELSE 10000000+cast(c as bigint) - 450000000
    END AS agreement_id_int
  FROM data_nt.test_gen9
  WHERE c < 500000000
) AS t
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.contractor (
  contractor_id_int bigint, 
  contractor_id_char string, 
  last_name string, 
  first_name string, 
  middle_name string, 
  birth_date date, 
  contractor_type_id_int bigint, 
  contractor_type_id_char string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.contractor
SELECT
  id AS contractor_id_int,
  concat(cast(id as string), '_varchar_key_value_', cast(id as string), '_test_', cast(id as string)) AS contractor_id_char,
  concat('LAST_NAME_mock_value_', cast((id div 20) as string)) AS last_name,
  concat('FIRST_NAME_mock_value_', cast((id div 50) as string)) AS first_name,
  concat('MIDDLE_NAME_mock_value_', cast((id div 100) as string)) AS middle_name,
  cast(date_add('1950-01-01', cast(id % 22000 as int)) as date) AS birth_date,
  10000000 + id % 100 AS contractor_type_id_int,
  concat(cast((10000000 + id % 100) as string), '_varchar_key_value_', 
         cast((10000000 + id % 100) as string), '_test_', cast((10000000 + id % 100) as string)) AS contractor_type_id_char
FROM (
  SELECT 10000000 + cast(c as bigint) AS id
  FROM data_nt.test_gen9
  WHERE c < 75000000
) AS t
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.contractor_type (
  contractor_type_id_int bigint, 
  contractor_type_id_char string, 
  name string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.contractor_type
SELECT
  id AS contractor_type_id_int,
  concat(cast(id as string),'_varchar_key_value_',cast(id as string), '_test_',cast(id as string)) AS contractor_type_id_char,
  concat('contractor_type_', cast(id as string)) AS name  
FROM (
  SELECT 10000000 + cast(c as bigint) AS id
  FROM data_nt.test_gen9
  WHERE c < 100
) AS t
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.account_type (
  account_type_id_int bigint, 
  account_type_id_char string, 
  name string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.account_type
SELECT
  id AS account_type_id_int,
  concat(cast(id as string), '_varchar_key_value_', cast(id as string), '_test_', cast(id as string)) AS account_type_id_char,
  concat('account_type_', cast(id as string)) AS name  
FROM (
  SELECT 10000000 + cast(c as bigint) AS id
  FROM data_nt.test_gen9
  WHERE c < 1000
) AS t
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS data_nt.agreement (
  agreement_id_int bigint, 
  agreement_id_char string, 
  contractor_id_int bigint, 
  contractor_id_char string, 
  agreement_nbr string, 
  start_date date, 
  value_date date, 
  planned_final_date date, 
  fact_final_date date, 
  amount decimal(38,10), 
  agreement_type_id_int bigint, 
  agreement_type_id_char string, 
  product_id_int bigint, 
  product_id_char string
) USING parquet
""")

spark.sql("""
INSERT INTO data_nt.agreement
SELECT
  agreement_id_int,
  concat(cast(agreement_id_int as string), '_varchar_key_value_', cast(agreement_id_int as string), '_test_XX') AS agreement_id_char,
  contractor_id_int,
  concat(cast(contractor_id_int as string), '_varchar_key_value_', cast(contractor_id_int as string), '_test_', cast(contractor_id_int as string)) AS contractor_id_char,
  concat('Agreement_', cast(agreement_id_int as string)) AS agreement_nbr,
  cast(start_date as date) AS start_date,
  cast(date_add(start_date, cast(agreement_id_int % 2 as int)) as date) AS value_date,
  cast(planned_final_date as date) AS planned_final_date,
  cast(CASE 
    WHEN agreement_id_int % 15 = 0 THEN NULL 
    WHEN agreement_id_int % 15 = 1 THEN planned_final_date 
    WHEN agreement_id_int % 15 = 2 THEN date_add(planned_final_date, 30)
    WHEN agreement_id_int % 15 = 3 THEN date_sub(planned_final_date, 30)
    WHEN planned_final_date <= '2017-01-01' THEN planned_final_date 
  END as date) AS fact_final_date,
  agreement_id_int / 50.0 AS amount,
  agreement_type_id_int,
  concat(cast(agreement_type_id_int as string), '_varchar_key_value_', 
         cast(agreement_type_id_int as string), '_test_', cast(agreement_type_id_int as string)) AS agreement_type_id_char,
  product_id_int,
  concat(cast(product_id_int as string), '_varchar_key_value_', 
         cast(product_id_int as string), '_test_', cast(product_id_int as string)) AS product_id_char
FROM (
  SELECT
    id AS agreement_id_int,
    CASE 
      WHEN id >= 10000000 AND id < 60000000 THEN id 
      WHEN id >= 60000000 AND id < 110000000 THEN id - 25000000 
      WHEN id >= 110000000 AND id < 135000000 THEN id - 50000000 
      WHEN id >= 135000000 THEN id - 75000000 
    END AS contractor_id_int,
    date_add('2000-01-01', cast(id % 6200 as int)) AS start_date,
    date_add('2000-01-01', cast(id % 6200 as int) + cast(id % 18000 as int) + 180) AS planned_final_date,
    10000000 + id % 1000 AS agreement_type_id_int,
    10000000 + id % 100000 AS product_id_int
  FROM (
    SELECT 10000000+cast(c as bigint) AS id
    FROM data_nt.test_gen9
    WHERE c < 150000000
  ) AS t
) AS t
""")

tables = ["document_type", "transaction", "transaction_part", "account", 
          "contractor", "contractor_type", "account_type", "agreement"]

for table in tables:
    spark.sql(f"ANALYZE TABLE data_nt.{table} COMPUTE STATISTICS")
    spark.sql(f"ANALYZE TABLE data_nt.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
    
    
===========================aggregations===========================


===========================1===========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number, sum as spark_sum, when, substring, to_date, to_timestamp, concat_ws, lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Ozone_Performance_Test") \
    .config("spark.sql.warehouse.dir", "ozone://path/to/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

params = {
    "transaction_start_date": "2023-01-01",
    "transaction_end_date": "2023-12-31",
    "document_start_date": "2023-01-01",
    "document_end_date": "2023-12-31",
    "account_nbr": "12345",
    "last_name": "Ivanov",
    "first_name": "Ivan",
    "middle_name": "Ivanovich",
    "birth_date": "1980-01-01",
    "acctp_name": "CURRENT"
}


===========================2===========================
q2_df = spark.sql("""
    SELECT 
        TRANSACTION_TYPE_ID_INT, 
        COUNT(*) as TRAN_QTY 
    FROM transaction_part 
    GROUP BY TRANSACTION_TYPE_ID_INT
""")

q2_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q2")

q1_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q1")

===========================3===========================
q3_df = spark.sql(f"""
    SELECT 
        TRANSACTION_ID_INT, TRANSACTION_ID_CHAR, ACCOUNT_ID_INT, ACCOUNT_ID_CHAR, 
        DOCUMENT_ID_INT, DOCUMENT_ID_CHAR, TRANSACTION_TYPE_ID_INT, TRANSACTION_TYPE_ID_CHAR,
        TRANSACTION_DATE, TRANSACTION_AMOUNT, TRANSACTION_SIGN, 
        ROW_NUMBER() OVER (ORDER BY TRANSACTION_DATE, DOCUMENT_ID_INT) AS ROW_NUM 
    FROM transaction_part 
    WHERE TRANSACTION_DATE > TO_TIMESTAMP('{params["transaction_start_date"]}', 'yyyy-MM-dd') 
      AND TRANSACTION_DATE < TO_TIMESTAMP('{params["transaction_end_date"]}', 'yyyy-MM-dd')
      AND TRANSACTION_MONTH >= SUBSTRING(TO_DATE('{params["transaction_start_date"]}'), 1, 7)
      AND TRANSACTION_MONTH <= SUBSTRING(TO_DATE('{params["transaction_end_date"]}'), 1, 7)
""")

q3_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q3")

q1_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q1")
===========================4===========================
q4_df = spark.sql(f"""
    SELECT 
        DOCUMENT_ID_INT, DOCUMENT_ID_CHAR, 
        PAYER_CONTRACTOR_ID_INT, PAYER_CONTRACTOR_ID_CHAR, 
        PAYEE_CONTRACTOR_ID_INT, PAYEE_CONTRACTOR_ID_CHAR,
        DOCUMENT_NBR, DOCUMENT_DATE, 
        DOCUMENT_TYPE_ID_INT, DOCUMENT_TYPE_ID_CHAR, 
        ROW_NUMBER() OVER (ORDER BY PAYEE_CONTRACTOR_ID_INT) AS ROW_NUM 
    FROM document_part 
    WHERE DOCUMENT_DATE > TO_TIMESTAMP('{params["document_start_date"]}', 'yyyy-MM-dd') 
      AND DOCUMENT_DATE < TO_TIMESTAMP('{params["document_end_date"]}', 'yyyy-MM-dd')
      AND DOCUMENT_MONTH >= SUBSTRING(TO_DATE('{params["document_start_date"]}'), 1, 7)
      AND DOCUMENT_MONTH <= SUBSTRING(TO_DATE('{params["document_end_date"]}'), 1, 7)
""")

q4_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q4")

===========================5===========================
q5_df = spark.sql(f"""
    SELECT
        tr.ACCOUNT_ID_INT, 
        ROW_NUMBER() OVER (PARTITION BY tr.ACCOUNT_ID_INT ORDER BY TRANSACTION_DATE DESC) as ROW_N
    FROM transaction_part tr
        INNER JOIN account acc ON tr.ACCOUNT_ID_INT = acc.ACCOUNT_ID_INT
    WHERE SUBSTRING(acc.ACCOUNT_NBR, 1, 5) = SUBSTRING('{params["account_nbr"]}', 1, 5)
""")

q5_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q5")
===========================6===========================
q6_df = spark.sql(f"""
    SELECT
        ctr.CONTRACTOR_ID_INT as CONTRACTOR_ID_INT,
        CONCAT_WS(' ', ctr.LAST_NAME, ctr.FIRST_NAME, ctr.MIDDLE_NAME) as FULL_NAME,
        ctr.BIRTH_DATE as BIRTH_DATE,
        ctr_tp.NAME as CONTRACTOR_TYPE,
        agr.AGREEMENT_NBR as AGREEMENT_NBR,
        agr.START_DATE as START_DATE,
        agr.PLANNED_FINAL_DATE as PLANNED_FINAL_DATE,
        agr.FACT_FINAL_DATE as FACT_FINAL_DATE,
        acc_tp.NAME as ACCOUNT_TYPE,
        acc.ACCOUNT_NBR as ACCOUNT_NBR,
        SUM(tr.TRANSACTION_AMOUNT * tr.TRANSACTION_SIGN) as RESULT_AMOUNT,
        SUM(CASE WHEN tr.TRANSACTION_SIGN = 1 THEN tr.TRANSACTION_AMOUNT ELSE 0 END) as INCOME_AMOUNT,
        SUM(CASE WHEN tr.TRANSACTION_SIGN = -1 THEN tr.TRANSACTION_AMOUNT ELSE 0 END) as EXPENSE_AMOUNT
    FROM contractor ctr
        INNER JOIN contractor_type ctr_tp ON ctr.CONTRACTOR_TYPE_ID_INT = ctr_tp.CONTRACTOR_TYPE_ID_INT
        INNER JOIN agreement agr ON agr.CONTRACTOR_ID_INT = ctr.CONTRACTOR_ID_INT 
            AND agr.START_DATE < TO_TIMESTAMP('{params["document_start_date"]}', 'yyyy-MM-dd')
        INNER JOIN account acc ON acc.AGREEMENT_ID_INT = agr.AGREEMENT_ID_INT
        INNER JOIN account_type acc_tp ON acc.ACCOUNT_TYPE_ID_INT = acc_tp.ACCOUNT_TYPE_ID_INT 
            AND acc_tp.NAME LIKE '{params["acctp_name"]}%'
        LEFT JOIN transaction_part tr ON tr.ACCOUNT_ID_INT = acc.ACCOUNT_ID_INT 
            AND tr.TRANSACTION_DATE BETWEEN TO_TIMESTAMP('{params["transaction_start_date"]}', 'yyyy-MM-dd') 
                AND TO_TIMESTAMP('{params["transaction_end_date"]}', 'yyyy-MM-dd')
            AND tr.TRANSACTION_MONTH >= SUBSTRING(TO_DATE('{params["transaction_start_date"]}'), 1, 7)
            AND tr.TRANSACTION_MONTH <= SUBSTRING(TO_DATE('{params["transaction_end_date"]}'), 1, 7)
    WHERE ctr.LAST_NAME = '{params["last_name"]}'
        AND ctr.FIRST_NAME = '{params["first_name"]}'
        AND ctr.MIDDLE_NAME = '{params["middle_name"]}'
        AND ctr.BIRTH_DATE = TO_TIMESTAMP('{params["birth_date"]}', 'yyyy-MM-dd')
    GROUP BY 
        ctr.CONTRACTOR_ID_INT, ctr.LAST_NAME, ctr.FIRST_NAME, ctr.MIDDLE_NAME, ctr.BIRTH_DATE, ctr_tp.NAME,
        agr.AGREEMENT_NBR, agr.START_DATE, agr.PLANNED_FINAL_DATE, agr.FACT_FINAL_DATE,
        acc_tp.NAME, acc.ACCOUNT_NBR
""")

q6_df.write.format("parquet").mode("overwrite").save("ozone://bucket/q6")
