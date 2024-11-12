# Databricks notebook source
# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.table(comment="bronze")
def lookup_time_table():
  return (
    spark.read
      .format("sqlserver")
      .option("dbtable", 'cdc.lsn_time_mapping')
      .option("host", 'dev-databricks.database.windows.net')
      .option("port", 1433)
      .option("database", 'DB01')
      .option("user", 'databrick')
      .option("password", 'P@ssw0rd@2024')
      .load()
      .select("start_lsn","tran_begin_time","tran_end_time")
  )

@dlt.table(comment="bronze")
def users_cdc_bronze():
  return (
    spark.read
      .format("sqlserver")
      .option("dbtable", 'cdc.dbo_TEST123_CT')
      .option("host", 'dev-databricks.database.windows.net')
      .option("port", 1433)
      .option("database", 'DB01')
      .option("user", 'databrick')
      .option("password", 'P@ssw0rd@2024')
      .load()
      .withColumnRenamed('__$operation','operation')
      .withColumnRenamed('__$start_lsn','start_lsn')
  )

# COMMAND ----------

drop_rules = {
  "valid_first_name": "FIRST_NAME IS NOT NULL",
  "valid_last_name": "LAST_NAME IS NOT NULL"
}

@dlt.table(comment="clean")
@dlt.expect_all_or_drop(drop_rules)
@dlt.expect_or_fail("IDConstraint", "ID > 0")
def users_cdc_clean():
    clean_one = dlt.read_stream("users_cdc_bronze")
    lookup_table = dlt.read_stream("lookup_time_table")
    return (
        clean_one.join(lookup_table, ["start_lsn"], how="inner")
            .select("ID", "EMAIL", "FIRST_NAME", "LAST_NAME", "GENDER", "T_ORDER", "operation","tran_begin_time")
    )

quarantine_rules = {}
quarantine_rules["invalid_records"] = f"NOT({' AND '.join(drop_rules.values())})"

@dlt.table
@dlt.expect_all_or_drop(quarantine_rules)
def users_cdc_quarantine():
    return (
        dlt.read_stream("users_cdc_bronze")
        )

# COMMAND ----------

dlt.create_streaming_live_table(name="SCD1_users")

dlt.apply_changes(
  target = "SCD1_users", 
  source = "users_cdc_clean",
  keys = ["ID"], 
  sequence_by = col("tran_begin_time"),
  apply_as_deletes = expr("operation = 1"),
  except_column_list = ["operation","tran_begin_time"],
  stored_as_scd_type = "1"
)

# COMMAND ----------

dlt.create_streaming_live_table(name="SCD2_users")

dlt.apply_changes(
  target = "SCD2_users", 
  source = "users_cdc_clean",
  keys = ["ID"],
  sequence_by = col("tran_begin_time"),
  apply_as_deletes = expr("operation = 1"),
  except_column_list = ["operation"],
  stored_as_scd_type = "2" 
)
