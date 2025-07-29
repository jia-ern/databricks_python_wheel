import string

from pyspark.sql import SparkSession, DataFrame


credentials = {
  "sfUrl": "DMJYBUT-KG34755.snowflakecomputing.com",
  "sfUser": "JIAERNFOO",
  "sfPassword": "HelloBear9900@",
  "sfDatabase": "SNOWFLAKE_LEARNING_DB",
  "sfSchema": "JIAERNFOO_LOAD_SAMPLE_DATA_FROM_S3",
  "sfWarehouse": "SNOWFLAKE_LEARNING_WH"
}


def create_spark_session():
  return SparkSession.builder.appName("Read Snowflake Table").getOrCreate()


def read_snowflake_table(spark, query: string) -> DataFrame:
  return spark.read \
  .format("snowflake") \
  .options(**credentials) \
  .option("dbtable", query) \
  .load()
