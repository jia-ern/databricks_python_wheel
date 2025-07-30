from pyspark.sql import functions as F, DataFrame

def read_csv(spark, file_path:str, start_date, end_date) -> DataFrame:
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", "true") \
        .load(f"file:{file_path}") \
        .filter(F.col("timestamp").between(start_date, end_date))
