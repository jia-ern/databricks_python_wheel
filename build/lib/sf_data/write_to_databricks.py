from pyspark.sql import DataFrame

def write_table(df: DataFrame):
    df.write.saveAsTable("sf_menu")
