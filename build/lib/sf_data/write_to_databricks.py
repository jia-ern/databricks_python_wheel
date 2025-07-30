from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from .config import database_name, schema_name

def merge_delta_table(spark: SparkSession, df: DataFrame, table_name: str, keys: list):
    full_table_name = f"""{database_name}.{schema_name}.{table_name}"""

    if spark.catalog.tableExists(full_table_name):
        delta_table = __delta_table(full_table_name)

        (delta_table.alias("target")
         .merge(
            df.alias("source"),
            " AND ".join([f"target.{key} = source.{key}" for key in keys])
        )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

def __delta_table(table_name: str) -> DeltaTable:
    return DeltaTable.forName(table_name)