from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config import statistics_table_name
from sf_data.write_to_databricks import merge_delta_table


def output_statistics(spark, df_clean: DataFrame) -> None:
    """
        Calculates the daily min, max, avg for power_output columns and writes to delta table.
    """

    df_statistics = (
        df_clean
        .groupBy(F.to_date(F.col("timestamp")), F.col("turbine_id"))
        .agg(
            F.min("power_output").alias("power_output_min"),
            F.max("power_output").alias("power_output_max"),
            F.avg("power_output").alias("power_output_avg"),
        )
    )

    keys = ["turbine_id", "date"]
    merge_delta_table(spark, df_statistics, statistics_table_name, keys)
