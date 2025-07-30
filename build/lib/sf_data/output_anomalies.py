from pyspark.sql import DataFrame, functions as F

from config import anomalies_table_name
from sf_data.write_to_databricks import merge_delta_table


def output_anomalies(spark, df_zscores: DataFrame) -> None:
    """
        Calculates anomalies which are any power_output's that are 2 standard deviations
        from the mean of power_output from that turbine for that hour slot,
        and writes to delta table.
    """
    df_anomalies = (
        df_zscores.filter(F.col("power_output_stddev") > 2)
        .select(F.col("timestamp"), F.col("turbine_id"), F.col("wind_speed"), F.col("wind_direction"),
                F.col("power_output"))
    )

    keys = ["turbine_id", "timestamp"]
    merge_delta_table(spark, df_anomalies, anomalies_table_name, keys)
