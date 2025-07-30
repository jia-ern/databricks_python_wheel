from config import cleaned_table_name
from pyspark.sql import DataFrame, functions as F

from sf_data.write_to_databricks import merge_delta_table


def output_cleaned(spark, df_zscores: DataFrame) -> DataFrame:
    """
    Cleans the data by replacing outliers (values beyond 3 standard deviations from the mean)
    with the mean for the corresponding turbine and hour. Saves the cleaned data to a Delta table.
    """

    df_cleaned = replace_outliers_with_means(df_zscores)

    keys = ["turbine_id", "timestamp"]
    merge_delta_table(spark, df_cleaned, cleaned_table_name, keys)

    return df_cleaned


def replace_outliers_with_means(df_zscores: DataFrame) -> DataFrame:
    """
        Replaces outlier values with the hourly mean if the z-score is greater than 3 (3 standard deviations from mean)
    """
    return (df_zscores
            .withColumn("wind_speed", F.when(F.col("wind_speed_zscore") > 3, F.col("wind_speed_mean")).otherwise(F.col("wind_speed")))
            .withColumn("wind_direction", F.when(F.col("wind_direction_zscore") > 3, F.col("wind_direction_mean")).otherwise(F.col("wind_direction")))
            .withColumn("power_output", F.when(F.col("power_output_zscore") > 3, F.col("power_output_mean")).otherwise(F.col("power_output")))
            .select(
                F.col("timestamp"), F.col("turbine_id"), F.col("wind_speed"), F.col("wind_direction"), F.col("power_output")
            )
        )
