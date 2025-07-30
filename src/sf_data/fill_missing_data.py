from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import TimestampType


def process_data(spark: SparkSession, df_source: DataFrame, start_date: datetime, end_date: datetime) -> DataFrame:
    """
    Pre-processes turbine data to prepare it for anomaly detection:

    1. Fills missing rows with the mean value for each turbine and hour.
    2. Computes z-scores for the data to standardize values.

    This intermediate DataFrame is used as input for anomaly detection in a separate module.
    """
    df_timestamps = get_all_timestamps(spark, start_date, end_date)
    df_imputed = impute_missing_values_with_means(df_source, df_timestamps)
    df_zscores = calculate_zscore(df_imputed)

    return df_zscores


def get_all_timestamps(spark: SparkSession, start_date: datetime, end_date: datetime) -> DataFrame:
    """
    Generates a dataframe with hourly timestamps between two dates (excluding end_date) and add an hour column.
    """

    end_date = end_date - timedelta(hours=1)

    df = spark.createDataFrame(
        [(start_date, end_date)],
        ["start_date", "end_date"]
    )

    df_timestamp = df.select(
        F.explode(
            F.sequence(
                F.col('start_date').cast(TimestampType()),
                F.col('end_date').cast(TimestampType()),
                F.expr("INTERVAL 1 HOUR")
            )
        ).alias("timestamp")
    ).withColumn("hour", F.hour("timestamp"))

    return df_timestamp


def impute_missing_values_with_means(df_raw: DataFrame, df_timestamps: DataFrame) -> DataFrame:
    """
    Processes turbine data by filling missing values for hourly timeslots:
    1. Extracts a list of turbines.
    2. Generates all expected hourly timeslots.
    3. Joins expected and actual timeslots to identify missing rows.
    4. Calculates hourly mean for each turbine.
    5. Fills missing values with the hourly mean.
    """

    df_turbines = df_raw.select(F.col("turbine_id")).distinct()
    df_turbines_with_timestamp = df_turbines.crossJoin(df_timestamps)
    df_raw_with_missing_rows = df_turbines_with_timestamp.join(df_raw, on=["timestamp", "turbine_id"], how="left")

    df_turbine_hourly_mean = (df_raw_with_missing_rows
                            .groupby(F.col("turbine_id"), F.col("hour"))
                            .agg(
                                F.mean("wind_speed").alias("wind_speed_mean"),
                                F.mean("wind_direction").alias("wind_direction_mean"),
                                F.mean("power_output").alias("power_output_mean"),
                                F.stddev("wind_speed").alias("wind_speed_stddev"),
                                F.stddev("wind_direction").alias("wind_direction_stddev"),
                                F.stddev("power_output").alias("power_output_stddev"),
                            ))

    df_raw_with_hourly_mean = df_raw_with_missing_rows.join(df_turbine_hourly_mean, on=["turbine_id", "hour"], how="left")

    return (df_raw_with_hourly_mean
                .withColumn("wind_speed", F.coalesce(F.col("wind_speed"), F.col("wind_speed_mean")))
                .withColumn("wind_direction", F.coalesce(F.col("wind_direction"), F.col("wind_direction_mean")))
                .withColumn("power_output", F.coalesce(F.col("power_output"), F.col("power_output_mean")))
                )


def calculate_zscore(df: DataFrame) -> DataFrame:
    """
        Calculates z-scores for wind_speed, wind_direction, power_output
    """
    return (
        df.withColumn("wind_speed_zscore",
                      abs((F.col("wind_speed") - F.col("wind_speed_mean")) / F.col("wind_speed_stddev"))
        )
        .withColumn("wind_direction_zscore",
                    abs((F.col("wind_direction") - F.col("wind_direction_mean")) / F.col("wind_direction_stddev"))
        )
        .withColumn("power_output_zscore",
                    abs((F.col("power_output") - F.col("power_output_mean")) / F.col("power_output_stddev"))
        )
    )

