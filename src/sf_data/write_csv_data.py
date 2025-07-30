from pyspark.sql import DataFrame


def write_csv(df: DataFrame, raw_data_path: str) -> None:
    df.write.csv(raw_data_path)
