from datetime import datetime
import argparse

from sf_data.extract_snowflake import create_spark_session, read_snowflake_table
from sf_data.fill_missing_data import process_data
from sf_data.load_csv_data import read_csv
from sf_data.output_anomalies import output_anomalies
from sf_data.output_cleaned import output_cleaned
from sf_data.output_statistics import output_statistics
from sf_data.write_csv_data import write_csv
from config import raw_data_path


def main():
  spark = create_spark_session("Wind Turbines Pipeline")

  # Parse command-line arguments
  parser = argparse.ArgumentParser(description="Run the pipeline")
  parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format")
  parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format")
  args = parser.parse_args()

  # Convert arguments to datetime objects
  start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
  end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

  # read WIND_TURBINE_1
  query = f"""(select * from WIND_TURBINE_1) as tempTable"""
  df = read_snowflake_table(spark, query)
  print(df.show(truncate=False))

  # store it in csv under Shared folder
  write_csv(df, raw_data_path)

  # Load raw data in a dataframe
  df_raw = read_csv(spark, raw_data_path, start_date, end_date)

  # Process data
  df_processed = process_data(spark, df_raw, start_date, end_date)

  # Write cleaned data
  df_cleaned = output_cleaned(spark, df_processed)

  # Write anomalies
  output_anomalies(spark, df_processed)

  # Write statistics
  output_statistics(spark, df_cleaned)


if __name__ == "__main__":
    main()