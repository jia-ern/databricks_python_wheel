from sf_data.extract_snowflake import create_spark_session, read_snowflake_table
from sf_data.write_to_databricks import write_table


def main():
  spark = create_spark_session()

  query = f"""(select * from MENU) as tempTable"""
  df = read_snowflake_table(spark, query)
  print(df.show(truncate=False))

  write_table(df)
