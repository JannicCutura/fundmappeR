import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())


def produce_tables(date, dataset):
    """
    This function creates four parquet files.

    Args:
      - date (str): A date in format 'YYYYMM', i.e., "202006"
      - dataset (str): One of "series_data", ""

    Returns:
      - saves four tables to S3

    """
    # get tables from Glue Table
    db_name = "fundmapper"
    tbl_name = dataset

    # S3 location for output
    output_dir = f"s3://fundmapper/04-StagingTables/{dataset}/{date}/"

    # Read data into a DynamicFrame using the Data Catalog metadata
    dyf = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)

    # turn to spark dataframe
    df = dyf.toDF()

    # keep only current date
    df = df.where(f"date = {date}")

    # reconvert to dynamic frame
    dyf = fromDF(df, glueContext, tbl_name)

    # repartition to spark puts everything in one file
    dyf = dyf.repartition(1)

    # Write it out in Parquet
    glueContext.write_dynamic_frame.from_options(frame=df, connection_type="s3",
                                                 connection_options={"path": output_dir}, format="parquet")


# Data Catalog: database and table name
datasets = ["holdings_data", "collateral_data", "class_data", "series_data"]
dataset = "series_data"
date = 202002
produce_tables(date, dataset)




