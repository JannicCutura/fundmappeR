import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime

glueContext = GlueContext(SparkContext.getOrCreate())


def my_months(date, last_n="All"):
    current_year = date[0:4]
    current_month = date[5:6]
    my_months_list = ["201212"]  # first one
    for year in range(2013, int(current_year) + 1):
        for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
            loopdate = str(year) + month
            if loopdate <= date:
                my_months_list.append(loopdate)
            else:
                if last_n == "All":
                    return my_months_list
                else:
                    return my_months_list[-last_n:]


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
    print(f"Working on {dataset} in {date}")
    db_name = "fundmapper"
    tbl_name = dataset

    # S3 location for output
    output_dir = f"s3://fundmapper/04-StagingTables/{dataset}/{date}/"

    # Read data into a DynamicFrame using the Data Catalog metadata
    dyf = (glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name,
                                                         additional_options={'useS3ListImplementation': True}))
    print(f"  - Read in")

    # turn to spark dataframe
    df = dyf.toDF()
    print(f"  - convert to spark df")

    # keep only current date
    df = df.where(f"date = {date}")
    print(f"  - filtered")

    # reconvert to dynamic frame
    dyf = DynamicFrame.fromDF(df, glueContext, tbl_name)
    print(f"  - converted back")

    # repartition to spark puts everything in one file
    dyf = dyf.repartition(1)
    print(f"  - repartitioned")

    # Write it out in Parquet
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={"path": output_dir}, format="parquet")

    # print some profress report
    print(f"  - Done with {dataset} in {date}")
    print(" ")


# Data Catalog: database and table name
datasets = ["holdings_data", "collateral_data", "class_data", "series_data"]
today = datetime.today().strftime('%Y%m')  ## todays month as YYYYMM
dates = my_months(today, last_n=61)
# dataset = "series_data"
# date = '202002'


for date in dates:
    for dataset in datasets:
        produce_tables(date, dataset)











