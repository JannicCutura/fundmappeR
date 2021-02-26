import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "fundmapper"
tbl_name = "series_data"

# S3 location for output
output_dir = "s3://fundmapper/04-FinalTables/anotheretltest/myresult_csv"

# Read data into a DynamicFrame using the Data Catalog metadata
df = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)
#dff = df.toDF()

#dff = dff.where("``")


df.printSchema()
#df = df.toDF().filter( (df.date < 202001) & (df.date >= 201901))

df = df.repartition(1)

# Write it out in Parquet
glueContext.write_dynamic_frame.from_options(frame = df, connection_type = "s3", connection_options = {"path": output_dir}, format = "csv")