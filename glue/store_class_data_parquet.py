import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "fundmapper", table_name = "class_data", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "fundmapper", table_name = "class_data", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("series_id", "string", "series_id", "string"), ("classesid", "string", "classesid", "string"), ("personpayforfundflag", "string", "personpayforfundflag", "string"), ("filing_type", "string", "filing_type", "string"), ("nameofpersondescexpensepay", "string", "nameofpersondescexpensepay", "string"), ("date", "long", "date", "long"), ("fridayweek1_weeklygrossredemptions", "double", "fridayweek1_weeklygrossredemptions", "double"), ("totalforthemonthreported_weeklygrosssubscriptions", "double", "totalforthemonthreported_weeklygrosssubscriptions", "double"), ("fridayweek4_weeklygrossredemptions", "double", "fridayweek4_weeklygrossredemptions", "double"), ("netshareholderflowactivityformonthended", "double", "netshareholderflowactivityformonthended", "double"), ("mininitialinvestment", "double", "mininitialinvestment", "double"), ("totalforthemonthreported_weeklygrossredemptions", "double", "totalforthemonthreported_weeklygrossredemptions", "double"), ("netassetvaluepershareincludingcapitalsupportagreement", "string", "netassetvaluepershareincludingcapitalsupportagreement", "string"), ("netassetsofclass", "double", "netassetsofclass", "double"), ("fridayweek1_weeklygrosssubscriptions", "double", "fridayweek1_weeklygrosssubscriptions", "double"), ("fridayweek2_weeklygrossredemptions", "double", "fridayweek2_weeklygrossredemptions", "double"), ("fridayweek3_weeklygrossredemptions", "double", "fridayweek3_weeklygrossredemptions", "double"), ("netassetpershare", "double", "netassetpershare", "double"), ("numberofsharesoutstanding", "double", "numberofsharesoutstanding", "double"), ("netassetvaluepershareexcludingcapitalsupportagreement", "string", "netassetvaluepershareexcludingcapitalsupportagreement", "string"), ("fridayweek4_weeklygrosssubscriptions", "double", "fridayweek4_weeklygrosssubscriptions", "double"), ("fridayweek3_weeklygrosssubscriptions", "double", "fridayweek3_weeklygrosssubscriptions", "double"), ("fridayweek2_weeklygrosssubscriptions", "double", "fridayweek2_weeklygrosssubscriptions", "double"), ("sevendaynetyield", "double", "sevendaynetyield", "double"), ("year", "string", "year", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("series_id", "string", "series_id", "string"), ("classesid", "string", "classesid", "string"), ("personpayforfundflag", "string", "personpayforfundflag", "string"), ("filing_type", "string", "filing_type", "string"), ("nameofpersondescexpensepay", "string", "nameofpersondescexpensepay", "string"), ("date", "long", "date", "long"), ("fridayweek1_weeklygrossredemptions", "double", "fridayweek1_weeklygrossredemptions", "double"), ("totalforthemonthreported_weeklygrosssubscriptions", "double", "totalforthemonthreported_weeklygrosssubscriptions", "double"), ("fridayweek4_weeklygrossredemptions", "double", "fridayweek4_weeklygrossredemptions", "double"), ("netshareholderflowactivityformonthended", "double", "netshareholderflowactivityformonthended", "double"), ("mininitialinvestment", "double", "mininitialinvestment", "double"), ("totalforthemonthreported_weeklygrossredemptions", "double", "totalforthemonthreported_weeklygrossredemptions", "double"), ("netassetvaluepershareincludingcapitalsupportagreement", "string", "netassetvaluepershareincludingcapitalsupportagreement", "string"), ("netassetsofclass", "double", "netassetsofclass", "double"), ("fridayweek1_weeklygrosssubscriptions", "double", "fridayweek1_weeklygrosssubscriptions", "double"), ("fridayweek2_weeklygrossredemptions", "double", "fridayweek2_weeklygrossredemptions", "double"), ("fridayweek3_weeklygrossredemptions", "double", "fridayweek3_weeklygrossredemptions", "double"), ("netassetpershare", "double", "netassetpershare", "double"), ("numberofsharesoutstanding", "double", "numberofsharesoutstanding", "double"), ("netassetvaluepershareexcludingcapitalsupportagreement", "string", "netassetvaluepershareexcludingcapitalsupportagreement", "string"), ("fridayweek4_weeklygrosssubscriptions", "double", "fridayweek4_weeklygrosssubscriptions", "double"), ("fridayweek3_weeklygrosssubscriptions", "double", "fridayweek3_weeklygrosssubscriptions", "double"), ("fridayweek2_weeklygrosssubscriptions", "double", "fridayweek2_weeklygrosssubscriptions", "double"), ("sevendaynetyield", "double", "sevendaynetyield", "double"), ("year", "string", "year", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", connection_options = {"path": "s3://fundmapper/04-FinalTables/class_data/", "partitionKeys": ["year"]}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://fundmapper/04-FinalTables/class_data/", "partitionKeys": ["year"]}, transformation_ctx = "DataSink0")
job.commit()