import json
import boto3
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    # parse the S3 triggered event
    record = event['Records'][0]

    # extract bucket from event
    bucket = record['s3']['bucket']['name']

    # extract key from event
    key = unquote_plus(record['s3']['object']['key'])

    ## for debugging
    # bucket = "fundmapper"
    # key = "04-StagingTables/series_data/202006/part-00000-39604d54-0f41-44be-8850-fafdc5733262-c000.snappy.parquet"

    # create s3 resource
    s3_resource = boto3.resource('s3')

    dest_prefix = "05-FinalTables"
    prefix, dataset, date, filename = key.split("/")
    print(bucket)
    # copy file from S3
    (s3_resource.Object(bucket, f"{dest_prefix}/{dataset}/{date}/{dataset}_{date}.parquet")
     .copy_from(CopySource=f"{bucket}/{key}"))

    # Delete the former file
    (s3_resource.Object(bucket, key)
     .delete())

