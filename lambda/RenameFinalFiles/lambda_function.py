import json
import boto3


def lambda_handler(event, context):
    # parse the S3 triggered event
    record = event['Records'][0]

    # extract bucket from event
    bucket = record['s3']['bucket']['name']

    # extract key from event
    key = unquote_plus(record['s3']['object']['key'])

    # create s3 resource
    s3_resource = boto3.resource('s3')

    prefix, dataset, date, filename = key.split("/")

    # copy file from S3
    (s3_resource.Object(bucket, f"05-FinalTables/{dataset}/{date}/{dataset}_{date}.parquet")
                 .copy_from(CopySource=key))

    # Delete the former file
    (s3_resource.Object(bucket, key)
                .delete())

