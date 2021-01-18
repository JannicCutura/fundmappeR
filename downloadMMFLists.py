import os
import boto3
import pandas as pd
import csv
s3 = boto3.resource('s3')
bucket = s3.Bucket("fundmapper")

def lambda_handler(event, context):
    df = pd.read_csv("https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-2020-11.csv")
    df.to_csv('/tmp/test.csv')
    bucket.upload_file('/tmp/test.csv','mmf-2020-11.csv' )
    return "Hello"