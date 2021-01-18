import os
import json
import pandas as pd
import boto3

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # TODO implement

    df = pd.read_csv("https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-2020-11.csv")
    df.to_csv('/tmp/mmf-2020-11.csv')
    s3_client.upload_file('/tmp/mmf-2020-11.csv', "fundmapper", "01-MMFLists/mmf-2020-11.csv")

    # print(event['key1'])
    return "Success"



