import os
import json
import pandas as pd
import boto3
from datetime import date

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

bucket = "fundmapper"
prefix = "01-MMFLists/"


def lambda_handler(event, context):
    today = date.today()
    mdate = today.strftime("%Y-%m")

    bucket = s3.Bucket('fundmapper')
    key = '01-MMFLists/'
    objs = list(bucket.objects.filter(Prefix=key))

    for obj in objs:
        if obj.key == prefix + "mmf-" + mdate + ".csv":
            return "File already present"
    print("Not stored yet; try to download")

    try:
        df = pd.read_csv(
            "https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-" + mdate + ".csv")
        df.to_csv('/tmp/mmf-"+mdate+".csv')
        s3_client.upload_file('/tmp/mmf-"+mdate+".csv', "fundmapper", "01-MMFLists/mmf-" + mdate + ".csv")
        # TO-DO: add notification
        return "Success; uploaded new file"
    except:
        return "Success; None found"

