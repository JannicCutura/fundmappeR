from __future__ import print_function
import pandas as pd
import boto3
from datetime import date
import base64

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
sns_client = boto3.client('sns')
topic_arn = 'arn:aws:sns:eu-central-1:540666357414:fundmapper'
bucket_name = "fundmapper"
prefix = "01-MMFLists/"


def lambda_handler(event, context):
    today = date.today()
    mdate = today.strftime("%Y-%m")
    #mdate = "2020-11"
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=prefix))

    # test whether file is already processed
    for obj in objs:
        if obj.key == prefix + "mmf-" + mdate + ".csv":
            sns_client.publish(TopicArn=topic_arn,
                               Message="Found a new list, mmf-" + mdate + ".csv processed: was already downloaded",
                               Subject='This months files is already there')
            return "File already present"

    print("Not stored yet; try to download")

    # download new file and store to s3
    try:
        # read from SEC website
        df = pd.read_csv(
            "https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-" + mdate + ".csv")

        # save vector of IDs
        ids = set(df.series_id.tolist())

        # store locally
        df.to_csv('/tmp/mmf-"+mdate+".csv')

        # save new version to S3
        s3_client.upload_file('/tmp/mmf-"+mdate+".csv', bucket_name, prefix + "mmf-" + mdate + ".csv")

        # send message
        sns_client.publish(TopicArn=topic_arn,
                           Message="Found a new list, mmf-" + mdate + ".csv; processed and downloaded",
                           Subject='Downloaded new report')

        # read existing ids
        series_ids = s3.get_object(Bucket=bucket_name, Key="series_ids.csv")
        series_ids = set(pd.read_csv(series_ids['Body']).series_ids.tolist())
        n_oldids = len(series_ids)
        series_ids = series_ids.union(ids)

        if len(series_ids) > n_oldids:
            # prepare a new file to be saved
            series_ids = pd.DataFrame({"series_ids": list(series_ids)})

            # store locally
            series_ids.to_csv("/tmp/series_ids.csv")

            # upload to S3
            s3_client.upload_file("/tmp/series_ids.csv", bucket_name, "series_ids.csv")
            sns_client.publish(TopicArn=topic_arn,
                               Message="Found a new list, mmf-" + mdate + ".csv; processed and downloaded and a new ID is found.",
                               Subject='A new fund was found!')


    except:
        print("None found")

    return "Success "


