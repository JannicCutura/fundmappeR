from __future__ import print_function
import pandas as pd
import boto3
from datetime import date
import datetime

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
sns_client = boto3.client('sns')
topic_arn = 'arn:aws:sns:eu-central-1:540666357414:fundmapper'
bucket = "fundmapper"
prefix = "01-MMFLists/"


def lambda_handler(event, context):
    # define objects we need
    bucket = s3.Bucket('fundmapper')
    objs = list(bucket.objects.filter(Prefix=prefix))
    mmfs_lists = [obj.key for obj in objs]  ## list of all reference files

    for i in range(0, 7):

        mdate = date.today() - i * datetime.timedelta(days=30)
        mdate = mdate.strftime("%Y-%m")
        print(mdate)

        if f"{prefix}mmf-{mdate}.csv" not in mmfs_lists:
            print(f"trying {prefix}mmf-{mdate}.csv")
            # download new file and store to s3
            try:
                # read from SEC website
                df = pd.read_csv(
                    f"https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-{mdate}.csv")

                # save vector of IDs
                ids = set(df.series_id.tolist())

                # store locally
                df.to_csv(f'/tmp/mmf-{mdate}.csv')

                # save new version to S3
                s3_client.upload_file(f'/tmp/mmf-{mdate}.csv', "fundmapper", f"01-MMFLists/mmf-{mdate}.csv")

                # send message
                sns_client.publish(TopicArn=topic_arn,
                                   Message="Found a new list, mmf-" + mdate + ".csv; processed and downloaded",
                                   Subject='Downloaded new report')

                # read existing ids
                series_ids = s3.get_object(Bucket="fundmapper", Key="series_ids.csv")
                series_ids = set(pd.read_csv(series_ids['Body']).series_ids.tolist())
                n_oldids = len(series_ids)
                series_ids = series_ids.union(ids)

                if len(series_ids) > n_oldids:
                    # prepare a new file to be saved
                    series_ids = pd.DataFrame({"series_ids": list(series_ids)})

                    # store locally
                    series_ids.to_csv("/tmp/series_ids.csv")

                    # upload to S3
                    s3_client.upload_file("/tmp/series_ids.csv", "fundmapper", "series_ids.csv")
                    sns_client.publish(TopicArn=topic_arn,
                                       Message="Found a new list, mmf-" + mdate + ".csv; processed and downloaded and a new ID is found.",
                                       Subject='A new fund was found!')

            except:
                print("None found")

    return "This is the end"


