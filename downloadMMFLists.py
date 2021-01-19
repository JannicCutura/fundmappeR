import pandas as pd
import boto3
from datetime import date
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = "fundmapper"
prefix = "01-MMFLists/"
def lambda_handler(event, context):
    # TODO implement

    today = date.today()
    mdate = today.strftime("%Y-%m")
    mdate = "2020-11"
    bucket = s3.Bucket('fundmapper')
    objs = list(bucket.objects.filter(Prefix=prefix))

    for obj in objs:
        if obj.key == prefix + "mmf-" + mdate + ".csv":
            return "File already present"

    print("Not stored yet; try to download")

    try:
        # download new file and store to s3
        df = pd.read_csv(
            "https://www.sec.gov/files/investment/data/other/money-market-fund-information/mmf-" + mdate + ".csv")
        newids = set(df.series_id.tolist())
        df.to_csv('/tmp/mmf-"+mdate+".csv')
        s3_client.upload_file('/tmp/mmf-"+mdate+".csv', "fundmapper", "01-MMFLists/mmf-" + mdate + ".csv")

        series_ids = s3.get_object(Bucket="fundmapper", Key="series_ids.csv")
        series_ids = set(pd.read_csv(series_ids['Body']).series_ids.tolist())
        n_oldids = len(series_ids)

        series_ids = series_ids.union(newids)
        if len(series_ids) > n_oldids:
            series_ids = pd.DataFrame({"series_ids": list(series_ids)})
            series_ids.to_csv("/tmp/series_ids.csv")
            s3_client.upload_file("/tmp/series_ids.csv", "fundmapper", "series_ids.csv")

        # TO-DO: add notification
    except:
        print("None found")

    return "Success"