import pandas as pd

table = "holdings_data"
date = 201906
df_aws = pd.read_parquet("https://fundmapper.s3.eu-central-1.amazonaws.com/05-FinalTables/series_data/201601/series_data_201601.parquet")




path = "C:/Users/janni/Dropbox/university/research/RepoCollateralMMFReform"



Class_data =          pd.read_feather(path = f"{path}/01-data/SEC/06-FinalTables/Class_data.feather")
Series_data =         pd.read_feather(path = f"{path}/01-data/SEC/06-FinalTables/Series_data.feather")
Holdings_data =       pd.read_feather(path = f"{path}/01-data/SEC/06-FinalTables/Holdings_data.feather")
All_collateral_data = pd.read_feather(path = f"{path}/01-data/SEC/06-FinalTables/All_collateral_data.feather")


Class_data = Class_data.assign(series_id = lambda x: x.series_year_filingmonth.str.slice(0,10),
                               year      = lambda x: x.series_year_filingmonth.str.slice(11,15),
                               month     = lambda x: x.series_year_filingmonth.str.slice(16,18),
                               date      = lambda x: x.year +x.month)

Series_data = Series_data.assign(series_id = lambda x: x.series_year_filingmonth.str.slice(0,10),
                               year      = lambda x: x.series_year_filingmonth.str.slice(11,15),
                               month     = lambda x: x.series_year_filingmonth.str.slice(16,18))


resss= Class_data.groupby(["date"]).size()


df = Series_data.query("year == '2016' & month == '01'")
series_ids = df.series_id.unique()

df_aws_seriesids = df_aws.series_id.unique()

set(df_aws_seriesids).difference(series_ids)
set(series_ids).difference(df_aws_seriesids)

import boto3
import os
import csv
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

bucket = "fundmapper"
key = "02-RawNMFPs/S000004822/2016-01-08-S000004822.txt"

prefix, series_id, filing = key.split("/")

s3_client.download_file(bucket, key, os.getcwd()+"/" + series_id + "_" + filing + ".txt")


def my_months(date, last_n="All"):

    current_year = date[0:4]
    current_month = date[5:6]
    my_months_list = ["201212"] # first one
    for year in range(2013, int(current_year)+1):
        for month in ["01","02","03","04","05","06","07","08","09","10","11","12"]:
            loopdate = str(year)+month
            if loopdate <= date:
                my_months_list.append(loopdate)
            else:
                if last_n =="All":
                    return my_months_list
                else:
                    return my_months_list[-last_n:]



my_months("202103",61)


