import re
import os
import xml.etree.ElementTree as ET
import pandas as pd
import boto3
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

from xml_2_data import mnfp_2_data, mnfp1_2_data, nmfp2_2_data


def lambda_handler(event, context):
    # parse the S3 triggered event
    # record =event['Records'][0]
    # bucket = record['s3']['bucket']['name']
    # key = unquote_plus(record['s3']['object']['key'])

    bucket = "fundmapper"
    key = "02-RawNMFPs/S000008702/2011-03-07-S000008702.txt"

    prefix, series_id, filing = key.split("/")

    # store temporarily
    s3_client.download_file(bucket, key, "/tmp/" + series_id + "_" + filing + ".txt")

    # read
    filing = open("/tmp/" + series_id + "_" + filing + ".txt", 'r').read()
    filing = filing.replace(":", "")
    filing_type = re.search("<TYPE>(.*)\n", filing).group(1)
    filing_date = re.sub("[^0-9]", "", re.search("CONFORMED PERIOD OF REPORT(.*)\n", filing).group(1))[0:6]
    filing = (filing.replace("\n", "")
              .replace(' xmlns="http//www.sec.gov/edgar/nmfpsecurities"', '')
              .replace(' xmlns="http//www.sec.gov/edgar/nmfpfund"', ""))
    print("convert")
    if filing_type in ["N-MFP", "N-MFP/A"]:
        series_df, class_df, holdings, all_collateral = mnfp_2_data(filing)

    if filing_type in ["N-MFP1", "N-MFP1/A"]:
        series_df, class_df, holdings, all_collateral = mnfp1_2_data(filing)

    if filing_type in ["N-MFP2", "N-MFP2/A"]:
        series_df, class_df, holdings, all_collateral = mnfp2_2_data(filing)

    series_df.to_csv("/tmp/" + series_id + "_" + filing + ".csv")
    # save new version to S3
    s3_client.upload_file('/tmp/testfile.txt', "fundmapper", "thisismynewtest/placedhere.txt")

    return series_df.shape[1]