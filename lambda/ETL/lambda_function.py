import re
import os
import xml.etree.ElementTree as ET
import pandas as pd
import boto3
import csv
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

from xml_2_data import mnfp_2_data
from xml_2_data import mnfp1_2_data
from xml_2_data import mnfp2_2_data
from nmfp_rename_vars import nmfp_rename_vars


def lambda_handler(event, context):
    # parse the S3 triggered event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    # bucket = "fundmapper"
    # key = "02-RawNMFPs/S000008702/2011-03-07-S000008702.txt"

    prefix, series_id, filing = key.split("/")

    # store temporarily
    s3_client.download_file(bucket, key, "/tmp/" + series_id + "_" + filing + ".txt")
    s3.Object(bucket, key).delete()
    # read
    filing = open("/tmp/" + series_id + "_" + filing + ".txt", 'r').read()
    filing = filing.replace(":", "")
    filing_type = re.search("<TYPE>(.*)\n", filing).group(1)
    filing_date = int(re.sub("[^0-9]", "", re.search("CONFORMED PERIOD OF REPORT(.*)\n", filing).group(1))[0:6])
    filing = (filing.replace("\n", "")
              .replace(' xmlns="http//www.sec.gov/edgar/nmfpsecurities"', '')
              .replace(' xmlns="http//www.sec.gov/edgar/nmfpfund"', ""))
    print("convert")
    if filing_type in ["N-MFP", "N-MFP/A"]:
        series_df, class_df, holdings, all_collateral = mnfp_2_data(filing)
        series_df, class_df, holdings, all_collateral = nmfp_rename_vars(filing_type, series_df, class_df, holdings,
                                                                         all_collateral)

    if filing_type in ["N-MFP1", "N-MFP1/A"]:
        series_df, class_df, holdings, all_collateral = mnfp1_2_data(filing)

    if filing_type in ["N-MFP2", "N-MFP2/A"]:
        series_df, class_df, holdings, all_collateral = mnfp2_2_data(filing)

    series_df['date'], class_df['date'], holdings['date'], all_collateral[
        'date'] = filing_date, filing_date, filing_date, filing_date
    series_df['filing_type'], class_df['filing_type'], holdings['filing_type'], all_collateral[
        'filing_type'] = filing_type, filing_type, filing_type, filing_type,

    series_df = series_df[['date'] + [col for col in series_df.columns if col != 'date']]
    class_df = class_df[['date'] + [col for col in class_df.columns if col != 'date']]
    holdings = holdings[['date'] + [col for col in holdings.columns if col != 'date']]
    all_collateral = all_collateral[['date'] + [col for col in all_collateral.columns if col != 'date']]

    series_df = series_df[['filing_type'] + [col for col in series_df.columns if col != 'filing_type']]
    class_df = class_df[['filing_type'] + [col for col in class_df.columns if col != 'filing_type']]
    holdings = holdings[['filing_type'] + [col for col in holdings.columns if col != 'filing_type']]
    all_collateral = all_collateral[['filing_type'] + [col for col in all_collateral.columns if col != 'filing_type']]

    file_format = ".csv"
    series_df.to_csv("/tmp/series_" + series_id + "_" + str(filing_date) + file_format)
    s3_client.upload_file("/tmp/series_" + series_id + "_" + str(filing_date) + ".parquet", "fundmapper",
                          "03-ParsedRecords/series_data/" + series_id + "/" + series_id + "_" + str(
                              filing_date) + file_format)

    class_df.to_csv("/tmp/class_" + series_id + "_" + str(filing_date) + file_format)
    s3_client.upload_file("/tmp/class_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                          "03-ParsedRecords/class_data/" + series_id + "/" + series_id + "_" + str(
                              filing_date) + file_format)

    holdings.to_csv("/tmp/holdings_" + series_id + "_" + str(filing_date) + file_format)
    s3_client.upload_file("/tmp/holdings_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                          "03-ParsedRecords/holdings_data/" + series_id + "/" + series_id + "_" + str(
                              filing_date) + file_format)

    all_collateral.to_csv("/tmp/collateral_" + series_id + "_" + str(filing_date) + file_format)
    s3_client.upload_file("/tmp/collateral_" + series_id + "_" + str(filing_date) + file_format, "fundmapper",
                          "03-ParsedRecords/collateral_data/" + series_id + "/" + series_id + "_" + str(
                              filing_date) + file_format)

    return "Success"