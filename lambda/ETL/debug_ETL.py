import boto3
s3 = boto3.client('s3')
import codecs
import csv
import pandas as pd
result = s3.get_bucket_acl(Bucket='fundmapper')
print(result)

filename = "2011-02-04-S000008702.txt"
s3.download_file('fundmapper', f'02-RawNMFPs/S000008702/{filename}', '2011-01-07-S000008702.txt')

filing = open(f"{filename}", 'r').read()
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

















