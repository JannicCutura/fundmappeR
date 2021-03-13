import pandas as pd


df = pd.read_parquet("https://fundmapper.s3.eu-central-1.amazonaws.com/05-FinalTables/series_data/202003/series_data_202003.parquet")


df2 = pd.read_parquet("https://fundmapper.s3.eu-central-1.amazonaws.com/04-FinalTables/series_data/202006/*")



