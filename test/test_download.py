import pandas as pd


df = pd.read_parquet("https://fundmapper.s3.eu-central-1.amazonaws.com/04-FinalTables/series_data/202006/part-00000-744e8aa1-d730-4079-8f92-bcb8eb0cd042-c000.snappy.parquet")



df2 = pd.read_parquet("https://fundmapper.s3.eu-central-1.amazonaws.com/04-FinalTables/series_data/202006/*")



