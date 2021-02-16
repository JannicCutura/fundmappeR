# fundmappeR
[![version](https://img.shields.io/badge/version-1.0.0-success.svg)](#)


## About 
This repo contains codes to build the `fundmappeR` project, an open source tool that webscraps portfolios
of money market funds (MMF). MMfs have been at the center of the great financial crisis
([Chernenko and Sunderam, 2014][Chernenko2014];
[Gorton and Metrick, 2012][Gorton2012]), the sovereign
debt crisis ([Corre et. al., 2012][correa2012]) and recently experienced some turmoil during 
the Covid-19 pandemic ([Reuters, 2020][reuters2020]) in March 2021. 

Research on MMFs received a lot
of attention, yet the barrier to enter the field is quite high since there is no off the shelve data available. 
The [SEC](https://www.sec.gov/) is collecting and publishing MMFs portfolios, but those are stored on 
their servers in an inconvenient format. `fundmappeR` parses the 
[SEC's website](https://www.sec.gov/open/datasets-mmf.html) 
for money market fund portfolio data and provides the data in an easily accessible format. 
The table is updated every month and can be accessed here. 

## Usage

This project is implemented using Python and R and runs on AWS, leveraging several of its proprietary
technologies. You can rebuild it using the codes published in this repo or you can access the final tables here.
There are four tables available, stored year by year:
- **Class table**: This table contains data on the individual fund share class. `class_id` and `date` act as primary key.
- **Series table**: This table contains data on the individual fund (i.e. a series). `series_id` and `date` act as primary key.
- **Holdings table**: This table contains data on the individual holdings. 
- **Collateral table**: This table contains data on the collateral posted for secured items. 

## Architecture
![This project is build on Amazon Web Services (AWS). AWS made it easy to automate the downloading, parsing and cleaning of the data. 
The data pipeline is run every month to fetch and add the latest data. In a first step, a lambda function checks the SEC website for new funds
and sends a notification if new funds are found. Next, an EC2 instance runs R code that downloads the raw report to S3. Any S3 object put serves
as an event trigger for lambda, which picks up the raw filing, parses its XML structure and creates four tables for a given fund-month report. 
These are stored in on S3, partitioned by year. AWS Glue crawler populates the data catalog, used for Adhoc queries in Athena. A Glue ETL 
job runs a pyspark script which transform the individial csv files into parquet tables and stores them in a public S3 bucket to make
the data available to the user. ](https://github.com/JannicCutura/fundmappeR/blob/main/docs/fundmapper.png) 






[Chernenko2014]: <https://academic.oup.com/rfs/article-abstract/27/6/1717/1598733?redirectedFrom=fulltext> "Mytitle"
[Gorton2012]: <https://www.sciencedirect.com/science/article/abs/pii/S0304405X1100081X> "Mytitle"
[Huang2011]: <https://www.sciencedirect.com/science/article/abs/pii/S104295731000029X>
[reuters2020]: <https://www.reuters.com/article/g20-markets-regulation/regulators-target-money-market-funds-after-covid-19-turmoil-idUSL8N2I22GO>
[correa2012]: <https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwj469Sp3rfuAhUMPuwKHd-pCuUQFjABegQIBhAC&url=https%3A%2F%2Fwww.ecb.europa.eu%2Fevents%2Fpdf%2Fconferences%2Fexliqmmf%2Fsession3_Correa_paper.pdf%3F1d92aade465b2b883a1a51d1b11f7295&usg=AOvVaw1A00b7DY74n4bnX5s3QaGL>
## Authors 
Jannic Cutura, 2020

[![Python](https://img.shields.io/static/v1?label=made%20with&message=Python&color=blue&style=for-the-badge&logo=Python&logoColor=white)](#)
[![R](https://img.shields.io/static/v1?label=made%20with&message=R&color=blue&style=for-the-badge&logo=R&logoColor=white)](#)

