# Welcome!  This RStudio Amazon AMI contains RStudio Server version 1.3.1073,
# running R 4.0.2 on Ubuntu 18.04 LTS.
# Includes support for Shiny (add /shiny/rstudio to URL).
# NEW: experimental support for CUDA 10.1 (incl. cuDNN 7.6.5) and Magma 2.5.3
#      enabling use of GPU packages in R and higher performance for deep
#      learning frameworks such as TensorFlow.
# AMI created by Louis Aslett (http://www.louisaslett.com/).  If you've
# any comments or suggestions please mail louis.aslett@durham.ac.uk

# NOTE: It is recommended that you change the password for logging into RStudio,
# which you can do by changing from the Console to the Terminal tab below,
# launching a new terminal, and running the passwd command.
# Alternatively, since this AMI was created to make RStudio Server accessible to
# those who are less comfortable with Linux commands you can follow the
# instructions below to change it without touching Linux.

# There is a mini package where functions to manipulate the server will be
# placed.  This includes a function to change the password.  First load the
# package:



rm(list=ls()) # clear env
cat('\014')
libaries =c('dplyr','tidyverse',"edgarWebR","aws.s3","aws.ec2metadata","RStudioAMI"  )  # define libraries
lapply(libaries, require, character.only=TRUE)

# wrapppter to read csv from S3
s3.read_csv <- function(s3_path, sep=",", row.names= NULL) {
  s3_pattern <- "^s3://(.+?)/(.*)$"
  s3_bucket <- gsub(s3_pattern, "\\1", s3_path)
  s3_object <- gsub(s3_pattern, "\\2", s3_path)
  read.csv(text = rawToChar(aws.s3::get_object(s3_object, s3_bucket)),  row.names=row.names, sep = sep)
}


MMFSLists= get_bucket("s3://fundmapper/", prefix="01-MMFLists")

MMFSLists[1]

df = s3.read_csv("s3://fundmapper/01-MMFLists/mmf-2019-07.csv",  row.names=NULL, sep = ";") %>%
  mutate(ReportMonth = as.character(ReportMonth))
year = substr(unique(df$ReportMonth), 7,11)
month = substr(unique(df$ReportMonth), 4,5) 
series_ids= unique(as.character(df$series_id))
counter = 1
for(series_id in series_ids) {
  print(paste0("Step ",counter," of ", length(series_ids),"; parsing ", series_id), sep="")
  tryCatch({
    # access filing for one fund and keep the last one available for the reference period
    filings = company_filings(series_id,type ="N-MFP", count=200) %>%
      mutate(filing_date = as.character(filing_date),
             filing_year = substr(filing_date,1,4),
             filing_month= substr(filing_date,6,7)) %>%
      filter(filing_year == year & filing_month == month) %>%
      top_n(1,filing_date) 
    
    # prepare file identifier
    url = filings$href[1]
    filing_date = filings$filing_date[1]
    
    # download file and save locally 
    download.file(url, destfile = "/home/rstudio/tmp.txt")
    
    # put to S3 data lake
    put_object(file = "tmp.txt", object = paste0("02-RawNMFPs/",series_id,"/",filing_date,"-",series_id,".txt"), bucket = "fundmapper")
    
    # clean up
    rm(filings)
    file.remove("/home/rstudio/tmp.txt")
    counter = counter +1
  }, error=function(e){})
}




