rm(list=ls()) # clear env
cat('\014')
libaries =c('dplyr','tidyverse',"edgarWebR","aws.s3","aws.ec2metadata","RStudioAMI","stringr")  # define libraries
lapply(libaries, require, character.only=TRUE)

# wrapper to read csv from S3
s3.read_csv <- function(s3_path, sep=",", row.names= NULL) {
  s3_pattern <- "^s3://(.+?)/(.*)$"
  s3_bucket <- gsub(s3_pattern, "\\1", s3_path)
  s3_object <- gsub(s3_pattern, "\\2", s3_path)
  read.csv(text = rawToChar(aws.s3::get_object(s3_object, s3_bucket)),  row.names=row.names, sep = sep)
}




series_ids= as.character(unique(s3.read_csv("s3://fundmapper/series_ids.csv",  row.names=NULL, sep = ",")$series_id))


for (series_id in series_ids) {
  series_id = series_ids[1]
  year = as.numeric(substr(Sys.time(),1,4))
  print(paste0("Parsing year ", year, sep=""))
  
  # access filings and keep the newest filing per month (i.e. the amended form if multiples exist) and keep the last 5 records
  filings = company_filings(series_id,type ="N-MFP", count=100, before=paste0(year+1,"0101", sep="")) %>%
    mutate(filing_date = as.character(filing_date),
           filing_year = substr(filing_date,1,4),
           filing_month= substr(filing_date,6,7)) %>%
    group_by(filing_year, filing_month) %>%
    top_n(1,filing_date) %>% head(5)
  
  
  for (row in 1:nrow(filings))   {
    # prepare file identifier
    url = filings$href[row]
    filing_date = filings$filing_date[row]
    
    if (!object_exists(bucket = "fundmapper", object =  paste0("02-RawNMFPs/",series_id,"/",filing_date,"-",series_id,".txt"))) {
      # download file and save locally 
      download.file(url, destfile = "/home/rstudio/tmp.txt")
      
      # put to S3 data lake
      put_object(file = "tmp.txt", object = paste0("02-RawNMFPs/",series_id,"/",filing_date,"-",series_id,".txt"), bucket = "fundmapper")
      file.remove("/home/rstudio/tmp.txt")
    }
  }
  rm(filings)
  
}







## get legacy documents
for (series_id in series_ids) {
  series_id = series_ids[1]
  for (year in seq(2011,2020,1)) {
    print(paste0("Parsing year ", year, sep=""))
    
    # access filings and keep the newest filing per month (i.e. the amended form if multiples exist)
    filings = company_filings(series_id,type ="N-MFP", count=100, before=paste0(year+1,"0101", sep="")) %>%
      mutate(filing_date = as.character(filing_date),
             filing_year = substr(filing_date,1,4),
             filing_month= substr(filing_date,6,7)) %>%
      filter(filing_year == as.character(year)) %>%
      group_by(filing_year, filing_month) %>%
      top_n(1,filing_date) 
    
    for (row in 1:nrow(filings))   {
      # prepare file identifier
      url = filings$href[row]
      filing_date = filings$filing_date[row]
      
      # download file and save locally 
      download.file(url, destfile = "/home/rstudio/tmp.txt")
      
      # put to S3 data lake
      put_object(file = "tmp.txt", object = paste0("02-RawNMFPs/",series_id,"/",filing_date,"-",series_id,".txt"), bucket = "fundmapper")
      
      # clean up
      file.remove("/home/rstudio/tmp.txt")
    }
    rm(filings)
  }
  
}





#create a list of series_ids
mmflists = get_bucket(bucket = "fundmapper", "01-MMFLists/")
mmflists = map_df(mmflists, ~ map_df(.x, ~ replace(.x, is.null(.x), NA)), .id = "my.var") %>%
  select(Key) %>%
  filter(str_detect(Key, ".csv"))
series_ids= list()
for (row in 1:nrow(mmflists))   {
  key = as.character(mmflists[row,"Key"])
  print(key)
  tmp = as.character(unique(s3.read_csv(paste0("s3://fundmapper/", key, sep=""), sep=";")$series_id))
  series_ids = union(series_ids, tmp)
}
series_ids = as.character(series_ids)
s3write_using(series_ids, bucket = "fundmapper", object = "series_ids.csv", FUN = utils::write.csv)

