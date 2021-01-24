

## get legacy documents
start_time <- Sys.time()
counter = 1
for (series_id in series_ids[1:2]) {
  print(paste0("This is for fund (", series_id,"), ", counter, " of ", length(series_ids),  sep=""))
  counter = counter + 1 
  for (year in seq(2011,2020,1)) {
    print(paste0("Parsing year ", year, sep=""))
    
    # access filings and keep the newest filing per month (i.e. the amended form if multiples exist)
    tryCatch({
      filings = company_filings(series_id,type ="N-MFP", count=100, before=paste0(year+1,"0101", sep="")) %>%
        mutate(filing_date = as.character(filing_date),
               filing_year = substr(filing_date,1,4),
               filing_month= substr(filing_date,6,7)) %>%
        filter(filing_year == as.character(year)) %>%
        group_by(filing_year, filing_month) %>%
        top_n(1,filing_date) %>%
        mutate(url = str_replace(href,"-index.htm",".txt"))  
      
      for (row in 1:nrow(filings))   {
        # prepare file identifier
        url = filings$url[row]
        filing_date = filings$filing_date[row]
        
        # download file and save locally 
        download.file(url, destfile = "/home/rstudio/tmp.txt")
        
        # put to S3 data lake
        put_object(file = "tmp.txt", object = paste0("02-RawNMFPs/",series_id,"/",filing_date,"-",series_id,".txt"), bucket = "fundmapper")
        
        # clean up
        file.remove("/home/rstudio/tmp.txt")
        
        svc$put_item(
          Item = list(
            series_id = list(S = series_id),
            date = list(N = 202006),
            downloaded = list(N=1),
            parsed = list(N=0)
          ),
          TableName = "fundmapper"
        )
        
        
      }
      rm(filings)
    }, error=function(e){})
  }
}

end_time <- Sys.time()
end_time - start_time



