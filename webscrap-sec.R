########################################################
#
#        Build Fund luist
#        Author: Jannic Cutura 
#
########################################################

## load libraries and set paths
rm(list=ls()) # clear env
cat('\014')
libaries =c("rstudioapi",'dplyr','tidyverse','openxlsx','readxl','reshape',
            'bsts','ggplot2','plyr','tm','zoo',"openxlsx", 'DescTools','igraph', 
            'readxl',"RCurl","edgarWebR","knitr","XML","plyr","foreach")  # define libraries
lapply(libaries, require, character.only=TRUE)
setwd(dirname(rstudioapi::getSourceEditorContext()$path))
setwd('..'); path = getwd()
source("02-code/functions/statasum.R")
mutate = dplyr::mutate; select = dplyr::select; summarise = dplyr::summarise


allfiles = list.files(paste(path,"/01-data/SEC/02-SECLinks/", sep = ""))
counter = 1

for(files in allfiles) {
  print(counter)
  if (counter == 1) {
    load(file = paste(path,"/01-data/SEC/02-SECLinks/",files, sep = "")) 
    allfilings = filings
    rm(filings)
  }
  if (counter > 1) {
    load(file = paste(path,"/01-data/SEC/02-SECLinks/",files, sep = "")) 

    allfilings = rbind(allfilings, filings)
    rm(filings)
  }
  counter = counter + 1
}


allfilings = allfilings %>%
  mutate(year = substr(filing_date,1,4),
         month =substr(filing_date, 6,7),
         seriesid_year_month = paste(series_id, year, month, sep = "_")) %>%
  group_by(seriesid_year_month) %>%
  mutate(count = n()) %>%
  select(seriesid_year_month, filing_date, type, count, everything()) %>% 
  ungroup() %>%
  group_by(seriesid_year_month) %>%
  top_n(1,filing_date) %>%
  mutate(url = str_replace(href,"-index.htm",".txt"))



cikseriesid_year_month = as.character(unique(allfilings$seriesid_year_month))
downloaded = substr(list.files(paste(path, '/01-data/SEC/03-RawNMFPs/', sep = "")),1,18)
lefttodownload = setdiff(cikseriesid_year_month, downloaded)

counter = 1
for(seriesid_year_month in lefttodownload) {
  print(counter)
  url = allfilings$url[allfilings$seriesid_year_month == seriesid_year_month]
  download.file(url,
                destfile = paste(path,"/01-data/SEC/03-RawNMFPs/",seriesid_year_month,".txt", sep=""))
  counter = counter + 1
 
}


a = 3; b= "c"
save(a,b,file = "filename.R")
