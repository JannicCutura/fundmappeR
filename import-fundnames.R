########################################################
#
#        Build Fund luist
#        Author: Jannic Cutura 
#
########################################################

## load libraries and set paths
rm(list=ls()) # clear env
cat('\014')
libaries =c("tidyr","rstudioapi",'dplyr','tidyverse','openxlsx','readxl','reshape',
            'bsts','ggplot2','plyr','tm','zoo',"openxlsx", 'DescTools','igraph', 
            'readxl',"RCurl","edgarWebR","knitr","XML","plyr","foreach", 'lubridate')  # define libraries
lapply(libaries, require, character.only=TRUE)
setwd(dirname(rstudioapi::getSourceEditorContext()$path))
setwd('..'); path = getwd()
source("02-code/functions/statasum.R")
mutate = dplyr::mutate; select = dplyr::select; summarise = dplyr::summarise



## attach holdings
fundlist = paste(path, '\\01-data\\SEC\\01-MMFLists', sep = ""); setwd(fundlist)
allfiles = list.files(fundlist)
counter = 1

#columnnames = colnames(read.csv("mmf-2019-07.csv"))

## stack it all together
for(filename in allfiles) {
    print(counter)

    if (counter ==1 ) {
      names = read.csv(file = filename, row.names=NULL, sep = ";")
      #colnames(names) = columnnames
    }
    if (counter > 1 ) {
      temp = read.csv(file = filename, row.names=NULL, sep = ";")
      #colnames(temp)= columnnames
      names = rbind.fill(names, temp)
    }    
  counter = counter +1
}


rm(temp)
names  =unique(names)


cikseries = as.character(unique(names$series_id))
downloaded = substr(list.files(paste(path, '/01-data/SEC/02-SECLinks/', sep = "")),1,10)
lefttodownload = setdiff(cikseries, downloaded)
counter = 1
for(fundcik in lefttodownload) {
  print(counter)
  tryCatch({
    filings= company_filings(fundcik,type ="N-MFP", count=100) %>%
      mutate(series_id = fundcik)
    save(filings, file = paste(path,"/01-data/SEC/02-SECLinks/",fundcik,".R", sep = ""))
    rm(filings)
  }, error=function(e){})
  counter = counter +1 
  
}

  
  
  
  
  
