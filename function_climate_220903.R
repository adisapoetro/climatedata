# rm(list=ls())
# rm(list = ls()[grep("df", ls())])
# rm(df.daily.manual.1)
########################################################################
## Integrated AWS Data Processing (Climatology Section)
##
## yy.mm.ddto.date: 22.02.17 to a date
##
## Created by : Didi Adisaputro
##
## https://www.youtube.com/watch?v=VJA47oHjOnY
########################################################################

# ..............................................................................
# Directory Options and credentials                                     ----------------------------------------------------------

# dir <-   "C:/Users/DidiA/Google Drive/SMARTRI/" # windows laptop 
# dir <- "F:/" 
# dir <-   "~/Google Drive/My Drive/" # maclaptop 
# dir <- "D:/Folder Share IP 8/" # PC Kantor
 dir <- "D:/"
 
 
# Load Library                                                          ----
 
library(reddPrec)
 
library(climatol)
library(rgeos)
library(climate)
library(stringi)
library(anytime)
library(readxl)
library(janitor)
library(zoo)
##library(tibble)
library(reshape2)
##library(ggplot2)
library(xlsx)
library(tidyverse)
library(gridExtra)
library(ggpubr)
library(ggpmisc)
library(data.table)
library(lattice)
library(leaflet)
library(hydroTSM)
library(xts)
library(lubridate)
library(writexl)
library(slider)
##library(purrr)
library(plotly)
library(tidyverse)
##library(tidyr)
library(RColorBrewer)
library(gplots)
library(timetk)
library(psychrolib)
##library(purrr)
library(IDPmisc)
library(padr)
library(readbulk)
library(rdist) # for imputation reddprec
library(IDPmisc)
library(FedData)
library(stringi)
#library(plyr)
library(dplyr)
library(miceRanger)
library(mixgb)
library(magrittr)
library(ggrepel)
library(googlesheets4)
#library(reader)
##library(readr)
#library(conflicted)
library(stringr)
library(hyfo)
library(utils)
library(RMySQL)
library(DBI) #Contains functions for interacting with the database
 

monthly.tmax <- function(df){
  
  
  
  fncols <- function(data, cname) {
    add <-cname[!cname%in%names(data)]
    if(length(add)!=0) data [add] <- NA
    data
    
  }
  
  df<- fncols(df, c("rain_mm.sum", "id" ,
                    "temperature_c.min", "temperature_c.mean",
                    "temperature_c.max" , "rh_percent.min" , "rh_percent.mean" ,
                    "rh_percent.max" , "pressure_mbar.mean" , "par_mol_m2_s.mean" ,
                    "solar_radiation_w_m2.mean" , "wind_speed_kmh.mean", "wind_direction_degree.mean",
                    "vpd_pa.mean" ))
  
  
  df <- df  %>%
    group_by(id) %>%
    mutate(tx90=ifelse(temperature_c.max>quantile(temperature_c.max,.9, na.rm = TRUE), 1,0)) %>%
    mutate(tx95=ifelse(temperature_c.max>quantile(temperature_c.max,.95, na.rm = TRUE), 1,0)) %>%
    mutate(tx98=ifelse(temperature_c.max>quantile(temperature_c.max,.98, na.rm = TRUE), 1,0)) %>%
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id,
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE),
                      pressure_mbar.mean = mean(pressure_mbar.mean, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s.mean, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2.mean, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh.mean, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree.mean, na.rm=TRUE),
                      tx90 = sum(tx90, na.rm=TRUE),
                      tx95 = sum(tx95, na.rm = TRUE),
                      tx98 = sum(tx98, na.rm = TRUE)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct()
}
monthly.clim.index <- function(df){
  
  df <- df %>% 
    dplyr::select(date, id, temperature_c.min, temperature_c.mean,
           temperature_c.max, rain_mm.sum, rh_percent.min, rh_percent.mean, rh_percent.max)
  
  df <- df  %>%
    group_by(id) %>%
    mutate(tx90=ifelse(temperature_c.max>quantile(temperature_c.max,.9, na.rm = TRUE), 1,0)) %>%
    mutate(tx95=ifelse(temperature_c.max>quantile(temperature_c.max,.95, na.rm = TRUE), 1,0)) %>%
    mutate(tx98=ifelse(temperature_c.max>quantile(temperature_c.max,.98, na.rm = TRUE), 1,0)) %>%
    mutate(tn5=ifelse(temperature_c.min<quantile(temperature_c.min,.5, na.rm = TRUE), 1,0)) %>%
    mutate(tn10=ifelse(temperature_c.min<quantile(temperature_c.min,.10, na.rm = TRUE), 1,0)) %>%
    mutate(tn15=ifelse(temperature_c.min<quantile(temperature_c.min,.15, na.rm = TRUE), 1,0)) %>%
    mutate(tn10=ifelse(temperature_c.min<quantile(temperature_c.min,.10, na.rm = TRUE), 1,0)) %>%
    mutate(tn15=ifelse(temperature_c.min<quantile(temperature_c.min,.15, na.rm = TRUE), 1,0)) %>%
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    mutate(rain_mm.sum_1 = ifelse(rain_mm.sum >1, rain_mm.sum, NA)) %>% 
    mutate(r90=ifelse(rain_mm.sum>quantile(rain_mm.sum_1,.9, na.rm = TRUE), 1,0)) %>% 
    mutate(r10=ifelse(rain_mm.sum<quantile(rain_mm.sum_1,.1, na.rm = TRUE), 1,0)) %>% 
    dplyr::select(-rain_mm.sum_1) %>% 
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id,
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE),
                      tx90 = sum(tx90, na.rm=TRUE),
                      tx95 = sum(tx95, na.rm = TRUE),
                      tx98 = sum(tx98, na.rm = TRUE),
                      tn5 = sum(tn5, na.rm=TRUE),
                      tn10 = sum(tn10, na.rm = TRUE),
                      tn15 = sum(tn15, na.rm = TRUE),
                      raindays = sum(raindays, na.rm = TRUE),
                      r90 = sum(r90, na.rm = TRUE),
                      r10 = sum(r10, na.rm = TRUE)
                      
    ) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct()
}
weekly.tmax <- function(df) {
  
  
  df <- df  %>%
    group_by(id) %>%
    pad_by_time(date) %>% 
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    dplyr::select(date, id, raindays, rain_mm.sum) %>% 
    mutate (date = as.Date(date)) %>%
    ungroup() %>% 
    group_by(id) %>%
    summarise_by_time(date, .by = "week", .week_start = 1, .type = "floor",
                      raindays=sum(raindays, na.rm = TRUE), rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE)) %>%
    ungroup() 
  
} # agg data to weekly  

# List of Function                                                      ####
agg.daily.hourly <- function(df){


  cols <- c(rain_mm  = NA_real_, id = NA_real_,
            temperature_c=NA_real_, rh_percent = NA_real_, pressure_mbar =NA_real_, par_mol_m2_s=NA_real_,
            solar_radiation_w_m2  = NA_real_, wind_speed_kmh = NA_real_,
            temperature_c.min=NA_real_, wind_direction_degree=NA_real_,
            vpd_pa =NA_real_)

  df <- df  %>%
    mutate(date = as.Date(date_time, format="%Y-%m-%d")) %>%
    add_column(df, !!!cols[setdiff(names(cols), names(df))]) %>%
    group_by(id) %>%
    summarise_by_time(date, .by = "day", id=id,
                      temperature_c.min= min(temperature_c, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm),
                      pressure_mbar.mean = mean(pressure_mbar, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree, na.rm=TRUE),
                      vpd_pa.mean = mean(vpd_pa, na.rm=TRUE)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct(date, .keep_all = TRUE) %>%
    ungroup()
  
}  # agregate daily from hourly AWS
agg.weekly.daily <- function(df) {
  
  df <- df  %>%
    #group_by(id) %>%
    pad_by_time(date) %>% 
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    dplyr::select(date, id, raindays, rain_mm.sum) %>% 
    mutate (date = as.Date(date)) %>%
    #ungroup() %>% 
    #group_by(id) %>%
    summarise_by_time(date, .by = "week", .week_start = 1, .type = "floor", region = region, estate = estate,
                      raindays=sum(raindays, na.rm = TRUE), rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE)) %>%
    ungroup() 
  
} # agg data to weekly
agg.monthly.daily <- function(df){

  fncols <- function(data, cname) {
   add <-cname[!cname%in%names(data)]
   if(length(add)!=0) data [add] <- NA
   data

  }

  df<- fncols(df, c("rain_mm.sum", "id" ,
                     "temperature_c.min", "temperature_c.mean",
                     "temperature_c.max" , "rh_percent.min" , "rh_percent.mean" ,
                     "rh_percent.max" , "pressure_mbar.mean" , "par_mol_m2_s.mean" ,
                     "solar_radiation_w_m2.mean" , "wind_speed_kmh.mean", "wind_direction_degree.mean",
                     "vpd_pa.mean",  "region", "estate" ))

  df <- df  %>%
    group_by(id) %>% 
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    mutate(rain_mm.sum_1 = ifelse(rain_mm.sum >1, rain_mm.sum, NA)) %>% 
    mutate(r90=ifelse(rain_mm.sum>quantile(rain_mm.sum_1,.9, na.rm = TRUE), 1,0)) %>% 
    mutate(r10=ifelse(rain_mm.sum<quantile(rain_mm.sum_1,.1, na.rm = TRUE), 1,0)) %>% 
    dplyr::select(-rain_mm.sum_1) %>% 
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id, region=region, estate=estate,
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm.sum),
                      pressure_mbar.mean = mean(pressure_mbar.mean, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s.mean, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2.mean, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh.mean, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree.mean, na.rm=TRUE),
                      raindays=sum(raindays, na.rm = TRUE),
                      r90=sum(r90, na.rm = TRUE),
                      r10=sum(r10, na.rm = TRUE),
                      vpd_pa.mean = mean(vpd_pa.mean, na.rm=TRUE)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct() %>%
    mutate (etp = ifelse(raindays >10, 120, 150)) %>%
    mutate(swr=200) %>%
    mutate(wd=0) %>% 
    group_by(id) %>% 
    drop_na(rain_mm.sum)

  for (i in (2:nrow(df))) {
    df$swr[i] <- df$swr[i-1] + (df$rain_mm.sum[i]) - (df$etp[i])
    df$swr[i][df$swr[i] > 200] <- 200
    df$swr[i][df$swr[i] < 0] <- 0

    df$wd[i] <-  (df$etp[i]) - df$swr[i-1] - (df$rain_mm.sum[i])
    df$wd[i][df$wd[i] < 0] <- 0
  }

  df <- df %>%
    group_by(id) %>% 
    mutate(drainage_mm= lag(swr)+rain_mm.sum-etp-200) %>%
    dplyr::rename(wd_mm =wd, etp_mm =etp, swr_mm=swr) %>%
    mutate(drainage_mm=replace(drainage_mm, drainage_mm<0, 0)) %>%
    dplyr::select(-etp_mm ,-swr_mm) %>% 
    pad_by_time() %>% #need revision
    ungroup() %>% 
    dplyr::select(where(~sum(!is.na(.x)) > 0))   #delete column that contain only NA

  
} # agregate monthly from daily  AWS
agg.monthly.daily.ombro.deprecated <- function(df){
  

  fncols <- function(data, cname) {
    add <-cname[!cname%in%names(data)]
    if(length(add)!=0) data [add] <- NA
    data
    
  }
  
  df<- fncols(df, c("rain_mm.sum", "id" ,
                    "temperature_c.min", "temperature_c.mean",
                    "temperature_c.max" , "rh_percent.min" , "rh_percent.mean" ,
                    "rh_percent.max" , "pressure_mbar.mean" , "par_mol_m2_s.mean" ,
                    "solar_radiation_w_m2.mean" , "wind_speed_kmh.mean", "wind_direction_degree.mean",
                    "vpd_pa.mean" ))
  
  df <- df  %>%
    group_by(id) %>% 
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    mutate(rain_mm.sum_1 = ifelse(rain_mm.sum >1, rain_mm.sum, NA)) %>% 
    mutate(r90=ifelse(rain_mm.sum>quantile(rain_mm.sum_1,.9, na.rm = TRUE), 1,0)) %>% 
    mutate(r10=ifelse(rain_mm.sum<quantile(rain_mm.sum_1,.1, na.rm = TRUE), 1,0)) %>% 
    dplyr::select(-rain_mm.sum_1) %>% 
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id,
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE),
                      pressure_mbar.mean = mean(pressure_mbar.mean, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s.mean, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2.mean, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh.mean, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree.mean, na.rm=TRUE),
                      raindays=sum(raindays, na.rm = TRUE),
                      r90=sum(r90, na.rm = TRUE),
                      r10=sum(r10, na.rm = TRUE),
                      vpd_pa.mean = mean(vpd_pa.mean, na.rm=TRUE)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct() %>%
    mutate (etp = ifelse(raindays >10, 120, 150)) %>%
    mutate(swr=200) %>%
    mutate(wd=0) %>% 
    group_by(id)
  
#df <- df_test
  

  for (i in (2:nrow(df))) {
    df$swr[i] <- df$swr[i-1] + (df$rain_mm.sum[i]) - (df$etp[i])
    df$swr[i][df$swr[i] > 200] <- 200
    df$swr[i][df$swr[i] < 0] <- 0
    
    df$wd[i] <-  (df$etp[i]) - df$swr[i-1] - (df$rain_mm.sum[i])
    df$wd[i][df$wd[i] < 0] <- 0
  }
  
  
  tes <- df %>%
    group_by(id) %>% 
    mutate(drainage_mm= dplyr::lag(swr)+rain_mm.sum-etp-200) %>%
    dplyr::rename(wd_mm =wd, etp_mm =etp, swr_mm=swr) %>%
    mutate(drainage_mm=replace(drainage_mm, drainage_mm<0, 0)) %>%
    ungroup()
  
  
} # agregate monthly from daily  AWS
agg.yearly.daily <- function(df){
  df <- df_ombro
  
  fncols <- function(data, cname) {
    add <-cname[!cname%in%names(data)]
    if(length(add)!=0) data [add] <- NA
    data
    
  }
  
  df<- fncols(df, c("rain_mm.sum", "id" ,
                    "temperature_c.min", "temperature_c.mean",
                    "temperature_c.max" , "rh_percent.min" , "rh_percent.mean" ,
                    "rh_percent.max" , "pressure_mbar.mean" , "par_mol_m2_s.mean" ,
                    "solar_radiation_w_m2.mean" , "wind_speed_kmh.mean", "wind_direction_degree.mean",
                    "vpd_pa.mean" ))
  
  df <- df  %>%
    group_by(id) %>%
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    mutate(rain_mm.sum_1 = ifelse(rain_mm.sum >1, rain_mm.sum, NA)) %>% 
    mutate(r90=ifelse(rain_mm.sum>quantile(rain_mm.sum_1,.9, na.rm = TRUE), 1,0)) %>% 
    mutate(r10=ifelse(rain_mm.sum<quantile(rain_mm.sum_1,.1, na.rm = TRUE), 1,0)) %>% 
    dplyr::select(-rain_mm.sum_1) %>% 
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id,
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE),
                      pressure_mbar.mean = mean(pressure_mbar.mean, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s.mean, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2.mean, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh.mean, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree.mean, na.rm=TRUE),
                      raindays=sum(raindays, na.rm = TRUE),
                      r90=sum(r90, na.rm = TRUE),
                      r10=sum(r10, na.rm = TRUE),
                      vpd_pa.mean = mean(vpd_pa.mean, na.rm=TRUE)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct() %>%
    mutate (etp = ifelse(raindays >10, 120, 150)) %>%
    mutate(swr=200) %>%
    mutate(wd=0)
  
  for (i in (2:nrow(df))) {
    df$swr[i] <- df$swr[i-1] + (df$rain_mm.sum[i]) - (df$etp[i])
    df$swr[i][df$swr[i] > 200] <- 200
    df$swr[i][df$swr[i] < 0] <- 0
    
    df$wd[i] <-  (df$etp[i]) - df$swr[i-1] - (df$rain_mm.sum[i])
    df$wd[i][df$wd[i] < 0] <- 0
  }
  
  df <- df %>%
    mutate(drainage_mm= lag(swr)+rain_mm.sum-etp-200) %>%
    dplyr::rename(wd_mm =wd, etp_mm =etp, swr_mm=swr) %>%
    mutate(drainage_mm=replace(drainage_mm, drainage_mm<0, 0)) %>%
    dplyr::select(-etp_mm ,-swr_mm) %>% 
    ungroup() %>% 
    dplyr::select(where(~sum(!is.na(.x)) > 0))  #delete column that contain only NA
  
  df <- df %>% 
    group_by(id) %>% 
    summarise_by_time(date, .by = "year", 
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c.mean, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent.min, na.rm=TRUE), rh_percent.mean= mean(rh_percent.mean, na.rm=TRUE), rh_percent.max= max(rh_percent.max, na.rm=TRUE),
                      pressure_mbar.mean = mean(pressure_mbar.mean, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s.mean, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2.mean, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh.mean, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree.mean, na.rm=TRUE),
                      raindays=sum(raindays, na.rm = TRUE),
                      r90=sum(r90, na.rm = TRUE),
                      r10=sum(r10, na.rm = TRUE),
                      vpd_pa.mean = mean(vpd_pa.mean, na.rm=TRUE),
                      raindays=sum(raindays, na.rm = TRUE),
                      rain_mm.sum = sum(rain_mm.sum, na.rm = TRUE),
                      wd_mm = sum(wd_mm, na.rm = TRUE))
  
  
} # agregate monthly from daily  AWS
climatol.tmean <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  wd0 <- setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  # Create .dat files Tmean 
  tmeantest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, temperature_c.mean) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, temperature_c.mean) %>% 
    arrange(date)
  

  
  tmeantest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('tmeantest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  write.table(stasiun, paste0('tmeantest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  
  homogen('tmeantest', as.numeric(min_year), as.numeric(max_year))
  dahstat('tmeantest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(tmeantest)==id_aws )-1)
  
  load(file=paste0('tmeantest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(tmeantest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(temperature_c.mean = 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(temperature_c.mean = coalesce(temperature_c.mean.y, temperature_c.mean.x)) %>%
    dplyr::select(-temperature_c.mean.x, -temperature_c.mean.y) 
  
  
  setwd(wd0)
  return(df.homogenized)
} # Homogenization of Temperature Mean
climatol.tmax <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  wd0 <- setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  # Create .dat files Tmax 
  tmaxtest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, temperature_c.max) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, temperature_c.max) %>% 
    arrange(date)
  
  
  tmaxtest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('tmaxtest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  write.table(stasiun, paste0('tmaxtest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  
  homogen('tmaxtest', as.numeric(min_year), as.numeric(max_year))
  dahstat('tmaxtest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(tmaxtest)==id_aws )-1)
  
  load(file=paste0('tmaxtest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(tmaxtest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(temperature_c.max = 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(temperature_c.max = coalesce(temperature_c.max.y, temperature_c.max.x)) %>%
    dplyr::select(-temperature_c.max.x, -temperature_c.max.y) 
  
  
  setwd(wd0)
  return(df.homogenized)
  
} # Homogenization of Temperature Max
climatol.tmin <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  wd0 <- setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  # Create .dat files Tmin 
  tmintest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, temperature_c.min) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, temperature_c.min) %>% 
    arrange(date)
  
  
  tmintest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('tmintest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  
  write.table(stasiun, paste0('tmintest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  homogen('tmintest', as.numeric(min_year), as.numeric(max_year))
  dahstat('tmintest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(tmintest)==id_aws )-1)
  
  load(file=paste0('tmintest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(tmintest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(temperature_c.min= 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(temperature_c.min = coalesce(temperature_c.min.y, temperature_c.min.x)) %>%
    dplyr::select(-temperature_c.min.x, -temperature_c.min.y) 
  
  
  setwd(wd0)
  return(df.homogenized)
  
} # Homogenization of Temperature Min
climatol.rhmean <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  

  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  # Create .dat files rhmean 
  rhmeantest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, rh_percent.mean) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, rh_percent.mean) %>% 
    arrange(date)
  
  rhmeantest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('rhmeantest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  write.table(stasiun, paste0('rhmeantest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  homogen('rhmeantest', as.numeric(min_year), as.numeric(max_year), vmax=100) # execute homogenization
  
  dahstat('rhmeantest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(rhmeantest)==id_aws )-1)
  
  load(file=paste0('rhmeantest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(rhmeantest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(rh_percent.mean= 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(rh_percent.mean = coalesce(rh_percent.mean.y, rh_percent.mean.x)) %>%
    dplyr::select(-rh_percent.mean.x, -rh_percent.mean.y) 
  
  
  setwd(wd0)
  return(df.homogenized)
} # Homogenization of rh Mean
climatol.rhmin <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  wd0 <- setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  
  # Create .dat files rhmin 
  rhmintest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, rh_percent.min) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, rh_percent.min) %>% 
    arrange(date)
  
  
  rhmintest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('rhmintest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  write.table(stasiun, paste0('rhmintest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  homogen('rhmintest', as.numeric(min_year), as.numeric(max_year), vmax=100) # execute homogenization
  
  dahstat('rhmintest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(rhmintest)==id_aws )-1)
  
  load(file=paste0('rhmintest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(rhmintest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(rh_percent.min = 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(rh_percent.min = coalesce(rh_percent.min.y, rh_percent.min.x)) %>%
    dplyr::select(-rh_percent.min.x, -rh_percent.min.y) 
  
  
  setwd(wd0)
  return(df.homogenized)
  
} # Homogenization of rh Min
climatol.rhmax <- function(df) {
  
  wd0 <- paste0(dir, "R project/Climate Trend")
  wd1 <- paste0(dir, "R project/Climate Trend/output")
  wd0 <- setwd(wd1)
  
  id_aws <- as.character (head(df$id, 1))  
  
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # Argument in function
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=6) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df %>% 
    bind_rows(df_meteostat) %>% 
    right_join(choosen_id, by="id") %>% 
    drop_na(date)
  
  sts_merged <- sts_bmkg %>% 
    bind_rows(sts_clim1005) 
  
  stasiun <- df_meteostat_refrenced %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    left_join(sts_merged, by="id") %>% 
    dplyr::select(lon, lat, alt, id) %>% 
    mutate(name=id) %>% 
    arrange((id))
  
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  # Create .dat files rhmax 
  rhmaxtest <- df_meteostat_refrenced %>%
    group_by(id) %>% 
    filter_by_time(date, .start_date = paste0(min_year,"-01-01"), .end_date = paste0(max_year,"-12-31")) %>% 
    pad_by_time() %>% 
    dplyr::select(date, id, rh_percent.max) %>% 
    ungroup() %>% 
    arrange(desc(id)) %>% 
    spread(id, rh_percent.max) %>% 
    arrange(date)
  
  rhmaxtest %>% 
    dplyr::select(-date) %>% 
    write.table(paste0('rhmaxtest_',min_year,'-',max_year,'.dat'), row.names=FALSE, col.names=FALSE) #save the data file
  
  write.table(stasiun, paste0('rhmaxtest_',min_year,'-',max_year,'.est'), row.names=FALSE, col.names=FALSE)
  homogen('rhmaxtest', as.numeric(min_year), as.numeric(max_year), vmax=100) # execute homogenization
  
  dahstat('rhmaxtest', as.numeric(min_year), as.numeric(max_year), stat='series')
  
  order <- (which( colnames(rhmaxtest)==id_aws )-1)
  
  load(file=paste0('rhmaxtest_',min_year,'-',max_year,'.rda'))
  
  df.homogenized <- as.data.frame(dat) %>% 
    bind_cols(rhmaxtest) %>% 
    dplyr::select(all_of(order),date) %>% 
    rename(rh_percent.max = 1)
  
  df.homogenized <- df %>% 
    left_join(df.homogenized , by = c('date')) %>%
    mutate(rh_percent.max = coalesce(rh_percent.max.y, rh_percent.max.x)) %>%
    dplyr::select(-rh_percent.max.x, -rh_percent.max.y) 

  
  setwd(wd0)
  
  return(df.homogenized)
  
} # Homogenization of Rh Max
download.merged.ogimet <- function(id, .start.date, .end.date) {
  id.deparse <- deparse(substitute(id))
  out <- list()
  out$ogimet_daily <- meteo_ogimet(interval = "daily", date = c(.start.date, .end.date),
                                   station = c(id.deparse)) %>%
    dplyr::select(Date, station_ID, Precmm) %>%
    dplyr::rename(date = Date, id = station_ID, rain_mm.sum=Precmm)

  out$ogimet_hourly <- meteo_ogimet(interval = "hourly", date = c(.start.date, .end.date),
                                    station = c(id.deparse), precip_split = FALSE) %>%
    #mutate(date_time = paste(Date, hour)) %>%
    #mutate(date_time =  anytime(date_time)) %>%
    dplyr::select(Date, TC, TdC, Hr, PseahPa ) %>%
    dplyr::rename(date_time = Date, temperature_c = TC, dew_temperature = TdC, rh_percent = Hr, pressure_mbar =  PseahPa) %>%
    distinct() %>%
    distinct(date_time, .keep_all = TRUE) %>%
    mutate(date_time = sapply(date_time, toString)) %>%
    mutate(date_time =  anytime(date_time)) %>%
    thicken('hour')  %>%
    pad_by_time(date_time_hour, .by= 'hour') %>%
    mutate(temperature_c = as.numeric(temperature_c),  dew_temperature = as.numeric(dew_temperature),
           rh_percent = as.numeric(rh_percent), pressure_mbar = as.numeric(pressure_mbar)) %>%
    dplyr::select(-date_time)  %>%
    dplyr::rename(date_time = date_time_hour) %>%
    dplyr::select (date_time, temperature_c, dew_temperature, rh_percent, pressure_mbar)
  cols <- c(pressure_mbar = NA_real_, par_mol_m2_s= NA_real_,
            solar_radiation_w_m2 = NA_real_, wind_speed_kmh =NA_real_, wind_direction_degree =NA_real_)
  out$ogimet_hourly<-add_column(out$ogimet_hourly, !!!cols[setdiff(names(cols), names(out$ogimet_hourly))])

  out$ogimet_hourly <- out$ogimet_hourly %>%
    mutate(date = as.Date(date_time, format="%Y-%m-%d")) %>%
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      pressure_mbar.mean = mean(pressure_mbar, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree, na.rm=TRUE)) %>%
    distinct(date, .keep_all = TRUE)

  df.ogimet <- out$ogimet_daily %>%
    pad_by_time(date) %>%
    left_join(out$ogimet_hourly, by="date")

  # return(out) # for producing list files
  return(df.ogimet) # For producing df.ogimet merge hourly and daily

} # rain_daily and other hourly
download.ogimet <- function(id, .start.date, .end.date) {
  id.deparse <- deparse(substitute(id))
  out <- list()
  out$ogimet_daily <- meteo_ogimet(interval = "daily", date = c(.start.date, .end.date),
                                   station = c(id.deparse))
  out$ogimet_hourly <- meteo_ogimet(interval = "hourly", date = c(.start.date, .end.date),
                                    station = c(id.deparse), precip_split = FALSE)

  return(out)


}
download.hourly.bmkg <- function(id, .start.date, .end.date) {
  id.deparse <- deparse(substitute(id))
  out <- list()
  out$ogimet_daily <- meteo_ogimet(interval = "daily", date = c(.start.date, .end.date),
                                   station = c(id.deparse)) %>%
    dplyr::select(Date, station_ID, Precmm) %>%
    dplyr::rename(date = Date, id = station_ID, rain_mm.sum=Precmm)

  out$ogimet_hourly <- meteo_ogimet(interval = "hourly", date = c(.start.date, .end.date),
                                    station = c(id.deparse), precip_split = FALSE) %>%
    #mutate(date_time = paste(Date, hour)) %>%
    #mutate(date_time =  anytime(date_time)) %>%
    dplyr::select(Date, TC, TdC, Hr, PseahPa ) %>%
    dplyr::rename(date_time = Date, temperature_c = TC, dew_temperature = TdC, rh_percent = Hr, pressure_mbar =  PseahPa) %>%
    distinct() %>%
    distinct(date_time, .keep_all = TRUE) %>%
    mutate(date_time = sapply(date_time, toString)) %>%
    mutate(date_time =  anytime(date_time)) %>%
    thicken('hour')  %>%
    pad_by_time(date_time_hour, .by= 'hour') %>%
    mutate(temperature_c = as.numeric(temperature_c),  dew_temperature = as.numeric(dew_temperature),
           rh_percent = as.numeric(rh_percent), pressure_mbar = as.numeric(pressure_mbar)) %>%
    dplyr::select(-date_time)  %>%
    dplyr::rename(date_time = date_time_hour) %>%
    dplyr::select (date_time, temperature_c, dew_temperature, rh_percent, pressure_mbar)
  cols <- c(pressure_mbar = NA_real_, par_mol_m2_s= NA_real_,
            solar_radiation_w_m2 = NA_real_, wind_speed_kmh =NA_real_, wind_direction_degree =NA_real_)
  out$ogimet_hourly<-add_column(out$ogimet_hourly, !!!cols[setdiff(names(cols), names(out$ogimet_hourly))])

  out$ogimet_hourly <- out$ogimet_hourly %>%
    mutate(date = as.Date(date_time, format="%Y-%m-%d")) %>%
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      pressure_mbar.mean = mean(pressure_mbar, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree, na.rm=TRUE)) %>%
    distinct(date, .keep_all = TRUE)

  df.ogimet <- out$ogimet_daily %>%
    pad_by_time(date) %>%
    left_join(out$ogimet_hourly, by="date")

  # return(out) # for producing list files
  return(df.ogimet) # For producing df.ogimet merge hourly and daily

}
impute.aws.rain.rf <- function (df){
  
  df[1, 'rain_mm.sum'] =NA
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id)
  
  df_imputed <- df_ombro %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    miceRanger::miceRanger(maxiter = 5, vars = "rain_mm_sum") %>% 
    completeData() %>%
    extract2(5) %>% 
    mutate(date = as.Date(date)) %>%
    dplyr::select(date, rain_mm_sum)
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join(df_imputed, by="date") %>%
    rename(rain_mm.sum = rain_mm_sum) %>% 
    mutate(id = id_aws)
  
  #plot.facet(df_imputed)
  
} # Impute Rainfall missing values using random forest
impute.aws.rain.rf.reddPrec <- function (df){
  
  gapFilling.mod <- function(prec,sts,inidate,enddate,parallel=TRUE,ncpu=2,thres=NA){
    
    #matrix of distances
    x1<- cbind(sts$X,sts$Y)
    x2<-  x1
    distanc <- rdist( x1,x2)/1000
    colnames(distanc)=sts$ID; rownames(distanc)=sts$ID
    
    #vector of dates
    datess=seq.Date(inidate,enddate,by='day')
    
    fillData <-function(x,datess,prec,distanc=distanc,sts=sts,thres=thres){
      
      dw=which(x==datess)
      d=prec[dw,]
      if(sum(is.na(d))==length(d)){
        print(paste('No data on day',x))
      } else{
        pred=data.frame(matrix(NA,ncol=7,nrow=length(d))); pred[,1] <- names(d)
        names(pred)=c('ID','obs','predb','pred1','pred2','pred3','err')
        pred$obs=as.numeric(d)
        if(max(pred$obs,na.rm=T)==0){
          pred$predb=0
          pred$pred1=0
          pred$pred2=0
          pred$err=0
        } else{
          for(h in 1:nrow(pred)){
            can=pred$obs[h]
            kk=data.frame(ID=rownames(distanc),D=distanc[,which(pred$ID[h]==colnames(distanc))],
                          obs=pred$obs[match(pred$ID,rownames(distanc))],
                          stringsAsFactors=F)
            kk=kk[order(kk$D),]
            wna=which(is.na(kk$obs))
            if(length(wna)>0) kk=kk[-c(wna),]#and removing which have no data
            kk=kk[-c(1),]
            if(!is.na(thres)){
              kk=kk[-c(which(kk$D>thres)),]
            }
            if(nrow(kk)<10) {kk=kk; print(paste('Less than 10 nearest observations in day',
                                                x,'and station',pred$ID[h]))} else {kk=kk[1:10,]
                                                if(max(kk$obs,na.rm=T)==0){
                                                  pred$predb[h]=0
                                                  pred$pred1[h]=0
                                                  pred$pred2[h]=0
                                                  pred$err[h]=0
                                                } else{
                                                  sts_can <- sts[which(pred$ID[h]==sts$ID),]
                                                  sts_nns <- sts[match(kk$ID,sts$ID),]
                                                  #binomial
                                                  b <- kk$obs; b[b>0]=1
                                                  DF <- data.frame(y=b,alt=sts_nns$ALT,lat=sts_nns$Y,lon=sts_nns$X)
                                                  fmtb <- suppressWarnings(glm(y~alt+lat+lon, data=DF, family=binomial()))
                                                  newdata=data.frame(alt=sts_can$ALT,lat=sts_can$Y,lon=sts_can$X)
                                                  pb <- predict(fmtb,newdata=newdata,type='response')
                                                  if(pb<0.001 & pb>0) pb <- 0.001
                                                  pb <- round(pb,3)
                                                  
                                                  #data
                                                  mini=min(kk$obs)/2
                                                  maxi=max(kk$obs)+(max(kk$obs)-min(kk$obs))
                                                  yr=as.numeric((kk$obs-mini)/(maxi-mini))
                                                  DF$y=yr
                                                  fmt <- suppressWarnings(glm(y~alt+lat+lon, data=DF, family=quasibinomial()))
                                                  p <- predict(fmt,newdata=newdata,type='response')
                                                  if(p<0.001 & p>0) p <- 0.001
                                                  p <- round((p*(maxi-mini))+mini,3)
                                                  
                                                  err <- sqrt(sum((DF$y-predict(fmt,type='response'))^2)/(length(DF$y)-3))#introduces standard error
                                                  if(err<0.001 & err>0) err <- 0.001
                                                  err <- round((err*maxi)+mini,3)
                                                  #asign data
                                                  pred$predb[h]=pb
                                                  pred$pred1[h]=p
                                                  pred$pred2[h]=p
                                                  if(pb<0.5) pred$pred2[h]=0
                                                  pred$err[h]=err
                                                  if(pred$predb[h]>=0.5 & pred$pred2[h]<1) pred$pred2[h]=1
                                                }#next pred calculation
                                                }
          }#next station
        }
      }
      dir.create('./days/',showWarnings = F)
      write.table(pred,paste('./days/',x,'.txt',sep=''),quote=F,row.names=F,sep='\t',na='')
    }
    #RUN gapFilling
    if(parallel){
      sfInit(parallel=T,cpus=ncpu)
    }
    if(parallel){
      print('Creating daily files')
      d=sfLapply(datess,fun=fillData,datess=datess,prec=prec,distanc=distanc,sts=sts,thres=thres)
    } else {
      d=lapply(datess,FUN=fillData,datess=datess,prec=prec,distanc=distanc,sts=sts,thres=thres)
    }
    gc()
    sfStop()
    
    #read predicted and standardization
    print('Re-reading data')
    aa=list.files('./days/')
    pred=matrix(NA,ncol=nrow(sts),nrow=length(datess)); colnames(pred)=sts$ID
    obs=pred
    for(i in 1:length(aa)){
      d=read.table(paste('./days/',aa[i],sep=''),header=T,sep='\t')
      obs[i,] <- d$obs
      pred[i,] <- d$pred2
    }
    print('Standardization')
    
    #monthly standardization
    pred3 = pred
    for (i in 1:ncol(obs)) {
      print(i)
      for(h in 1:12){
        w = which(h==as.numeric(substr(datess,6,7)))
        ww = which(obs[w,i]+pred[w,i] != 0)
        ww2 = which(obs[w,i]+pred[w,i] == 0)
        pred3[w,i][ww2] = 0
        pred3[w,i] = pred[w,i]/((sum(pred[w,i][ww],na.rm=T)+1)/(sum(obs[w,i][ww],na.rm=T)+1))
      }
    }
    
    print("Writing final files")
    for (i in 1:length(aa)) {
      d = read.table(paste("./days/", aa[i], sep = ""), header = T,
                     sep = "\t")
      d$pred3 <- round(pred3[i, ], 1)
    }
    for (i in 1:ncol(obs)) {
      w = which(is.na(obs[, i]))
      if (length(w) > 0)
        obs[w, i] <- pred3[w, i]
    }
    filled = obs
    rm(obs)
    
    return(filled)
    
  }
  
  df[1, 'rain_mm.sum'] =NA
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  id <- unique(df$id)
  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id)
  
  df_imputed <- df_ombro %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    pad_by_time()
  
  min_date_2 <- as.character (min(df_imputed$date))
  max_date_2 <- as.character (max(df_imputed$date))
  
  sts_reddPrec <- df_imputed %>% 
    select(-estate) %>% 
    pivot_longer(!date, names_to = "ID", values_to = "value") %>% 
    distinct(ID) %>% 
    mutate(ALT=70, X=0, Y=0)
  
  df_imputed1 <- df_imputed %>% 
    pad_by_time() %>% 
    select(-rain_mm_sum, -estate, -date) %>% 
    miceRanger::miceRanger(maxiter = 5) %>% 
    completeData() %>%
    extract2(5) %>% 
    mutate(date =seq.Date(as.Date(min_date_2), as.Date(max_date_2),by='day')) %>% 
    left_join(df_imputed %>% select(date, rain_mm_sum))
  
  df_imputed2 <- df_imputed1 %>% 
    select(-date) 
  
  df_imputed3 <- gapFilling.mod(prec=df_imputed2,sts=sts_reddPrec,inidate=as.Date(min_date_2),
                                enddate=as.Date(max_date_2),parallel=TRUE,ncpu=2,thres=NA) %>% 
    as.data.frame() %>% 
    miceRanger::miceRanger(maxiter = 5) %>% 
    completeData() %>%
    extract2(5) 
  
  df_imputed3 <- df_imputed3 %>% 
    mutate(date = seq.Date(as.Date(min_date_2), as.Date(max_date_2),by='day'))
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join((df_imputed3 %>% select(rain_mm_sum, date)), by="date") %>%
    rename(rain_mm.sum = rain_mm_sum) %>% 
    mutate(id = id_aws) %>%
    select(-(contains("divisions")))
  
  return(df_imputed)
  
} # Use combination between redprec (to fill 0) and Randomforest
impute.aws.rain.rf.hyfo <- function (df){
  

  
  df[1, 'rain_mm.sum'] =NA
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  id <- unique(df$id)
  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id) 
  
  df_imputed <- df_ombro %>% 
    mutate(estate = substr(id,1,4)) %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    pad_by_time()
  
  min_date_2 <- as.character (min(df_imputed$date))
  max_date_2 <- as.character (max(df_imputed$date))
  
  df_imputed1 <- df_imputed %>% 
    pad_by_time() %>% 
    select(-rain_mm_sum, -estate, -date) %>% 
    miceRanger::miceRanger(maxiter = 5) %>% 
    completeData() %>%
    extract2(5) %>% 
    mutate(date =seq.Date(as.Date(min_date_2), as.Date(max_date_2),by='day')) %>% 
    left_join(df_imputed %>% select(date, rain_mm_sum))
  
  df_imputed2 <- df_imputed1 %>% 
    relocate(date) %>% 
    as.data.frame() %>% 
    rename(Date=date)
  
  df_imputed3 <-  fillGap(df_imputed2) %>% 
    rename(date=Date)
  
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join((df_imputed3 %>% select(rain_mm_sum, date)), by="date") %>%
    rename(rain_mm.sum = rain_mm_sum) %>% 
    mutate(id = id_aws) %>%
    select(-(contains("divisions")))
  
  return(df_imputed)
  
} # Use combination between redprec (to fill 0) and Randomforest
impute.aws.rain.xgb <- function(df) {

#df <- read.cimel("BAME01CML00203") %>% agg.daily.hourly()
  
#df <- tes 

  df[1, 'rain_mm.sum'] =NA
  
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id)
  
  df_imputed <- df_ombro %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    mutate(date=as.numeric(date)) %>% 
    dplyr::select(-estate) %>% 
    mixgb( m = 5, xgboost_verbose = 1) 
  
  df_imputed <- df_imputed[[5]] %>%
    rename(rain_mm.sum=rain_mm_sum) %>% 
    dplyr::select(date, rain_mm.sum) %>% 
    mutate(date = as.Date(date))
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join(df_imputed, by="date") %>%
    mutate(id = id_aws)
  
} # Impute Rainfall missing values using xgb 
impute.aws.rain.hyfo <- function(df) {
  
  #df <- tes 
  df[1, 'rain_mm.sum'] =NA
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id)
  
  df_imputed <- df_ombro %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    #dplyr::select(-contains(estate)) %>% 
    relocate(date, rain_mm_sum) %>% 
    filter(!is.na(date))
  
  
  df_imputed <- fillGap(df_imputed)

  
  df_imputed <- df_imputed %>%
    rename(rain_mm.sum=rain_mm_sum) %>% 
    dplyr::select(date, rain_mm.sum) %>% 
    mutate(date = as.Date(date))
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join(df_imputed, by="date") %>%
    mutate(id = id_aws)
  
} # Impute Rainfall missing values using xgb 
impute.aws.rain.mice <- function(df) {

  
  #df <- read.cimel("BAME01CML00203") %>% agg.daily.hourly()
  
  #df <- tes 
  
  df[1, 'rain_mm.sum'] =NA
  
  
  ombro_selected_id <- df %>% 
    dplyr::select(id) %>% 
    distinct() %>% 
    na.omit() %>% 
    stringr::str_extract( "^.{4}")
  
  min_date <- as.character (min(df$date))
  id_aws <- as.character (head(df$id, 1))  
  
  df <- df %>% 
    pad_by_time(date) %>% 
    mutate(id=ombro_selected_id)
  
  df_imputed <- df_ombro %>% 
    dplyr::filter(grepl(ombro_selected_id , id)) %>%  # filter based on argument in function #
    spread(id, rain_mm.sum) %>% 
    merge(y = df[, c("date", "rain_mm.sum")], by = "date", all=TRUE) %>% # select data based on argument in function #
    clean_names() %>% 
    filter_by_time(.start_date = min_date) %>% 
    mutate(date=as.numeric(date)) %>% 
    dplyr::select(-estate) %>% 
    mice() %>% 
    complete() 
  
  df_imputed <- df_imputed %>%
    rename(rain_mm.sum=rain_mm_sum) %>% 
    dplyr::select(date, rain_mm.sum) %>% 
    mutate(date = as.Date(date))
  
  df_imputed  <- df %>%
    dplyr::select(-rain_mm.sum) %>% 
    left_join(df_imputed, by="date") %>%
    mutate(id = id_aws)
  
} # Impute Rainfall missing values using xgb 
impute.aws.allvars.rf <- function(df) { 
  
  #IDN <- stations_ogimet(country = "Indonesia", add_map = TRUE)
  
  df[1, 'temperature_c.mean'] =NA  
  
  id_aws <- as.character (head(df$id, 1))  
  
  #  bmkg_station <- IDN %>% 
  #  dplyr::rename(id=wmo_id) %>% 
  #  dplyr::select(id, lon, lat) 
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # filter based on argument in function 
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id_1 <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=10) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df_meteostat %>% 
    right_join(choosen_id_1, by="id") %>%  # Filtering by using right join.
    drop_na(date) %>% 
    melt(id=c("date", "id")) %>% 
    unite(var_id, variable, id) %>% 
    spread(var_id, value)
  
  df_imputed_1 <- df %>% 
    pad_by_time() %>% 
    left_join(df_meteostat_refrenced, by="date") %>%
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    dplyr::select(-contains("id")) %>% 
    miceRanger::miceRanger()
  
  ncol_df <- as.numeric(ncol(df))
  
  df_imputed_2 <- df_imputed_1  %>% 
    completeData() %>%
    extract2(5) %>%
    mutate(id=id_aws) %>% 
    relocate(date, id) %>% 
    mutate(date = as.Date(date)) %>% 
    as.data.frame() %>% 
    dplyr::select(1:ncol_df) %>%                                            
    clean_names() %>% 
    rename_with(function(x){gsub("_x","",x)}) %>% 
    rename_with(function(x){gsub("_mean",".mean",x)}) %>% 
    rename_with(function(x){gsub("_min",".min",x)}) %>% 
    rename_with(function(x){gsub("_max",".max",x)}) %>%  
    rename_with(function(x){gsub("_sum",".sum",x)}) 
  
  return(df_imputed_2)
  
} # Impute AWS Rainfall missing values using random Forest
impute.aws.allvars.xgb <- function(df) { 


  
  df[1, 'temperature_c.mean'] =NA    
    
#df <- tes
  #IDN <- stations_ogimet(country = "Indonesia", add_map = TRUE)
  
  id_aws <- as.character (head(df$id, 1))  
  
  #  bmkg_station <- IDN %>% 
  #  dplyr::rename(id=wmo_id) %>% 
  #  dplyr::select(id, lon, lat) 
  
  clim_station <- sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% # filter based on argument in function 
    bind_rows(sts_bmkg) 
  
  coordinates(clim_station) <- ~lon+lat
  choosen_id_1 <- gDistance(clim_station  , byid=T) %>% 
    as.data.frame() %>%
    rename(distance_km= 1) %>% 
    mutate(distance_km = distance_km*111) %>% 
    dplyr::select(distance_km ) %>% 
    cbind(clim_station) %>% 
    dplyr::select(-optional) %>% 
    arrange(distance_km) %>% 
    slice_head(n=10) %>% 
    dplyr::select(id)
  
  df_meteostat_refrenced <- df_meteostat %>% 
    right_join(choosen_id_1, by="id") %>%  # Filtering by using right join.
    #dplyr::select(-rain_mm.sum) %>% 
    drop_na(date) %>% 
    reshape2::melt(id=c("date", "id")) %>% 
    unite(var_id, variable, id) %>% 
    spread(var_id, value)
  
  df_imputed_1 <- df %>% 
    pad_by_time() %>% 
    left_join(df_meteostat_refrenced, by="date") %>%
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    dplyr::select(-contains("id")) %>% 
    mutate(date=as.numeric(date)) %>% 
    mixgb( m = 5, xgboost_verbose = 1) 
  
  #slice_head(n=10) %>% 
  
  df_imputed_1  <- df_imputed_1 [[5]] %>%
    mutate(date = as.Date(date))
  
  ncol_df <- as.numeric(ncol(df))
  
  df_imputed_2 <- df_imputed_1  %>% 
    mutate(id=id_aws) %>% 
    relocate(date, id) %>% 
    mutate(date = as.Date(date)) %>% 
    as.data.frame() %>% 
    dplyr::select(1:ncol_df) %>%                                            
    clean_names() %>% 
    rename_with(function(x){gsub("_x","",x)}) %>% 
    rename_with(function(x){gsub("_mean",".mean",x)}) %>% 
    rename_with(function(x){gsub("_min",".min",x)}) %>% 
    rename_with(function(x){gsub("_max",".max",x)}) %>%  
    rename_with(function(x){gsub("_sum",".sum",x)}) 
  
  return(df_imputed_2)
  
} # Impute AWS Rainfall missing values using xgb
killDbConnections <- function () {
  
  all_cons <- dbListConnections(MySQL())
  
  print(all_cons)
  
  for(con in all_cons)
    +  dbDisconnect(con)
  
  print(paste(length(all_cons), " connections killed."))
  
}
merge.ogimet <- function (df.hourly, df.daily) {

  #---
  list.daily <- df.daily %>%
    dplyr::select(Date, station_ID, Precmm) %>%
    dplyr::rename(date = Date, id = station_ID, rain_mm.sum=Precmm)

  #---

  list.hourly <-  df.hourly %>%
    dplyr::select(Date, TC, TdC, Hr, PseahPa ) %>%
    dplyr::rename(date_time = Date, temperature_c = TC, dew_temperature = TdC, rh_percent = Hr, pressure_mbar =  PseahPa) %>%
    distinct() %>%
    distinct(date_time, .keep_all = TRUE) %>%
    mutate(date_time = sapply(date_time, toString)) %>%
    mutate(date_time =  anytime(date_time)) %>%
    thicken('hour')  %>%
    pad_by_time(date_time_hour, .by= 'hour') %>%
    mutate(temperature_c = as.numeric(temperature_c),  dew_temperature = as.numeric(dew_temperature),
           rh_percent = as.numeric(rh_percent), pressure_mbar = as.numeric(pressure_mbar)) %>%
    dplyr::select(-date_time)  %>%
    dplyr::rename(date_time = date_time_hour) %>%
    dplyr::select (date_time, temperature_c, dew_temperature, rh_percent, pressure_mbar)

  cols <- c(pressure_mbar = NA_real_, par_mol_m2_s= NA_real_,
            solar_radiation_w_m2 = NA_real_, wind_speed_kmh =NA_real_, wind_direction_degree =NA_real_)


  list.hourly<-add_column(list.hourly, !!!cols[setdiff(names(cols), names(list.hourly))])  %>%
    mutate(date = as.Date(date_time, format="%Y-%m-%d")) %>%
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      pressure_mbar.mean = mean(pressure_mbar, na.rm=TRUE),
                      par_mol_m2_s.mean = mean(par_mol_m2_s, na.rm=TRUE),
                      solar_radiation_w_m2.mean =  mean(solar_radiation_w_m2, na.rm=TRUE),
                      wind_speed_kmh.mean = mean(wind_speed_kmh, na.rm=TRUE),
                      wind_direction_degree.mean = mean(wind_direction_degree, na.rm=TRUE)) %>%
    distinct(date, .keep_all = TRUE)

  df.ogimet <- list.daily %>%
    pad_by_time(date) %>%
    left_join(list.hourly, by="date")

  return(df.ogimet) # For producing df.ogimet merge hourly and daily

} #merge hourly and daily ogimet
plot.anomaly.daily <- function(df) {
  df <- df %>%
    mutate(rain_mm.sum = replace(rain_mm.sum, rain_mm.sum<5, NA)) %>%
    reshape2::melt(id.var = c('date', 'id')) %>%
    group_by(variable, id) %>%
    NaRV.omit() %>%
    plot_anomaly_diagnostics(date, value, .facet_ncol = 2,  .alpha = 0.05, .interactive = FALSE)
} #plot anomaly daily AWS
plot.climpact.ts.ann <- function(dir, index.name, unit) {
  unit.deparse <- deparse(substitute(unit))
  index.name.deparse <- deparse(substitute(index.name))

  df <- read_csv(dir,skip = 6)

  tes <- df[,2]

  ggplot(df, aes_string(x ="time", y =index.name.deparse)) +
    geom_area(size = 0.5, fill="red") +
    labs(x = "Year", y = unit.deparse, title = index.name.deparse) +
    theme_bw() +
    theme(
      text = element_text(size = 15),
      plot.title = element_text(size = 20),
      axis.title.y = element_text(size = 14L,
                                  face = "bold"),
      axis.title.x = element_text(size = 14L,
                                  face = "bold")
    )

} #Plot annual climate extremes from climpact
plot.diagram.climate <- function (df, .start_date, .end_date,  ref.year) {
  ref.year.deparse <- deparse(substitute(ref.year))
  
  diagram.climate <- function (df, .start_date, .end_date, seasons, ref.year) {
    
    seasons.deparse <- deparse(substitute(seasons))
    
    seasons.deparse.case <- case_when(seasons.deparse %in% "winter" ~ "December-January-February",
                                      seasons.deparse %in% "spring" ~ "March-April-May",
                                      seasons.deparse %in% "summer" ~ "Juni-July-August",
                                      seasons.deparse %in% "autumn" ~ "September-October-November")
    
    
    df.daily <- df %>%
      dplyr::select (date, rain_mm.sum, temperature_c.mean ) %>%
      dplyr::rename( ta =temperature_c.mean, pr = rain_mm.sum) %>%
      pad_by_time(date) %>%
      filter_by_time(date,.start_date = .start_date,.end_date = .end_date )
    
    
    meteo_yr <- function(dates, start_month = NULL) {
      # convert to POSIXlt
      dates.posix <- as.POSIXlt(dates)
      # year offset
      offset <- ifelse(dates.posix$mon >= start_month - 1, 1, 0)
      # new year
      adj.year = dates.posix$year + 1900 + offset
      return(adj.year)
    }
    
    data <- mutate(df.daily,
                   winter_yr = meteo_yr(date, 12),
                   month = month(date),
                   season = case_when(month %in% c(12,1:2) ~ "winter",
                                      month %in% 3:5 ~ "spring",
                                      month %in% 6:8 ~ "summer",
                                      month %in% 9:11 ~ "autumn"))
    
    data_inv <-  dplyr::filter(data, season == seasons.deparse) %>%
      group_by(winter_yr) %>%
      summarize(pr = sum(pr, na.rm = T), ta= mean(ta, na.rm = T))
    
    data_inv <-  mutate(data_inv, pr_mean = mean(pr[winter_yr <= ref.year]),
                        ta_mean = mean(ta[winter_yr <= ref.year]),
                        pr_anom = (pr*100/pr_mean)-100,
                        ta_anom = ta-ta_mean,
                        
                        labyr = case_when(pr_anom < 0 & ta_anom < 0 ~ winter_yr,
                                          pr_anom < 0 & ta_anom > 0 ~ winter_yr,
                                          pr_anom > 0 & ta_anom < -0 ~ winter_yr,
                                          pr_anom > 0 & ta_anom > 0 ~ winter_yr),
                        symb_point = ifelse(!is.na(labyr), "yes", "no"),
                        lab_font = ifelse(labyr == 2020, "bold", "plain"))
    
    
    data_inv_p <- mutate(data_inv, pr_anom = pr_anom * -1)
    
    bglab <- data.frame(x = c(-Inf, Inf, -Inf, Inf),
                        y = c(Inf, Inf, -Inf, -Inf),
                        hjust = c(1, 1, 0, 0),
                        vjust = c(1, 0, 1, 0),
                        lab = c("Wet-Warm", "Dry-Warm",
                                "Wet-Cold", "Dry-Cold"))
    
    
    g1 <- ggplot(data_inv_p,
                 aes(pr_anom, ta_anom)) +
      annotate("rect", xmin = -Inf, xmax = 0, ymin = 0, ymax = Inf, fill = "#fc9272", alpha = .6) + #wet-warm
      annotate("rect", xmin = 0, xmax = Inf, ymin = 0, ymax = Inf, fill = "#cb181d", alpha = .6) + #dry-warm
      annotate("rect", xmin = -Inf, xmax = 0, ymin = -Inf, ymax = 0, fill = "#2171b5", alpha = .6) + #wet-cold
      annotate("rect", xmin = 0, xmax = Inf, ymin = -Inf, ymax = 0, fill = "#c6dbef", alpha = .6) + #dry-cold
      geom_hline(yintercept = 0,
                 linetype = "dashed") +
      geom_vline(xintercept = 0,
                 linetype = "dashed") +
      geom_text(data = bglab,
                aes(x, y, label = lab, hjust = hjust, vjust = vjust),
                fontface = "italic", size = 5,
                angle = 90, colour = "white")
    
    g1
    
    
    g2 <- g1 + geom_point(aes(fill = symb_point, colour = symb_point),
                          size = 2.8, shape = 21, show.legend = FALSE) +
      geom_text_repel(aes(label = labyr, fontface = lab_font),
                      max.iter = 5000,
                      size = 3.5)
    g2
    
    captions <- paste0( "Data: Manual\nNormal period begining-", ref.year.deparse)
    
    g3 <- g2 + scale_x_continuous("Precipitation anomaly in %",
                                  breaks = seq(-100, 250, 10) * -1,
                                  labels = seq(-100, 250, 10),
                                  limits = c(min(data_inv_p$pr_anom), 100)) +
      scale_y_continuous("Mean temperature anomaly in ºC",
                         breaks = seq(-2, 2, 0.5)) +
      scale_fill_manual(values = c("black", "white")) +
      scale_colour_manual(values = rev(c("black", "white"))) +
      labs(title = seasons.deparse.case ,
           caption = captions) +
      theme_bw()
    
    g3
  }
  
  winter <- diagram.climate (df, .start_date, .end_date, winter, ref.year)
  spring <- diagram.climate (df, .start_date, .end_date, spring, ref.year)
  summer <- diagram.climate (df, .start_date, .end_date, summer, ref.year)
  autumn <- diagram.climate (df, .start_date, .end_date, autumn, ref.year)
  
  
  list_plot <- return(list(summer = summer, winter = winter, autumn =autumn, spring=spring))
  plot.climate.diagram <- gridExtra::grid.arrange(grobs = list_plot)
  
  return(plot.climate.diagram )
  
}
plot.diagram.climate2 <- function (df, .start_date, .end_date,  ref.year, musim) {
  ref.year.deparse <- deparse(substitute(ref.year))

  diagram.climate <- function (df, .start_date, .end_date, seasons, ref.year) {

    seasons.deparse <- deparse(substitute(seasons))

    seasons.deparse.case <- case_when(seasons.deparse %in% "winter" ~ "December-January-February",
                                      seasons.deparse %in% "spring" ~ "March-April-May",
                                      seasons.deparse %in% "summer" ~ "Juni-July-August",
                                      seasons.deparse %in% "autumn" ~ "September-October-November")


    df.daily <- df %>%
      dplyr::select (date, rain_mm.sum, temperature_c.mean ) %>%
      dplyr::rename( ta =temperature_c.mean, pr = rain_mm.sum) %>%
      pad_by_time(date) %>%
      filter_by_time(date,.start_date = .start_date,.end_date = .end_date )


    meteo_yr <- function(dates, start_month = NULL) {
      # convert to POSIXlt
      dates.posix <- as.POSIXlt(dates)
      # year offset
      offset <- ifelse(dates.posix$mon >= start_month - 1, 1, 0)
      # new year
      adj.year = dates.posix$year + 1900 + offset
      return(adj.year)
    }

    data <- mutate(df.daily,
                   winter_yr = meteo_yr(date, 12),
                   month = month(date),
                   season = case_when(month %in% c(12,1:2) ~ "winter",
                                      month %in% 3:5 ~ "spring",
                                      month %in% 6:8 ~ "summer",
                                      month %in% 9:11 ~ "autumn"))

    data_inv <-  dplyr::filter(data, season == seasons.deparse) %>%
      group_by(winter_yr) %>%
      summarize(pr = sum(pr, na.rm = T), ta= mean(ta, na.rm = T))

    data_inv <-  mutate(data_inv, pr_mean = mean(pr[winter_yr <= ref.year]),
                        ta_mean = mean(ta[winter_yr <= ref.year]),
                        pr_anom = (pr*100/pr_mean)-100,
                        ta_anom = ta-ta_mean,

                        labyr = case_when(pr_anom < 0 & ta_anom < 0 ~ winter_yr,
                                          pr_anom < 0 & ta_anom > 0 ~ winter_yr,
                                          pr_anom > 0 & ta_anom < -0 ~ winter_yr,
                                          pr_anom > 0 & ta_anom > 0 ~ winter_yr),
                        symb_point = ifelse(!is.na(labyr), "yes", "no"),
                        lab_font = ifelse(labyr == 2020, "bold", "plain"))


    data_inv_p <- mutate(data_inv, pr_anom = pr_anom * -1)

    bglab <- data.frame(x = c(-Inf, Inf, -Inf, Inf),
                        y = c(Inf, Inf, -Inf, -Inf),
                        hjust = c(1, 1, 0, 0),
                        vjust = c(1, 0, 1, 0),
                        lab = c("Wet-Warm", "Dry-Warm",
                                "Wet-Cold", "Dry-Cold"))


    g1 <- ggplot(data_inv_p,
                 aes(pr_anom, ta_anom)) +
      annotate("rect", xmin = -Inf, xmax = 0, ymin = 0, ymax = Inf, fill = "#fc9272", alpha = .6) + #wet-warm
      annotate("rect", xmin = 0, xmax = Inf, ymin = 0, ymax = Inf, fill = "#cb181d", alpha = .6) + #dry-warm
      annotate("rect", xmin = -Inf, xmax = 0, ymin = -Inf, ymax = 0, fill = "#2171b5", alpha = .6) + #wet-cold
      annotate("rect", xmin = 0, xmax = Inf, ymin = -Inf, ymax = 0, fill = "#c6dbef", alpha = .6) + #dry-cold
      geom_hline(yintercept = 0,
                 linetype = "dashed") +
      geom_vline(xintercept = 0,
                 linetype = "dashed") +
      geom_text(data = bglab,
                aes(x, y, label = lab, hjust = hjust, vjust = vjust),
                fontface = "italic", size = 5,
                angle = 90, colour = "white")

    g1


    g2 <- g1 + geom_point(aes(fill = symb_point, colour = symb_point),
                          size = 2.8, shape = 21, show.legend = FALSE) +
      geom_text_repel(aes(label = labyr, fontface = lab_font),
                      max.iter = 5000,
                      size = 3.5)
    g2

    captions <- paste0( "Data: Manual\nNormal period begining-", ref.year.deparse)

    g3 <- g2 + scale_x_continuous("Precipitation anomaly in %",
                                  breaks = seq(-100, 250, 10) * -1,
                                  labels = seq(-100, 250, 10),
                                  limits = c(min(data_inv_p$pr_anom), 100)) +
      scale_y_continuous("Mean temperature anomaly in ºC",
                         breaks = seq(-2, 2, 0.5)) +
      scale_fill_manual(values = c("black", "white")) +
      scale_colour_manual(values = rev(c("black", "white"))) +
      labs(title = seasons.deparse.case ,
           caption = captions) +
      theme_bw()

    g3
  }

  musim <- diagram.climate (df, .start_date, .end_date, musim, ref.year)

  return(musim)

}
plot.facet <- function(df) {
  df %>%
    dplyr::select(-contains("id")) %>%
    reshape2::melt(id.var = 'date')  %>%
    ggplot(aes(x = date, y = value, group = variable)) +
    geom_line(aes(col=variable)) +
    theme_bw() +
    facet_wrap(~ variable, ncol = 2, scales = "free_y") +
    theme(legend.position = "none", axis.title.x= element_blank(), axis.title.y= element_blank())
} # facet plot function
plot.heatmap.monthly <- function(df, var) {
  
  column_name = enquo(var)
  id.deparse <- deparse(substitute(var))
  cols <- colorRampPalette(rev(brewer.pal(10,"Spectral")) )
  
  df <- df %>% 
    group_by(id) %>% 
    mutate(year = year(date)) %>% 
    pad_by_time()
  
  df.date <- df %>% 
    na.omit()
  
  min_year <- as.character (min(df.date$year))
  max_year <- as.character (max(df.date$year))
  
  min_year_num <- min(df.date$year)
  max_year_num <- max(df.date$year)
  
  
  df <- df %>% 
    pad_by_time(date, .start_date = paste0(min_year, "-01-01"), .end_date =  paste0(max_year, "-12-31") ) %>% 
    agg.monthly.daily() %>% 
    dplyr::select(!!column_name) %>% 
    data.matrix()
  
  rowcolNames <- list(as.character(min_year_num:max_year_num), month.abb)
  
  matrix <- matrix(df, 
                   ncol = 12, 
                   byrow = TRUE, 
                   dimnames = rowcolNames) 
  
  print(levelplot(matrix,
                  col.regions=cols, 
                  xlab = "year",
                  ylab = "month", 
                  main = id.deparse))
  
}  # df = data, var = (ex, temperature_c.mean)
plot.heatmap <- function(df, var) {
  
df <-  tx_monthly
  
  column_name = enquo(var)
  id.deparse <- deparse(substitute(var))
  cols <- colorRampPalette(rev(brewer.pal(10,"Spectral")) )
  
  
  df <- df %>% 
    group_by(id) %>% 
    mutate(year = year(date)) %>% 
    pad_by_time()
  
  df.date <- df %>% 
    na.omit()
  
  min_year <- as.character (min(df.date$year))
  max_year <- as.character (max(df.date$year))
  
  min_year_num <- min(df.date$year)
  max_year_num <- max(df.date$year)
  
  
  df_90 <- df %>% 
    ungroup() %>% 
    filter_by_time(date, .start_date = "2002-01-01", .end_date = "2021-12-01") %>% 
    dplyr::select(tx90) %>% 
    data.matrix()
  
  df_95 <- df %>% 
    ungroup() %>% 
    filter_by_time(date, .start_date = "2002-01-01", .end_date = "2021-12-01") %>% 
    dplyr::select(tx95) %>% 
    data.matrix()
  
  df_98 <- df %>% 
    ungroup() %>% 
    filter_by_time(date, .start_date = "2002-01-01", .end_date = "2021-12-01") %>% 
    dplyr::select(tx98) %>% 
    data.matrix()

    
  rowcolNames <- list(as.character(2002:2021), month.abb)
  
  matrix_90 <- matrix(df_90, 
                   ncol = 12, 
                   byrow = TRUE, 
                   dimnames = rowcolNames) 
  
  matrix_95 <- matrix(df_95, 
                      ncol = 12, 
                      byrow = TRUE, 
                      dimnames = rowcolNames) 
  
  matrix_98 <- matrix(df_98, 
                      ncol = 12, 
                      byrow = TRUE, 
                      dimnames = rowcolNames) 
  
  
  print(levelplot(matrix_90,
                  col.regions=cols, 
                  xlab = "year",
                  ylab = "month", 
                  main = "TX90 (33.9)"))
  
  print(levelplot(matrix_95,
                  col.regions=cols, 
                  xlab = "year",
                  ylab = "month", 
                  main = "TX95 (34.4)"))
  
  print(levelplot(matrix_98,
                  col.regions=cols, 
                  xlab = "year",
                  ylab = "month", 
                  main = "TX98 (35)"))
  
  
  
  
}  # df = data, var = (ex, temperature_c.mean)
plot.rain.heatmap.monthly <- function(df, var) {
  
  column_name = enquo(var)
  id.deparse <- deparse(substitute(var))
  cols <- colorRampPalette(brewer.pal(11,"Spectral")) 
  
  df <- df %>% 
    group_by(id) %>% 
    mutate(year = year(date)) %>% 
    pad_by_time()
  
  df.date <- df %>% 
    na.omit()
  
  min_year <- as.character (min(df.date$year))
  max_year <- as.character (max(df.date$year))
  
  min_year_num <- min(df.date$year)
  max_year_num <- max(df.date$year)
  
  
  df <- df %>% 
    pad_by_time(date, .start_date = paste0(min_year, "-01-01"), .end_date =  paste0(max_year, "-12-31") ) %>% 
    agg.monthly.daily() %>% 
    dplyr::select(!!column_name) %>% 
    data.matrix()
  
  rowcolNames <- list(as.character(min_year_num:max_year_num), month.abb)
  
  matrix <- matrix(df, 
                   ncol = 12, 
                   byrow = TRUE, 
                   dimnames = rowcolNames) 
  
  print(levelplot(matrix,
                  col.regions=cols, 
                  xlab = "year",
                  ylab = "month", 
                  main = id.deparse))
  
}  # df = data, var = (ex, rain_mm.sum)
plot.xy.compare <- function (df, var1, var2) {
  
  column_name1 = enquo(var1)
  column_name2 = enquo(var2)  
  
  df.plot <- df %>% 
    dplyr::select(!!column_name1, !!column_name2) 
  
  temp_mean <- ggplot(data = df.plot, aes(x = !!column_name1, y = !!column_name2)) +
    geom_smooth(method = "lm", se=FALSE, color="black", formula = y ~ x) +
    stat_poly_eq(formula = my.formula, 
                 aes(label = paste(..eq.label.., ..rr.label.., sep = "~~~")), 
                 parse = TRUE) +         
    geom_point(color = "#00AFBB") + theme_bw() + geom_smooth(method = lm)
  ggExtra::ggMarginal(temp_mean, type = "histogram")
  
} # Comparing two dataset using marginal distribution
plotly.facet <- function(df) {
  plot <- df %>%
    dplyr::select(-contains("id")) %>%
    reshape2::melt(id.var = 'date')  %>%
    ggplot(aes(x = date, y = value, group = variable)) +
    geom_line(aes(col=variable)) +
    theme_bw() +
    facet_wrap(~ variable, ncol = 2, scales = "free_y")
  
  plotly <- ggplotly(plot)
  
  return(plotly)
  
} # facet plot function
plot.facet.ombro <- function(df) {
  ggplot(df) +
    aes(x = date,y = rain_mm.sum, fill = id,colour = id,group = id) +
    geom_line(size = 0.5) +
    scale_fill_brewer(palette = "Set1", direction = 1) +
    scale_color_brewer(palette = "Set1", direction = 1) +
    theme_bw() +
    facet_wrap(vars(id))
}
plot.hist <- function(var) {
  var1<-paste0 (var,"_hmg")
  var2<-paste0(var)

  df.daily.hom.merged  %>%
    left_join(df_daily_smse, by="date") %>%
    dplyr::select(date, var1, var2) %>%
    reshape2::melt(id.var = 'date')  %>%
    gghistogram(x = "value",
                add = "mean", rug = TRUE,
                fill = "variable",
                palette = c(rgb(1,0,0,0.5), rgb(0,0,1,0.5)),
                add_density = TRUE)
} # hisplot homogenization
plot.hydro.ombro <- function (id) {

  df <- df %>%
    dplyr::filter(id=="ADPE-Division 2" ) %>%
    dplyr::rename (x=rain_mm.sum ) %>%
    dplyr::select(date,x) %>%
    mutate(x=as.numeric(x))
  df <-  xts(x = df$x, order.by =  df$date)

  hydroplot(df , var.type="Precipitation", main="()",
            pfreq = "dm")


} # id = "ADPE-Division 2"
qc.hourly <- function(df) {
  df <- as.data.frame(df)
  
  cols <- c(rain_mm  = NA_real_, id = NA_real_,
            temperature_c=NA_real_, rh_percent = NA_real_, pressure_mbar =NA_real_, par_mol_m2_s=NA_real_,
            solar_radiation_w_m2  = NA_real_, wind_speed_kmh = NA_real_,
            temperature_c.min=NA_real_, wind_direction_degree=NA_real_,
            vpd_pa =NA_real_)


  df <- add_column(df, !!!cols[setdiff(names(cols), names(df))])
  
    
    df <- df %>% 
    mutate(temperature_c = replace(temperature_c,temperature_c>40, NA)) %>%
    mutate(temperature_c = replace(temperature_c,temperature_c<5, NA)) %>%
    mutate(pressure_mbar = replace(pressure_mbar, pressure_mbar>1500, NA))  %>%
    mutate(pressure_mbar = replace(pressure_mbar, pressure_mbar<800, NA))  %>%
    mutate(rain_mm = replace(rain_mm, rain_mm<0, NA)) %>%
    mutate(rain_mm = replace(rain_mm, rain_mm>300, NA))  %>%
    mutate(rh_percent = replace(rh_percent, rh_percent<25, NA))  %>%
    mutate(rh_percent = replace(rh_percent, rh_percent>150, NA))  %>%
    mutate(wind_speed_kmh = replace(wind_speed_kmh, wind_speed_kmh>30, NA))  %>%
    mutate(par_mol_m2_s = replace(par_mol_m2_s, par_mol_m2_s>10, NA))  %>%
    dplyr::filter(!is.na(date_time)) %>%
    arrange(date_time) %>%
    distinct() %>% 
    dplyr::select(where(~sum(!is.na(.x)) > 0)) 
    
} # qc hourlydata
qc.hourly.stl <-  function(df) {
  df <- df %>%
    reshape2::melt(id.var = c('date_time', 'id'), na.rm =TRUE ) %>%
    group_by(variable, id) %>%
    NaRV.omit() %>%
    tk_anomaly_diagnostics(date_time, value, .frequency = "auto",
                           .trend = "auto", .alpha = 0.05)  %>%
    mutate(anomaly = replace(anomaly, anomaly== "Yes", 2)) %>%
    mutate(anomaly = replace(anomaly, anomaly== "No", 1))  %>%
    dplyr::select(date_time,id,observed,anomaly)  %>%
    mutate(anomaly=replace(anomaly, variable=="rain_mm.sum", 1))  %>%
    mutate(observed=replace(observed, anomaly==2, NA)) %>%
    dplyr::select(-anomaly) %>%
    group_by(variable,id) %>%
    distinct(date_time, .keep_all = TRUE) %>%
    ungroup() %>%
    spread(variable, observed)
}
qc.daily <- function(df) {
  df <- df %>%
    reshape2::melt(id.var = c('date', 'id'), na.rm =TRUE ) %>%
    group_by(variable, id) %>%
    NaRV.omit() %>%
    tk_anomaly_diagnostics(date, value, .frequency = "auto",
                           .trend = "auto", .alpha = 0.05)  %>%
    mutate(anomaly = replace(anomaly, anomaly== "Yes", 2)) %>%
    mutate(anomaly = replace(anomaly, anomaly== "No", 1))  %>%
    dplyr::select(date,id,observed,anomaly)  %>%
    mutate(anomaly=replace(anomaly, variable=="rain_mm.sum", 1))  %>%
    mutate(observed=replace(observed, anomaly==2, NA)) %>%
    dplyr::select(-anomaly) %>%
    group_by(variable,id) %>%
    distinct(date, .keep_all = TRUE) %>%
    ungroup() %>%
    spread(variable, observed) # %>% 
    # mutate(rain_mm.sum = replace(rain_mm.sum, rain_mm.sum>600, NA))
} # qc daily data
read.daily.all.aws <- function() { 

  list_cimel <- dplyr::filter(sts_clim1005, grepl('CML', id))
  list_davis <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_hobo <- dplyr::filter(sts_clim1005, grepl('HBO', id))
  list_davis_cloud <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_cbl <- dplyr::filter(sts_clim1005, grepl('CBL', id))
  
  list_cimel <- lapply(list_cimel$id, possibly(read.cimel, NA))
  list_davis <- lapply(list_davis$id, possibly(read.davis, NA))
  list_hobo <- lapply(list_hobo$id, possibly(read.hobo, NA))
  list_davis_cloud <- lapply(list_davis_cloud$id, possibly(read.davis.cloud, NA))
  list_cbl <- lapply(list_cbl$id, possibly(read.campbell, NA))
  list_hoborg <- lapply(list_hobo$id, possibly(read.hobo.rg, NA))
  
  list_aws <- c(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl, list_hoborg)

  rm(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  
  list_aws <- lapply(list_aws, possibly(qc.hourly, NA)) 
  list_aws <- lapply(list_aws, possibly(agg.daily.hourly, NA)) 
  
  list_aws <- discard(list_aws, ~all(is.na(.x))) 
  
  return(list_aws)
  
} # Read AWS without imputation return as list (Daily agregated)
read.hourly.all.aws <- function() { 
  
  list_cimel <- dplyr::filter(sts_clim1005, grepl('CML', id))
  list_davis <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_hobo <- dplyr::filter(sts_clim1005, grepl('HBO', id))
  list_davis_cloud <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_cbl <- dplyr::filter(sts_clim1005, grepl('CBL', id))
  
  list_cimel <- lapply(list_cimel$id, possibly(read.cimel, NA))
  list_davis <- lapply(list_davis$id, possibly(read.davis, NA))
  list_hobo <- lapply(list_hobo$id, possibly(read.hobo, NA))
  list_davis_cloud <- lapply(list_davis_cloud$id, possibly(read.davis.cloud, NA))
  list_cbl <- lapply(list_cbl$id, possibly(read.campbell, NA))
  
  df_list <- c(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  df_list <- discard(df_list, ~all(is.na(.x)))
  
  rm(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  
  df_aws <- df_list %>%
    lapply(qc.hourly) 
  #%>% lapply(qc.daily)  
  # %>%  lapply(impute.aws.rain.rf) 
  
  rm(df_list, list_aws)
  
  df_aws <- discard(df_aws, ~all(is.na(.x))) 
  

  
} # Read AWS without imputation return as list (Daily agregated)
read.all.aws.rain <- function() { 
  list_cimel <- dplyr::filter(sts_clim1005, grepl('CML', id))
  list_davis <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_hobo <- dplyr::filter(sts_clim1005, grepl('HBO', id))
  list_davis_cloud <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_cbl <- dplyr::filter(sts_clim1005, grepl('CBL', id))
  
  list_cimel <- lapply(list_cimel$id, possibly(read.cimel, NA))
  list_davis <- lapply(list_davis$id, possibly(read.davis, NA))
  list_hobo <- lapply(list_hobo$id, possibly(read.hobo, NA))
  list_davis_cloud <- lapply(list_davis_cloud$id, possibly(read.davis.cloud, NA))
  list_cbl <- lapply(list_cbl$id, possibly(read.campbell, NA))
  
  df_list <- c(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  df_list <- discard(df_list, ~all(is.na(.x)))
  
  rm(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  
  list_aws <- df_list %>%
    lapply(qc.hourly) %>% 
    lapply(agg.daily.hourly) 
  # %>% lapply(qc.daily)  
  # %>%  lapply(impute.aws.rain.rf) 
  
  df_aws1 <- lapply(list_aws, possibly(impute.aws.rain.xgb, NA)) # Imputing rainfall Data
  
  rm(df_list, list_aws,  df_aws1)
  
  df_aws <- discard(df_aws2, ~all(is.na(.x))) %>% 
    rbindlist(fill = TRUE)
  
} # USe XGB to fill the rainfal data
read.all.aws.allvars <- function() { 
  list_cimel <- dplyr::filter(sts_clim1005, grepl('CML', id))
  list_davis <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_hobo <- dplyr::filter(sts_clim1005, grepl('HBO', id))
  list_davis_cloud <- dplyr::filter(sts_clim1005, grepl('DVS', id))
  list_cbl <- dplyr::filter(sts_clim1005, grepl('CBL', id))
  
  list_cimel <- lapply(list_cimel$id, possibly(read.cimel, NA))
  list_davis <- lapply(list_davis$id, possibly(read.davis, NA))
  list_hobo <- lapply(list_hobo$id, possibly(read.hobo, NA))
  list_davis_cloud <- lapply(list_davis_cloud$id, possibly(read.davis.cloud, NA))
  list_cbl <- lapply(list_cbl$id, possibly(read.campbell, NA))
  
  df_list <- c(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  df_list <- discard(df_list, ~all(is.na(.x)))
  
  rm(list_cimel, list_davis, list_hobo, list_davis_cloud, list_cbl)
  
  list_aws <- df_list %>%
    lapply(qc.hourly) %>% 
    lapply(agg.daily.hourly) 
  # %>% lapply(qc.daily)  
  # %>%  lapply(impute.aws.rain.rf) 
  
  df_aws1 <- lapply(list_aws, possibly(impute.aws.rain.xgb, NA)) # Imputing rainfall Data
  df_aws2<- lapply(df_aws1,  possibly(impute.aws.allvars.xgb,NA)) # Imputing rainfall Data
  
  rm(df_list, list_aws,  df_aws1)
  
  df_aws <- discard(df_aws2, ~all(is.na(.x))) %>% 
    rbindlist(fill = TRUE)
  
} # USe XGB to fill the rainfal data and all variables
read.data <- function(file)  {
  dat <- read.table(file,header=TRUE,sep="\t")
  dat$fname <- file
  dat$fname <- stri_sub(dat$fname,-19,-5)
  return(dat)
} # read data
read.campbell <- function(name.station) {
  
name.station.deparse <- name.station  
files <- list.files(path = paste0(dir,"Climate Data/05 Campbell/",name.station,"/for r"), pattern = ".txt", full.names = TRUE)

## Bind the excel
df.list <- lapply(files, function(x)read.table(x, header=T, sep="\t", skip = 1))


df <- rbindlist(df.list, fill=TRUE) %>% 
  clean_names() %>% 
  mutate(date_time = dmy_hm(timestamp), wind_speed_kmh = as.numeric(wind_speed_w_vc_1)*3.6 ) %>% 
  rename(temperature_c = tair_avg, rh_percent = rh, solar_radiation_w_m2 =rg_avg, pressure_mbar =pressure,
         wind_direction_degree = wind_speed_w_vc_2, rain_mm = rain_tot) %>% 
  dplyr::select(date_time, temperature_c, rh_percent, rain_mm, wind_speed_kmh, wind_direction_degree, wind_speed_kmh, 
         pressure_mbar, solar_radiation_w_m2) %>% 
  mutate_if(is.character, as.numeric) %>% 
  mutate(id=name.station, pressure_mbar=pressure_mbar*10 ) %>% 
  drop_na(date_time) %>% 
  distinct(date_time, .keep_all = TRUE) %>% 
  as.data.frame()

} # read Campbell
read.cimel <- function(name.station) {
  
  name.station.deparse <- name.station
  
  
  pth <- paste0(dir, "Climate Data/01 CIMEL/",name.station.deparse,"/for r")
  list<-lapply(list.files(path =pth,
                          pattern = ".txt", full.names = TRUE),read.data)
  

  # list to store all data.frames

  df.hourly <- rbindlist(list,  fill=TRUE) %>%
    add_column(x='.00') %>%
    mutate(Hour = paste(Hour, x, sep = "")) %>%
    mutate(date_time = paste(Date, Hour, sep = " ")) %>%
    dplyr::select(-x) %>%
    mutate(date_time = dmy_hm(date_time)) %>%
    relocate(date_time)
  cols <- c(RR = NA_real_, PR = NA_real_, pressure_mbar = NA_real_,
            uv=NA_real_, wind_direction_degree=NA_real_ )
  df.hourly <- add_column(df.hourly, !!!cols[setdiff(names(cols), names(df.hourly))])  %>%
    mutate_if(is.character,as.numeric) %>%
    add_column(id = name.station.deparse) %>%
    dplyr::select(date_time, id, T,  pressure_mbar, RR, U,
                  PR, RG, VT, `wind_direction_degree`) %>%
    dplyr::rename(temperature_c=T, rain_mm = RR, rh_percent = U, par_mol_m2_s = PR,
                  solar_radiation_w_m2 =RG, wind_speed_kmh = VT) %>% 
    distinct(date_time, .keep_all = TRUE) 

  return(df.hourly)
} # Read cimel hourly data only
read.davis <- function (id) {
  
  id.deparse <- id
  
  input <- paste0(dir, "Climate Data/07 DAVIS/", id.deparse,"/for r")
  
  files <- list.files(path = input, pattern = ".txt", full.names = TRUE)
  
  ## Function for changing type of data into integer and Posixt
  df.list <- lapply(files, function(x)read.table(x, header=T, sep="\t",  fill=TRUE))
  
  
  ## Apply Function and bind the rows of the dfs together
  #  df.list <- lapply(df.list, ChangeType)
  df <- rbindlist(df.list, fill=TRUE) %>% 
    mutate(date_time = paste0(X, " ", X.1)) %>% 
    mutate(date_time = dmy_hm(date_time)) %>% 
    drop_na(date_time) 
  
  
  ## Add column if variable does not exist
  cols <- c(Rain = NA_real_, PR = NA_real_, Out = NA_real_,
            uv=NA_real_, Wind...9=NA_real_, In.Air=NA_real_)
  df<-add_column(df, !!!cols[setdiff(names(cols), names(df))])
  
  # Output file
  df <- df %>%
    dplyr::select(date_time, Temp, In.Air, X.3, Out,
                  PR,Solar, UV, Wind, Wind.2)
  
  colnames(df) <- c("date_time", "temperature_c", "pressure_bar", "rain_mm", "rh_percent",
                    "par_mol_m2_s", "solar_radiation_w_m2", "uv",
                    "wind_speed_kmh", "wind_direction_degree")
  
  
  # Convert units
  df<- df %>%
    mutate_if(is.character,as.numeric) %>% 
    mutate(pressure_mbar=pressure_bar*1000) %>% 
    mutate(id= id.deparse) %>% 
    mutate(wind_direction_degree = case_when(wind_direction_degree == "NE" ~ 45,
                                             wind_direction_degree == "SE" ~ 135,
                                             wind_direction_degree == "SW" ~ 225,
                                             wind_direction_degree == "NW" ~ 315,
                                             wind_direction_degree == "N" ~ 0,
                                             wind_direction_degree == "E" ~ 90,
                                             wind_direction_degree == "S" ~ 180,
                                             wind_direction_degree == "W" ~ 270,
                                             wind_direction_degree == "NNE" ~ 22.5,
                                             wind_direction_degree == "ENE" ~ 67.5,
                                             wind_direction_degree == "ESE" ~ 112.5,
                                             wind_direction_degree == "SSE" ~ 157.5,
                                             wind_direction_degree == "SSW" ~ 202.5,
                                             wind_direction_degree == "WSW" ~ 247.5,
                                             wind_direction_degree == "WNW" ~ 292.5,
                                             wind_direction_degree == "NNW" ~ 337.5)) %>% 
    distinct(date_time, .keep_all = TRUE) %>% 
    as.data.frame()
  
  
}
read.davis.cloud <- function (id) {
  
  id <- "LIBE01DVS02418"
  
  id.deparse <- id
  
  input <- paste0(dir, "Climate Data/15 Davis Cloud/", id.deparse,"/for r")
  
 
  files <- list.files(path = input, pattern = ".csv", full.names = TRUE)
  
  ## Function for changing type of data into integer and Posixt
  df.list <- lapply(files, function(x)read.csv(x, header=T,  fill=TRUE, skip = 5))

  ## Apply Function and bind the rows of the dfs together
  #  df.list <- lapply(df.list, ChangeType)
  df <- rbindlist(df.list, fill=TRUE) %>% 
    clean_names() %>% 
    dplyr::select(date_time, temp_c, hum, wind_speed_km_h, wind_direction, solar_rad_w_m_2, 
           rain_mm, barometer_mb) %>% 
    dplyr::rename(temperature_c = temp_c, rh_percent = hum, pressure_mbar= barometer_mb,
           solar_radiation_w_m2 = solar_rad_w_m_2,   wind_speed_kmh = wind_speed_km_h,
           wind_direction_degree = wind_direction) %>% 
    mutate_all(funs(str_replace(., ",", "."))) %>% 
    mutate(date_time = dmy_hm(date_time)) %>% 
    mutate(wind_direction_degree = case_when(wind_direction_degree == "NE" ~ 45,
                                             wind_direction_degree == "SE" ~ 135,
                                             wind_direction_degree == "SW" ~ 225,
                                             wind_direction_degree == "NW" ~ 315,
                                             wind_direction_degree == "N" ~ 0,
                                             wind_direction_degree == "E" ~ 90,
                                             wind_direction_degree == "S" ~ 180,
                                             wind_direction_degree == "W" ~ 270,
                                             wind_direction_degree == "NNE" ~ 22.5,
                                             wind_direction_degree == "ENE" ~ 67.5,
                                             wind_direction_degree == "ESE" ~ 112.5,
                                             wind_direction_degree == "SSE" ~ 157.5,
                                             wind_direction_degree == "SSW" ~ 202.5,
                                             wind_direction_degree == "WSW" ~ 247.5,
                                             wind_direction_degree == "WNW" ~ 292.5,
                                             wind_direction_degree == "NNW" ~ 337.5)) %>% 
    mutate_if(is.character, as.numeric) %>% 
    distinct(date_time, .keep_all = TRUE) %>% 
    mutate(id=id.deparse) %>% 
    as.data.frame()
    
    
  
}
read.hobo <- function (id) {
  
  id.deparse <- id
  input <- paste0(dir,"Climate Data/03 HOBO/", id.deparse,"/for r")
  files <- list.files(path = input, pattern = ".csv", full.names = TRUE)
  
  # Bind the excel and remove the second row
  df.list <- lapply(files, read.csv) # read excel into a list of dfs
  
  ChangeType <- function(df.list){
    df.list[,3] <- as.numeric(unlist(df.list[,3]))
    df.list #return the data.frame 
  }
  
  # df.list <- lapply(df.list, ChangeNames) 
  df.list <- lapply(df.list, ChangeType) 
  df <- bind_rows(df.list, .id = "id") %>% # bind the rows of the dfs together
    dplyr::rename(date_time=contains("Date"), pressure_mbar = contains("Pressure"), 
                  solar_radiation_w_m2 = contains("Solar.radiation"), par_mol_m2_s = contains("PAR"),
                  rain_mm = contains("Rain"), wind_speed_kmh  = contains("Wind.Speed"), 
                  wind_dir_degree  = contains("Wind.Direction"), temperature_c  = contains("Temp"),
                  rh_percent  = contains("RH")) %>% 
    mutate( wind_speed_kmh =  wind_speed_kmh*3.6) %>% 
    mutate(par_mol_m2_s = par_mol_m2_s/1000)%>% 
    dplyr::select(-contains("Gust")) %>% 
    mutate(date_time = dmy_hms(date_time)) %>% 
    mutate(id= id.deparse) %>% 
    distinct(date_time, .keep_all = TRUE) 
  
}
read.hobo.rg <- function (id) {
  
  df <- read_excel(paste0(dir,"Climate Data/11 HOBO Raingauge/",id,"/for r")) %>% 
    mutate(rain_mm = as.numeric(rain_mm)) %>% 
    mutate(date_time = dmy_hm(date_time)) %>% 
    distinct(date_time, .keep_all = TRUE) 
  
}
read.manual.deprecated<- function() {

  
  path1 <- paste0(dir,"Climate Data/00 Station Cuaca Smartri/for r/df.daily.libz.homogenized.imputed_2022_03_31_1.xlsx")
  
  ## Read all of the data
  df.daily.manual.homogenized.1 <- read_excel(path1, 
                               col_types = c("text", "date", "numeric", 
                                             "numeric", "numeric", "numeric", 
                                             "numeric", "numeric", "numeric")) 
  
  
  path2 <- paste0(dir,"Climate Data/00 Station Cuaca Smartri/for r/Data klimatologi harian since 97 (Rev-DAS) - 2022-02-28.xlsx")
  df.daily.manual.homogenized.2 <- read_excel(path2, 
                                                                    col_types = c("date", "numeric", "numeric", 
                                                                                  "numeric", "numeric", "numeric", 
                                                                                  "numeric", "numeric", "numeric", 
                                                                                  "numeric"))
  
  
  gs4_deauth()
  cell_numbers <- function() {
    diff_in_days<- (as.numeric(floor(difftime( Sys.Date(), "2021-01-01",  units = c("days")))) * 4)
  }
  df.daily.manual.gsheet.1 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", skip = 1, sheet = 1,
                                                  range= paste0("A2:W",cell_numbers()), col_types = "c") 
  
  df.daily.manual.gsheet.2 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", sheet = 2, skip = 1, 
                                                    range= paste0("A2:W",cell_numbers()), col_types = "c")
  
  
  # Merging the data 
    
  df.daily.manual.gsheet.2 <- df.daily.manual.gsheet.2  %>% 
    dplyr::select(1:13) %>% 
    drop_na(1) %>% 
    mutate(date= dmy(`Date Entry`)) %>% 
    mutate_if(is.character,as.numeric) %>% 
    arrange(date) %>% 
    distinct(date, .keep_all = TRUE) %>% 
#   mutate(sunshine_hour = sum(2:13)) %>%
    dplyr::select(-1) %>% 
    mutate(sunshine_hour.sum = rowSums(across(where(is.numeric)))) %>% 
    mutate(sunshine_hour.sum = lead(sunshine_hour.sum)) %>% 
    mutate(sunshine_hour.sum = replace(sunshine_hour.sum ,sunshine_hour.sum >12, NA))  %>%
    dplyr::select(13:14) %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .))  %>% 
    mutate(date=as.Date(date))#replace NAN
  
  
  
  df.daily.manual.gsheet <- df.daily.manual.gsheet.1 %>%
    distinct(ID, .keep_all = TRUE) %>% 
    #  filter(rowSums(is.na(df.daily.manual.raw)) != ncol(df.daily.manual.raw)) %>% 
    clean_names() %>% 
    mutate(date_time=paste0(date_entry," ",entry_hour)) %>% 
    mutate(date= dmy_hm(date_time)) %>% 
    arrange(date) %>% 
    relocate(date) %>%
    mutate(wind_direction_degree = case_when(wind_direction == "NE" ~ 45,
                                             wind_direction == "SE" ~ 135,
                                             wind_direction == "SW" ~ 225,
                                             wind_direction == "NW" ~ 315,
                                             wind_direction == "N" ~ 0,
                                             wind_direction == "E" ~ 90,
                                             wind_direction == "S" ~ 180,
                                             wind_direction == "W" ~ 270)) %>% 
    mutate_if(is.character,as.numeric) %>%
    mutate(radiation_cal_cm2_h = 30.18760919*(gun_bellani_p1-gun_bellani_p0)-24.0215024) %>%
    mutate(radiation_cal_cm2_h = lead(radiation_cal_cm2_h)) %>% 
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h<0, NA))  %>%
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h>800, NA))  %>%
    mutate(evaporation_mm =  pan_class_a_p0 - pan_class_a_p1+ rainfall_observed ) %>% 
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm<0, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm>8, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,rainfall_observed>80, NA))  %>%
    mutate(evaporation_mm = lead(evaporation_mm)) %>% 
    mutate(wind_speed_kmh = windrun_meter-lag(windrun_meter, default = first(windrun_meter))) %>% 
    mutate(wind_speed_kmh = replace(wind_speed_kmh,wind_speed_kmh<0, NA))  %>%
    dplyr::rename(temperature_c = dry_bulb, rh_percent=humidity, temperature_c.max =temp_max, 
                  temperature_c.min=temp_min, rain_mm=rainfall_observed, soil_temp_3cm_c = soil_temp_3_cm , 
                  soil_temp_5cm_c = soil_temp_5_cm, soil_temp_10cm_c = soil_temp_10_cm, soil_temp_20cm_c = soil_temp_20_cm,
                  soil_temp_25cm_c = soil_temp_25_cm,
                  temperature_wet_c = wet_bulb) %>% 
    dplyr::select(date, temperature_c.min, temperature_c,temperature_c.max,  rh_percent,
           rain_mm, evaporation_mm, radiation_cal_cm2_h, wind_speed_kmh, wind_direction_degree, soil_temp_3cm_c , soil_temp_5cm_c , 
           soil_temp_10cm_c , soil_temp_20cm_c , soil_temp_25cm_c , temperature_wet_c ) %>% 
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm, na.rm=TRUE),
                      evaporation_mm.mean = mean(evaporation_mm, na.rm=TRUE),
                      radiation_cal_cm2_h.mean = mean(radiation_cal_cm2_h, na.rm=TRUE),
                      wind_speed_kmh.mean = mean (wind_speed_kmh, na.rm=TRUE),
                      soil_temp_3cm_c.mean = mean(soil_temp_3cm_c, na.rm=TRUE), soil_temp_5cm_c.mean = mean(soil_temp_5cm_c, na.rm=TRUE),
                      soil_temp_10cm_c.mean = mean(soil_temp_10cm_c, na.rm=TRUE),
                      soil_temp_20cm_c.mean = mean(soil_temp_20cm_c, na.rm=TRUE), soil_temp_25cm_c.mean = mean(soil_temp_25cm_c, na.rm=TRUE),
                      temperature_wet_c.mean = mean(temperature_wet_c, na.rm=TRUE)) %>%
    mutate(date=as.Date(anytime(date))) %>% 
    drop_na(date) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    pad_by_time() %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate(date=as.Date(date)) %>% 
    left_join(df.daily.manual.gsheet.2, by = "date")
  
  
  # Merge with older homogenized data
  df.daily.manual <- df.daily.manual.homogenized.1 %>% 
    dplyr::select(-1) %>% 
    full_join(df.daily.manual.homogenized.2, by= "date") %>% 
    filter_by_time(date, .end_date = "2021-12-31") %>% 
    bind_rows( df.daily.manual.gsheet ) %>% 
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .))   # replace Inf
  
  
  list.daily.manual.imputed <- df.daily.manual %>%
    miceRanger::miceRanger() 
  
  df.daily.manual.imputed <- completeData(list.daily.manual.imputed) %>%
    extract2(5) %>% 
    mutate(date = anytime(date)) %>% 
    mutate(date = as.Date(date)) 
  
  
} # qc imputation since the begining of recording
read.manual.gsheet.deprecated <- function() {
  
  path1 <- paste0(dir,"Climate Data/00 Station Cuaca Smartri/for r/manual_homogenized_imputed.xlsx")
  
  
  ## Read all of the data
  df.manual.homogenized.imputed <- read_excel(path1, 
                                           col_types = c("text", "date", "numeric", 
                                                         "numeric", "numeric", "numeric", 
                                                         "numeric", "numeric", "numeric", 
                                                         "numeric", "numeric", "numeric", 
                                                         "numeric", "numeric", "numeric", 
                                                         "numeric", "numeric", "numeric", 
                                                         "numeric")) 
    


  
  gs4_deauth()
  cell_numbers <- function() {
    diff_in_days<- as.character(as.numeric(floor(difftime( Sys.Date(), "2021-01-01",  units = c("days")))) * 4)
  }
  

  
  df.daily.manual.gsheet.1 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", skip = 1, sheet = 1,
                                                        range= paste0("A2:W",cell_numbers()), col_types = "c") 
  
  df.daily.manual.gsheet.2 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", sheet = 2, skip = 1, 
                                                        range= paste0("A2:W",cell_numbers()), col_types = "c")
  
  
  # Merging the data 
  df.daily.manual.gsheet.2 <- df.daily.manual.gsheet.2  %>% 
    dplyr::select(1:13) %>% 
    drop_na(1) %>% 
    mutate(date= dmy(`Date Entry`)) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    mutate_if(is.character,as.numeric) %>% 
    #    mutate(sunshine_hour = sum(2:13)) %>%
    dplyr::select(-1) %>% 
    mutate(sunshine_hour.sum = rowSums(across(where(is.numeric)))) %>% 
    mutate(sunshine_hour.sum = lead(sunshine_hour.sum)) %>% 
    mutate(sunshine_hour.sum = replace(sunshine_hour.sum,sunshine_hour.sum>12, NA)) %>% 
    dplyr::select(13:14) %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .))  %>% 
    mutate(date=as.Date(date))#replace NAN
  
  df.daily.manual.gsheet <- df.daily.manual.gsheet.1 %>%
    distinct(ID, .keep_all = TRUE) %>% 
    #  dplyr::filter(rowSums(is.na(df.daily.manual.raw)) != ncol(df.daily.manual.raw)) %>% 
    clean_names() %>% 
    mutate(date_time=paste0(date_entry," ",entry_hour)) %>% 
    mutate(date= dmy_hm(date_time)) %>% 
    arrange(date) %>% 
    relocate(date) %>%
    mutate(wind_direction_degree = case_when(wind_direction == "NE" ~ 45,
                                             wind_direction == "SE" ~ 135,
                                             wind_direction == "SW" ~ 225,
                                             wind_direction == "NW" ~ 315,
                                             wind_direction == "N" ~ 0,
                                             wind_direction == "E" ~ 90,
                                             wind_direction == "S" ~ 180,
                                             wind_direction == "W" ~ 270)) %>% 
    mutate_if(is.character,as.numeric) %>%
    mutate(radiation_cal_cm2_h = 30.18760919*(gun_bellani_p1-gun_bellani_p0)-24.0215024) %>%
    mutate(radiation_cal_cm2_h = lead(radiation_cal_cm2_h)) %>% 
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h<0, NA))  %>%
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h>800, NA))  %>%
    mutate(evaporation_mm =  pan_class_a_p0 - pan_class_a_p1+ rainfall_observed ) %>% 
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm<0, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm>8, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,rainfall_observed>80, NA))  %>%
    mutate(evaporation_mm = lead(evaporation_mm)) %>% 
    mutate(wind_speed_kmh = windrun_meter-lag(windrun_meter, default = first(windrun_meter))) %>% 
    mutate(wind_speed_kmh = replace(wind_speed_kmh,wind_speed_kmh<0, NA))  %>%
    dplyr::rename(temperature_c = dry_bulb, rh_percent=humidity, temperature_c.max =temp_max, 
                  temperature_c.min=temp_min, rain_mm=rainfall_observed, soil_temp_3cm_c = soil_temp_3_cm , 
                  soil_temp_5cm_c = soil_temp_5_cm, soil_temp_10cm_c = soil_temp_10_cm, soil_temp_20cm_c = soil_temp_20_cm,
                  soil_temp_25cm_c = soil_temp_25_cm,
                  temperature_wet_c = wet_bulb) %>% 
    dplyr::select(date, temperature_c.min, temperature_c,temperature_c.max,  rh_percent,
           rain_mm, evaporation_mm, radiation_cal_cm2_h, wind_speed_kmh, wind_direction_degree, soil_temp_3cm_c , soil_temp_5cm_c , 
           soil_temp_10cm_c , soil_temp_20cm_c , soil_temp_25cm_c , temperature_wet_c ) %>% 
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm, na.rm=TRUE),
                      evaporation_mm.mean = mean(evaporation_mm, na.rm=TRUE),
                      radiation_cal_cm2_h.mean = mean(radiation_cal_cm2_h, na.rm=TRUE),
                      wind_speed_kmh.mean = mean (wind_speed_kmh, na.rm=TRUE),
                      soil_temp_3cm_c.mean = mean(soil_temp_3cm_c, na.rm=TRUE), soil_temp_5cm_c.mean = mean(soil_temp_5cm_c, na.rm=TRUE),
                      soil_temp_10cm_c.mean = mean(soil_temp_10cm_c, na.rm=TRUE),
                      soil_temp_20cm_c.mean = mean(soil_temp_20cm_c, na.rm=TRUE), soil_temp_25cm_c.mean = mean(soil_temp_25cm_c, na.rm=TRUE),
                      temperature_wet_c.mean = mean(temperature_wet_c, na.rm=TRUE)) %>%
    mutate(date=as.Date(anytime(date))) %>% 
    left_join(df.daily.manual.gsheet.2, by = "date") %>% 
    drop_na(date) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    pad_by_time() %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    miceRanger::miceRanger() %>% 
    completeData() %>%
    extract2(5) %>% 
    mutate(date = as.Date(date)) 
    
  
  
  # Merge with older homogenized data
  df.daily.manual <- df.manual.homogenized.imputed %>% 
    dplyr::select(-1) %>% 
#   full_join(df.daily.manual, by= "date") %>% 
    filter_by_time(date, .end_date = "2021-12-31") %>% 
    bind_rows(df.daily.manual.gsheet) 
  
} # QC and imputation since Gsheet (2021)
read.manual.gsheet <- function() {
  
  path1 <- paste0(dir,"Climate Data/00 Station Cuaca Smartri/for r/manual_homogenized_imputed.xlsx")
  
  
  ## Read all of the data
  df.manual.homogenized.imputed <- read_excel(path1, 
                                              col_types = c("text", "date", "numeric", 
                                                            "numeric", "numeric", "numeric", 
                                                            "numeric", "numeric", "numeric", 
                                                            "numeric", "numeric", "numeric", 
                                                            "numeric", "numeric", "numeric", 
                                                            "numeric", "numeric", "numeric", 
                                                            "numeric")) 
  
  
  
  
  gs4_deauth()
  cell_numbers <- function() {
    diff_in_days<- as.character(as.numeric(floor(difftime( Sys.Date(), "2021-01-01",  units = c("days")))) * 4)
  }
  
  
  
  df.daily.manual.gsheet.1 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", skip = 1, sheet = 1,
                                                        range= paste0("A2:W",cell_numbers()), col_types = "c") 
  
  df.daily.manual.gsheet.2 <- googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/13Woh2GDGr2xq3-OJaWzM3IEHYeTn0JWZ8886n2DPMVI/edit?usp=sharing", sheet = 2, skip = 1, 
                                                        range= paste0("A2:W",cell_numbers()), col_types = "c")
  
  
  # Merging the data 
  df.daily.manual.gsheet.2 <- df.daily.manual.gsheet.2  %>% 
    dplyr::select(1:13) %>% 
    drop_na(1) %>% 
    mutate(date= dmy(`Date Entry`)) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    mutate_if(is.character,as.numeric) %>% 
    #    mutate(sunshine_hour = sum(2:13)) %>%
    dplyr::select(-1) %>% 
    mutate(sunshine_hour.sum = rowSums(across(where(is.numeric)))) %>% 
    mutate(sunshine_hour.sum = lead(sunshine_hour.sum)) %>% 
    mutate(sunshine_hour.sum = replace(sunshine_hour.sum,sunshine_hour.sum>12, NA)) %>% 
    dplyr::select(13:14) %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .))  %>% 
    mutate(date=as.Date(date))#replace NAN
  
  df.daily.manual.gsheet <- df.daily.manual.gsheet.1 %>%
    distinct(ID, .keep_all = TRUE) %>% 
    #  dplyr::filter(rowSums(is.na(df.daily.manual.raw)) != ncol(df.daily.manual.raw)) %>% 
    clean_names() %>% 
    mutate(date_time=paste0(date_entry," ",entry_hour)) %>% 
    mutate(date= dmy_hm(date_time)) %>% 
    arrange(date) %>% 
    relocate(date) %>%
    mutate(wind_direction_degree = case_when(wind_direction == "NE" ~ 45,
                                             wind_direction == "SE" ~ 135,
                                             wind_direction == "SW" ~ 225,
                                             wind_direction == "NW" ~ 315,
                                             wind_direction == "N" ~ 0,
                                             wind_direction == "E" ~ 90,
                                             wind_direction == "S" ~ 180,
                                             wind_direction == "W" ~ 270)) %>% 
    mutate_if(is.character,as.numeric) %>%
    mutate(radiation_cal_cm2_h = 30.18760919*(gun_bellani_p1-gun_bellani_p0)-24.0215024) %>%
    mutate(radiation_cal_cm2_h = lead(radiation_cal_cm2_h)) %>% 
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h<0, NA))  %>%
    mutate(radiation_cal_cm2_h = replace(radiation_cal_cm2_h,radiation_cal_cm2_h>800, NA))  %>%
    mutate(evaporation_mm =  pan_class_a_p0 - pan_class_a_p1+ rainfall_observed ) %>% 
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm<0, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,evaporation_mm>8, NA))  %>%
    mutate(evaporation_mm = replace(evaporation_mm,rainfall_observed>80, NA))  %>%
    mutate(evaporation_mm = lead(evaporation_mm)) %>% 
    mutate(wind_speed_kmh = windrun_meter-lag(windrun_meter, default = first(windrun_meter))) %>% 
    mutate(wind_speed_kmh = replace(wind_speed_kmh,wind_speed_kmh<0, NA))  %>%
    dplyr::rename(temperature_c = dry_bulb, rh_percent=humidity, temperature_c.max =temp_max, 
                  temperature_c.min=temp_min, rain_mm=rainfall_observed, soil_temp_3cm_c = soil_temp_3_cm , 
                  soil_temp_5cm_c = soil_temp_5_cm, soil_temp_10cm_c = soil_temp_10_cm, soil_temp_20cm_c = soil_temp_20_cm,
                  soil_temp_25cm_c = soil_temp_25_cm,
                  temperature_wet_c = wet_bulb) %>% 
    dplyr::select(date, temperature_c.min, temperature_c,temperature_c.max,  rh_percent,
                  rain_mm, evaporation_mm, radiation_cal_cm2_h, wind_speed_kmh, wind_direction_degree, soil_temp_3cm_c , soil_temp_5cm_c , 
                  soil_temp_10cm_c , soil_temp_20cm_c , soil_temp_25cm_c , temperature_wet_c ) %>% 
    summarise_by_time(date, .by = "day",
                      temperature_c.min= min(temperature_c.min, na.rm=TRUE), temperature_c.mean= mean(temperature_c, na.rm=TRUE) ,temperature_c.max= max(temperature_c.max, na.rm=TRUE),
                      rh_percent.min= min(rh_percent, na.rm=TRUE), rh_percent.mean= mean(rh_percent, na.rm=TRUE), rh_percent.max= max(rh_percent, na.rm=TRUE),
                      rain_mm.sum = sum(rain_mm, na.rm=TRUE),
                      evaporation_mm.mean = mean(evaporation_mm, na.rm=TRUE),
                      radiation_cal_cm2_h.mean = mean(radiation_cal_cm2_h, na.rm=TRUE),
                      wind_speed_kmh.mean = mean (wind_speed_kmh, na.rm=TRUE),
                      soil_temp_3cm_c.mean = mean(soil_temp_3cm_c, na.rm=TRUE), soil_temp_5cm_c.mean = mean(soil_temp_5cm_c, na.rm=TRUE),
                      soil_temp_10cm_c.mean = mean(soil_temp_10cm_c, na.rm=TRUE),
                      soil_temp_20cm_c.mean = mean(soil_temp_20cm_c, na.rm=TRUE), soil_temp_25cm_c.mean = mean(soil_temp_25cm_c, na.rm=TRUE),
                      temperature_wet_c.mean = mean(temperature_wet_c, na.rm=TRUE)) %>%
    mutate(date=as.Date(anytime(date))) %>% 
    left_join(df.daily.manual.gsheet.2, by = "date") %>% 
    drop_na(date) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    pad_by_time() %>% 
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>%  # replace Inf
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    miceRanger::miceRanger() %>% 
    completeData() %>%
    extract2(5) %>% 
    mutate(date = as.Date(date)) 
  
  
  
  # Merge with older homogenized data
  df.daily.manual <- df.manual.homogenized.imputed %>% 
    dplyr::select(-1) %>% 
    #   full_join(df.daily.manual, by= "date") %>% 
    filter_by_time(date, .end_date = "2021-12-31") %>% 
    bind_rows(df.daily.manual.gsheet) 
  
} # QC and imputation since Gsheet (2021)
read.meteostat.hourly <- function() {
  read.csv.gz <- function(file)  {
    dat <- fread(file)
    dat$fname <- file
    dat$fname <- stri_sub(dat$fname,-12,-8)
    return(dat)
  } # read data
  

  
  pth <- paste0(dir, "Climate Data/09 BMKG/hourly")
  list <-lapply(list.files(path =pth,
                           pattern = ".csv.gz", full.names = TRUE),read.csv.gz)
  
  
  
  df <- rbindlist (list,  fill=TRUE) 
  
  df <- df %>% 
  dplyr::select(V1, V2, V3, V4, V5,V8, V9, V11, fname) %>% 
    dplyr::rename(date = V1, time =V2, temperature_c =V3, dew_temperature_c =V4, rh_percent= V5, 
                  pressure_mbar = V11, id = fname, wind_speed_kmh = V9, wind_direction_degree=V8) %>% 
    mutate(date_time = paste(date, time, sep = " ")) %>%
    mutate(date_time = paste(date_time, ":00", sep="")) %>% 
    mutate(date_time = ymd_hm(date_time)) %>% 
    dplyr::select(-date, -time) %>% 
    relocate(date_time) %>% 
    ungroup() 
  
  
  return(df)
  
} # hourly data # Get the data first  from python visual studio
read.meteostat.daily.deprecated <- function() {
  read.csv.gz <- function(file)  {
    dat <- fread(file)
    dat$fname <- file
    dat$fname <- stri_sub(dat$fname,-12,-8)
    return(dat)
  } # read data
  
  
  pth <- paste0(dir, "Climate Data/09 BMKG/daily")
  list <-lapply(list.files(path =pth,
                           pattern = ".csv.gz", full.names = TRUE),read.csv.gz)
  
  df <- rbindlist (list, fill=TRUE) 
  
  
  
  return(df)
  
} # Daily Data # Get the data first  from python visual studio
read.meteostat.deprecated <- function () {

  
  df_meteostat_hourly <- read.meteostat.hourly() 
  df_meteostat_daily <-read.meteostat.daily() 
  
  df_meteostat_hourly <- df_meteostat_hourly %>% 
    dplyr::select(V1, V2, V3, V4, V5,V8, V9, V11, fname) %>% 
    dplyr::rename(date = V1, time =V2, temperature_c =V3, dew_temperature_c =V4, rh_percent= V5, 
                  pressure_mbar = V11, id = fname, wind_speed_kmh = V9, wind_direction_degree=V8) %>% 
    mutate(date_time = paste(date, time, sep = " ")) %>%
    mutate(date_time = paste(date_time, ":00", sep="")) %>% 
    mutate(date_time = ymd_hm(date_time)) %>% 
    dplyr::select(-date, -time) %>% 
    relocate(date_time) %>% 
    agg.daily.hourly() %>% 
    group_by(id) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    ungroup() 
  
  df_meteostat<- read.daily.meteostat() %>% 
    dplyr::select(V1, V5, fname) %>% 
    dplyr::rename(id=fname, date=V1, rain_mm.sum = V5) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    mutate(date=ymd(date)) %>% 
    right_join(df_meteostat_hourly, by=c("id", "date")) 
  
} # combine hourly temperature|RH and daily rain
read.ombro.all <- function () { 
  
  pth1 <- paste0(dir, "Climate Data/02 SAP/for r")
  
  pth3 <- paste0(dir, "Climate Data/02 SAP/estate_code.xlsx")
  
  ## SAP Format
  files <- list.files(path = pth1, pattern = ".txt", full.names = TRUE)
  
  estate_code <- read_excel(pth3) %>% 
    select(-Estate, -Position_Record) %>% 
    mutate(id = coalesce(id,cost_ctr))
  
  
  ## Function for changing type of data into integer and Posixt
  df.list <- lapply(files, function(x) read.delim2(x,sep="\t", header=T, na.strings=c("","NA")))
  
  ## Apply Function and bind the rows of the dfs together
  df_sap <- rbindlist(df.list) %>% 
    select(where(~sum(!is.na(.x)) > 0)) %>% 
    clean_names() %>%
    select(-contains("x")) %>% 
    dplyr::select(1,2,3) %>%
    na.omit() %>% 
    dplyr::rename(date = postg_date, rain_mm.sum = rainfall_quantity) %>% 
    left_join(estate_code, by="cost_ctr") %>% 
    select(-cost_ctr) %>% 
    mutate(rain_mm.sum = str_replace(rain_mm.sum, ",", ".")) %>% 
    mutate(rain_mm.sum = as.numeric(rain_mm.sum)) %>%
    mutate(date=dmy(date)) %>% 
    group_by(id, date) %>%
    summarise(rain_mm.sum = sum(rain_mm.sum)) %>% 
    ungroup() %>% 
    group_by(id) %>%
    pad_by_time() %>%
    mutate_all(~ifelse(is.na(.), 0, .)) %>% #replace NA
    ungroup() %>% 
    relocate(id) %>% 
    mutate(date=as.Date(date)) 
  
  
  df <- read.table(paste0(dir, "Climate Data/02 SAP/sap_ombro_2006_2021.txt"), header = TRUE) %>% 
    mutate(date = ymd(date)) %>% 
    rbind(df_sap) 
  
  
  df <- df %>% 
    group_by(id) %>% 
    pad_by_time(date) %>% 
    #filter(id=="ADPE-Division 3") %>%
    #mutate(new = replace_na(new, 0)) %>% 
    mutate(new=cumsum(replace_na(rain_mm.sum, 0))) %>% 
    #mutate(new = cumsum(coalesce(rain_mm.sum, 0)) + rain_mm.sum*0) %>% 
    mutate(new = case_when(new == 0 ~ 1,
                            new > 0 ~ new)) %>% 
    group_by(grp = rleid(rain_mm.sum)) %>%
    mutate(rain_mm.sum  = if(n() >90 & all(rain_mm.sum == 0 | is.na(rain_mm.sum)) & all(new > 0)) NA else rain_mm.sum) %>%
    ungroup() %>%
    select(-grp, -new)
  

  
  
}
read.ombro.all.monthly <- function () { 
  
  pth1 <- paste0(dir, "Climate Data/02 SAP/for r")
  pth3 <- paste0(dir, "Climate Data/02 SAP/estate_code.xlsx")
  pth4 <- paste0(dir, "Climate Data/02 SAP/CH_HH_BULANAN_from_90s")
  
  ## SAP Format
  files <- list.files(path = pth1, pattern = ".txt", full.names = TRUE)
  
  files4 <- list.files(path = pth4, pattern = ".xlsx", full.names = TRUE)
  
  estate_code <- read_excel(pth3) %>% 
    select(-Estate, -Position_Record) %>% 
    mutate(id = coalesce(id,cost_ctr))
  
  ## Function for changing type of data into integer and Posixt
  df.list4 <- lapply(files4, function(x) read_excel(x)) %>% 
    rbindlist() %>% 
    rename(rain_mm.sum = rain_mm_sum) %>% 
    mutate(date=ym(date), rain_mm.sum=as.numeric(rain_mm.sum), raindays=as.numeric(raindays))
  
  
  ## Function for changing type of data into integer and Posixt
  df.list <- lapply(files, function(x) read.delim2(x,sep="\t", header=T, na.strings=c("","NA")))
  
  ## Apply Function and bind the rows of the dfs together
  df_sap <- rbindlist(df.list) %>% 
    select(where(~sum(!is.na(.x)) > 0)) %>% 
    clean_names() %>%
    select(-contains("x")) %>% 
    dplyr::select(1,2,3) %>%
    na.omit() %>% 
    dplyr::rename(date = postg_date, rain_mm.sum = rainfall_quantity) %>% 
    left_join(estate_code, by="cost_ctr") %>% 
    select(-cost_ctr) %>% 
    mutate(rain_mm.sum = str_replace(rain_mm.sum, ",", ".")) %>% 
    mutate(rain_mm.sum = as.numeric(rain_mm.sum)) %>%
    mutate(date=dmy(date)) %>% 
    group_by(id, date) %>%
    summarise(rain_mm.sum = sum(rain_mm.sum)) %>% 
    ungroup() %>% 
    group_by(id) %>%
    pad_by_time() %>%
    mutate_all(~ifelse(is.na(.), 0, .)) %>% #replace NA
    ungroup() %>% 
    relocate(id) %>% 
    mutate(date=as.Date(date)) 
  
  
  df <- read.table(paste0(dir, "Climate Data/02 SAP/sap_ombro_2006_2021.txt"), header = TRUE) %>% 
    mutate(date = ymd(date)) %>% 
    rbind(df_sap) 
  
  
  df <- df %>% 
    group_by(id) %>% 
    pad_by_time(date) %>% 
    #filter(id=="ADPE-Division 3") %>%
    #mutate(new = replace_na(new, 0)) %>% 
    mutate(new=cumsum(replace_na(rain_mm.sum, 0))) %>% 
    #mutate(new = cumsum(coalesce(rain_mm.sum, 0)) + rain_mm.sum*0) %>% 
    mutate(new = case_when(new == 0 ~ 1,
                           new > 0 ~ new)) %>% 
    group_by(grp = rleid(rain_mm.sum)) %>%
    mutate(rain_mm.sum  = if(n() >90 & all(rain_mm.sum == 0 | is.na(rain_mm.sum)) & all(new > 0)) NA else rain_mm.sum) %>%
    ungroup() %>%
    select(-grp, -new)
  
  
  df <- df %>% 
    group_by(id) %>% 
    mutate(raindays = ifelse(rain_mm.sum > 0, 1, 0)) %>%
    mutate (date = as.Date(date)) %>%
    summarise_by_time(date, .by = "month", id=id,
                      rain_mm.sum = sum(rain_mm.sum),
                      raindays=sum(raindays)) %>%
    dplyr::select(where(~sum(!is.na(.x)) > 0)) %>%  #delete column that contain only NA
    mutate_all(~ifelse(is.nan(.), NA, .)) %>% #replace NAN
    mutate_all(~ifelse(is.infinite(.), NA, .)) %>% # replace Inf
    distinct() %>% 
    ungroup()
  
  df <- df.list4 %>% 
    bind_rows(df) %>% 
    group_by(id) %>% 
    distinct(date, .keep_all = TRUE) %>% 
    ungroup()
    
  df <- df %>% 
    group_by(id) %>% 
    pad_by_time(date) %>% 
    #filter(id=="ADPE-Division 3") %>%
    #mutate(new = replace_na(new, 0)) %>% 
    mutate(new=cumsum(replace_na(rain_mm.sum, 0))) %>% 
    #mutate(new = cumsum(coalesce(rain_mm.sum, 0)) + rain_mm.sum*0) %>% 
    mutate(new = case_when(new == 0 ~ 1,
                           new > 0 ~ new)) %>% 
    group_by(grp = rleid(rain_mm.sum)) %>%
    mutate(rain_mm.sum  = if(n() >6 & all(rain_mm.sum == 0 | is.na(rain_mm.sum)) & all(new > 0)) NA else rain_mm.sum) %>%
    ungroup() %>%
    select(-grp, -new)
  
  df <- df %>% 
    mutate (etp = ifelse(raindays >10, 120, 150)) %>%
    mutate(swr=200) %>%
    mutate(wd=0) %>% 
    group_by(id) 

  df <- df %>%
    group_by(id) %>% 
    mutate (etp = ifelse(raindays >10, 120, 150)) %>%
    mutate(swr=200) %>%
    mutate(wd=0) %>% 
    group_by(id) %>% 
    drop_na(rain_mm.sum)
  
  mylist <- split(df, df$id)

  wd <- function(df) {
    
  for (i in (2:nrow(df))) {
    df$swr[i] <- 200
    df$wd[i] <- 0
    df$swr[i] <- df$swr[i-1] + (df$rain_mm.sum[i]) - (df$etp[i])
    df$swr[i][df$swr[i] > 200] <- 200
    df$swr[i][df$swr[i] < 0] <- 0
    
    df$wd[i] <-  (df$etp[i]) - df$swr[i-1] - (df$rain_mm.sum[i])
    df$wd[i][df$wd[i] < 0] <- 0
  }
  
    return(df)
    
  }
  
  df_wd <- lapply(mylist, possibly(wd, NA))
  
  df <- rbindlist(df_wd)

    
  df <- df %>%
    group_by(id) %>% 
    mutate(drainage_mm= lag(swr)+rain_mm.sum-etp-200) %>%
    dplyr::rename(wd_mm =wd, etp_mm =etp, swr_mm=swr) %>%
    mutate(drainage_mm=replace(drainage_mm, drainage_mm<0, 0)) %>%
    dplyr::select(-etp_mm ,-swr_mm) %>% 
    ungroup() %>% 
    dplyr::select(where(~sum(!is.na(.x)) > 0))  #delete column that contain only NA
  
  
  
  
} #from database that begins in early 1990's
read.mysql <- function(df) {
  
  df.deparse <- deparse(substitute(df))
  
  df <- con %>%
    dplyr::tbl(df.deparse) %>%
    dplyr::collect() %>% 
    as.data.frame() %>% 
    mutate(date=ymd(date)) %>% 
    select(-contains("row_names"))
  
}
write.db.bulk <- function(con, name, x, append = TRUE, chunkSize = 10000, verbose = TRUE, ...) {
  
  n = nrow(x)
  i.to.n = 1:n
  ii = split(i.to.n, ceiling(seq_along(i.to.n)/chunkSize) )
  
  o = vector(length = length(ii))
  
  if(verbose) pb = txtProgressBar(max = length(ii), style = 3)
  for(i in 1:length(ii) )   {
    
    z = x[ (ii[[i]]) ]
    
    o[i] = RMariaDB::dbWriteTable(conn = con, name = name, value = z, append = TRUE, row.names = FALSE, ...)
    if(verbose) setTxtProgressBar(pb, i)
    
  }
  
  all(o)
  
}
write.all.ombro <- function(df_ombro) {
  
  write.all <-  function(df) {
    write_xlsx(df,paste0("Monthly_rain_",unique(df$ID),".xlsx"))
    return(df)
  }
  
  tes <- df_ombro %>% 
    group_by(id) %>% 
    agg.monthly.daily.ombro() 
  
  tes %>% 
    ungroup() %>% 
    mutate( `Monthly Rain (mm)` = rain_mm.sum, `Raindays (days)` = raindays, 
            `Monthly ETP (mm)` = etp_mm, `Monthly SWR (mm)` = swr_mm, 
            `Monthly wd (mm)` = wd_mm , `Monthly Drainage (mm)` = drainage_mm) %>% 
    mutate(Month=lubridate::month(date, label = TRUE)) %>% 
    mutate(Year= year(date)) %>% 
    rename(ID=id, Date=date) %>% 
    dplyr::select(ID, Year,Month, Date, `Monthly Rain (mm)`, `Raindays (days)`, `Monthly wd (mm)`, 
           `Monthly ETP (mm)`, `Monthly SWR (mm)`, `Monthly Drainage (mm)`) %>% 
    group_by(ID) %>% 
    do(write.all(.))
} # export excel files to be stored in shared folder
write.climpact <-  function(df) {
  
  id_aws <- as.character (head(df$id, 1))  
  
  min_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 1) %>% 
    dplyr::filter(day == 1) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()
  
  max_year <- df %>% 
    mutate(month = month(date), year = year(date), day= day(date)) %>% 
    dplyr::filter(month == 12) %>% 
    dplyr::filter(day == 31) %>% 
    arrange(desc(year)) %>% 
    slice_head(n=1) %>% 
    dplyr::select(year) %>% 
    as.character()  
  
  df %>% 
    NaRV.omit() %>%
    pad_by_time(date) %>%
    mutate(year=year(date), month=month(date), day=day(date)) %>%
    replace(is.na(.), -99.9) %>%
    dplyr::select(year, month, day, rain_mm.sum, temperature_c.max, temperature_c.min) %>% 
    write.table(file=paste0(id_aws,'_climpact.txt'), row.names = FALSE,
                col.names = FALSE  ,sep="\t")
  
  sts_clim1005 %>% 
    dplyr::filter(id==id_aws) %>% 
    write.table(file=paste0(id_aws,'sts_climpact.txt'), row.names = FALSE,
                col.names = FALSE  ,sep="\t")
  
  
  
}
write.ombro <- function(id, .start_date, .end_date) {
  
  no.id <- as.character(id)
  
  df <- daily_ombro %>% 
    dplyr::filter(grepl(no.id, id)) %>% 
    filter_by_time(.start_date = .start_date) %>% 
    bind_rows(df) %>% 
    group_by(id) %>% 
    agg.weekly.daily() %>%
    reshape2::melt(id=c("id", "date")) %>% 
    reshape::cast(date~ id+variable) %>% 
    mutate(week=week(date)) %>% 
    mutate(month=month(date)) %>% 
    relocate(date,month, week) %>% 
    write_xlsx("no.id .xlsx")
  
  
  
}

# Sample ->  plot.climpact.ts.ann("www/output/df.daily.libz.homogenized.imputed_climpact/indices/df.daily.libz.homogenized.imputed_climpact_tn90p_ANN.csv",tn90p, Percent)
#  .. . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .


# Station Information                                                   ---------------------------------------------------

read_sts_clim1005 <- function() {
  
  files <- list.files(path = paste0(dir,"Climate Data/13 Information/sts_clim1005"), 
                      pattern = ".xlsx",
                      full.names = TRUE) %>% 
    lapply(function(x)read_excel(x, skip = 8)) %>%    
    rbindlist() %>% 
    drop_na(id) %>% 
    clean_names() %>% 
    dplyr::select(id, lon, lat, alt, region) %>% 
    mutate(lon=as.numeric(lon), lat=as.numeric(lat))
  
}
sts_clim1005 <- read_sts_clim1005()
read_sts_clim1005_map <- function() {
  
  files <- list.files(path = paste0(dir,"Climate Data/13 Information/sts_clim1005"), 
                      pattern = ".xlsx",
                      full.names = TRUE) %>% 
    lapply(function(x)read_excel(x, skip = 8)) %>%    
    rbindlist() %>% 
    drop_na(id) %>% 
    dplyr::rename(region = Region) %>% 
    dplyr::select(id, lon, lat, alt, region, `AWS Status`, `AWS Type`, `Manufacturer`) %>% 
    mutate(estate=id) %>% 
    mutate(lon=as.numeric(lon), lat=as.numeric(lat))

  
  
}
sts_clim1005_map <- read_sts_clim1005_map()

sts_bmkg <- read_csv(paste0(dir,"Climate Data/13 Information/sts_bmkg/sts_bmkg.csv")) %>% 
 mutate(id = as.character(id))# Or you can use the following figure to read the data
#sts_bmkg <-  stations_ogimet(country = "Indonesia", add_map = TRUE) %>% rename(id=wmo_id) %>% dplyr::dplyr::select(id, lon, lat, alt)

sts_ombro <-  read_excel(paste0(dir, "Climate Data/13 Information/sts_ombro/Kode Kebun Komersial.xlsx")) 

sts <- sts_clim1005 %>% 
  bind_rows(sts_bmkg) %>% 
  bind_rows(sts_ombro)


# Station Maps                                                          ----------------------------------------------------------- 

library(RgoogleMaps)

pal <- colorFactor(palette = 'Set1', domain = sts_clim1005_map$`AWS Status`)
pal2 <- colorFactor(palette = 'Dark2', domain = sts_clim1005_map$Manufacturer)

leaflet(sts_clim1005_map, options = leafletOptions(zoomControl = FALSE)) %>% 
  htmlwidgets::onRender("function(el, x) {L.control.zoom({ position: 'bottomleft' }).addTo(this)}") %>% 
  addTiles(group = "Open Street Map") %>% 
  addProviderTiles("Esri.WorldImagery", group = "Esri World Imagery") %>% 
  addScaleBar(position = "bottomleft") %>%
  addCircleMarkers(lng = ~lon, lat = ~lat, radius=4, label=~id,  layerId = ~id, color=~pal(`AWS Status`), group = "AWS Status")%>%
  addCircleMarkers(lng = sts_bmkg$lon, lat = sts_bmkg$lat, radius=1, label=sts_bmkg$id,  layerId = sts_bmkg$id, group = sts_bmkg$id) %>% 
 # addCircleMarkers(lng = ~lon, lat = ~lat, radius=4, label=~id,  color=~pal2(`Manufacturer`), group = "Manufacturer") %>%
  leaflet::addLegend(pal = pal, values = ~`AWS Status`,position = "topright",  group = "AWS Status") %>% 
#  leaflet::addLegend(pal = pal2, values = ~Manufacturer,position = "topright",  group = "Manufacturer") %>% 
  addLayersControl(overlayGroups = c("AWS Status", "Manufacturer"), baseGroups = c("Open StreetMap", "Esri World Imagery"), 
                   options = layersControlOptions(collapsed = TRUE), position = "bottomright") %>% 
  addMeasure(primaryLengthUnit = "meters",
             primaryAreaUnit = "sqmeters",
             completedColor = "#ff0000",
             activeColor = "#ad3b3b", position = "bottomleft")

rm(pal, pal2)
 
# General Data                                                          ------------------------------------
# df_meteostat <- read.meteostat()
# df_ombro <- read.ombro.all()
# df_manual <- read.manual.gsheet()
# MySQL Connection                                                      --------------------------------------------------------
#killDbConnections() # to kill all dbconnecion
con <- dbConnect(RMySQL::MySQL(),user = user.name,password = pass, dbname = 'climdata',host = 'smartri1005.mysql.database.azure.com')
#con <- dbConnect(RMySQL::MySQL(), user = 'adisapoetro', password = 'gedungbaru09!', dbname = 'climate_data', host = 'climatedata.mysql.database.azure.com')

dbListTables(con)

# dbWriteTable(conn = con, name = "df_aws",  value = df_aws, overwrite = TRUE)  ## x is any data frame
# dbWriteTable(conn = con, name = "df_ombro_aws",  value = df_ombro_aws, overwrite = TRUE)  ## x is any data frame
# dbWriteTable(conn = con, name = "df_meteostat",  value = df_meteostat, overwrite = TRUE)  ## x is any data frame
# dbWriteTable(conn = con, name = "df_ombro",  value = df_ombro, overwrite = TRUE)  ## x is any data frame
# dbWriteTable(conn = con, name = "df_manual",  value = df_manual, overwrite = TRUE)  ## x is any data frame

########################################################################


