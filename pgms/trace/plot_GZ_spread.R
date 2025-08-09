#----------------------------
# Plot the GZ spread from 
#   1) Board
#   2) Estimates
#----------------------------

rm(list = ls())


setwd("~/WRDS_EBP/data")

library(data.table)
library(zoo)
library(readxl)

#--------------------
# Read the Board EBP
#--------------------
EBP = fread("EBP.csv", sep=",", stringsAsFactors=F)
EBP[,date := as.Date(date)]

#---------------------------
# Read the Atlanta Fed data
#---------------------------
EBP_atl = read_xlsx("EBP_atl.xlsx", sheet="spr")
setDT(EBP_atl)
EBP_atl[,date := as.Date(date)]

#-------------------
# Get the estimates
#-------------------
df = readRDS("trace_enhanced_rf_spreads.rds")

df = df[,list(trades = .N,
	      GZ_trace = mean(rf_spread_recalc, na.rm=T)/100), by=list(date = trd_exctn_dt)]

#------------
# merge data
#------------

df = merge(df, EBP_atl, by=("date"), all.x=T)
df = merge(EBP[,list(date,gz_spread)], df, by.y=c("trd_exctn_dt"), by.x=c("date"))

#---------------
# Plot the data
#---------------
plot(df$date, df$spr_agg, type="l", lwd=2.25, lty=1, col="black")
lines(df$date, df$GZ_trace, lwd=2.25, lty=2, col="red")
lines(EBP$date, EBP$gz_spread, lwd=2.25, lty=3, col="blue")
legend(as.Date('2012-03-31'), 6, legend=c("GZ Spread", "Atl GZ Spread", "Trace Spread"), col=c("blue", "black", "red"), bty="n", lty=1, lwd=2.25)


