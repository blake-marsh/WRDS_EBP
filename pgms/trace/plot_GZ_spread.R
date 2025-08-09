#----------------------------
# Plot the GZ spread from 
#   1) Board
#   2) Estimates
#----------------------------

rm(list = ls())


setwd("~/WRDS_EBP/data")

library(data.table)
library(zoo)

#--------------------
# Read the Board EBP
#--------------------
EBP = fread("EBP.csv", sep=",", stringsAsFactors=F)

#-------------------
# Get the estimates
#-------------------
df = readRDS("trace_enhanced_rf_spreads.rds")

df = df[,list(GZ_trace = mean(rf_spread_recalc, na.rm=T)/100), by=list(trd_exctn_dt)]

#------------
# merge data
#------------

df = merge(EBP[,list(date,gz_spread)], df, by.y=c("trd_exctn_dt"), by.x=c("date"))

#---------------
# Plot the data
#---------------
plot(df$date, df$gz_spread, type="l", lwd=2.25, lty=1, col="black")
lines(df$date, df$GZ_trace, lwd=2.25, lty=2, col="red")
legend(as.Date('2012-03-31'), 6, legend=c("GZ Spread", "Trace Spread"), col=c("black", "red"), bty="n", lty=1, lwd=2.25)


