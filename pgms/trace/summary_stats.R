#-------------------------
# Calculate summary stats
#------------------------

rm(list=ls())

setwd("~/WRDS_EBP/")

library(data.table)
library(zoo)

#-----------------------
# Read the trace sample
#-----------------------

## read the sample data
df = readRDS("./data/trace_enhanced_rf_spreads.rds")

## recalculate variables
df[,maturity_orig := as.numeric((maturity_date - issue_date))/365]
df[,maturity_remaining := as.numeric((maturity_date - trd_exctn_dt))/365]
df[,offering_amt := offering_amt/1E3]
df[,credit_spread := rf_spread_recalc/100]

## assign rating rankings
df[,rating := ifelse(rating == "AA-/A-1+", "AA-", rating)]
df[,rating_rank := NA]

ratings = c("AAA+", "AAA", "AAA-",
	    "AA+", "AA", "AA-",
	    "A+", "A", "A-",
	    "BBB+", "BBB", "BBB-",
	    "BB+", "BB", "BB-",
	    "B+", "B", "B-",
	    "CCC+", "CCC", "CCC-",
	    "CC+", "CC", "CC-",
	    "C+", "C", "C-",
	    "D")

rank = length(ratings)
for (i in ratings) { 
  print(paste(rank, i))
  df[,rating_rank := ifelse(rating == i, rank, rating_rank)]
  rank = rank - 1
}

#-----------------------
# Summary stats function 
#------------------------

sum_stats <- function(df, x) {
     
     r = df[,list(var = x,
		  mean = mean(get(x), na.rm=T),
                  sd = sd(get(x), na.rm=T),
   	          min = min(get(x), na.rm=T),
	          P25 = quantile(get(x), 0.25, na.rm=T),
	          median = quantile(get(x), 0.50, na.rm=T),
	          P75 = quantile(get(x), 0.75, na.rm=T),
	          max = max(get(x), na.rm=T))]

     return(r)
}

#--------------------
# Bonds per firm/day
#--------------------

## check one bond trade per day
any(duplicated(df[,list(cusip_id, trd_exctn_dt)]))

## count bonds per firm
bonds_firm_day = df[,list(bonds_firm_day = .N),by=list(cusip6, trd_exctn_dt)]

## trades per firm day
bonds_firm_day = sum_stats(bonds_firm_day,"bonds_firm_day")


#-------------------
# Get summary stats
#-------------------
t = list()
for (i in c("maturity_orig", "maturity_remaining", "coupon", "credit_spread", "offering_amt", "rating_rank")) {
   temp = sum_stats(df, i)
   #temp[,var := i]
   t[[i]] = temp
}

## add bonds per firm day
t[["bonds_firm_day"]] = bonds_firm_day

t = setDT(do.call(rbind.data.frame, t))

## round numbers
t = t[,lapply(.SD, round, 2), .SDcols=c('mean', 'sd', 'min', 'P25', 'median', 'P75', 'max'), by=list(var)]

## display table
print(t)

#--------
# Counts
#--------

bonds = length(unique(df$cusip_id))
firms = length(unique(df$cusip6))
obs = nrow(df)

print(paste("Total trades:", obs))
print(paste("Number of bonds:", bonds))
print(paste("Number of firms:", firms))



