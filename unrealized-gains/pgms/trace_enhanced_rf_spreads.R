rm(list = ls())

library(data.table)
library(zoo)
library(lubridate)
library(RPostgres)
library(stats)
library(parallel)
setwd("~/WRDS_EBP/unrealized-gains")

#----------------
# Parallel setup
#----------------
cores = detectCores()
#cores = min(8, cores)
print(paste("Cores:", cores))

#-------------------------
# FUNCTION: coupon dates
#-------------------------
cpn_dates <- function(issue_date, maturity_date, trade_date=NA, interest_frequency=NA, rtn="pmts"){
     
  ## Figure out the months between coupons

  if (is.na(interest_frequency)) { 
    interest_frequency = 1
  }

   cpn_freq_months = 12/interest_frequency
  
   ## vector to store the dates
   coupon_dates = c()

   ## generate payment dates
   count = 0
   repeat {
      temp = maturity_date %m-% months(cpn_freq_months*count)
      if (temp > issue_date) {
         coupon_dates = c(coupon_dates,temp)
         count = count + 1
      } else {
         break }
   }   

  ## get dates since trade dates
      coupon_dates = coupon_dates[which(coupon_dates >= trade_date)]
 
  ## sort dates
  coupon_dates = sort(as.Date(coupon_dates))
  
  ## number of payments remaining
  pmts = length(coupon_dates)
  
  ## setup return
  if (rtn == "pmts") {
     r = pmts 
  } else if (rtn == "coupon_dates") {
	  r = coupon_dates
  } else if (rtn == "all") {
     r = list('coupon_dates'=coupon_dates, 'pmts'=pmts) 
  }
 
  return(r)
}

#---------------------------------------
# FUNCTION: Present value calcuation of risk-free
# using market rates
#----------------------------------------
PV_calc <- function(trade_date, issue_date, maturity_date, coupon, interest_frequency, method="continuous") {
    if (is.na(trade_date) | is.na(issue_date) | is.na(maturity_date) | is.na(coupon)) return (NA)
    h15_trade = h15[which(date == trade_date),]
    if (nrow(h15_trade) == 0) return (NA)
    

    ## get coupon payment 
    if (is.na(interest_frequency)) {
         
	coupon_pmt = 0
	interest_frequency = 1

    } else {

       coupon_pmt = 100*(coupon/100)/interest_frequency
    }
    
    ## determine coupon dates
    d = cpn_dates(issue_date, maturity_date, trade_date=trade_date, interest_frequency=interest_frequency, rtn="coupon_dates") 
    if (length(d) == 0 || is.null(d)) return (NA)
    
    # number of days to each coupon payment
    # from trade date
    cpn_days = as.numeric(d - trade_date)

    # Calculate present value of coupons
    #  loop over days to coupon payment
    #    1) Get interpolated treasury rate
    #    2) discount the coupon payment
    h15_trade = h15[which(date == trade_date),]
    h15_pts = sort(unique(h15_trade$days))
    nper = 0
    PV = 0
 
    for (i in cpn_days) {
        nper = nper + 1

        ## get the treasury yield
        l = sort(h15_pts[which(h15_pts <= i)], decreasing=T)[1]
        u = h15_pts[which(h15_pts >= i)][1]
        a = as.numeric(h15_trade[which(days == l),list(yield)])
        b = as.numeric(h15_trade[which(days == u),list(yield)])
        step = ifelse(u-l != 0, (b-a)/(u-l), 0)
        r = a + step*(i -l)

        ## coupon present-value
        if (method == "discrete") {
            coupon_PV = coupon_pmt/((1+r/interest_frequency)^nper)
        } else if (method == "continuous") {
            coupon_PV = coupon_pmt/exp((r/interest_frequency)*nper)
        } 
        PV = PV + coupon_PV
    }

    ## This assumes the last coupon 
    ## is paid on the maturity date
    if (method == "discrete") {
       PV = PV + (100/(1+r/interest_frequency)^nper)
    } else if (method == "continuous") {
       PV = PV + (100/(exp((r/interest_frequency)*nper)))
    }

    ## price as percent of face value
    price = PV/100*100
    
    #return(list('PV' = PV, 'price' = price))
    return(as.numeric(price))
}

#----------------------------------------------
# FUNCTION: solve for YTM
#    for zero coupon pmts = years to maturity
#----------------------------------------------
ytm_func <- function(r, PV, nper, coupon, interest_frequency, method="continuous") {

  if (is.na(coupon) | coupon == 0){
     if (method == "discrete") {
         price = 100/(1 + r)^nper
     } else if (method == "continuous") {
         price = 100/exp(r*nper)    
     }

  } else {

     c = 100*(coupon/100)/interest_frequency 
     if (method == "discrete") {
         price = 100/(1+r/interest_frequency)^nper
         for (i in seq(nper)){
           coupon_PV = c/((1+r/interest_frequency)^i)
           price = price + coupon_PV
         }
     } else if (method == "continuous") {
         price = 100/exp((r/interest_frequency)*nper)
         for (i in seq(nper)){
           coupon_PV = c/exp((r/interest_frequency)*i)
           price = price + coupon_PV
         }
      }
  }
  diff = PV - price
  return(diff)
}

duration_macaulay <- function(maturity, coupon_rate, ytm, principal, freq) {
	if (is.na(ytm) || ytm <= 0 || freq <= 0 || is.na(maturity) || is.na(coupon_rate)) return (NA)
	n <- maturity * freq
	t <- seq(1, n) / freq
	c <- principal * coupon_rate / freq
	
	cashflows <- rep(c, n)
	cashflows[n] <- cashflows[n] + principal
	discount_factors <- 1 / (1 + ytm / freq) ^ (t * freq)
	
	PV_total <- sum(cashflows * discount_factors)
	D_mac <- sum(t * cashflows * discount_factors) / PV_total
	return(D_mac)
}
#I guess we pass in interest_freq into freq.
duration_investopedia <- duration_macaulay(3, 0.06, 0.06, 1000, 2)
print(paste("Macaulay duration:", duration_investopedia))

get_ytm <- function(PV, nper, coupon, interest_frequency, method="continuous") {

 r = tryCatch(
    uniroot(f=ytm_func, interval=c(1E-15, 10), PV=PV, nper=nper, coupon=coupon, interest_frequency=interest_frequency, method=method, tol=1E-6)$root, 
    error = function(e) { return(NA) }
 )
 return(r)
}

#------------------
# Load TRACE data
#------------------

start.time = Sys.time()

df = readRDS("/scratch/frbkc/trace_enhanced_sample.rds")
df[, time_to_maturity := as.numeric(maturity_date - trd_exctn_dt) / 365]
df[, interest_frequency := as.numeric(interest_frequency)]

#df_chunks = split(df, cut(1:nrow(df), breaks = cores, labels = FALSE))
#df[is.na(offering_date) | is.na(maturity_date) | is.na(trd_exctn_dt) | is.na(coupon), .N]
#df[coupon == 0 | is.na(coupon), .N]
#str(df[, .(trd_exctn_dt, offering_date, maturity_date, coupon, interest_frequency, price)])

end.time = Sys.time()

print(paste("trace load time:", end.time - start.time))
print(paste("TRACE sample rows:", nrow(df)))

#--------------------
# Get Treasury rates
#--------------------

## read the data
h15 = readRDS("./data/H15.rds")
h15 = h15[which(date >= as.Date('2015-01-01')),]

## reshape the data
cols = names(h15)[which(substr(names(h15), 1, 3) == "tyd")]
h15 = reshape(h15,
              idvar = "date",
              varying = cols,
              v.names = "yield",
              timevar = "maturity",
              times = cols,
              direction = "long")
setDT(h15)

# Calculate days
h15[,fstub := substring(maturity,6,6)]
h15[,nstub := as.numeric(substring(maturity,4,5))]
h15[,days := ifelse(fstub == "m", floor(nstub/12*365), nstub*365)]
h15[,(c("fstub", "nstub")) := NULL]
#any(df$trd_exctn_dt %in% h15$date)

#----------------
# Sample data
#----------------
#df = df[which(trd_exctn_dt == as.Date('2019-01-07') & cusip_id == '46625HJZ4'),]
#df = df[which(trd_exctn_dt == as.Date('2019-01-04') & cusip_id == '46625HJZ4'),]
#print(nrow(df))
df = df[3600000:3620000,]
#df = df[which(zero_cpn == 1),]

safe_PV_calc <- function(trade_date, issue_date, maturity_date, coupon, cpn_freq, method = "continuous") {
	out <- tryCatch(
	PV_calc(trade_date, issue_date, maturity_date, coupon, cpn_freq, method),
	error = function(e) NA_real_
)
#print(paste("error:", error, trade_date, issue_date, maturity_date, coupon, cpn_freq))
if (is.null(out) || length(out) != 1 || is.na(out)) return(NA_real_)
return(as.numeric(out))
}
#bad_rows <- which(sapply(1:nrow(df), function(i) {
#	tryCatch({
#		is.na(PV_calc(df$trd_exctn_dt[i], df$offering_date[i], df$maturity_date[i], df$coupon[i], df$interest_frequency[i], method="continuous"))
#}, error = function(e) TRUE)
#}))
#df[bad_rows]
#--------------------------------------------
# Determine the number of payments remaining
#--------------------------------------------
output_file <- "/scratch/frbkc/trace_enhanced_rf_spreads.psv"
count_file <- "./data/trace_enhanced_observation_count.rds"
first_chunk <- TRUE
obs_counts <- list()
#100000 for full run?
#610000
#600000
#620000
chunk_size <- 10000
num_chunks <- ceiling(nrow(df) / chunk_size)
for (i in 1:num_chunks) {
	print(paste("Processing chunk", i, "of", num_chunks))
	start_row <- ((i - 1) * chunk_size) + 1
	end_row <- min(i * chunk_size, nrow(df))
	chunk <- df[start_row:end_row]
	
chunk[,nper := mcmapply(cpn_dates, offering_date, maturity_date, trd_exctn_dt, interest_frequency, rtn="pmts", mc.cores=cores)]

chunk = chunk[!is.na(trd_exctn_dt) & !is.na(offering_date) & !is.na(maturity_date) & !is.na(coupon) & !is.na(interest_frequency)]
chunk[,price_rf := mcmapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date, coupon, interest_frequency,method="continuous", mc.cores=cores)]
chunk[,price_rf_discrete := mcmapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date, coupon, interest_frequency, method="discrete", mc.cores=cores)]

chunk[,ytm_rf := mcmapply(get_ytm, price_rf, nper, coupon, interest_frequency, method="continuous", mc.cores=cores)]
chunk[,ytm_rf_discrete := mcmapply(get_ytm, price_rf_discrete, nper, coupon, interest_frequency, method="discrete", mc.cores=cores)]

chunk[,ytm_trade := mcmapply(get_ytm, price, nper, coupon, interest_frequency, method="continuous", mc.cores=cores)]
chunk[,ytm_trade_discrete := mcmapply(get_ytm, price, nper, coupon, interest_frequency, method="discrete",mc.cores=cores)]

chunk[, duration_mac := mcmapply(duration_macaulay, maturity = time_to_maturity, coupon_rate = coupon / 100, ytm = ytm_rf, principal = principal_amt, freq = interest_frequency, mc.cores = cores)]

chunk[,rf_spread := 100*100*(yield/100 - ytm_rf)]
chunk[,rf_spread_discrete := 100*100*(yield/100 - ytm_rf_discrete)]
## spreads with calculate yield from TRACE price
chunk[,rf_spread_recalc := 100*100*(ytm_trade - ytm_rf)]
chunk[,rf_spread_recalc_discrete := 100*100*(ytm_trade_discrete - ytm_rf_discrete)]


missing_count <- chunk[(!is.na(yield) | !is.na(ytm_trade)),list(trades = .N,
                                          CUSIPs = length(unique(cusip_id)),
                                          banks = length(unique(id_rssd)))]
missing_count[, step := paste0("Missing Spread - chunk", i)]
obs_counts[[i]] <- missing_count
list_cols <- sapply(chunk, is.list)
print(which(list_cols))
print(names(chunk)[list_cols])
fwrite(chunk, output_file, sep="|", append=!first_chunk, col.names=first_chunk, na="NA")
first_chunk <- FALSE
rm(chunk)
gc()
}

start.time = Sys.time()
#df[,nper := mcmapply(cpn_dates, offering_date, maturity_date, trd_exctn_dt, interest_frequency, rtn="pmts", mc.cores=cores)]
end.time = Sys.time()
#print(paste("nper time:", end.time - start.time))
#df[nper == 0 | is.na(nper), .N]

#saveRDS(df, "/scratch/frbkc/trace_enhanced_sample_w_payments.rds")
#double check this line!


#----------------------------------------------------
# Calculate present value of the risk free synthetic
#----------------------------------------------------
start.time = Sys.time()
#df = df[!is.na(trd_exctn_dt) & !is.na(offering_date) & !is.na(maturity_date) & !is.na(coupon) & !is.na(interest_frequency)]
#df[,price_rf := mcmapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date, coupon, interest_frequency, method="continuous", mc.cores=cores)]
#calc_price_rf_chunk = function(chunk, h15_local) {
#	chunk[, price_rf := mapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date,
#				coupon, interest_frequency, MoreArgs = list(method = "continuous", h15_local = h15_local))]
#return(chunk[, .(price_rf)])
#}
end.time = Sys.time()
print(paste("RF continuous pricing time:", end.time - start.time))

start.time = Sys.time()
#df[,price_rf_discrete := mcmapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date, coupon, interest_frequency, method="discrete", mc.cores=cores)]
#calc_price_rf_discrete_chunk <- function(chunk, h15_local) {
#	chunk[, price_rf_discrete := mapply(safe_PV_calc, trd_exctn_dt, offering_date, maturity_date,
#					coupon, interest_frequency,
#					MoreArgs = list(method = "discrete", h15_local = h15_local))]
#	return(chunk[, .(price_rf_discrete)])

#}
end.time = Sys.time()
print(paste("RF discrete pricing time:", end.time - start.time))

#df[is.na(price_rf) | is.na(price_rf_discrete), .N]

#-------------------------
# Solve for risk-free YTM
#-------------------------
start.time = Sys.time()
#df[,ytm_rf := mcmapply(get_ytm, price_rf, nper, coupon, interest_frequency, method="continuous", mc.cores=cores)]
#calc_ytm_rf_chunk <- function(chunk) {
#	chunk[, ytm_rf := mapply(get_ytm, price_rf, nper, coupon, interest_frequency,
#			MoreArgs = list(method = "continuous"))]
#	return(chunk[, .(ytm_rf)])
#}
end.time = Sys.time()
print(paste("Risk-free continuous YTM time:", end.time - start.time))

start.time = Sys.time()
#df[,ytm_rf_discrete := mcmapply(get_ytm, price_rf_discrete, nper, coupon, interest_frequency, method="discrete", mc.cores=cores)]
#calc_ytm_rf_discrete_chunk <- function(chunk) {
#	chunk[, ytm_rf_discrete := mapply(get_ytm, price_rf_discrete, nper, coupon, interest_frequency, MoreArgs = list(method = "discrete"))]
#	return(chunk[, .(ytm_rf_discrete)])
#}
end.time = Sys.time()
print(paste("Risk-free discrete YTM time:", end.time - start.time))

#----------------------
# Solve for trade YTM
#---------------------
start.time = Sys.time()
#df[,ytm_trade := mcmapply(get_ytm, price, nper, coupon, interest_frequency, method="continuous", mc.cores=cores)]
#calc_ytm_trade_chunk <- function(chunk) {
#	chunk[, ytm_trade := mapply(get_ytm, price, nper, coupon, interest_frequency, MoreArgs = list(method = "continuous"))]
#	return(chunk[, .(ytm_trade)])
#}
end.time = Sys.time()
print(paste("Trade continuous YTM time:", end.time - start.time))

start.time = Sys.time()
#df[,ytm_trade_discrete := mcmapply(get_ytm, price, nper, coupon, interest_frequency, method="discrete", mc.cores=cores)]
#calc_ytm_trade_discrete_chunk <- function(chunk) {
#	chunk[, ytm_trade_discrete := mapply(get_ytm, price, nper, coupon, interest_frequency, MoreArgs = list(method = "discrete"))]
#	return(chunk[, .(ytm_trade_discrete)])
#}
end.time = Sys.time()
print(paste("Trade discrete YTM time:", end.time - start.time))


#Macaulay Duration
#df[, duration_mac := mcmapply(duration_macaulay, 
#			      maturity = time_to_maturity,
#		      coupon_rate = coupon / 100,
#		      ytm = ytm_rf,
#		      principal = principal_amt,
#		      freq = interest_frequency,
#			      mc.cores = cores)]
#calc_duration <- function(chunk) {
#	chunk[, duration_mac := mapply(duration_macaulay, maturity = time_to_maturity, coupon_rate = coupon / 100, ytm = ytm_rf, principal = principal_amt, freq = interest_frequency)]
#	return (chunk[, .(duration_mac)])
#}
#cl <- makeCluster(cores)
#clusterExport(cl, varlist = c("safe_PV_calc", "cpn_dates", "get_ytm", "ytm_func"))
#clusterEvalQ(cl, {
#	library(data.table)
#	library(lubridate)
#})
#results_price_rf <- clusterMap(cl, calc_price_rf_chunk, df_chunks, MoreArgs= list(h15_local = h15))
#results_price_rf_discrete <- clusterMap(cl, calc_price_rf_discrete_chunk, df_chunks, MoreArgs= list(h15_local = h15))
#results_ytm_rf <- clusterMap(cl, calc_ytm_rf_chunk, df_chunks)
#results_ytm_rf_discrete <- clusterMap(cl, calc_ytm_rf_discrete_chunk, df_chunks)
#results_ytm_trade <- clusterMap(cl, calc_ytm_trade_chunk, df_chunks)
#results_ytm_trade_discrete <- clusterMap(cl, calc_ytm_trade_discrete_chunk, df_chunks)
#results_duration <- clusterMap(cl, calc_duration, df_chunks)

#stopCluster(cl)

#df[, price_rf := unlist(results_price_rf, use.names = FALSE)] etc. 

#-------------------
# calculate spreads
#-------------------

## spreads from reported TRACE yields
#df[,rf_spread := 100*100*(yield/100 - ytm_rf)]
#df[,rf_spread_discrete := 100*100*(yield/100 - ytm_rf_discrete)]

## spreads with calculate yield from TRACE price
#df[,rf_spread_recalc := 100*100*(ytm_trade - ytm_rf)]
#df[,rf_spread_recalc_discrete := 100*100*(ytm_trade_discrete - ytm_rf_discrete)]

#--------------------------------------
# Count obs with missing yield spreads
#--------------------------------------

#obs_count = readRDS("./data/trace_enhanced_observation_count.rds")

#missing_count = df[(!is.na(yield) | !is.na(ytm_trade)),list(trades = .N,
#                                          CUSIPs = length(unique(cusip_id)),
#                                          banks = length(unique(id_rssd)))]
#missing_count[,step := "Missing Spread"]

obs_count_all = rbindlist(obs_counts, use.names=T, fill=T)
saveRDS(obs_count_all, "/scratch/frbkc/trace_enhanced_observation_count.rds")
print(obs_count_all)


#-------------
# Export data
#-------------
#saveRDS(df, "/scratch/frbkc/trace_enhanced_rf_spreads.rds")
#write.table(df, "/scratch/frbkc/trace_enhanced_rf_spreads.psv", sep="|", row.names=F, na="")



