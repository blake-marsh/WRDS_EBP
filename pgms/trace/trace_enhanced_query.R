rm(list = ls())

library(RPostgres)
library(data.table)
library(zoo)
library(feather)

setwd("~/WRDS_EBP/data/")

#---------------------------
# Connect to WRDS database
#---------------------------
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')



#----------------------------------
# Issuer and issue characteristics
#----------------------------------


q = "SELECT a.complete_cusip, a.issue_id, TRIM(a.issue_cusip) as issue_cusip,
            substring(CAST(TRIM(a.complete_cusip) AS varchar(8)), 1, 6) as cusip6,
            CAST(a.delivery_date AS DATE) as issue_date,
            CAST(a.maturity AS DATE) as maturity_date,
            a.bond_type, a.offering_amt, a.coupon, a.coupon_type, 
	    CAST(a.interest_frequency as int) as interest_frequency,
            a.redeemable, a.security_level,
            b.naics_code, b.sic_code, b.country, b.legal_name, b.cusip_name, b.parent_id,
            c.rating, c.rating_type, c.rating_date
     FROM fisd.fisd_mergedissue as a
       LEFT JOIN fisd.fisd_mergedissuer as b
              ON a.issuer_id = b.issuer_id
       LEFT JOIN (SELECT issue_id, CAST(rating_date AS DATE) as rating_date, rating, rating_type
                  FROM fisd.fisd_rating
                  WHERE rating_type = 'SPR') as c
              ON a.issue_id = c.issue_id
     WHERE a.coupon_type IN ('Z', 'F')
       AND a.bond_type IN ('CMTZ', 'CMTN', 'CZ', 'CDEB', 'RTN')
       AND b.country = 'USA'
       AND a.offering_amt >= 1E3
       AND security_level = 'SEN'"
q <- dbSendQuery(wrds, q)
fisd <- dbFetch(q)
setDT(fisd)
dbClearResult(q) 

any(duplicated(fisd$complete_cusip))
nrow(fisd)


#------------
# CRSP table
#------------
q = "SELECT d.date, d.permno, d.permco, TRIM(d.cusip) as cusip, 
            d.prc, e.htick, f.gvkey,
            substring(CAST(e.hcusip AS varchar(8)), 1, 6) as cusip6
     FROM crspq.dsf62 as d
       INNER JOIN (SELECT permno, permco, htick, hcusip, begdat, enddat
		  FROM crspq.dsfhdr62
	          WHERE hcusip IS NOT NULL
		    AND hshrcd IN (10,11)) as e
              ON  d.permco = e.permco
              AND d.permno = e.permno
              AND d.date between e.begdat and e.enddat
       INNER JOIN (SELECT gvkey, lpermno, linkdt, linkenddt
                  FROM crspq.ccmxpf_linktable
                  WHERE linktype IN ('LU', 'LC')
                    AND linkprim in ('P', 'C')) as f
              ON  d.permno = f.lpermno
              AND d.date between f.linkdt AND coalesce(f.linkenddt, CAST('9999-12-31' AS DATE))
     WHERE d.prc IS NOT NULL 
       AND d.prc > 0"
q <- dbSendQuery(wrds, q)
crsp <- dbFetch(q)
setDT(crsp)
dbClearResult(q)

any(duplicated(crsp[,list(date, permno, permco)]))
nrow(crsp)


#---------------------
# Merge fisd and crsp
#---------------------

## merge data
fisd = merge(fisd, crsp, by.x=c("cusip6", "issue_date"), by.y=c("cusip6", "date"))

## dedup (need to link compustat later)
fisd = fisd [which(!duplicated(complete_cusip)),]

## check uniqueness
any(duplicated(fisd$complete_cusip))
nrow(fisd)

#------------------
# query trace data
#------------------

start_time = Sys.time()

q = "SELECT bond_sym_id, msg_seq_nb, orig_msg_seq_nb, trc_st, asof_cd,
            CAST(trd_exctn_dt AS DATE) as trd_exctn_dt, trd_exctn_tm,
            TRIM(cusip_id) as cusip_id, substring(TRIM(cusip_id), 1, 6) as cusip6, 
	    company_symbol, bloomberg_identifier, yld_pt as yield, rptd_pr as price,
            entrd_vol_qt as quantity, sub_prdct, rpt_side_cd, cntra_mp_id
     FROM trace.trace_enhanced"
#             ON e.cusip_id = f.complete_cusip
q <- dbSendQuery(wrds, q)
df <- dbFetch(q)
setDT(df)
dbClearResult(q)

end_time = Sys.time()
print(paste("TRACE Query time:", end_time - start_time))

## close wrds connection
dbDisconnect(wrds)

## Create a date time
df[,trd_exctn_dt_tm := as.POSIXct(paste(
			format(trd_exctn_dt, format="%Y-%m-%d"), 
			format(trd_exctn_tm, format="%H:%M:%S")), 
		    tz="GMT", format="%Y-%m-%d %H:%M:%S")]


## check total obs
print(paste("Raw data count: ", nrow(df)))

#---------------------------------------------
# Keep non-reversal and non-cancelled records
#----------------------------------------------
df_TR = df[which(trc_st %in% c('T', 'R')),]

#--------------------------------------------------------
# Step 1: Remove same-day cancellations and corrections
#--------------------------------------------------------

## get all the cancellation records
cancellations = df[which(trc_st %in% c('X', 'C')),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]
cancellations[,cancellation := 1]
any(duplicated(cancellations[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
df_TR = merge(df_TR, cancellations, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancellations
df_TR = df_TR[is.na(cancellation),]
df_TR[,cancellation := NULL]

## check remaining obs
print(paste("Count after removing cancellations:", nrow(df_TR)))

#--------------------------
# Step 2: Remove reversals
#--------------------------

reversals = df[which(trc_st == 'Y'),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]

reversals[,reversal := 1]
any(duplicated(reversals[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
df_TR = merge(df_TR, reversals, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancelations
df_TR = df_TR[is.na(reversal),]
df_TR[,reversal := NULL]

## check remaining obs
print(paste("Count after removing reversals:", nrow(df_TR)))


#-----------------------------------------
# Step 3: Remove duplicated agency records
#------------------------------------------

## agency sales
agency_s = df_TR[which(rpt_side_cd == 'S' & cntra_mp_id == 'D'),]
setnames(agency_s, old=c('rpt_side_cd'), new=c('rpt_side_cd_s'))

## agency buys
agency_b = df_TR[which(rpt_side_cd == 'B' & cntra_mp_id == 'D'),]

## non-duplicated
agency_bnodup = merge(agency_b, 
		      agency_s[,list(cusip_id, trd_exctn_dt, price, quantity, rpt_side_cd_s)], 
		      by=c('cusip_id', 'trd_exctn_dt', 'price', 'quantity'), all.x=T, allow.cartesian=T)
agency_bnodup = agency_bnodup[is.na(rpt_side_cd_s),]
agency_bnodup[,rpt_side_cd_s := NULL]

## clean up agency_s
setnames(agency_s, old=c("rpt_side_cd_s"), new=c("rpt_side_cd"))

## stack datasets
df_clean = rbindlist(list(df_TR[which(cntra_mp_id == 'C')], agency_s, agency_bnodup), use.names=T, fill=T)

## count
print(paste("After deduping agency trades:", nrow(df_clean)))

#---------------------------------------
# Keep trades with fisd characteristics
#---------------------------------------
df_clean = merge(df_clean, fisd, by.x=c("cusip_id", "cusip6"), by.y=c("complete_cusip", "cusip6"))
print(paste("FISD sample count: ", nrow(df_clean)))

#--------------------------------------
# keep trades with remaining maturity 
# between 6 mo and 30 yr
#-------------------------------------
df_clean[,remaining_maturity := as.numeric(maturity_date - trd_exctn_dt)/365]
df_clean = df_clean[which(!is.na(remaining_maturity) & 0.5 <= remaining_maturity & remaining_maturity <= 30),]

print(paste("sample count after dropping maturity: ", nrow(df_clean)))


#--------------------------------------------
# Keep the last trade by between 9AM and 4PM
#--------------------------------------------

## keep trades between 9AM and 4PM
df_clean = df_clean[which(9 <= as.numeric(format(trd_exctn_dt_tm, format="%H")) & as.numeric(format(trd_exctn_dt_tm, format="%H")) <= 16),]

## sort trades
df_clean = df_clean[order(cusip_id, trd_exctn_dt, trd_exctn_tm),]

## sequence trades
df_clean[,seq := seq_len(.N), by=list(cusip_id, trd_exctn_dt)]
df_clean[,max_seq := max(seq), by=list(cusip_id, trd_exctn_dt)]
df_clean = df_clean[which(seq == max_seq),]
df_clean[,(c("seq", "max_seq")) := NULL]

#------------------------------------------
# Check duplicates by cusip and trade date 
#------------------------------------------
any(duplicated(df_clean[,list(cusip_id, trd_exctn_dt)]))

#-------------------------------
# Print final clean sample size
#-------------------------------
print(paste("Final trade count:", nrow(df_clean)))

#--------------------
# Export the dataset
#--------------------
saveRDS(df_clean, "trace_enhanced_clean.rds")
write_feather(df_clean, "trace_enhanced_clean.feather")
#write.table(df_clean, "trace_enhanced_clean.psv", row.names=F, sep="|", na = ".")


