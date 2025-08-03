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


q = "SELECT a.complete_cusip, a.issue_id, a.issue_cusip,
            substring(CAST(TRIM(a.complete_cusip) AS varchar(8)), 1, 6) as cusip6,
            CAST(a.effective_date AS DATE) as issue_date,
            CAST(a.maturity AS DATE) as maturity_date,
            a.bond_type, a.offering_amt, a.coupon, a.coupon_type, a.interest_frequency,
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
q = "SELECT d.date, d.permno, d.permco, d.cusip, d.prc, e.htick, f.gvkey,
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
fisd = merge(crsp, fisd, by.x=c("cusip6", "date"), by.y=c("cusip6", "issue_date"))


#------------------
# query trace data
#------------------

start_time = Sys.time()

q = "SELECT e.bond_sym_id, e.msg_seq_nb, e.orig_msg_seq_nb, e.trc_st, e.asof_cd,
            CAST(e.trd_exctn_dt AS DATE) as trd_exctn_dt, e.trd_exctn_tm,
            e.cusip_id, substring(e.cusip_id, 1, 6) as cusip6, e.company_symbol,
            e.bloomberg_identifier, e.yld_pt as yield, e.rptd_pr as price,
            e.entrd_vol_qt as quantity, e.sub_prdct, e.rpt_side_cd, e.cntra_mp_id, f.*
     FROM trace.trace_enhanced as e
     INNER JOIN (SELECT a.complete_cusip, a.issue_id, a.issue_cusip,
                        substring(CAST(a.issue_cusip AS varchar(8)), 1, 6) as cusip6,
                        CAST(a.effective_date AS DATE) as issue_date,
                        CAST(a.maturity AS DATE) as maturity_date,
                        a.bond_type, a.offering_amt, a.coupon, a.coupon_type, a.interest_frequency,
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
                   AND b.country = 'USA'
                   AND a.offering_amt >= 1E3
                   AND security_level = 'SEN') as f
             ON e.cusip_id = f.complete_cusip"
q <- dbSendQuery(wrds, q)
df <- dbFetch(q)
setDT(df)
dbClearResult(q)

end_time = Sys.time()
print(paste("TRACE Query time:", end_time - start_time))

## close wrds connection
dbDisconnect(wrds)

## Create a date time
df[,trd_exctn_dt_tm_gmt := as.POSIXct(trd_exctn_tm, origin=trd_exctn_dt, tz="GMT",format="%H:%M:%S")]


## keep trades with remaining maturity between 6 mo and 30 yr
#df[,remaining_maturity := (maturity_date - trd_exctn_dt)]
#df = df[which(0.5 <= remaining_maturity & remaining_maturity <= 30),]

## raw obs counts
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


