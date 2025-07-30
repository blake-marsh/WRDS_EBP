rm(list = ls())

library(RPostgres)
library(data.table)
library(zoo)

setwd("/scratch/frbkc/")

#---------------------------
# Connect to WRDS database
#---------------------------
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')


#-------------------
# query trace data
#  since 2019:Q1
#-------------------

## Query to limit dates
#q = "SELECT bond_sym_id, msg_seq_nb, orig_msg_seq_nb, trc_st, asof_cd,
#                  CAST(trd_exctn_dt AS DATE) as trd_exctn_dt, trd_exctn_tm,
#                  cusip_id, substring(cusip_id, 1, 6) as cusip6, company_symbol, 
#		  bloomberg_identifier, yld_pt as yield, rptd_pr as price,
#                  entrd_vol_qt as quantity, sub_prdct, rpt_side_cd, cntra_mp_id
#            FROM trace.trace_enhanced
#            WHERE trd_exctn_dt >= CAST('2021-01-01' AS DATE)
#	      AND cusip_id IS NOT NULL"

## full query
count_q <- "SELECT COUNT(*) FROM trace.trace_enhanced WHERE cusip_id IS NOT NULL;"
total_rows <- dbGetQuery(wrds, count_q)[1, 1]
chunk_size <- 10000000
num_chunks <- ceiling(total_rows / chunk_size)
output_file <- "trace_enhanced_clean_long.psv"
first_chunk <- TRUE

for (i in 1:num_chunks) {
print(paste("Processing chunk", i, "of", num_chunks))

q <- sprintf("SELECT bond_sym_id, msg_seq_nb, orig_msg_seq_nb, trc_st, asof_cd,
            CAST(trd_exctn_dt AS DATE) as trd_exctn_dt, trd_exctn_tm,
            cusip_id, substring(cusip_id, 1, 6) as cusip6, company_symbol,
            bloomberg_identifier, yld_pt as yield, rptd_pr as price,
            entrd_vol_qt as quantity, sub_prdct, rpt_side_cd, cntra_mp_id
     FROM trace.trace_enhanced
     WHERE cusip_id IS NOT NULL
     OFFSET %d LIMIT %d;", (i - 1)*chunk_size, chunk_size)

chunk <- setDT(dbGetQuery(wrds, q))

## Create a date time
chunk[,trd_exctn_dt_tm_gmt := as.POSIXct(trd_exctn_tm, origin=trd_exctn_dt, tz="GMT",format="%H:%M:%S")]

## raw obs counts
print(paste("Raw data count: ", nrow(df)))

#---------------------------------------------
# Keep non-reversal and non-cancelled records
#----------------------------------------------
chunk_TR = chunk[which(trc_st %in% c('T', 'R')),]

#--------------------------------------------------------
# Step 1: Remove same-day cancellations and corrections
#--------------------------------------------------------

## get all the cancellation records
cancellations = chunk[which(trc_st %in% c('X', 'C')),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]
cancellations[,cancellation := 1]
any(duplicated(cancellations[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
chunk_TR = merge(chunk_TR, cancellations, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancellations
chunk_TR = chunk_TR[is.na(cancellation),]
chunk_TR[,cancellation := NULL]

## check remaining obs
print(paste("Count after removing cancellations:", nrow(chunk_TR)))

#--------------------------
# Step 2: Remove reversals
#--------------------------

reversals = chunk[which(trc_st == 'Y'),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]

reversals[,reversal := 1]
any(duplicated(reversals[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
chunk_TR = merge(chunk_TR, reversals, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancelations
chunk_TR = chunk_TR[is.na(reversal),]
chunk_TR[,reversal := NULL]

## check remaining obs
print(paste("Count after removing reversals:", nrow(chunk_TR)))


#-----------------------------------------
# Step 3: Remove duplicated agency records
#------------------------------------------

## agency sales
agency_s = chunk_TR[which(rpt_side_cd == 'S' & cntra_mp_id == 'D'),]
setnames(agency_s, old=c('rpt_side_cd'), new=c('rpt_side_cd_s'))

## agency buys
agency_b = chunk_TR[which(rpt_side_cd == 'B' & cntra_mp_id == 'D'),]

## non-duplicated
agency_bnodup = merge(agency_b, 
		      agency_s[,list(cusip_id, trd_exctn_dt, price, quantity, rpt_side_cd_s)], 
		      by=c('cusip_id', 'trd_exctn_dt', 'price', 'quantity'), all.x=T, allow.cartesian=T)
agency_bnodup = agency_bnodup[is.na(rpt_side_cd_s),]
agency_bnodup[,rpt_side_cd_s := NULL]

## clean up agency_s
setnames(agency_s, old=c("rpt_side_cd_s"), new=c("rpt_side_cd"))

## stack datasets
chunk_clean = rbindlist(list(chunk_TR[which(cntra_mp_id == 'C')], agency_s, agency_bnodup), use.names=T, fill=T)

fwrite(chunk_clean, output_file, sep="|", append=!first_chunk, col.names=first_chunk)
first_chunk <- FALSE
rm(chunk, chunk_TR, cancellations, reversals, agency_s, agency_b, agency_bnodup, chunk_clean)
gc()
}
dbDisconnect(wrds)
## count
#print(paste("After deduping agency trades:", nrow(df_clean)))

#-------------------------------
# Print final clean sample size
#-------------------------------
#print(paste("Final trade count:", nrow(df_clean)))

#--------------------
# Export the dataset
#--------------------
#saveRDS(df_clean, "trace_enhanced_clean.rds")
#write.table(df_clean, "trace_enhanced_clean.psv", row.names=F, sep="|", na = ".")


