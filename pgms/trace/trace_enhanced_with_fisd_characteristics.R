rm(list = ls())
library(RPostgres)
library(data.table)
library(tidyverse)
library(zoo)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')

setwd("$HOME/WRDS_EBP/")

trace_df = readRDS("./data/trace_enhanced_clean.rds")
print(paste("Number of trades:", nrow(trace_df)))

#-----------------------
# Read the mergent data
#-----------------------

q = "SELECT
            a.*,
            b.amount_outstanding,
            c.dated_date,
            c.interest_frequency,
            c.coupon,
            c.pay_in_kind,
            c.coupon_change_indicator,
            d.rating,
            d.rating_type
FROM
  (
    SELECT
      issue_id,
      complete_cusip as cusip,
      issuer_cusip,
      issue_name,
      bond_type,
      security_level,
      coupon_type,
      active_issue,
      principal_amt,
      foreign_currency,
      asset_backed,
      putable,
      convertible,
      slob,
      exchangeable,
      redeemable,
      offering_price,
      offering_yield,
      treasury_spread,
      treasury_maturity,
      CAST(offering_date AS DATE) as offering_date,
      CAST(maturity AS DATE) as maturity_date,
      date_part('year', CAST(offering_date AS DATE)) as offering_year,
      date_part('year', CAST(maturity AS DATE)) as maturity_year
    FROM
      fisd.fisd_issue
    WHERE
      security_pledge IS NULL
      AND (asset_backed = 'N' OR asset_backed IS NULL)
      AND (putable = 'N' OR putable IS NULL)
      AND (convertible = 'N' OR convertible IS NULL)
      AND (exchangeable = 'N' OR exchangeable IS NULL)
      AND (slob = 'N' OR slob IS NULL)
      AND (yankee = 'N' OR yankee IS NULL)
      AND (canadian = 'N' OR canadian IS NULL)
      AND (private_placement = 'N' OR private_placement IS NULL)
      AND (preferred_security = 'N' OR preferred_security IS NULL)
      AND (perpetual = 'N' OR perpetual IS NULL)
      AND (rule_144a = 'N' OR rule_144a IS NULL)
      AND bond_type IN ('CDEB', 'CMTN', 'CNTZ', 'CZ', 'USSN')
      AND CAST('2010-01-01' AS DATE) <= offering_date
      AND offering_date <= CAST('2024-06-30' AS DATE)
  ) as a
  LEFT JOIN (
    SELECT
      issue_id,
      action_type,
      MAX(amount_outstanding) as amount_outstanding
    FROM (
      SELECT issue_id, action_type, amount_outstanding FROM fisd.fisd_amt_out_hist
      UNION
      SELECT issue_id, action_type, amount_outstanding FROM fisd.fisd_amount_outstanding
    )
    WHERE action_type = 'I'
    GROUP BY issue_id, action_type
  ) as b ON a.issue_id = b.issue_id
  LEFT JOIN (
    SELECT
      issue_id,
      dated_date,
      interest_frequency,
      coupon,
      pay_in_kind,
      coupon_change_indicator
    FROM
      fisd.fisd_coupon_info
  ) as c ON a.issue_id = c.issue_id
  LEFT JOIN (
    SELECT DISTINCT ON (issue_id)
      issue_id,
      rating,
      rating_type
    FROM
      fisd.fisd_ratings
    ORDER BY
      issue_id,
      rating_date DESC NULLS LAST  -- Adjust this date field as needed
  ) as d ON a.issue_id = d.issue_id

  LEFT JOIN (
	SELECT DISTINCT ON (lpermno)
	gvkey, 
	lpermno
	FROM crspq.ccmxpf_linktable
	WHERE linktype IN ('LU', 'LC')
	AND linkprim IN ('P', 'C')
) AS e on SUBSTRING(a.issuer_cusip FROM 1 FOR 8) = CAST(e.lpermno AS TEXT)

LEFT JOIN crspq.dsfhdr62 AS f
	ON e.lpermno = f.permno

WHERE
  (
    c.pay_in_kind = 'N'
    OR c.pay_in_kind IS NULL
  )
  AND b.amount_outstanding IS NOT NULL;"

res <- dbSendQuery(wrds, query)
fisd <- dbFetch(res)
dbClearResult(res)
cat(paste("Fetched", nrow(fisd), "rows from FISD query\n"))

fisd <- as.data.table(fisd)
fisd[, fisd := 1]
setnames(fisd, "cusip", "cusip_id")
##fisd indicator
trace_df <- as.data.table(trace_df)
merged <- merge(trace_df, fisd, by = "cusip_id", all.x = TRUE)
cat(paste("Merged rows:", nrow(merged), "\n"))

saveRDS(merged, file = "./data/trace_enhanced_with_fisd_characteristics.rds")
write.table(merged, "/scratch/frbkc/trace_enhanced_with_fisd_characteristics.psv", row.names=F, sep="|", na = ".")

cat("Saved merge dataset!")


