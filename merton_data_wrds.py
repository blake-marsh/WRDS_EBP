import sys
old_stdout = sys.stdout
log_file = open("./logfiles/merton_DD_data_WRDS_2010_2025.log","w")
sys.stdout = log_file

# Log time stamp
import datetime
print("Log created on " + str(datetime.datetime.now()) + '\n')

#print(help('modules'))
#----------------------------------
# Build the dataset required 
# to estimate the Merton EDF model
#----------------------------------

import os 
import wrds
import psycopg2
import numpy as np
import pandas as pd
#import pandasql as ps

#modules = dir()
#print(modules)

#wary of ./data or /data
os.chdir("./data/compustat/policy/")

#-------------------------------
# Establish database connection
#-------------------------------
#dbconn = psycopg2.connect(host = 'wrds-pgdata.wharton.upenn.edu', dbname = 'wrds', port='9737')
db = wrds.Connection()
#----------------------
# Daily stock prices
# From CRSP data 
# with GVKEY from CCM
#----------------------
#a.cusip
query = """ SELECT a.*, b.htick, b.hcomnam, b.hshrcd, b.hnaics, c.gvkey,
                   abs(a.prc)*a.shrout/1E3 as mkt_cap
            FROM crspq.dsf62 as a
             LEFT JOIN crspq.dsfhdr62 as b
                   ON  a.permco = b.permco
                   AND a.permno = b.permno
                   AND a.date between b.begdat and b.enddat
             LEFT JOIN (SELECT gvkey, lpermno, linkdt, linkenddt
                        FROM crspq.ccmxpf_linktable
                        WHERE linktype IN ('LU', 'LC')
                          AND linkprim in ('P', 'C')) as c
                    ON  a.permno = c.lpermno
                    AND a.date between c.linkdt AND coalesce(c.linkenddt, CAST('9999-12-31' AS DATE))
             WHERE a.date between CAST('2020-01-01' AS DATE) AND CAST('2025-12-31' AS DATE)
               AND b.hshrcd IN (10,11)
               AND a.prc IS NOT NULL """

#crsp = pd.read_sql(query, dbconn)
crsp = db.raw_sql(query)
crsp['date'] = pd.to_datetime(crsp['date'])

#---------------------
# Compustat
# company information
#---------------------
query = """ SELECT * 
            FROM comp.company """
#company = pd.read_sql(query, dbconn)
company = db.raw_sql(query)


#---------------------
# Compustat quarterly 
# balance sheet info
#---------------------
query = """ SELECT datadate, gvkey, fyr, lctq as current_liabilities, ltq as total_liabilities,
                   CASE 
                      WHEN lctq IS NOT NULL AND ltq IS NOT NULL THEN ltq - lctq
                      WHEN lctq IS NULL AND ltq IS NOT NULL THEN ltq
                   END AS lt_liabilities, 
                   CASE
                      WHEN lctq IS NOT NULL AND ltq IS NOT NULL THEN lctq + 0.5*ltq
                      WHEN lctq IS NULL AND ltq IS NOT NULL THEN 0.5*ltq
                      WHEN lctq IS NOT NULL AND ltq IS NULL THEN lctq
                   END AS face_value_debt, atq as assets
            FROM comp.co_ifndq
            WHERE indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C' AND popsrc = 'D' """

#co_ifndq = pd.read_sql(query, dbconn)
co_ifndq = db.raw_sql(query)
co_ifndq['datadate'] = pd.to_datetime(co_ifndq['datadate'])


#----------------------------------------------------------
# Risk free rates (1-year constant maturity Treasury rate)
#----------------------------------------------------------
H15 = pd.read_csv("./H15_B.csv", na_values=["."])
H15 = H15[['TIME_PERIOD','RIFLGFCY01_N.B']]
H15 = H15.rename(columns={"TIME_PERIOD":"date", "RIFLGFCY01_N.B":"tyd01y"})
H15['tyd01y'] = H15['tyd01y']/100
H15['date'] = pd.to_datetime(H15['date'])

#------------------------
# business daily dates
#-----------------------
query = """ SELECT date
            FROM crspq.dsi62
        """
#market_dates = pd.read_sql(query, dbconn)
market_dates = db.raw_sql(query)
market_dates['date'] = pd.to_datetime(market_dates['date'])

#----------------------------------------
# match stock prices to risk-free rates
#----------------------------------------
crsp = crsp.merge(H15, how='left', on = 'date')

#-------------------------------
# Add company names and sectors
#-------------------------------
crsp = crsp.merge(company[['gvkey', 'conm', 'gsubind', 'fic']], on='gvkey', how='left')

#------------------------------------------
# Set reporting period start and end dates
#------------------------------------------
## sort data
co_ifndq = co_ifndq.sort_values(['gvkey', 'fyr', 'datadate'])

## get start of reporting period (start of quarter)
co_ifndq['dt_end'] = co_ifndq['datadate']
co_ifndq['dt_start'] = pd.to_datetime(co_ifndq['dt_end'].values.astype('datetime64[M]')) - pd.DateOffset(months=2)

#------------------------------------------------------------
# Repeat the last quarter financial data if not reported yet
#------------------------------------------------------------

current_qtr = crsp.date.max().to_pydatetime() + pd.tseries.offsets.QuarterEnd()
back_1qtr = current_qtr.to_pydatetime() - pd.tseries.offsets.QuarterEnd(1)
back_2qtr = current_qtr.to_pydatetime() - pd.tseries.offsets.QuarterEnd(2)
back_3qtr = current_qtr.to_pydatetime() - pd.tseries.offsets.QuarterEnd(3)

## all data from 2 quarters ago
co_ifndq_2qtr = co_ifndq.loc[(back_3qtr < co_ifndq.dt_start) & (co_ifndq.dt_end <= back_2qtr)].copy()

## all gvkeys from 2 quarters ago
gvkeys_2qtr = co_ifndq_2qtr.gvkey.unique().tolist()

## repeat data not reported 1 quarter ago
gvkeys_1qtr = co_ifndq.gvkey.loc[(back_2qtr < co_ifndq.dt_start) & (back_1qtr <= co_ifndq.dt_end)].unique().tolist()
gvkeys_missing = np.setdiff1d(gvkeys_2qtr, gvkeys_1qtr)
co_ifndq_1qtr = co_ifndq_2qtr.loc[co_ifndq_2qtr.gvkey.isin(gvkeys_missing)].copy()
co_ifndq_1qtr['dt_start'] = back_2qtr + pd.DateOffset(1)
co_ifndq_1qtr['dt_end'] = back_1qtr

## repeat data for current quarter
gvkeys_current_qtr = co_ifndq.gvkey.loc[(back_1qtr < co_ifndq.dt_start) & (current_qtr <= co_ifndq.dt_end)].unique().tolist()
gvkeys_missing = np.setdiff1d(gvkeys_2qtr, gvkeys_current_qtr)
co_ifndq_current_qtr = co_ifndq_2qtr.loc[co_ifndq_2qtr.gvkey.isin(gvkeys_missing)].copy()
co_ifndq_current_qtr['dt_start'] = back_1qtr + pd.DateOffset(1)
co_ifndq_current_qtr['dt_end'] = current_qtr

## append data
#co_ifndq = co_ifndq.append(co_ifndq_1qtr)
#co_ifndq = co_ifndq.append(co_ifndq_current_qtr)
pdlist = [co_ifndq, co_ifndq_1qtr, co_ifndq_current_qtr]
co_ifndq = pd.concat(pdlist)
co_ifndq.to_parquet("./co_ifndq.parquet")
crsp.to_parquet("./crsp.parquet")

#------------------------------------------------------
print("Log closed on " + str(datetime.datetime.now()))
#------------------------------------------------------
sys.stdout = old_stdout
log_file.close()
