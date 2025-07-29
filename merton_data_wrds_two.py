import sys
old_stdout = sys.stdout
log_file = open("./logfiles/merton_DD_data_WRDS_2020_2025.log","w")
sys.stdout = log_file

# Log time stamp
import datetime
print("Log created on " + str(datetime.datetime.now()) + '\n')

import os
import wrds
import psycopg2
import numpy as np
import pandas as pd
import pandasql as ps
import gc

#modules = dir()
#print(modules)                                                                                                                             
#wary of ./data or /data
os.chdir("./data/compustat/policy/")

## now merge between dates
co_ifndq = pd.read_parquet("./co_ifndq.parquet")
crsp = pd.read_parquet("./crsp.parquet")
col_needed = ['date', 'gvkey', 'permco', 'permno', 'cusip', 'datadate', 'conm', 'gsubind',
            'fyr', 'fic', 'mkt_cap', 'assets', 'face_value_debt', 'tyd01y']

first_chunk = True

n = 50000
output_file = "/scratch/frbkc/merton_DD_data_WRDS_2020_2025.txt"
for i in range(0, crsp.shape[0], n):
    crsp_chunk = crsp.iloc[i:i + n].copy()
    merged_chunk = crsp_chunk.merge(co_ifndq, on='gvkey', how='inner')
    crsp_chunk = None
    gc.collect()
    merged_chunk = merged_chunk[(merged_chunk['date'] >= merged_chunk['dt_start']) &
                                (merged_chunk['date'] <= merged_chunk['dt_end'])]
    merged_chunk = merged_chunk[col_needed]
    if first_chunk:
        merged_chunk.to_csv(output_file, sep="|", index=False, mode='w')
        first_chunk = False
        print("first chunk", len(merged_chunk))
    else:
        merged_chunk.to_csv(output_file, sep="|", index=False, mode='a', header=False)
        print("append chunk", len(merged_chunk))
    merged_chunk = None
    gc.collect()

crsp_full = None
co_ifndq = None
gc.collect()


#crsp = pysqldf(""" SELECT a.date, a.gvkey, a.permco, a.permno, b.datadate, a.conm, a.gsubind, b.fyr, a.fic,
#                          a.mkt_cap, b.assets, b.face_value_debt, a.tyd01y
#                        FROM crsp as a
#                          INNER JOIN co_ifndq as b
#                        ON a.gvkey = b.gvkey
#                          AND a.date between b.dt_start and b.dt_end; """)

#------------------
# Check duplicates
#------------------
#print("\n" + "Any duplicated?")
#print(crsp.duplicated(crsp[['gvkey', 'permco', 'permno', 'date', 'fyr']]).any())

#---------------------
# Export the raw data
#---------------------
#crsp.to_csv("/scratch/frbkc/merton_DD_data_WRDS_1970_1989.txt", sep="|", index=False)
#------------------------------------------------------
print("Log closed on " + str(datetime.datetime.now()))
#------------------------------------------------------
sys.stdout = old_stdout
log_file.close()
