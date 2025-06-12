import sys
old_stdout = sys.stdout
log_file = open("./logfiles/merton_DD_WRDS_1970_1989.log","w")
sys.stdout = log_file

# Log time stamp
import datetime
print("Log created on " + str(datetime.datetime.now()) + '\n')

import os, time, warnings
import psycopg2
import numpy as np
import pandas as pd
import multiprocessing as mp
from scipy.stats import norm
from scipy.optimize import fsolve

os.chdir("/data/compustat/policy/")

#---------------------
# Database connection
#---------------------
dbconn = psycopg2.connect(host = 'wrds-pgdata.wharton.upenn.edu', dbname = 'wrds', port='9737')

#------------------------
# Simultaneous procedure
#------------------------

# Simultaneous Merton Model function
def merton_sim(x, E, D, sigma_E, r, T):

    V = x[0]
    sigma_V = x[1]
    
    d1 = (np.log(V/D) + (r + 0.5*(sigma_V**2))*T)/(sigma_V*np.sqrt(T))
    d2 = d1 - sigma_V*np.sqrt(T)
    
    y1 = E - (V*norm.cdf(d1) - np.exp(-r*T)*D*norm.cdf(d2))
    y2 = E*sigma_E - V*norm.cdf(d1)*sigma_V

    return [y1, y2]

## single date simultaneous solver
def sim_process(df, T=1.0):

    ## Initialize variables
    row = df.iloc[-1]
    gvkey = row.gvkey; fyr = row.fyr; permco = row.permco; permno = row.permno; date = row.date.date(); gsubind = row.gsubind; fic = row.fic
    convergence = False

    ## sort dataframe
    df = df.sort_values(['gvkey', 'fyr', 'permco', 'permno', 'date'])

    ## calculate returns and standard deviations
    df['E_ret'] = np.log(df.E/df.E.shift(1))*252
    sigma_E = (df.E_ret/252).std()*np.sqrt(252)
    sigma_V = sigma_E*(df.E.iloc[-1]/(df.E.iloc[-1]+df.D.iloc[-1]))
    if sigma_V < 0.01:
        sigma_V = 0.01

    t0 = time.time()
    ## Solve the model
    warnings.simplefilter("error")
    try: 
        fsolve_out = fsolve(merton_sim, x0=np.array([row.E+row.D, sigma_V]), args=(row.E, row.D, sigma_E, row.rf, T))
        convergence = True
    except:
        #print("Error in row: ", (gvkey, fyr, permco, permno, date))
        fsolve_out = [np.nan, np.nan]

    iteration_time = time.time() - t0

    ## Distance -to-Default and PD
    if ~any(np.isnan(fsolve_out)):
        DD = (np.log(fsolve_out[0]/row.D) +(row.rf - 0.5*fsolve_out[1])*T)/(fsolve_out[1]*np.sqrt(T))
        PD = norm.cdf(-DD)
    else:
        DD = np.nan; PD = np.nan

    ## return a dictionary 
    result = {"gvkey":gvkey, "fyr":fyr, "permco":permco, "permno":permno, "date":date, "gsubind":gsubind, "fic":fic,
              "A":row.A, "E":row.E, "D":row.D, "rf":row.rf, "DD":DD, "PD":PD,
              "V":fsolve_out[0], "sigma_V":fsolve_out[1],  "convergence":convergence, 
              "iteration_time":iteration_time}

    return result
     
#------------------------------
# Two step iterative procedure
#------------------------------

## firm value solver takes daily inputs
def merton_iter(V, sigma_V, E, sigma_E, D, rf, T):
    
    d1 = (np.log(V/D) + (rf + 0.5*(sigma_V**2))*T)/(sigma_V*np.sqrt(T))
    d2 = d1 - sigma_V*np.sqrt(T)
    
    y = E - (V*norm.cdf(d1) - np.exp(-rf*T)*D*norm.cdf(d2))

    return y

## two step process takes time series inputs by firm
## df must have: gvkey, date, A, D, E, rf
def two_step_process(df, T, max_iter=10, tol=1E-4):

    ## Initialize variables
    last_row = df.iloc[-1]
    df = df.sort_values(['gvkey', 'fyr', 'permco', 'permno', 'date'])
    df['E_ret'] = np.log(df.E/df.E.shift(1))*252
    sigma_E = (df.E_ret/252).std()*np.sqrt(252)
    sigma_V = sigma_E*(df.E.iloc[-1]/(df.E.iloc[-1]+df.D.iloc[-1]))
    if sigma_V < 0.01:
        sigma_V = 0.01
    df['V'] = np.nan
    gvkey = last_row.gvkey; fyr = last_row.fyr; permco = last_row.permco; permno = last_row.permno; date = last_row.date.date(); gsubind = last_row.gsubind; fic=last_row.fic
    #print("Processing row:", gvkey, fyr, permco, permno, date)
    num_iter = 0; converge_check= np.nan; convergence = False; iteration_time = np.nan

    ## start timer
    t0 = time.time()

    while num_iter <= max_iter:

        num_iter += 1

        ## Step 1: Calculate implied firm value daily
        warnings.simplefilter("error")
        for idx, row in df.iterrows():
            if row[['E', 'D', 'rf']].isna().any() == False:
                try:
                    ## solve for daily firm value given initial sigma_V
                    V = fsolve(merton_iter, x0=row.E+row.D, args=(sigma_V, row.E, sigma_E, row.D, row.rf, T))[0]
                    df.loc[(df.gvkey == row.gvkey) & (df.fyr == row.fyr) & (df.permco == row.permco) & (df.permno == row.permno) & (df.date == row.date), 'V'] = V
                except:
                     pass
                #    print("Error in: ", row.gvkey, row.permco, row.permno, row.fyr)
   
        ## Step 2: Calculate firm value parameters
        if df.loc[~df.V.isna()].shape[0] >= 50:
            df['V_lag'] = df.V.shift(1)
            df['V_ret'] = np.log(df.V/df.V_lag)*252
            mean_V = df.V_ret.mean()
            sigma_V_prime = (df.V_ret/252).std()*np.sqrt(252)

            ## check tolerance
            converge_check = np.abs(sigma_V_prime - sigma_V)
            if converge_check <= tol:
                convergence = True
                break
            else:
                sigma_V = sigma_V_prime

        else:
            convergence = False

    ## calculate distance to default and probability of default
    df = df.iloc[-1]
    if convergence == True:
        DD = (np.log(df.V/df.D) + (mean_V - 0.5*sigma_V**2))/sigma_V
        PD = norm.cdf(-DD)
    else:
       DD = np.nan; PD = np.nan; V = np.nan; sigma_V = np.nan; mean_V = np.nan
    iteration_time = time.time() - t0

    ## return a dictionary 
    result = {"gvkey":gvkey, "fyr":fyr, "permco":permco, "permno":permno, "date":date, "gsubind":gsubind, "fic":df.fic,
              "A":df.A, "E":df.E, "D":df.D, "rf":df.rf, 
              "DD":DD, "PD":PD, "V":df.V, "sigma_V":sigma_V, "mean_V":mean_V, 
              "convergence":convergence, "iters":num_iter, 
              "converge_check":converge_check, "iteration_time":iteration_time}
     
    return result

#-------------------------------------
print("Preparing raw data..." + "\n")
#-------------------------------------

## read the raw data file
df = pd.read_csv("./data/merton_DD_data_WRDS_1970_1989.txt", sep="|")
df['date'] = pd.to_datetime(df.date)

## check duplicates
print("\n" + "Are any duplicated?")
print(df.duplicated(['gvkey', 'fyr', 'permco', 'permno', 'date']).any())

## drop observations missing market cap
df = df.dropna(subset=['mkt_cap'])

## sort by primary keys
df = df.sort_values(['gvkey', 'fyr', 'permco', 'permno', 'date'])

## rename columns
df = df.rename(columns={"assets":"A", "mkt_cap":"E", "face_value_debt":"D", "tyd01y":"rf"})

# Keep obs that are non-zero
df = df[(df[['E','D','rf']]!=0).all(1)]

#-----------------------------------------------
print("Reading business daily dates..." + "\n")
#-----------------------------------------------
query = """ SELECT date
            FROM crspq.dsi62
        """
market_dates = pd.read_sql(query, dbconn)
market_dates['date'] = pd.to_datetime(market_dates['date'])
market_dates = market_dates.sort_values('date')

## create 250 day lag
market_dates['date_lag_250'] = market_dates.date.shift(250)

## merge into main data
df = df.merge(market_dates, on="date", how="left")

#----------------------------------------------------------
# Chunk the data by gvkey, fyr, permco, permno combination
#----------------------------------------------------------

df_by_groups = df.groupby(['gvkey', 'fyr', 'permco', 'permno'])
df_by_groups = [i[1] for i in df_by_groups]

#-------------------------------------------
print("""Estimate DD for all gvkeys\n 
Use the simultaneous method """ + "\n")
#-------------------------------------------

## function to process the results by group
def sim_by_group(sample):

    ## DataFrame to append results
    results_df = pd.DataFrame()

    ## get month end dates
    dates = sample.date.groupby([sample.date.dt.year,sample.date.dt.month]).last()

    ## run simultaneous process for month end dates
    for idx,daily_dt in dates.iteritems():
        dt_lag = sample.date_lag_250.loc[sample.date == daily_dt].iloc[0]
        daily_sample = sample.loc[(dt_lag < sample.date) & (sample.date <= daily_dt)]
        if daily_sample.shape[0] >= 50 and ~(daily_sample[['E', 'D', 'rf']].iloc[-1].isna().any()):
            x = sim_process(daily_sample, T=1)
            x = pd.DataFrame([x], columns=x.keys())
            results_df = results_df.append(x, ignore_index=True)

    return results_df

## parallel run over samples
num_cpus = mp.cpu_count()
pool = mp.Pool(processes=num_cpus)
output = [pool.apply_async(sim_by_group, args=(sample,)) for sample in df_by_groups]
print("Simultaneous processing done.")

## get results and write output
results_sim = pd.concat([p.get()for p in output])
results_sim.to_csv("./data/merton_simultaneous_WRDS_1970_1989.txt", sep="|", index=False)

#--------------------------------------
print("""Estimate DD for all gvkeys\n 
Use the iterated method """ + "\n")
#--------------------------------------

## function to process the results by group
def iter_by_group(sample):

    ## DataFrame to append results
    results_df = pd.DataFrame()

    ## get month end dates
    dates = sample.date.groupby([sample.date.dt.year,sample.date.dt.month]).last()

    ## run simultaneous process for month end dates
    for idx,daily_dt in dates.iteritems():
        dt_lag = sample.date_lag_250.loc[sample.date == daily_dt].iloc[0]
        daily_sample = sample.loc[(dt_lag < sample.date) & (sample.date <= daily_dt)]
        if daily_sample.shape[0] >= 50 and ~(daily_sample[['E', 'D', 'rf']].iloc[-1].isna().any()):
            x = two_step_process(daily_sample, T=1)
            x = pd.DataFrame([x], columns=x.keys())
            results_df = results_df.append(x, ignore_index=True)

    return results_df

## parallel run over samples
num_cpus = mp.cpu_count()
pool = mp.Pool(processes=num_cpus)
output = [pool.apply_async(iter_by_group, args=(sample,)) for sample in df_by_groups]
print("Two step processing done.")

## get results and write output
results_iter = pd.concat([p.get() for p in output])
results_iter.to_csv("./data/merton_iterated_WRDS_1970_1989.txt", sep="|", index=False)

#------------------------------------------------------
print("Log closed on " + str(datetime.datetime.now()))
#------------------------------------------------------
sys.stdout = old_stdout
log_file.close()
