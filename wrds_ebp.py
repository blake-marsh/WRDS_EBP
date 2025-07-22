import pandas as pd
import numpy as np
import statsmodels.api as sm

iter_df = pd.read_csv("./test_iter_output_cusip.txt", sep="|")
vars_needed = ['DD', 'date', 'cusip']
iter_df = iter_df.dropna(subset=vars_needed)
trace_fisd = pd.read_csv("/scratch/frbkc/trace_enhanced_rf_spreads.psv", sep="|")
vars_needed = ['cusip_id', 'cusip6', 'principal_amt', 'offering_date', 'trd_exctn_dt', 'coupon', 'redeemable', 'coupon_type', 'rf_spread', 'rf_spread_discrete', 'rf_spread_recalc', 'rf_spread_recalc_discrete']
trace_fisd = trace_fisd.dropna(subset=vars_needed)

trace_fisd = trace_fisd.rename(columns={'cusip_id': 'cusip'})
df = trace_fisd.merge(iter_df, on=['cusip'], suffixes=('_trace', '_iter'))

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df['trd_exctn_dt'] = pd.to_datetime(df['trd_exctn_dt'])
df['offering_date'] = pd.to_datetime(df['offering_date'])
df['date'] = pd.to_datetime(df['date'])

df['age'] = (df['trd_exctn_dt'] - df['offering_date']).dt.days

df['call'] = df['redeemable'].map({'Y': 1, 'N': 0}).astype(int)

df['lspr'] = np.log(df['spr'])
df['lduration'] = np.log(df['durati5on'])
df['lparvalue'] = np.log(df['principal_amt'])
df['lcoupon'] = np.log(df['coupon'])
df['lage'] = np.log(df['age'])

df['call_dd'] = df['call'] * df['DD']
df['call_lduration'] = df['call'] * df['lduration']
df['call_lparvalue'] = df['call'] * df['lparvalue']
df['call_lcoupon'] = df['call'] * df['lcoupon']
df['call_lage'] = df['call'] * df['lage']

X = df[['DD', 'lduration', 'lparvalue', 'lcoupon', 'lage', 
        'call', 'call_dd', 'call_lduration', 'call_lparvalue', 'call_lcoupon', 'call_lage']]

X = sm.add_constant(X)
y = df['lspr']

model = sm.OLS(y, X, missing='drop').fit()
df['lspr_p'] = model.predict(X)

sig2 = model.mse_resid
df['spr_p'] = np.exp(df['lspr_p'] + 0.5*sig2)

df['ebp_oa'] = df['spr'] - df['spr_p']



