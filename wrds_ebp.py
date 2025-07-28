import pandas as pd
import numpy as np
import statsmodels.api as sm

#Get redeemable merged in
iter_df = pd.read_csv("./test_iter_output_cusip.txt", sep="|")
vars_needed = ['DD', 'date', 'cusip']
iter_df = iter_df.dropna(subset=vars_needed)
trace_fisd = pd.read_csv("/scratch/frbkc/trace_enhanced_rf_spreads.psv", sep="|")
trace_fisd_plus = pd.read_csv("/scratch/frbkc/trace_enhanced_with_fisd_characteristics.psv", sep="|")
key=['cusip_id', 'bloomberg_identifier', 'trd_exctn_dt', 'bond_sym_id']
lookup = (trace_fisd_plus.groupby(key)['redeemable'].first())
trace_fisd['redeemable'] = trace_fisd.set_index(key).index.map(lookup)

#merge on cusip and trade date. 
#trace_fisd = trace_fisd.rename(columns={'cusip6':'cusip'})
trace_fisd['cusip'] = trace_fisd['cusip_id'].str[:8] #maybe use in logic later?

iter_df['date'] = pd.to_datetime(iter_df['date'])
iter_df['month_year'] = iter_df['date'].dt.to_period('M')
dd_last = (
    iter_df
    .sort_values(['cusip', 'date'])
    .groupby(['cusip', 'month_year'])
    .tail(1))

trace_fisd['trd_exctn_dt'] = pd.to_datetime(trace_fisd['trd_exctn_dt'])
trace_fisd['month_year'] = trace_fisd['trd_exctn_dt'].dt.to_period('M')


print("--------------------------------------------------------")
print(trace_fisd['cusip'].dtype, dd_last['cusip'].dtype)
print(trace_fisd['cusip'].head())
print(dd_last['cusip'].head())
print(trace_fisd['trd_exctn_dt'].dt.to_period('M').head())
print(dd_last['date'].dt.to_period('M').head())
print("--------------------------------------------------------")

merged = trace_fisd.merge(
    dd_last[['cusip', 'month_year', 'DD']],
    on=['cusip', 'month_year'],
    how='left')
print("with dd merge shape:", merged.shape)
print(merged.keys())

#filtered_iter = iter_df[(iter_df['date'].dt.year == year) & (iter_df['date'].dt.month == month)]
#filtered_sim = sim_df[(sim_df['date'].dt.year == year) & (sim_df['date'].dt.month == month)]
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
print(merged[:3])
merged = merged[merged['DD'].notna()]
print("no na dd", merged.shape)

print(mistake)
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



