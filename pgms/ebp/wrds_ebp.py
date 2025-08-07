import pandas as pd
import numpy as np
import statsmodels.api as sm

# Get redeemable merged in
iter_df = pd.read_csv("../../test_iter_cusip_2020_2025.txt", sep="|")
'''trace_fisd = pd.read_csv("/scratch/frbkc/trace_enhanced_rf_spreads.psv", sep="|")
trace_fisd_plus = pd.read_csv("/scratch/frbkc/trace_enhanced_with_fisd_characteristics.psv", sep="|")
key=['cusip_id', 'bloomberg_identifier', 'trd_exctn_dt', 'bond_sym_id']
lookup = (trace_fisd_plus.groupby(key)['redeemable'].first())
trace_fisd['redeemable'] = trace_fisd.set_index(key).index.map(lookup)
trace_fisd.to_csv("/scratch/frbkc/trace_fisd_full.psv", sep="|", index=False)'''
trace_fisd = pd.read_csv("/scratch/frbkc/trace_fisd_full.psv", sep="|")

# Merge on cusip and trade date.
trace_fisd['cusip'] = trace_fisd['cusip_id'].str[:6]
iter_df['cusip'] = iter_df['cusip'].str[:6] 
trace_fisd['cusip'] = trace_fisd['cusip'].str.upper().str.strip()
iter_df['cusip'] = iter_df['cusip'].str.upper().str.strip()

iter_df['date'] = pd.to_datetime(iter_df['date'])
iter_df['month_year'] = iter_df['date'].dt.to_period('M')
trace_fisd['trd_exctn_dt'] = pd.to_datetime(trace_fisd['trd_exctn_dt'])
trace_fisd['month_year'] = trace_fisd['trd_exctn_dt'].dt.to_period('M')

dd_last = (
    iter_df
    .sort_values(['cusip', 'date'])
    .groupby(['cusip', 'month_year'])
    .head(1))

common_cusip = set(trace_fisd['cusip']) & set(iter_df['cusip'])
print("Common cusip count:", len(common_cusip))
common_month_year = set(trace_fisd['month_year']) & set(dd_last['month_year'])
print("Common month year count:", len(common_month_year))

print("TRACE fisd month year range:", trace_fisd['month_year'].min(), "to", trace_fisd['month_year'].max())
print("DD month year range:", dd_last['month_year'].min(), "to", dd_last['month_year'].max())
print("dd_last shape:", dd_last.shape)
print("trace_fisd shape:", trace_fisd.shape)

merged = trace_fisd.merge(
    dd_last[['cusip', 'month_year', 'date', 'DD']],
    on=['cusip', 'month_year'],
    how='left')
print("Merged shape:", merged.shape)
print("With dd merge shape:", merged.shape)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = merged[merged['DD'].notna()]
print("no na dd", df.shape)
df = df[df['redeemable'].notna()]
print("no redeemable dd", df.shape)

df['trd_exctn_dt'] = pd.to_datetime(df['trd_exctn_dt'])
df['offering_date'] = pd.to_datetime(df['offering_date'])

# Calculate the Excess Bond Premium
df['age'] = (df['trd_exctn_dt'] - df['offering_date']).dt.days

df['call'] = df['redeemable'].map({'Y': 1, 'N': 0}).astype(int)

df['lspr'] = np.log(df['rf_spread']) #rf_spread_discrete?
df['lduration'] = np.log(df['duration_mac'])
df['lparvalue'] = np.log(df['principal_amt'])
df['lcoupon'] = np.log(df['coupon'])
df['lage'] = np.log(df['age'])

df['call_dd'] = df['call'] * df['DD']
df['call_lduration'] = df['call'] * df['lduration']
df['call_lparvalue'] = df['call'] * df['lparvalue']
df['call_lcoupon'] = df['call'] * df['lcoupon']
df['call_lage'] = df['call'] * df['lage']

X = df[['DD', 'lduration', 'lparvalue', 
        'lcoupon', 'lage', 'call', 
        'call_dd', 'call_lduration', 
        'call_lparvalue', 'call_lcoupon', 
        'call_lage']]
X = sm.add_constant(X)
y = df['lspr']

model = sm.OLS(y, X, missing='drop').fit()
df['lspr_p'] = model.predict(X)

sig2 = model.mse_resid
df['spr_p'] = np.exp(df['lspr_p'] + 0.5*sig2)

df['ebp'] = df['rf_spread'] - df['spr_p']

# Summary statistics
df['maturity_date'] = pd.to_datetime(df['maturity_date'])
df['offering_date'] = pd.to_datetime(df['offering_date'])
df['maturity_at_issue'] = (df['maturity_date'] - df['offering_date']).dt.days / 365
df['time_to_maturity'] = (df['maturity_date'] - df['trd_exctn_dt']).dt.days / 365
df = df[['trd_exctn_dt', 'date', 'ebp', 'DD', 'maturity_at_issue', 'time_to_maturity', 'duration_mac', 'coupon','rf_spread','principal_amt']]
print(df.describe())

# Save to File
df.to_csv("/scratch/frbkc/ebp.csv", sep= "|", index=False)
ebp_timeseries = df.groupby(df['date'].dt.to_period('M'))['ebp'].mean().reset_index()
ebp_timeseries['date'] = ebp_timeseries['date'].dt.to_timestamp()
ebp_timeseries.to_csv('/scratch/frbkc/ebp_timeseries.csv', sep="|", index=False)
print(ebp_timeseries[:100])

