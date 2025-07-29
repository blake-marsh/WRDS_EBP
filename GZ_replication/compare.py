import pandas as pd
import sys

convds_df = pd.read_csv("/scratch/frbkc/convds_output.csv")
#iter_df = pd.read_csv("/scratch/frbkc/merton_iterated_WRDS_1970_1989.txt", sep="|")
iter_df = pd.read_csv("../test_iter_output_cusip.txt", sep="|")
sim_df = pd.read_csv("../test_sim_output.txt", sep="|")
#sim_df = pd.read_csv("/scratch/frbkc/merton_simultaneous_WRDS_1970_1989.txt", sep="|")

year = int(sys.argv[1])
month = int(sys.argv[2])

iter_df['date'] = pd.to_datetime(iter_df['date'])
sim_df['date'] = pd.to_datetime(sim_df['date'])
convds_df['date'] = pd.to_datetime(convds_df['date'])
filtered_iter = iter_df[(iter_df['date'].dt.year == year) & (iter_df['date'].dt.month == month)]
filtered_sim = sim_df[(sim_df['date'].dt.year == year) & (sim_df['date'].dt.month == month)]

merged = convds_df.merge(filtered_iter, on=['gvkey','date'], suffixes=('_GZ', '_iter'))
merged = merged.merge(filtered_sim, on=['gvkey','date'])
print("check duplicates")
print(convds_df.duplicated().sum())
print(filtered_iter.duplicated().sum())
print(filtered_sim.duplicated().sum())
print(merged.duplicated().sum())

print("check permco")
print(convds_df['permco'].duplicated().sum())
print(filtered_iter['permco'].duplicated().sum())
print(filtered_sim['permco'].duplicated().sum())



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

merged = merged.rename(columns={'DD':'DD_sim'})
merged['difference_iter'] = abs(merged['DD_merton'] - merged['DD_iter'])
merged['difference_sim'] = abs(merged['DD_merton'] - merged['DD_sim'])
print(merged.columns)
#maybe permco too?
merged = merged[['DD_merton', 'DD_iter', 'DD_sim', 'difference_iter', 'difference_sim', 'date', 'gvkey', 'permco', 'permno']] 
#print("Null values:\n", merged.isnull().sum())
print("Summary Stats:\n")
print(merged.describe())
print("Data Preview:\n")
print(merged.head())

max_difference_iter = merged[merged['difference_iter'] == merged['difference_iter'].max()]
print("max diff iter:\n", max_difference_iter)

max_outlier = merged[merged['DD_merton'] == merged['DD_merton'].max()]
print("Maxoutlier DD Merton:\n", max_outlier)
min_outlier = merged[merged['DD_merton'] == merged['DD_merton'].min()]
print("Minoutlier DD Merton:\n", min_outlier)
max_outlier = merged[merged['DD_sim'] == merged['DD_sim'].max()]
print("Maxoutlier DD sim:\n", max_outlier)
min_outlier = merged[merged['DD_sim'] == merged['DD_sim'].min()]
print("Minoutlier DD sim:\n", min_outlier)

print("GZ_replication:", convds_df[convds_df['permco'] == 22326][['date', 'DD', 'DD_merton', 'A', 'E', 'D']])
print("Iter:", iter_df.loc[(iter_df['permco'] == 22326) & (iter_df['date'] == '1988-04-29'), ['date', 'DD', 'A', 'E', 'D']])
print("Sim:", sim_df.loc[(sim_df['permco'] == 22326) & (sim_df['date'] == '1988-04-29'), ['date', 'DD', 'A', 'E', 'D']])


