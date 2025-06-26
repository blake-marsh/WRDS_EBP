import pandas as pd
import sys

convds_df = pd.read_csv("/scratch/frbkc/convds_output.csv")
iter_df = pd.read_csv("/scratch/frbkc/merton_iterated_WRDS_1970_1989.txt", sep="|")
sim_df = pd.read_csv("/scratch/frbkc/merton_simultaneous_WRDS_1970_1989.txt", sep="|")

year = int(sys.argv[1])
month = int(sys.argv[2])

iter_df['date'] = pd.to_datetime(iter_df['date'])
sim_df['date'] = pd.to_datetime(sim_df['date'])
filtered_iter = iter_df[(iter_df['date'].dt.year == year) & (iter_df['date'].dt.month == month)]
filtered_sim = sim_df[(sim_df['date'].dt.year == year) & (sim_df['date'].dt.month == month)]


merged = convds_df.merge(filtered_iter, on='permco', suffixes=('_GZ', '_iter'))
merged = merged.merge(filtered_sim, on='permco')

merged = merged.rename(columns={'DD':'DD_sim'})
merged['difference_iter'] = abs(merged['DD_merton'] - merged['DD_iter'])
merged['difference_sim'] = abs(merged['DD_merton'] - merged['DD_sim'])
merged = merged[['DD_merton', 'DD_iter', 'DD_sim', 'difference_iter', 'difference_sim']] 
print("Null values:\n", merged.isnull().sum())
print("Summary Stats:\n", merged.describe())
print("Data Preview:\n", merged.head())

print("GZ_replication:", convds_df[convds_df['permco'] == 22326][['date', 'DD', 'DD_merton', 'A', 'E', 'D']])
print("Iter:", iter_df.loc[(iter_df['permco'] == 22326) & (iter_df['date'] == '1988-04-29'), ['date', 'DD', 'A', 'E', 'D']])
print("Sim:", sim_df.loc[(sim_df['permco'] == 22326) & (sim_df['date'] == '1988-04-29'), ['date', 'DD', 'A', 'E', 'D']])


