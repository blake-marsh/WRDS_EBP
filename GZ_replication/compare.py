import pandas as pd

convds_df = pd.read_csv("/scratch/frbkc/convds_output.csv")
iter_df = pd.read_csv("/scratch/frbkc/merton_iterated_WRDS_1970_1989.txt", sep="|")
sim_df = pd.read_csv("/scratch/frbkc/merton_simultaneous_WRDS_1970_1989.txt", sep="|")


convds = set(convds_df['permco'].dropna().astype(int))
iterds = set(iter_df['permco'].dropna().astype(int))
sim = set(sim_df['permco'].dropna().astype(int))
common = convds & iterds & sim

#print(common)

#print("GZ_replication", convds_df[convds_df['permco'] == 22326][['date', 'DD', 'DD_merton', 'A', 'E', 'D']])
#print("Iter:", iter_df[iter_df['permco'] == 22326][['date', 'DD', 'PD', 'A', 'E', 'D']])
print(iter_df.loc[(iter_df['permco'] == 22326) & (iter_df['date'] == '1988-04-29'), ['date', 'DD', 'A', 'E', 'D']])
#print("Sim:", sim_df[sim_df['permco'] ==22326][['date', 'DD', 'PD', 'A', 'E', 'D']])


