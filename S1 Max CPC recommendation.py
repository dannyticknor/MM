import pandas as pd
####here to import data from Quicksight and cpc from Google Ads
data = pd.read_csv('/Users/faye/Downloads/Max_of_Target_campai_1626384608136.csv')
cpc = pd.read_csv('/Users/faye/Downloads/Search keyword report (1) 2.csv',sep = "\,,/",header=None)
#####data clean
cpc.columns=['cname']
cpc = cpc.cname.str.split(",",expand=True)
cpc = cpc.iloc[2:] 
cpc = cpc.reset_index()
new_header = cpc.iloc[0]
cpc = cpc[1:]
cpc.columns = new_header
cpc = cpc[['Keyword','Keyword ID','Campaign','Campaign ID','Ad group ID','Max. CPC']]
import numpy as np
result = result.rename(columns={'Max. CPC':'target_cur_max_cpc'})
result = result[result.target_cur_max_cpc != '--']
result = result.fillna(0)
####algorithm
def f(row):
    if row['target_conversions_total'] >= 5:
        if row['legacy_cpc'] != 0 and row['target_conversion_value_per_click'] != 0:
            val = min(1.1*(row['legacy_cpc']),1.1*row['target_conversion_value_per_click'])
        elif row['legacy_cpc'] == 0 and row['target_conversion_value_per_click'] != 0:
            val = 1.1*row['target_conversion_value_per_click']
        else:
            val = 1.1*row['legacy_cpc']
    else:
        val = max(1.2*row['legacy_cpc'],1.2*float(row['target_cur_max_cpc']))
    return val
result['Max. CPC'] = result.apply(f,axis=1)
result['Max. CPC'] = result['Max. CPC'].round(2)
fin = result[['Keyword','Account ID','Campaign ID','Ad group ID','Keyword ID','Max. CPC']]
####make sure account accounts are what you expected and output the result 
account = ['Account ID']
d={i:  y for i , (x , y) in enumerate(fin.groupby(account))}
for i in range(len(d)):
    d[i].to_csv('/Users/faye/Downloads/JE-Vehicles-' + str(i) + ' KW_Bids_Bulk Upload_715.csv', index= None)
