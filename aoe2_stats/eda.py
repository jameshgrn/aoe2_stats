#%%
import pandas as pd
import os
os.chdir('/Users/jakegearon/CursorProjects/aoe2_stats')
df = pd.read_csv('data/aoe_data.csv', index_col=0)
print(df.info())
# %%
