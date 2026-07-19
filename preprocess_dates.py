import pandas as pd
from datetime import datetime

# with open('./notes-00000.tsv', 'r') as n:
#     notes = pd.read_csv(n, sep='\t')

# in 2026 we had multiple notes and I merged them into 1 parquet file
notes = pd.read_parquet('./data2026proc/notes.parquet', engine='auto')

# create a column with datetime format
notes['createdAt'] = pd.to_datetime(notes['createdAtMillis'], unit='ms')
# add a columns for month and year
# notes[]
notes['createdAtYear'] = notes['createdAt'].dt.year
notes['createdAtMonth'] = notes['createdAt'].dt.month

# save in parquet format
notes.to_parquet('./data2026proc/notes.parquet')