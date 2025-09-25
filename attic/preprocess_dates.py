import pandas as pd
from datetime import datetime

with open('./notes-00000.tsv', 'r') as n:
    notes = pd.read_csv(n, sep='\t')


# create a column with datetime format
notes['createdAt'] = pd.to_datetime(notes['createdAtMillis'], unit='ms')
# add a columns for month and year
# notes[]
notes['createdAtYear'] = notes['createdAt'].dt.year
notes['createdAtMonth'] = notes['createdAt'].dt.month

# save in parquet format
notes.to_parquet('notes.parquet')