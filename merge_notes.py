import pandas as pd

dfs = []

for idx in range(3):
    dfs.append(
        pd.read_csv("./data2026raw/notes-0000" + str(idx) + ".tsv", \
         sep="\t"))
   
notes = pd.concat(dfs, ignore_index=True)
print(len(notes))

notes.to_parquet("./data2026proc/notes.parquet")