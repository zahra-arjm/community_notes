# extract 100 random notes from helpful and unhelpful ones

import pandas as pd

notes = pd.read_parquet('rated_notes_compact.parquet', engine='auto')

helpful = notes\
    [(notes['noteTopic'].values == 'GazaConflict') \
         & (notes['language'] == 'en') \
         & (notes['finalRatingStatus'] == 'CURRENTLY_RATED_HELPFUL')]\
    [['summary', 'finalRatingStatus']].sample(n=100)

notHelpful = notes\
    [(notes['noteTopic'].values == 'GazaConflict') \
         & (notes['language'] == 'en') \
         & (notes['finalRatingStatus'] == 'CURRENTLY_RATED_NOT_HELPFUL')]\
    [['summary', 'finalRatingStatus']].sample(n=100)

df = pd.concat([helpful, notHelpful])

df.to_csv('Gaza_notes_200.csv')

