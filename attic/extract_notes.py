# extract 100 random notes from helpful and unhelpful ones

import pandas as pd

notes = pd.read_parquet('rated_notes_compact.parquet', engine='auto')

helpful = notes\
    [(notes['noteTopic'].values == 'GazaConflict') \
         & (notes['language'] == 'en') \
         & (notes['finalRatingStatus'] == 'CURRENTLY_RATED_HELPFUL')]\
    [['summary', 'finalRatingStatus', 'tweetId']].sample(n=100, random_state=2025)

notHelpful = notes\
    [(notes['noteTopic'].values == 'GazaConflict') \
         & (notes['language'] == 'en') \
         & (notes['finalRatingStatus'] == 'CURRENTLY_RATED_NOT_HELPFUL')]\
    [['summary', 'finalRatingStatus', 'tweetId']].sample(n=100, random_state=2025)

df = pd.concat([helpful, notHelpful])

# change tweedId type to show the complete number
df['tweetId'] = df['tweetId'].astype(int)

df.to_csv('Gaza_notes_200.csv')

