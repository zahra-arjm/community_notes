import pandas as pd
import os
import socket #to get host machine identity

print("identifying host machine")
#test which machine we are on and set working directory
if 'tom' in socket.gethostname():
    print('Tom! please set the path first. The code may run anyway')
elif 'zahra' in socket.gethostname():
    os.chdir('/home/zahra/Documents/Tom_Stafford/Community_Notes/analysis/community_notes')
else:
    print("Not sure whose machine we are on. Maybe the script will run anyway...")

print("We are in :" + os.getcwd())


# use parquet format for faster loading
scored_notes = pd.read_parquet('./scored_notes.parquet', engine='auto')

with open('./notes-00000.tsv', 'r') as n:
    notes = pd.read_csv(n, sep='\t')
    
# scored_notes = pd.read_csv('./scored_notes.tsv',
#                             sep='\t')

# print(scored_notes['finalRatingStatus'].unique())

#  filter the enough rated notes and merge the two dataframes
df = pd.merge(scored_notes[scored_notes['finalRatingStatus'] != 'NEEDS_MORE_RATINGS'],
            notes, how='left', on='noteId')

# check columns and keep the useful ones
print(df.columns)

# # my suggestions for keeping columns
df_short = df[
    ['noteId',
    'finalRatingStatus',
    'firstTag',
    'secondTag',
    'classification_x', # what the writer thinks about the original tweet
    'createdAtMillis_x',
    'numRatings',
    'noteTopic', # most of them are nan, but there are 3 unique topics
    'topicNoteConfident', # seems to be if the model is confident about topic label
    'noteAuthorParticipantId',
    'tweetId',
    # 'believable', deprecated!
    # 'harmful', 
    # 'validationDifficulty',
    'misleadingOther', 
    'misleadingFactualError',
    'misleadingManipulatedMedia', 
    'misleadingOutdatedInformation',
    'misleadingMissingImportantContext', 
    'misleadingUnverifiedClaimAsFact',
    'misleadingSatire', 
    'notMisleadingOther',
    'notMisleadingFactuallyCorrect',
    'notMisleadingOutdatedButNotWhenWritten', 
    'notMisleadingClearlySatire',
    'notMisleadingPersonalOpinion', 
    'trustworthySources',
    'summary',
    'isMediaNote',]
]

df.to_parquet('rated_notes.parquet')
df_short.to_parquet('rated_notes_compact.parquet')

