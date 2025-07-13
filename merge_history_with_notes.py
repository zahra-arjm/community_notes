'''
Code to analyse community notes
- assumes you have scores notes already
- Tom uses the conda environment stored in communitynotes.yml
'''


import pandas as pd
import os
import socket #to get host machine identity

print("identifying host machine")
#test which machine we are on and set working directory
if 'tom' in socket.gethostname():
    os.chdir('/home/tom/Desktop/communitynotes/data2024-12-19')
elif 'zahra' in socket.gethostname():
    os.chdir('/home/zahra/Documents/Tom_Stafford/Community_Notes/analysis/community_notes')
else:
    print("Not sure whose machine we are on. Maybe the script will run anyway...")

print("We are in :" + os.getcwd())



with open('./noteStatusHistory-00000.tsv', 'r') as n:
    note_history = pd.read_csv(n, sep='\t')

notes = pd.read_parquet('./notes.parquet', engine='auto')

# note_history = pd.read_parquet('./note_status_history.parquet', engine='auto')

# print(scored_notes['finalRatingStatus'].unique())

#  merge the two dataframes
df = pd.merge(notes, note_history, how='left', on='noteId')


print(df.columns)


# rename duplicate columns
df.rename(columns={'noteAuthorParticipantId_x': 'noteAuthorParticipantId',
                   }, inplace=True)

# # my suggestions for keeping columns
df_short = df[
    ['noteId',
    # 'finalRatingStatus',
    # 'firstTag',
    # 'secondTag',
    'classification', # what the writer thinks about the original tweet
    # 'createdAtMillis',
    'createdAt',
    'createdAtYear',
    'createdAtMonth',
    # 'numRatings',
    # 'noteTopic', # most of them are nan, but there are 3 unique topics
    # 'topicNoteConfident', # seems to be if the model is confident about topic label
    'noteAuthorParticipantId',
    'tweetId',
    # 'firstNonNMRStatus',
    'currentStatus',
    # 'mostRecentNonNMRStatus',
    # 'lockedStatus',
    # 'coreNoteIntercept', 'coreNoteFactor1', 'coreRatingStatus',
    # 'decidedBy', 
    # 'expansionNoteIntercept', 'expansionNoteFactor1', 'expansionRatingStatus',
    # 'coverageNoteIntercept', 'coverageNoteFactor1', 'coverageRatingStatus',
    # 'coreNoteInterceptMin', 'coreNoteInterceptMax',
    # 'expansionNoteInterceptMin', 'expansionNoteInterceptMax',
    # 'coverageNoteInterceptMin', 'coverageNoteInterceptMax',
    # 'groupNoteIntercept', 'groupNoteFactor1', 'groupRatingStatus',
    # 'groupNoteInterceptMax', 'groupNoteInterceptMin', 
    # 'modelingGroup',
    # 'believable', deprecated!
    # 'harmful', 
    # 'validationDifficulty',
    # 'misleadingOther', 
    # 'misleadingFactualError',
    # 'misleadingManipulatedMedia', 
    # 'misleadingOutdatedInformation',
    # 'misleadingMissingImportantContext', 
    # 'misleadingUnverifiedClaimAsFact',
    # 'misleadingSatire', 
    # 'notMisleadingOther',
    # 'notMisleadingFactuallyCorrect',
    # 'notMisleadingOutdatedButNotWhenWritten', 
    # 'notMisleadingClearlySatire',
    # 'notMisleadingPersonalOpinion', 
    # 'trustworthySources',
    'summary',
    # 'isMediaNote',
    ]
]


# df.to_parquet('rated_notes.parquet')
df_short.to_parquet('notes_current_stat.parquet')

