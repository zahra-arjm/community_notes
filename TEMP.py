# TEMP script to demonstrate value range without numRating criteria


####
# FIRST IMPORT NOTES BUT SKIP
# 1. Removing by numRatings criteria
# 2. Only looking at the top three models


# - - - - - import data 
# First let's inmport the data and count how many notes we have
rated = pd.read_parquet('./data2026post/' + 'rated_notes_compact.parquet', engine='auto')
# check size
print("Imported data for " + str(len(rated)) + " notes") # 2747570 for 2026 data (was 1,946,619 for 2025 data)


## UPDATE This now accounts for the change in minimum number of ratings need before a Note published
rated['rating_group'] = np.where((rated.createdAt >= pd.Timestamp(2025,4,8)) & (rated['numRatings'] >= 10) |
                                    (rated.createdAt < pd.Timestamp(2025,4,8)) & (rated['numRatings'] >= 5), 1, 0)

# define timeranges for Author's first created note
note_from = pd.Timestamp(2024, 1, 1)
note_to = pd.Timestamp(2026, 6, 2)
#(follow up can be at any point)


#now generate first_notes outside of function, so we can do things differently

# below here is copied from get_first_notes function

# ----> we still need this
def model_to_column(s):
    # a helper function to map decidedBy to intercept
    base = s.split("Model")[0]
    return base[:1].lower() + base[1:] + "NoteIntercept"

# Find the first note written by each author, and if that author goes on to write another note
first_notes = rated.groupby('noteAuthorParticipantId')\
    [['createdAt', 'finalRatingStatus', 'numRatings', 'rating_group',
    'decidedBy','coreNoteFactor1', 'coreNoteIntercept', 'coreNoteInterceptMax',
    'expansionNoteFactor1', 'expansionNoteIntercept', 'expansionNoteInterceptMax',
    'coreWithTopicsNoteFactor1', 'coreWithTopicsNoteIntercept', 'coreWithTopicsNoteInterceptMax',
    'classification']]\
    .first().reset_index()
print("Now we have " + str(len(first_notes)) + " first time note authors (all time)")
# Restricting our analysis to authors who joined in 2024 or 2025 
first_notes = first_notes[(first_notes.createdAt >= note_from) &
                        (first_notes.createdAt <= note_to)]

print("Now we have " + str(len(first_notes)) + " first time note authors " + note_from.strftime("%Y-%m-%d") + " to " + note_to.strftime("%Y-%m-%d"))

# define outcome variable

# Restricting our analysis to notes after 2024
rated_2024 = rated[(rated.createdAt >= pd.Timestamp(2024,1,1))]

have_another_note = rated_2024[rated_2024.groupby('noteAuthorParticipantId').cumcount() >= 2]\
    ['noteAuthorParticipantId'].to_list()

# add to primary dataframe
first_notes['if_written_again'] = first_notes['noteAuthorParticipantId'].isin(have_another_note)

# filter to only notes written by the 3 major algorithms
first_notes = first_notes[first_notes['decidedBy']\
    .isin(['CoreModel (v1.1)', 'ExpansionModel (v1.1)', 'CoreWithTopicsModel (v1.1)'])]    
print("Now we have " + str(len(first_notes)) + " notes written by the 3 major algorithms")

# remove NNNs
first_notes = first_notes[first_notes['classification'] != 'NOT_MISLEADING']
## NNNs are not misleading, the remainder are classified as misleading
print("Now we have " + str(len(first_notes)) + " notes classified as MISLEADING")

# adding intercept fro them deciding algorithm
cols = first_notes["decidedBy"].apply(model_to_column)
first_notes["intercept"] = first_notes.to_numpy()[
np.arange(len(first_notes)),
first_notes.columns.get_indexer(cols)
]

# ----> skipping this
# keep only notes with enough rating
#print("...Dropping", len(first_notes) - len(first_notes[first_notes['rating_group'] == 1]), "notes with too few ratings")
#first_notes = first_notes[first_notes['rating_group'] == 1]
#print("Now we have " + str(len(first_notes)) + " notes which have enough ratings")

# keep only aligned notes (current status and final status align)
   
mask_alignment = (((first_notes["intercept"] > cutoff) & (first_notes["finalRatingStatus"] == "CURRENTLY_RATED_HELPFUL")) |
    ((first_notes["intercept"] < cutoff) & (first_notes["finalRatingStatus"] != "CURRENTLY_RATED_HELPFUL")))
#print("Dropping", len(first_notes) - len(first_notes[mask_alignment]), "notes with different current and final status")
first_notes = first_notes[mask_alignment]
print("Now we have " + str(len(first_notes)) + " aligned notes") #130522

####
# SECOND - DIAGNOSTICS

first_notes.groupby('finalRatingStatus')['intercept'].min()

# -> the lowest intercept for a 'CURRENTLY_RATED_HELPFUL' Note is 0.400019
# ie only Notes with intercept>0.4 are 'CURRENTLY_RATED_HELPFUL'

first_notes.groupby('finalRatingStatus')['intercept'].max()

# -> the highest intercept for a 'NEEDS_MORE_RATINGS' Note is 0.399994
# ie only Notes with intercept<0.4 are 'NEEDS_MORE_RATINGS'
