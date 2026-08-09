import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import pickle
import calendar
from sklearn import linear_model
import statsmodels.api as sm
import statsmodels.formula.api as smf
import fastparquet

# First let's inmport the data anbd count how many notes we have

# Tom thinkpad location
# rated = pd.read_parquet('/home/tomstafford/Desktop/communitynotes/data2025post/' + 'rated_notes_compact.parquet', engine='auto')

# Tom laptop location
#rated = pd.read_parquet('/home/tom/Desktop/communitynotes/data2025post/' + 'rated_notes_compact.parquet', engine='auto')

rated = pd.read_parquet('./data2026post/' + 'rated_notes_compact.parquet', engine='auto')

len(rated) #1,946,619 for 2025 data

# For RDD we need to remove the notes which don't have enough rating to be helpful
rated['rating_group'] = np.where(rated['numRatings'] >= 5,
                                        1, 0)

rated[rated['rating_group'] == 1]['decidedBy'].value_counts(normalize=True)

# Restricting our analysis to notes after 2024
rated_2024 = rated[(rated.createdAt >= pd.Timestamp(2024, 1, 1)) &
                    (rated.createdAt <= pd.Timestamp(2026, 6, 1))]
# cap our data 
rated = rated[(rated.createdAt <= pd.Timestamp(2026, 6, 1))]
# Find the first note written by each author, and if that author goes on to write another note
first_notes = rated.groupby('noteAuthorParticipantId')\
    [['createdAt', 'finalRatingStatus', 'numRatings', 'rating_group',
     'decidedBy','coreNoteFactor1', 'coreNoteIntercept', 'coreNoteInterceptMax',
     'expansionNoteFactor1', 'expansionNoteIntercept', 'expansionNoteInterceptMax',
     'coreWithTopicsNoteFactor1', 'coreWithTopicsNoteIntercept', 'coreWithTopicsNoteInterceptMax',
      'classification']]\
    .first().reset_index()
# Restricting our analysis to authors who joined in 2024 or 2025 
first_notes = first_notes[(first_notes.createdAt >= pd.Timestamp(2024, 1, 1)) &
                          (first_notes.createdAt < pd.Timestamp(2026, 1, 1))]
have_another_note = rated_2024[rated_2024.groupby('noteAuthorParticipantId').cumcount() >= 2]\
    ['noteAuthorParticipantId'].to_list()
first_notes['if_written_again'] = first_notes['noteAuthorParticipantId'].isin(have_another_note)
first_notes = first_notes[first_notes['decidedBy']\
    .isin(['CoreModel (v1.1)', 'ExpansionModel (v1.1)', 'CoreWithTopicsModel (v1.1)'])]
# first_notes = first_notes[first_notes['decidedBy'].isin(['CoreModel (v1.1)'])]
# remove NNNs
first_notes = first_notes[first_notes['classification'] != 'NOT_MISLEADING']

# a function to map decidedBy to intercept
def model_to_column(s):
    base = s.split("Model")[0]
    return base[:1].lower() + base[1:] + "NoteIntercept"

cols = first_notes["decidedBy"].apply(model_to_column)

first_notes["intercept"] = first_notes.to_numpy()[
np.arange(len(first_notes)),
first_notes.columns.get_indexer(cols)
]

# Most notes have a small number of rating, a small number have very many ratinhgs
# This means the distribution of ratings is very skewed.
# We take the log of the number of ratings to make the distribution more normal


first_notes['log_numRatings']=np.log(first_notes['numRatings'])
# some don't even have 1 rating
first_notes[first_notes['numRatings'] > 0]['log_numRatings'].hist() #normal
plt.xlabel('log(numRatings)')
plt.ylabel('count')
# plt.show()

# Plotting log(numRatings) vs NoteInterceptMax, with colours according to Note final statuys (finalRatingStatus )  
plt.clf()
groups = ['CURRENTLY_RATED_HELPFUL','NEEDS_MORE_RATINGS','CURRENTLY_RATED_NOT_HELPFUL']
colours=['green','orange','red']

ms=3;alphaval=0.1;


for i in range(3):
    plt.scatter(first_notes[first_notes['finalRatingStatus']==groups[i]]['log_numRatings'], first_notes[first_notes['finalRatingStatus']==groups[i]]['intercept'], s = ms,alpha=alphaval,color=colours[i],label=groups[i])  
    # j+=1

plt.xlabel('log Number of ratings')
plt.ylabel('note intercept')
plt.title('Number of ratings vs note intercept')
plt.legend(loc=0)
plt.savefig('scatter_numRatings_NoteIntercept.png',dpi=120,bbox_inches='tight')
# plt.show()

#TODO make the legend markers bigger and clearer

# prepare the common vars for all models
# RDD 
cutoff = 0.4 #threshold at which a note is rated as helpful
bandwidth = 0.1 #window around cutoff
# keep only notes with enough rating
first_notes = first_notes[first_notes['rating_group'] == 1]

# all notes
# most recent score

running_variable = first_notes['intercept']
is_na_mask = first_notes['intercept'].isna()
# Treatment = 1 if running_variable >= cutoff, else 0
treatment = np.where(running_variable >= cutoff, 1, 0)

#
outcome_variable = first_notes['if_written_again'].astype(int) # y value, binary output

# we can restrict the analysis to a window (bandwidth) around the cutoff.

mask = (running_variable > cutoff - bandwidth)  & (running_variable < cutoff + bandwidth)
mask_alignment = (((first_notes["intercept"] > cutoff) & (first_notes["finalRatingStatus"] == "CURRENTLY_RATED_HELPFUL")) |
    ((first_notes["intercept"] < cutoff) & (first_notes["finalRatingStatus"] != "CURRENTLY_RATED_HELPFUL")))

data = pd.DataFrame({
    'running_variable': running_variable[mask & ~is_na_mask & mask_alignment],
    'treatment': treatment[mask & ~is_na_mask & mask_alignment],
    'outcome_variable': outcome_variable[mask & ~is_na_mask & mask_alignment]
})

# Center the running variable at the cutoff for easier interpretation of the intercept
data['running_variable_centered'] = data['running_variable'] - cutoff
print("All data, current score")
print(f"\nNumber of treated units: {data['treatment'].sum()}")
print(f"Number of control units: {data['treatment'].count() - data['treatment'].sum()}")
# original
rdd_model_formula = 'outcome_variable ~ running_variable_centered * treatment'

rdd_model = smf.ols(formula=rdd_model_formula, data=data).fit()

# The coefficient for 'treatment' is the estimated effect at the cutoff.
estimated_effect = rdd_model.params['treatment']
print(f"\nEstimated Treatment Effect at the Cutoff: {estimated_effect:.4f}")

print("I think we can intepret this as a % change the chance of authoring again if published")

p_value_treatment = rdd_model.pvalues['treatment']
t_statistic_treatment = rdd_model.tvalues['treatment']

# print out p value and t-statistic for treatment
print(f"\nP-value for treatment: {p_value_treatment:.4f}")
print(f"T-statistic for treatment: {t_statistic_treatment:.4f}")

