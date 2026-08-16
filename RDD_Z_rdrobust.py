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

len(rated) #1,946,619 for 2025 data # 2747570 for 2026 data

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

print("- - - - - - - - OLS RDD regression")


# prepare the common vars for all models
# RDD 
cutoff = 0.4 #threshold at which a note is rated as helpful
bandwidth = 0.087 #window around cutoff
# keep only notes with enough rating
first_notes = first_notes[first_notes['rating_group'] == 1]

# only aligned notes 

running_variable = first_notes['intercept']

# # add note factor
# factor = np.where(
#     first_notes["decidedBy"] == "CoreModel (v1.1)",
#     first_notes["coreNoteFactor1"],
#     first_notes["expansionNoteFactor1"]
# )
# first_notes['factor'] = factor

# Treatment = 1 if running_variable >= cutoff, else 0
treatment = np.where(running_variable >= cutoff, 1, 0)
outcome_variable = first_notes['if_written_again'].astype(int)
# keep when intercept and locked status align
# keep when intercept and locked status align & factor is less than .5
# mask_alignment = (((first_notes["intercept"] > cutoff) & 
# (first_notes["finalRatingStatus"] == "CURRENTLY_RATED_HELPFUL") &
# (abs(first_notes["factor"]) < .5)) |
#     ((first_notes["intercept"] < cutoff) & (first_notes["finalRatingStatus"] != "CURRENTLY_RATED_HELPFUL")))
mask_alignment = (((first_notes["intercept"] > cutoff) & (first_notes["finalRatingStatus"] == "CURRENTLY_RATED_HELPFUL")) |
    ((first_notes["intercept"] < cutoff) & (first_notes["finalRatingStatus"] != "CURRENTLY_RATED_HELPFUL")))

mask = (running_variable > cutoff - bandwidth)  & (running_variable < cutoff + bandwidth)
data = pd.DataFrame({
    'running_variable': running_variable[mask & mask_alignment],
    'treatment': treatment[mask & mask_alignment],
    'outcome_variable': outcome_variable[mask & mask_alignment]
})

# Center the running variable at the cutoff for easier interpretation of the intercept
data['running_variable_centered'] = data['running_variable'] - cutoff
print(f"\nAligned data, current score")
print(f"\nNumber of treated units: {data['treatment'].sum()}")
print(f"Number of control units: {data['treatment'].count() - data['treatment'].sum()}")
# original
rdd_model_formula = 'outcome_variable ~ running_variable_centered * treatment'


# Without data sanitisation this doesn't run
if False:
    rdd_model = smf.ols(formula=rdd_model_formula, data=data).fit()


# data inspection
data['running_variable_centered'].dtype # dtype('O') <- problem!

# but each individual value is a float
data['running_variable_centered'].apply(type).value_counts()

# let's just convert to float
data = data.astype({'running_variable_centered': float})

# Now the RDD runs
if True:
    rdd_model = smf.ols(formula=rdd_model_formula, data=data).fit()
# # assuming the model runs you can run the rest


# The coefficient for 'treatment' is the estimated effect at the cutoff.
estimated_effect = rdd_model.params['treatment']
print(f"\nEstimated Treatment Effect at the Cutoff: {estimated_effect:.4f}")

print("I think we can intepret this as a % change the chance of authoring again if published")

p_value_treatment = rdd_model.pvalues['treatment']
t_statistic_treatment = rdd_model.tvalues['treatment']

# print out p value and t-statistic for treatment
print(f"\nP-value for treatment: {p_value_treatment:.4f}")
print(f"T-statistic for treatment: {t_statistic_treatment:.4f}")

print("- - - - - - - - rdrobust regression")

# pip install rdrobust # or update conda env

from rdrobust import rdrobust, rdplot

fn = first_notes.copy()
fn = fn[mask_alignment]   # optional; mirror your current choice

fn['intercept'] = pd.to_numeric(fn['intercept'], errors='coerce')
fn = fn.dropna(subset=['intercept', 'if_written_again']) #no effect, no nans?

y = fn['if_written_again'].astype(int).to_numpy()
x = fn['intercept'].to_numpy(dtype=float)
c = 0.4

# --- Headline: MSE-optimal bandwidth, triangular kernel, local linear (defaults) ---
fit = rdrobust(y=y, x=x, c=c)
print(fit)                              # full CCT-style table
h_mse = float(fit.bws.iloc[0, 0])       # selected h (left; symmetric by default)
print(f"\nMSE-optimal h = {h_mse:.4f}")

#report the bias-corrected point estimate with the robust 95% CI
print(f"\nBias-corrected point estimate: {fit.coef.iloc[1, 0]:.4f}  "
      f"95% CI [{fit.ci.iloc[2, 0]:.4f}, {fit.ci.iloc[2, 1]:.4f}]")

# --- Bandwidth sensitivity: h/2, h, 2h ---
# Report bias-corrected point + robust 95% CI (rows 1 and 2 of coef/ci).
print("\nBandwidth sensitivity (bias-corrected point, robust 95% CI):")
h_mse = float(fit.bws.iloc[0, 0])
b_mse = float(fit.bws.iloc[1, 0])   # b lives in row 1 of bws
for mult in (0.5, 1.0, 2.0):
    h=h_mse*mult
    b= b_mse*mult # b and h covary so that rho stays fixed (as dervied from intial fit)
    r = rdrobust(y=y, x=x, c=c,
                 h=h, b=b)
    tau  = float(r.coef.iloc[0, 0]) # note that tau is for the conventional model
    lo   = float(r.ci.iloc[2, 0]) #but the CI is for the robust model
    hi   = float(r.ci.iloc[2, 1])
    nL, nR = int(r.N_h[0]), int(r.N_h[1])
    print(f"  h = {h:.4f} ({mult:.1f}x)  tau = {tau:+.4f}  "
          f"95% CI [{lo:+.4f}, {hi:+.4f}]  N_L/N_R = {nL}/{nR}")

# --- Paper figure: bin-scatter with local-linear fits either side ---
rdplot(y=y, x=x, c=c, kernel='triangular', p=1,
       title='RDD: publication threshold at 0.4',
       x_label='note intercept (current score)',
       y_label='P(author writes again)')

'''Mephi suggested text "At the MSE-optimal bandwidth we estimate τ̂ = 6.3pp in the probability of writing a second note (conventional local-linear estimator; robust bias-corrected 95% CI [+1.0, +11.1], p = 0.02)."
'''

#references for this method
'''
@article{calonico2014robust,
  title   = {Robust Nonparametric Confidence Intervals for Regression-Discontinuity Designs},
  author  = {Calonico, Sebastian and Cattaneo, Matias D. and Titiunik, Rocio},
  journal = {Econometrica},
  volume  = {82},
  number  = {6},
  pages   = {2295--2326},
  year    = {2014},
  doi     = {10.3982/ECTA11757}
}

@article{calonico2018effect,
  title   = {On the Effect of Bias Estimation on Coverage Accuracy in Nonparametric Inference},
  author  = {Calonico, Sebastian and Cattaneo, Matias D. and Farrell, Max H.},
  journal = {Journal of the American Statistical Association},
  volume  = {113},
  number  = {522},
  pages   = {767--779},
  year    = {2018},
  doi     = {10.1080/01621459.2017.1285776}
}

@article{calonico2014rdrobust,
  title   = {Robust Data-Driven Inference in the Regression-Discontinuity Design},
  author  = {Calonico, Sebastian and Cattaneo, Matias D. and Titiunik, Rocio},
  journal = {The Stata Journal},
  volume  = {14},
  number  = {4},
  pages   = {909--946},
  year    = {2014},
  doi     = {10.1177/1536867X1401400413}
}

'''


#Next, manipulation test/density smoothness at cutoff 

'''if people can select to be just above cutoff then density will not be smooth
(think the pass grade on an exam)'''


#pip install rddensity

from rddensity import rddensity
dens = rddensity(X=x, c=0.4)

print(dens.test)   # T-stat and p-value; three rows for VCE variants (conventional / TW / jackknife)
print(dens.hat)    # left/right density point estimates and the jump
print(dens.h)      # bandwidths h_L, h_R
print(dens.n)      # effective N on each side within bandwidth
print(dens.bino)   # binomial-count local test at the cutoff (defaults on)

print([a for a in dir(dens) if not a.startswith('_')])


from rddensity import rdplotdensity

import numpy as np
mask = (x >= 0.3) & (x <= 0.5)
x_near = x[mask]

# Direct matplotlib histogram — most informative
import matplotlib.pyplot as plt
plt.hist(x_near, bins=50, edgecolor='black')
plt.axvline(0.4, color='red', linestyle='--')
plt.xlabel('score'); plt.ylabel('count')
plt.show()

# Donut RDD
mask = (x < 0.395) | (x > 0.405)
r_donut = rdrobust(y=y[mask], x=x[mask], c=0.4)
print(r_donut)

# Note at 0
print((x == 0).sum(), "notes at exactly 0")
print((x == 0).mean() * 100, "% of full sample")
