# region Setup environment


# ------ libraries ------

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
import os
from rdrobust import rdrobust, rdplot
from rddensity import rddensity, rdplotdensity
import json

# ------- environment vars ------

# static
plotloc = 'plots' #subfolder to save plots





# -------- functions ------

def get_first_notes(rated, note_from, note_to, follow_up_from, follow_up_to,cutoff):
    '''function to define data for analysis - running variable, outcome variable, sample defined by date'''

    def model_to_column(s):
        # a helper function to map decidedBy to intercept
        base = s.split("Model")[0]
        return base[:1].lower() + base[1:] + "NoteIntercept"

    # Restricting our analysis to notes after 2024
    rated_2024 = rated[(rated.createdAt >= note_from) &
                        (rated.createdAt <= note_to)]
    # cap our data where we look for First Notes
    rated = rated[(rated.createdAt >= follow_up_from) &
                        (rated.createdAt <= follow_up_to)]
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
    # define outcome variable
    have_another_note = rated_2024[rated_2024.groupby('noteAuthorParticipantId').cumcount() >= 2]\
        ['noteAuthorParticipantId'].to_list()

    # add to primary dataframe
    first_notes['if_written_again'] = first_notes['noteAuthorParticipantId'].isin(have_another_note)
    # filter to only notes written by the 3 major algorithms
    first_notes = first_notes[first_notes['decidedBy']\
        .isin(['CoreModel (v1.1)', 'ExpansionModel (v1.1)', 'CoreWithTopicsModel (v1.1)'])]
    # first_notes = first_notes[first_notes['decidedBy'].isin(['CoreModel (v1.1)'])]
    # remove NNNs
    first_notes = first_notes[first_notes['classification'] != 'NOT_MISLEADING']

    # adding intercept fro them deciding algorithm
    cols = first_notes["decidedBy"].apply(model_to_column)
    first_notes["intercept"] = first_notes.to_numpy()[
    np.arange(len(first_notes)),
    first_notes.columns.get_indexer(cols)
    ]

    # keep only notes with enough rating
    print("Dropping", len(first_notes) - len(first_notes[first_notes['rating_group'] == 1]), "notes with too few ratings")
    first_notes = first_notes[first_notes['rating_group'] == 1]
    

    # keep only aligned notes (current status and final status align)
    
    mask_alignment = (((first_notes["intercept"] > cutoff) & (first_notes["finalRatingStatus"] == "CURRENTLY_RATED_HELPFUL")) |
        ((first_notes["intercept"] < cutoff) & (first_notes["finalRatingStatus"] != "CURRENTLY_RATED_HELPFUL")))
    print("Dropping", len(first_notes) - len(first_notes[mask_alignment]), "notes with different current and final status")
    first_notes = first_notes[mask_alignment]

    print("Now we have", len(first_notes), "Notes")
    print(f"\nNB Aligned data, current score")


    return first_notes

def robust_rdd(x,y,c):
    '''wrapper function to run rdrobust and print results'''
    # declare outinputs
    print("running variable (x) length", len(x))
    print("binary outcome (y) length", len(y))
    print("cutoff (c) is", c)

    # next line is the regression
    fit = rdrobust(y=y, x=x, c=c)
    # now we're just formatting the output
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


# endregion


# region set up data for RDD

# - - - - - import data
# First let's inmport the data and count how many notes we have
rated = pd.read_parquet('./data2026post/' + 'rated_notes_compact.parquet', engine='auto')
# check size
len(rated) # 2747570 for 2026 data (was 1,946,619 for 2025 data)
# For RDD we need to remove the notes which don't have enough rating to be helpful
rated['rating_group'] = np.where(rated['numRatings'] >= 5,1, 0)
# proportion of Notes decided by each algorithm
rated[rated['rating_group'] == 1]['decidedBy'].value_counts(normalize=True)


# define timeranges 
note_from = pd.Timestamp(2024, 1, 1)
note_to = pd.Timestamp(2026, 6, 1)
follow_up_from = pd.Timestamp(2024, 1, 1)
follow_up_to = pd.Timestamp(2026, 6, 1)



# process data to produce running var and outcome var
cutoff = 0.4 #threshold at which a note is rated as helpful, needed to check for status alignment
first_notes = get_first_notes(rated, note_from, note_to, follow_up_from, follow_up_to,cutoff)


# endregion


# region Plots

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

# endregion plots


# region OLS RDD regression

print("- - - - - - - - OLS RDD regression")

# RDD parameters
cutoff = 0.4 #threshold at which a note is rated as helpful
bandwidth = 0.087 #window around cutoff

# data for OLS RDD
running_variable = first_notes['intercept']
# Treatment = 1 if running_variable >= cutoff, else 0
treatment = np.where(running_variable >= cutoff, 1, 0)
outcome_variable = first_notes['if_written_again'].astype(int)
# we only fit data within the window
mask = (running_variable > cutoff - bandwidth)  & (running_variable < cutoff + bandwidth)
data = pd.DataFrame({
    'running_variable': running_variable[mask],
    'treatment': treatment[mask],
    'outcome_variable': outcome_variable[mask]
})

# Center the running variable at the cutoff for easier interpretation of the intercept
data['running_variable_centered'] = data['running_variable'] - cutoff
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

# endregion



# region rdrobust regression

print("- - - - - - - - rdrobust regression")

# pip install rdrobust # or update conda env



fn = first_notes.copy()
fn = fn[mask_alignment]   # optional; mirror your current choice

fn['intercept'] = pd.to_numeric(fn['intercept'], errors='coerce')
fn = fn.dropna(subset=['intercept', 'if_written_again']) #no effect, no nans?

y = fn['if_written_again'].astype(int).to_numpy()
x = fn['intercept'].to_numpy(dtype=float)
c = 0.4

# --- Headline: MSE-optimal bandwidth, triangular kernel, local linear (defaults) ---
robust_rdd(x,y,c)


# --- Paper figure: bin-scatter with local-linear fits either side ---



STEM = 'RDDrobust_scatter'
STEM = os.path.join(plotloc, STEM)

out = rdplot(y=y, x=x, c=c, kernel='triangular', p=1, h=0.35,
             binselect='es', ci=95,
             title='RDD: publication threshold at 0.4',
             x_label='note intercept (current score)',
             y_label='P(author writes again)')

# ---- figure ----
p = getattr(out, 'rdplot', out)
if hasattr(p, 'save'):                      # plotnine ggplot
    p.save(f'{STEM}.png', dpi=120, width=7, height=5, verbose=False)
else:                                       # matplotlib figure
    p.savefig(f'{STEM}.png', dpi=120, bbox_inches='tight')

# ---- underlying data ----
bins = out.vars_bins.copy()                 # binned sample means (the dots)
poly = out.vars_poly.copy()                 # local polynomial fit (the lines)

# tag which side of the cutoff each row sits on
bins['side'] = (bins['rdplot_mean_bin'] >= c).map({True: 'right', False: 'left'})
poly['side'] = (poly['rdplot_x'] >= c).map({True: 'right', False: 'left'})

bins['layer'], poly['layer'] = 'bin', 'fit'
tidy = pd.concat([bins, poly], ignore_index=True)
tidy['cutoff'] = c
tidy.to_csv(f'{STEM}_data.csv', index=False)

print(bins.columns.tolist())                # sanity check on names
print(f'{STEM}.png + {STEM}_data.csv written ({len(bins)} bins, {len(poly)} fit points)')

meta = {
    'cutoff': c, 'p': 1, 'kernel': 'triangular',
    'J_left': int(np.ravel(out.J)[0]), 'J_right': int(np.ravel(out.J)[-1]),
    'J_IMSE': np.ravel(out.J_IMSE).tolist(),
    'J_MV': np.ravel(out.J_MV).tolist(),
    'scale': np.ravel(out.scale).tolist(),
    'bin_avg': np.ravel(out.bin_avg).tolist(),
    'bin_med': np.ravel(out.bin_med).tolist(),
    'n_obs': int(len(x)),
}
with open(f'{STEM}_meta.json', 'w') as f:
    json.dump(meta, f, indent=2)

# polynomial coefficients (left/right fits) as their own table
coef = pd.DataFrame(out.coef)
coef.to_csv(f'{STEM}_coef.csv')

# ---- readout ----
print(f"\nRD plot at c = {meta['cutoff']}  (p={meta['p']}, {meta['kernel']}, N={meta['n_obs']})")
print(f"  bins used   : {meta['J_left']} left / {meta['J_right']} right")
print(f"  J_IMSE      : {meta['J_IMSE']}")
print(f"  J_MV        : {meta['J_MV']}")
print(f"  scale       : {meta['scale']}")
print(f"  bin width   : avg {meta['bin_avg']}, median {meta['bin_med']}")
print("\ncoefficients:")
print(coef.to_string())
print(f"\nwrote {STEM}_meta.json, {STEM}_coef.csv")
'''Mephi suggested text "At the MSE-optimal bandwidth we estimate τ̂ = 6.3pp in the probability of writing a second note (conventional local-linear estimator; robust bias-corrected 95% CI [+1.0, +11.1], p = 0.02)."
'''

# Matplot lib version of figure

out = rdplot(y=y, x=x, c=c, kernel='triangular', p=1,
             binselect='es', ci=95,
             title='RDD: publication threshold at 0.4',
             x_label='note intercept (current score)',
             y_label='P(author writes again)')

# ---- figure ----
p = getattr(out, 'rdplot', out)


fig, ax = plt.subplots(figsize=(7, 4.5))

for side, col in [('left', '#4C72B0'), ('right', '#4C72B0')]:
    b = bins[bins['side'] == side]
    f = poly[poly['side'] == side]

    ax.scatter(b['rdplot_mean_x'], b['rdplot_mean_y'],
               s=6 + 30 * (b['rdplot_N'] / b['rdplot_N'].max()),
               alpha=0.25, color=col, edgecolors='none', zorder=2)
    ax.plot(f['rdplot_x'], f['rdplot_y'], color='#C44E52', lw=2, zorder=3)

ax.axvline(c, color='0.35', lw=1, ls='--', zorder=1)

ax.set_xlabel('note intercept (current score)')
ax.set_ylabel('P(author writes again)')
ax.set_title('RDD: publication threshold at 0.4', loc='left', fontsize=11)
ax.spines[['top', 'right']].set_visible(False)
ax.margins(x=0.02)


fig.savefig(f'{STEM}_mpl.png', dpi=300, bbox_inches='tight')
fig.savefig(f'{STEM}_mpl.pdf', bbox_inches='tight')   # vector, for the paper



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
# endregion

# region density test

#Next, manipulation test/density smoothness at cutoff 

'''if people can select to be just above cutoff then density will not be smooth
(think the pass grade on an exam)'''


#pip install rddensity

dens = rddensity(X=x, c=0.4)

print(dens.test)   # T-stat and p-value; three rows for VCE variants (conventional / TW / jackknife)
print(dens.hat)    # left/right density point estimates and the jump
print(dens.h)      # bandwidths h_L, h_R
print(dens.n)      # effective N on each side within bandwidth
print(dens.bino)   # binomial-count local test at the cutoff (defaults on)

print([a for a in dir(dens) if not a.startswith('_')])


import numpy as np
mask = (x >= 0.3) & (x <= 0.5)
x_near = x[mask]

# Direct matplotlib histogram — most informative
import matplotlib.pyplot as plt
plt.hist(x_near, bins=50, edgecolor='black')
plt.axvline(0.4, color='red', linestyle='--')
plt.xlabel('score'); plt.ylabel('count')
plt.show()

# endregion


# region Donut RDD


# Donut RDD

# Assumes: x = running variable (current score), y = binary outcome (next note)
# Cutoff at 0.4.

donut_widths = [0.005, 0.010, 0.020]
rows = []

# Baseline (no donut) for the first row of the table
r0 = rdrobust(y=y, x=x, c=0.4)
rows.append({
    "donut": "none",
    "n_eff_L": int(r0.N_h[0]),
    "n_eff_R": int(r0.N_h[1]),
    "h": round(float(r0.bws.iloc[0, 0]), 3),
    "coef_conv": round(float(r0.coef.iloc[0, 0]), 4),
    "se_conv":   round(float(r0.se.iloc[0, 0]),   4),
    "p_robust":  round(float(r0.pv.iloc[2, 0]),   4),
    "ci_robust_lo": round(float(r0.ci.iloc[2, 0]), 4),
    "ci_robust_hi": round(float(r0.ci.iloc[2, 1]), 4),
})

for w in donut_widths:
    mask = (x < 0.4 - w) | (x > 0.4 + w)
    xd, yd = x[mask], y[mask]
    r = rdrobust(y=yd, x=xd, c=0.4)
    rows.append({
        "donut": f"±{w:.3f}",
        "n_eff_L": int(r.N_h[0]),
        "n_eff_R": int(r.N_h[1]),
        "h": round(float(r.bws.iloc[0, 0]), 3),
        "coef_conv": round(float(r.coef.iloc[0, 0]), 4),
        "se_conv":   round(float(r.se.iloc[0, 0]),   4),
        "p_robust":  round(float(r.pv.iloc[2, 0]),   4),
        "ci_robust_lo": round(float(r.ci.iloc[2, 0]), 4),
        "ci_robust_hi": round(float(r.ci.iloc[2, 1]), 4),
    })

df = pd.DataFrame(rows)
print(df.to_string(index=False))


# endregion


# region RDD sensitivity with different cutoffs

c=0.3;robust_rdd(x,y,c) # all ns and inconsistent effect directions
c=0.35;robust_rdd(x,y,c) # all ns and inconsistent effect directions
mask=x>0.4
c=0.45;robust_rdd(x[mask],y[mask],c) # all ns and inconsistent effect directions
c=0.4 # setting it back for sanity

# Converting rate to baseline outcome rate

base_all = y.mean()
mask_local = (x >= 0.4 - 0.087) & (x < 0.4)
base_local = y[mask_local].mean()
print(f"sample-wide: {base_all:.3%}, local-untreated: {base_local:.3%}")

'''Mephi suggested text: Untreated authors at the cutoff re-author at 50.7%; treated authors at the cutoff re-author at ~57.0%. Treatment effect = 6.3pp, a ~12.4% relative lift over the counterfactual baseline.'''

# region date split sensisitivty check




# endregion