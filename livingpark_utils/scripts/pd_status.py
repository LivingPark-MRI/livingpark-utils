#!/usr/bin/env python
# coding: utf-8

# # 👋 Introduction

# The medication status of PD patients is important as medication importantly affects clinical measures such as the Hoehn & Yahr score used in many studies. In PPMI, medication is available through the following main variables:
# * PDSTATE (ON/OFF): the current functional state of the patient
# * PDTRTMNT (0/1): 1 if the participant is on PD medication or receives deep brain stimulation, 0 otherwise
# * ON/OFFPDMEDTM: time of most recent PD medication dose
# * ON/OFFPDMEDDT: date of most recent PD medication dose
# 
# As mentioned in the "Methods for Defining PD Med Use" in PPMI study data, OFF state requires that the last dose of levodopa or dopamine agonist was taken 6 hours or more before MDS-UPDRS Part III assessment.
# 
# The goal of this notebook is (1) to identify and correct inconsistencies among these variables, (2) to impute missing data for PDSTATE and PDTRTMNT, and (3) to check the sanity of the corrected dataset.

# In[1]:


from IPython.display import HTML

HTML(
    """<script>
code_show=true; 
function code_toggle() {
 if (code_show){
 $('div.input').hide();
 } else {
 $('div.input').show();
 }
 code_show = !code_show
} 
$( document ).ready(code_toggle);
</script>
<form action="javascript:code_toggle()"><input type="submit" value="Click here to toggle on/off the Python code."></form>"""
)


# In[2]:


import datetime
import warnings

import pytz


# warnings.filterwarnings('ignore')

now = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S %Z %z")
print(f"This notebook was run on {now}")


# # 🔽 Data download
# 
# The above-mentioned variables are available in PPMI file `MDS_UPDRS_Part_III.csv`. To download this file, we will use package `ppmi-downloader` available on PyPi. The package will ask you for your PPMI login and password.

# In[3]:


from livingpark_utils.download import ppmi
import livingpark_utils
import pandas as pd
import os

utils = livingpark_utils.LivingParkUtils()
utils.notebook_init()

updrs_file_name = "MDS-UPDRS_Part_III.csv"
downloader = ppmi.Downloader(utils.study_files_dir, headless=False)
utils.get_study_files([updrs_file_name], default=downloader)
df = pd.read_csv(os.path.join(utils.study_files_dir, updrs_file_name))

print("File downloaded")


# # Assign exam date and time for PDSTATE ON and OFF.

# In[4]:


df = df.merge(
    df[df["PDSTATE"] == "OFF"][
        ["PATNO", "EVENT_ID", "EXAMDT", "EXAMTM", "PDMEDDT", "PDMEDTM"]
    ].rename(
        columns={
            "EXAMDT": "OFFEXAMDT",
            "EXAMTM": "OFFEXAMTM",
            "PDMEDDT": "OFFPDMEDDT",
            "PDMEDTM": "OFFPDMEDTM",
        }
    ),
    how="left",
    on=("PATNO", "EVENT_ID"),
).merge(
    df[df["PDSTATE"] == "ON"][
        ["PATNO", "EVENT_ID", "EXAMDT", "EXAMTM", "PDMEDDT", "PDMEDTM"]
    ].rename(
        columns={
            "EXAMDT": "ONEXAMDT",
            "EXAMTM": "ONEXAMTM",
            "PDMEDDT": "ONPDMEDDT",
            "PDMEDTM": "ONPDMEDTM",
        }
    ),
    how="left",
    on=("PATNO", "EVENT_ID"),
)


# # ⁉️ Inconsistencies

# ## PDTRTMNT=0 and PDSTATE=ON
# 
# <div class="alert alert-block alert-danger">
#      	&#10060; <b>Problem:</b> a few records have PDSTATE=ON and PDTRTMNT=0, which is inconsistent:
# </div>

# In[5]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# In[6]:


errors = df[(df["PDSTATE"] == "ON") & (df["PDTRTMNT"] == 0)]
# print the time difference between EXAMTM and PDMEDTM
# (pd.to_datetime(errors["ONEXAMTM"]) - pd.to_datetime(errors["ONPDMEDTM"]))


# <div class="alert alert-block alert-success">
#      	&#10003; <b>Proposed correction</b>: set PDTRTMNT to 1 for these records. It doesn't seem realistic that a plausible PDMEDTM and PDSTATE=ON have been entered by mistake while the patient was not under medication.
# </div>

# ⚙️ Implementation
# 
# 

# In[7]:


df.loc[(df["PDSTATE"] == "ON") & (df["PDTRTMNT"] == 0), "PDTRTMNT"] = 1


# Let's verify that the inconsistency is now resolved:

# In[8]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# ## EVENT_ID = SC and PDTRTMNT = 1

# <div class="alert alert-block alert-danger">
#      	&#10060; <b>Problem</b>: Some patients were on medication at screening time while PPMI patients were supposed to be unmedicated at screening time.
#     </div>

# Number of patients on medication at screening time:

# In[9]:


len(df[(df["EVENT_ID"] == "SC") & (df["PDTRTMNT"] == 1)])


# <div class="alert alert-block alert-success">
#      	&#10003; <b>Proposed correction:</b> keep the records. Maybe the patients started medication between recruitment and screening time.
# </div>

# ## PDSTATE=ON and EXAMTM<PDMEDTM

# <div class="alert alert-block alert-danger">
#      	&#10060; <b>Problem</b>: Some records have PDSTATE=ON but PDMEDTM is after EXAMTM. 
#     </div>

# Number of records where PDSTATE=ON and EXAMTM<PDMEDTM:

# In[10]:


# ON records
on = df[df["PDSTATE"] == "ON"]


def to_secs(x):
    """
    Convert time from hh:mm to seconds since midnight

    x: time in hh:ss format
    return: number of seconds elapsed since midnight
    """
    if str(x) == "nan":
        import numpy as np

        return np.NaN
    try:
        hour, mn, sec = x.split(":")
    except Exception as e:
        print(f'Cannot process "{x}"')
        raise (e)
    return int(hour) * 3600 + int(mn) * 60 + int(sec)


on["delta"] = on["ONEXAMTM"].apply(to_secs) - on["ONPDMEDTM"].apply(to_secs)
len(on[on["delta"] < 0])


# <div class="alert alert-block alert-success">
#      	&#10003; <b>Proposed correction:</b> discard the records. 
# </div>

# Note: for some of these records, medication date was likely on the day before the exam, although this cannot be verified since dates only contain a year and a month but no day. Even in such cases, duration between medication time and exam time was more than 6 hours which is inconsistent with the rule used for the other records.

# ⚙️ Implementation

# In[11]:


before = len(df)
df = df[
    ~(
        (df["PDSTATE"] == "ON")
        & df["ONEXAMTM"].notnull()
        & df["ONPDMEDTM"].notnull()
        & (df["ONEXAMTM"] < df["ONPDMEDTM"])
    )
]
print(f"Removed {before-len(df)} records where PDSTATE=ON and EXAMTM<PDMEDTM")


# ## Visits with 3 exams

# <div class="alert alert-block alert-danger">
#     	&#10060; <b>Problem</b>: some visits have 3 exams while a maximum of two exams per visit are expected, one in OFF state and one in ON state.
# </div>

# Number of records that belong to a visit with more than 3 exams:

# In[12]:


pb = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
len(pb)


# Each exams triple has an exam with missing ONEXAMTM, missing OFFEXAMTM, and missing PDSTATE:

# In[13]:


pb = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
pb_trunc = pb[["EVENT_ID", "PDSTATE", "ONEXAMTM", "OFFEXAMTM"]]
from IPython.display import HTML

HTML(pb_trunc.to_html(index=False))


# <div class="alert alert-block alert-success">
#   	&#10003;   <b>Proposed correction:</b> remove exam with ONEXAMTM=NaN and OFFEXAMTM=NaN and PDSTATE=NaN when visit has 3 exams.
# </div>

# ⚙️ Implementation

# In[14]:


a = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
index = (
    a[(a["PDSTATE"].isnull()) & (a["ONEXAMTM"].isnull()) & (a["OFFEXAMTM"].isnull())]
).index

before_len = len(df)
df = df[~df.index.isin(index)]
print(f"Number of removed records: {before_len-len(df)}")


# Let's verify that the inconsistency is solved by counting the number of records that belong to a visit with more than 3 exams:

# In[15]:


pb = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
len(pb)


# There are still records that belong to a visit with more than 3 exams.

# <div class="alert alert-block alert-success">
#   	&#10003;   <b>Proposed correction:</b> remove duplicated records.
# </div>

# ⚙️ Implementation

# In[16]:


before_len = len(df)
df = df[~df.drop(["REC_ID"], axis=1).duplicated(keep="first")]
print(f"Number of removed records: {before_len-len(df)}")


# There are still records that belong to a visit with more than 3 exams. The corresponding visits all have 2 exams with PDSTATE=OFF, however, only one of these visits has HRPOSTMED != NaN. 

# In[17]:


pb = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
pb[["EVENT_ID", "PDSTATE", "HRPOSTMED", "OFFPDMEDDT", "OFFPDMEDTM"]]


# <div class="alert alert-block alert-success">
#   	&#10003;   <b>Proposed correction:</b> remove exam with HRPOSTMED=NaN and PDSTATE=OFF when visit has 3 exams.
# </div>

# ⚙️ Implementation

# In[18]:


a = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
index = (a[(a["HRPOSTMED"].isnull()) & (a["PDSTATE"] == "OFF")]).index

before_len = len(df)
df = df[~df.index.isin(index)]
print(f"Number of removed records: {before_len-len(df)}")


# Let's verify that the inconsistency is solved by counting the number of records that belong to a visit with more than 3 exams:

# In[19]:


pb = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) > 2)
len(pb)


# # 🕵️ Imputation of missing PDSTATE or PDTRTMNT

# <div class="alert alert-block alert-danger">
#     	&#10060; <b>Problem:</b> variables PDSTATE and PDTRTMNT have missing data, which makes it difficult to identify when/if a patient was under medication.
# </div>

# The following table summarizes the number of records for which PDSTATE or PDTRTMNT is missing:

# In[20]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# The following cases will be treated separately in the following sections:
# 
# |               |PDSTATE | PDTRTMNT | Number of records |
# |---------------|--------|----------|-------------------|
# | **Case 1** | NaN    | 0        | 8674          |
# | **Case 2**|  NaN   | 1        | 2               |
# | **Case 3** | NaN    | NaN      | 2318             |

# In[21]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# ## Case 1: PDSTATE=NaN and PDTRTMNT=0
# 
# <div class="alert alert-block alert-success">
#    	&#10003;   <b>Proposed correction</b>: set PDSTATE=OFF. The patient is not medicated and for this reason PDSTATE is likely to not have been entered.
# </div>

# ⚙️ Implementation

# In[22]:


df.loc[(df["PDSTATE"].isnull()) & (df["PDTRTMNT"] == 0), "PDSTATE"] = "OFF"


# Let's verify that case 2 is now resolved:

# In[23]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# ## Case 2: PDSTATE=NaN and PDTRTMNT=1
# 
# <div class="alert alert-block alert-success">
#     	&#10003; <b>Proposed correction</b>: drop the record as there are only 2 of them. 
# </div>

# In[24]:


df = df[~(df["PDSTATE"].isnull()) | (df["PDTRTMNT"] != 1)]


# Updated records distribution:

# In[25]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# ## Case 3: PDSTATE=NaN and PDTRTMNT=NaN
# 
# Similar to case 1, no record in case 3 has a medication date (ON/OFFPDMEDDT), a medication time (ON/OFFPDMEDTM), or
# a DBS status (DBSYN):

# In[26]:


case_3 = df[(df["PDSTATE"].isnull()) & (df["PDTRTMNT"].isnull())]
case_3.groupby(
    [
        "OFFPDMEDDT",
        "OFFPDMEDTM",
        "ONPDMEDDT",
        "ONPDMEDTM",
        "DBSYN",
        "HRPOSTMED",
        "DBSONTM",
        "DBSOFFTM",
        "HRDBSOFF",
        "HRDBSON",
    ],
    dropna=False,
).count()


# <div class="alert alert-block alert-success">
#     	&#10003; <b>Proposed solution</b>: set PDSTATE=OFF and PDTRTMNT=0. It is very unlikely that the patient was medicated and none of the medication-related variables were set.
# </div>

# ⚙️ Implementation

# In[27]:


df_1 = df.copy()
df_1.loc[(df["PDSTATE"].isnull()) & (df["PDTRTMNT"].isnull()), "PDSTATE"] = "OFF"
df_1.loc[(df["PDSTATE"].isnull()) & (df["PDTRTMNT"].isnull()), "PDTRTMNT"] = 0
df = df_1


# Let's verify that case 3 is now resolved:

# In[28]:


df.groupby(["PDSTATE", "PDTRTMNT"], dropna=False)[["REC_ID"]].count()


# There's no remaining missing PDSTATE or PDTRTMNT value in the data!
# 
# Let's save the cleaned file:

# In[29]:


filename = "MDS_UPDRS_Part_III_clean.csv"
df.to_csv(os.path.join(utils.study_files_dir, filename), index=False)
print(f"Cleaned file saved in {filename}")


# # ⚕️ Sanity checks
# 
# The following sanity checks are applied to the cleaned UPDRS-III data.

# **IF** visit has two exam **THEN** one is ON and the other one is OFF:

# In[30]:


a = df.groupby(["PATNO", "EVENT_ID"]).filter(lambda x: len(x) == 2)
a.groupby(["PATNO", "EVENT_ID"]).filter(
    lambda x: x.iloc[[0]]["PDSTATE"].to_string() == x.iloc[[1]]["PDSTATE"].to_string()
).empty


# **IF** PDSTATE=ON **THEN** ONEXAMTM>ONPDMEDTM

# In[31]:


df[
    (df["PDSTATE"] == "ON")
    & (df["ONEXAMTM"].apply(to_secs) < df["ONPDMEDTM"].apply(to_secs))
].empty


# **IF** PDTRTMNT=0 **THEN** there is a single visit and PDSTATE=OFF

# In[32]:


assert (
    df[df["PDTRTMNT"] == 0]
    .groupby(["PATNO", "EVENT_ID"])
    .filter(lambda x: len(x) > 1)
    .empty
), "False!"
assert (
    df[df["PDTRTMNT"] == 0]
    .groupby(["PATNO", "EVENT_ID"])
    .filter(lambda x: x["PDSTATE"] != "OFF")
    .empty
), "False!"
print("True")


# A patient cannot become unmedicated after being medicated:

# In[33]:


def wrong_pairs(x):
    rows = [row for index, row in x.iterrows()]
    for a in rows:
        for b in rows:
            if a["EVENT_ID"] == b["EVENT_ID"]:
                continue
            # If dates are equal, we cannot say anything
            if pd.to_datetime(a["INFODT"]) == pd.to_datetime(b["INFODT"]):
                return False
            # If a is later than b, a['PDTRTMNT'] has to be larger or equal to b['PDTRTMNT']
            if pd.to_datetime(a["INFODT"]) > pd.to_datetime(b["INFODT"]):
                if int(a["PDTRTMNT"]) < int(b["PDTRTMNT"]):
                    return True
                return False
            # a is earlier than b: a['PDTRTMNT'] has to be lower or equal to b['PDTRTMNT']
            if int(a["PDTRTMNT"]) > int(b["PDTRTMNT"]):
                return True
            return False


df.groupby(["PATNO"]).filter(lambda x: x["PDTRTMNT"].nunique() > 1).groupby(
    "PATNO"
).filter(wrong_pairs).empty

