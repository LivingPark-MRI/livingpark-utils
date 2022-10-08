"""Constant variables for Zeighami et al. notebooks."""
# PPMI file names
FILENAME_DEMOGRAPHICS = "Demographics.csv"
FILENAME_AGE = "Age_at_visit.csv"
FILENAME_PARTICIPANT_STATUS = "Participant_Status.csv"
FILENAME_MOCA = "Montreal_Cognitive_Assessment__MoCA_.csv"
FILENAME_UPDRS1A = "MDS-UPDRS_Part_I.csv"
FILENAME_UPDRS1B = "MDS-UPDRS_Part_I_Patient_Questionnaire.csv"  # patient questionnaire
FILENAME_UPDRS2 = "MDS_UPDRS_Part_II__Patient_Questionnaire.csv"
FILENAME_UPDRS3 = "MDS_UPDRS_Part_III.csv"
FILENAME_ADL = "Modified_Schwab___England_Activities_of_Daily_Living.csv"

# other file names
FILENAME_T1_INFO = "3D_T1_info_idaSearch.csv"

# useful column names
COL_PAT_ID = "PATNO"
COL_STATUS = "COHORT_DEFINITION"
COL_VISIT_TYPE = "EVENT_ID"
COL_MRI_COMPLETE = "MRICMPLT"
COL_DATE_INFO = "INFODT"
COLS_DATE = [
    COL_DATE_INFO,
    "LAST_UPDATE",
    "ORIG_ENTRY",
]
COL_PD_STATE = "PDSTATE"
COL_AGE = "AGE_AT_VISIT"
COL_SEX = "SEX"
COL_EDUCATION = "EDUCYRS"
COL_MOCA = "MCATOT"
COL_ADL = "MSEADLG"
COL_UPDRS1A = "NP1RTOT"
COL_UPDRS1B = "NP1PTOT"
COL_UPDRS1 = f"{COL_UPDRS1A}+{COL_UPDRS1B}"
COL_UPDRS2 = "NP2PTOT"
COL_UPDRS3 = "NP3TOT"
# PIGD: Postural Instability and Gait Disturbance score
# computed from UPDRS III measures (Stebbins et al. 2013)
COL_PIGD = "PIGD"
COLS_PIGD_COMPONENTS_UPDRS2 = [
    "NP2WALK",
    "NP2FREZ",
]
COLS_PIGD_COMPONENTS_UPDRS3 = [
    "NP3FRZGT",
    "NP3GAIT",
    "NP3PSTBL",
]
COLS_PIGD_COMPONENTS = COLS_PIGD_COMPONENTS_UPDRS2 + COLS_PIGD_COMPONENTS_UPDRS3
COL_GCO = "GCO"  # global composite outcome
COLS_SCORES_WITHOUT_GCO = [COL_UPDRS2, COL_UPDRS3, COL_ADL, COL_PIGD, COL_MOCA]
COLS_SCORES = COLS_SCORES_WITHOUT_GCO + [COL_GCO]
COL_FOLLOWUP = "is_followup"
COL_IMAGING_PROTOCOL = "Imaging Protocol"  # from IDA search results

# codes for COHORT_DEFINITION field
STATUS_PD = "Parkinson's Disease"
STATUS_HC = "Healthy Control"

# codes for EVENT_ID field
VISIT_BASELINE = "BL"
VISIT_SCREENING = "SC"
REGEX_VISIT_FOLLOWUP = "^V((0[1-9])|(1[0-9])|(20))$"  # V01-V20

# codes for SEX field
SEX_FEMALE = 0
SEX_MALE = 1

# main/validation cohorts for analysis
MAIN_COHORT = "main"
VALIDATION_COHORT = "validation"

# column names/values obtained from searching the Image and Data Archive (IDA)
# are different from those used in the PPMI csv study files
# so they need to be remapped
IDA_COLNAME_MAP = {
    "Subject ID": COL_PAT_ID,
    "Visit": COL_VISIT_TYPE,
    "Study Date": COL_DATE_INFO,
}
IDA_VISIT_MAP = {
    "Baseline": VISIT_BASELINE,
    "Screening": VISIT_SCREENING,
    "Month 12": "V04",
    "Month 24": "V06",
    "Month 36": "V08",
    "Month 48": "V10",
    "Symptomatic Therapy": "ST",
    "Unscheduled Visit 01": "U01",
    "Unscheduled Visit 02": "U02",
    "Premature Withdrawal": "PW",
}

# main/validation cohort filtering parameters
MIN_DATES = {
    MAIN_COHORT: None,
    VALIDATION_COHORT: None,
}
MAX_DATES = {
    MAIN_COHORT: "31/01/2014",
    VALIDATION_COHORT: "31/10/2017",
}
FIELD_STRENGTHS = {
    MAIN_COHORT: "3.0",
    VALIDATION_COHORT: "1.5",
}
STATUS_GROUPS = {
    MAIN_COHORT: [STATUS_PD, STATUS_HC],
    VALIDATION_COHORT: [STATUS_PD],
}
