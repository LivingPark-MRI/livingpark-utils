"""Helper functions for Zeighami et al. notebooks."""
from pathlib import Path

import pandas as pd
from ppmi_downloader import PPMIDownloader

from .. import livingpark_utils
from .constants import COL_DATE_INFO
from .constants import COL_IMAGING_PROTOCOL
from .constants import COL_PAT_ID
from .constants import COL_STATUS
from .constants import COL_VISIT_TYPE
from .constants import COLS_DATE
from .constants import FIELD_STRENGTHS
from .constants import FILENAME_PARTICIPANT_STATUS
from .constants import IDA_COLNAME_MAP
from .constants import IDA_VISIT_MAP
from .constants import MAIN_COHORT
from .constants import MAX_DATES
from .constants import MIN_DATES
from .constants import STATUS_GROUPS
from .constants import VALIDATION_COHORT
from .constants import VISIT_BASELINE


def load_ppmi_csv(
    utils: livingpark_utils.LivingParkUtils,
    filename: str,
    from_ida_search: bool = False,
    convert_dates: bool = True,
    alternative_dir: str = ".",
    cols_to_impute: str | list | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Load PPMI csv file as a pandas dataframe.

    Parameters
    ----------
    utils : livingpark_utils.LivingParkUtils
        the notebook's LivingParkUtils instance
    filename : str
        name of file to be loaded
    from_ida_search : bool, optional
        if True, column names and values will be converted from IDA format
        to match the other PPMI study files, by default False
    convert_dates : bool, optional
        if True, date columns will be converted to pd.datetime format, by default True
    alternative_dir : str, optional
        fallback directory if file is not found in utils.study_files_dir, by default "."
    cols_to_impute : str | list, optional
        column(s) where missing values should be imputed with the mean, by default None
    **kwargs : optional
        additional keyword arguments to be passed to convert_date_cols()

    Returns
    -------
    pd.DataFrame
        loaded/preprocessed dataframe

    Raises
    ------
    FileNotFoundError
        file not found in either utils.study_files_dir or alternative_dir
    RuntimeError
        IDA format conversion issue
    """
    filepath = Path(utils.study_files_dir, filename)

    if not filepath.exists():
        filepath = Path(alternative_dir, filename)
    if not filepath.exists():
        raise FileNotFoundError(
            f"File {filename} is not in either "
            f"{utils.study_files_dir} or {alternative_dir}"
        )
    df_ppmi = pd.read_csv(filepath)

    # convert IDA search results to the same format as other PPMI study files
    if from_ida_search:
        df_ppmi = df_ppmi.rename(columns=IDA_COLNAME_MAP)
        df_ppmi[COL_PAT_ID] = pd.to_numeric(df_ppmi[COL_PAT_ID], errors="coerce")

        # convert visit code
        missing_keys = set(df_ppmi[COL_VISIT_TYPE]) - set(IDA_VISIT_MAP.keys())
        if len(missing_keys) != 0:
            raise RuntimeError(f"Missing keys in conversion map: {missing_keys}")
        df_ppmi[COL_VISIT_TYPE] = df_ppmi[COL_VISIT_TYPE].map(IDA_VISIT_MAP)

    if convert_dates:
        df_ppmi = convert_date_cols(df_ppmi, **kwargs)

    if cols_to_impute is not None:
        df_ppmi = mean_impute(df_ppmi, cols_to_impute)

    return df_ppmi


def convert_date_cols(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    """Convert date columns from str to pandas datetime type.

    Parameters
    ----------
    df : pd.DataFrame
        input dataframe
    cols : list, optional
        list of date columns to convert

    Returns
    -------
    pd.DataFrame
        dataframe with converted columns
    """
    if cols is None:
        cols = COLS_DATE
    for col in cols:
        try:
            df[col] = pd.to_datetime(df[col])
        except KeyError:
            continue
    return df


def filter_date(
    df: pd.DataFrame,
    max_date=None,
    min_date=None,
    dayfirst=True,
    col_date: str = COL_DATE_INFO,
) -> pd.DataFrame:
    """Filter a dataframe based on values in a date columns.

    Parameters
    ----------
    df : pd.DataFrame
        input dataframe
    max_date : str or datetime, optional
        maximum date to keep
    min_date : str or datetime, optional
        minimum date to keep
    dayfirst : bool, optional
        passed to pd.datetime(), by default True
    col_date : str, optional
        reference column for filtering,
        by default value stored in COL_DATE_INFO

    Returns
    -------
    pd.DataFrame
        filtered dataframe
    """
    if type(min_date) == str:
        min_date = pd.to_datetime(min_date, dayfirst=dayfirst)
    if type(max_date) == str:
        max_date = pd.to_datetime(max_date, dayfirst=dayfirst)

    df = convert_date_cols(df, cols=[col_date])

    if min_date is not None:
        df = df.loc[df[col_date] >= min_date]
    if max_date is not None:
        df = df.loc[df[col_date] <= max_date]
    return df


def mean_impute(df: pd.DataFrame, cols: str | list) -> pd.DataFrame:
    """Impute missing values with the mean.

    Parameters
    ----------
    df : pd.DataFrame
        input dataframe
    col : str | list
        columns to impute

    Returns
    -------
    pd.DataFrame
        dataframe with imputed missing values
    """
    if type(cols) == str:
        cols = [cols]

    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col].isna(), col] = df[col].mean()

    return df


def get_t1_cohort(
    utils: livingpark_utils.LivingParkUtils,
    filename: str,
    cohort_name: str = MAIN_COHORT,
    sagittal_only=True,
) -> pd.DataFrame:
    """Extract base main or validation cohort for Zeighami et al. papers.

    Parameters
    ----------
    utils : livingpark_utils.LivingParkUtils
        the notebook's LivingParkUtils instance
    filename : str
        name of 3D T1 search result file.
        This file will be downloaded if it doesn't exist
    cohort_name : str, optional
        must match MAIN_COHORT or VALIDATION_COHORT,
        by default value stored in MAIN_COHORT
    sagittal_only : bool, optional
        whether to only use sagittal scans, by default True

    Returns
    -------
    pd.DataFrame
        dataframe with 3D T1 information for selected cohort

    Raises
    ------
    ValueError
        invalid value for cohort_name parameter
    RuntimeError
        duplicate subjects in output dataframe
    """
    valid_cohort_names = [MAIN_COHORT, VALIDATION_COHORT]
    if cohort_name not in valid_cohort_names:
        raise ValueError(
            f"Invalid cohort_name: {cohort_name}. "
            f"Valid values are: {valid_cohort_names}"
        )

    min_date = MIN_DATES[cohort_name]
    max_date = MAX_DATES[cohort_name]
    field_strength = FIELD_STRENGTHS[cohort_name]
    status_groups = STATUS_GROUPS[cohort_name]

    dirname = utils.study_files_dir
    filepath = Path(dirname, filename)

    # download and move file if it doesn't exist yet
    if not filepath.exists():
        downloader = PPMIDownloader()
        filename_tmp = downloader.download_3D_T1_info(destination_dir=dirname)
        Path(dirname, filename_tmp).rename(filepath)

    # load csv files
    df_t1 = load_ppmi_csv(utils, filename, from_ida_search=True)
    df_t1 = df_t1.drop_duplicates()  # some rows are identical
    df_status = load_ppmi_csv(utils, FILENAME_PARTICIPANT_STATUS)

    # drop subjects with NA for ID (conversion to numerical value failed)
    df_t1_subset = df_t1
    df_t1_subset = df_t1_subset.dropna(axis="index", subset=[COL_PAT_ID])

    # filter by date
    df_t1_subset = filter_date(df_t1_subset, max_date=max_date, min_date=min_date)

    # filter by field strength
    df_t1_subset = df_t1_subset.loc[
        df_t1[COL_IMAGING_PROTOCOL].str.contains(f"Field Strength={field_strength}")
    ]

    # filter plane
    if sagittal_only:
        df_t1_subset = df_t1_subset.loc[
            df_t1[COL_IMAGING_PROTOCOL].str.contains("Acquisition Plane=SAGITTAL")
        ]

    # only keep PD patients and healthy controls
    # and only use baseline scans
    df_t1_subset = df_t1_subset.merge(df_status, on=COL_PAT_ID)
    df_t1_subset = df_t1_subset.loc[
        (df_t1_subset[COL_STATUS].isin(status_groups))
        & (df_t1_subset[COL_VISIT_TYPE] == VISIT_BASELINE)
    ]

    # some subjects in the validation cohort have repeat scans on the same day
    # these scans have the word "Repeat" in the description column
    # we keep the repeat scan
    subject_counts = df_t1_subset[COL_PAT_ID].value_counts()
    duplicate_subjects = subject_counts.loc[subject_counts != 1].index
    print(f"Removing extra scans for {len(duplicate_subjects)} subjects")
    # print(df_t1_subset.loc[df_t1_subset[COL_PAT_ID].isin(duplicate_subjects)])

    if cohort_name == VALIDATION_COHORT:
        df_t1_subset = df_t1_subset.loc[
            ~df_t1_subset[COL_PAT_ID].isin(duplicate_subjects)
            | df_t1_subset["Description"].str.contains("Repeat")
        ]
    else:
        # description of repeat scans
        # ['AX 3D FSPGR straight brain lab',
        #  'sag 3D FSPGR BRAVO straight']       -> keep sagittal
        # ['T1W_3D_FFE COR', 'T1W_3D_FFE AX']   -> unsure which to keep
        # ['MPRAGE GRAPPA_ND', 'MPRAGE GRAPPA'] -> unsure which to keep
        df_t1_subset = df_t1_subset.loc[
            (~df_t1_subset[COL_PAT_ID].isin(duplicate_subjects))
            | (
                (~df_t1_subset["Description"].str.contains("AX"))
                & (~df_t1_subset["Description"].str.contains("_ND"))
            )
        ]

    if df_t1_subset[COL_PAT_ID].nunique() != len(df_t1_subset[COL_PAT_ID]):
        raise RuntimeError(f"Duplicate subjects in {cohort_name} cohort")

    return df_t1_subset
