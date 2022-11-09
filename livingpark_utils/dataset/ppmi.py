"""Provide utilies to work with the PPMI dataset."""
import glob
import os.path
from pprint import pprint

import numpy as np
import pandas as pd
from dateutil.parser import parse  # type: ignore
from dateutil.relativedelta import relativedelta  # type: ignore

from ..download import ppmi


def cohort_id(cohort: pd.DataFrame) -> str:
    """Return a unique id for the cohort.

    The id is built as the hash of the sorted list of patient ids in the cohort.
    Since cohort_ids may be used to create file names, negative signs ('-')
    are replaced with underscore characters ('_') since SPM crashes on file names
    containing negative signs. Therefore, the cohort id is a string that cannot
    be cast to an integer.

    Parameters
    ----------
    cohort: pd.DataFrame
        A Pandas DataFrame with a column named 'PATNO'.

    Returns
    -------
    cohort_id: string
        A string containing the unique id of the cohort.
    """
    return str(hash(tuple(sorted(cohort["PATNO"])))).replace("-", "_")


def disease_duration(study_data_dir: str, *, force: bool = False) -> pd.DataFrame:
    """Return a DataFrame containing disease durations.

    Returns
    -------
    pd.DataFrame
        DataFrame containing disease durations for each (patient,event) pair found
        in "MDS_UPDRS_Part_III.csv".
    """
    ppmi_downloader = ppmi.Downloader(study_data_dir)
    required_files = ["MDS_UPDRS_Part_III.csv", "PD_Diagnosis_History.csv"]

    missing = ppmi_downloader.missing_study_files(required_files, force=force)
    if len(missing) == 0:
        print("Download skipped: No missing files!")
    else:
        pprint(f"Downloading files: {missing}")
        _, missing = ppmi_downloader.get_study_files(missing)

    pddxdt = pd.read_csv(os.path.join(study_data_dir, "PD_Diagnosis_History.csv"))[
        ["PATNO", "EVENT_ID", "PDDXDT"]
    ]
    pddxdt = pddxdt[(pddxdt["EVENT_ID"] == "SC") & pddxdt["PDDXDT"].notna()]
    pdxdur = pd.read_csv(
        os.path.join(study_data_dir, "MDS_UPDRS_Part_III.csv"),
        low_memory=False,
    )[["PATNO", "EVENT_ID", "INFODT"]]

    PDDXDT_map = dict(zip(pddxdt["PATNO"].values, pddxdt["PDDXDT"].values))
    pdxdur["PDDXDT"] = pdxdur["PATNO"].map(PDDXDT_map)

    pdxdur["PDXDUR"] = pdxdur.apply(
        lambda row: relativedelta(parse(row["INFODT"]), parse(row["PDDXDT"])).months
        if row["PDDXDT"] is not np.nan
        else np.nan,
        axis=1,
    )
    pdxdur.drop(labels=["INFODT", "PDDXDT"], inplace=True, axis=1)

    return pdxdur


def clean_protocol_description(desc: str) -> str:
    """Create valid protocol description for file names (as done by PPMI).

    Parameters
    ----------
    str
        Protocol description. Example: "MPRAGE GRAPPA"
    """
    return desc.replace(" ", "_").replace("(", "_").replace(")", "_").replace("/", "_")


def find_nifti_file_in_cache(
    subject_id: str,
    event_id: str,
    protocol_description: str,
    *,
    cache_dir: str = ".cache",
    base_dir: str = "inputs",
) -> str:
    """Return cached nifti files, if any.

    Search for nifti file matching `subject_id`, `event_id` and
    `protocol_description` in the cache directory.
    If not found, search for nifti file matching `subject_id` and `event_id` only,
    and return it if a single file is found.

    Parameters
    ----------
    subject_id: str
        Subject ID
    event_id: str
        Event ID. Example: BL
    protocol_description: str
        Protocol description. Example: "MPRAGE GRAPPA"

    Returns
    -------
    str:
        File name matching the `subject_id`, `event_id`, and if possible
        `protocol_description`. Empty string if no matching file is found.
    """
    expression = os.path.join(
        cache_dir,
        base_dir,
        f"sub-{subject_id}",
        f"ses-{event_id}",
        "anat",
        f"PPMI_*{clean_protocol_description(protocol_description)}*.nii",
    )
    files = glob.glob(expression)
    assert len(files) <= 1, f"More than 1 Nifti file matched by {expression}"
    if len(files) == 1:
        return files[0]

    expression = os.path.join(
        cache_dir,
        base_dir,
        f"sub-{subject_id}",
        f"ses-{event_id}",
        "anat",
        "PPMI_*.nii",
    )
    files = glob.glob(expression)
    assert len(files) <= 1, f"More than 1 Nifti file matched by {expression}"
    if len(files) == 1:
        return files[0]

    return ""
