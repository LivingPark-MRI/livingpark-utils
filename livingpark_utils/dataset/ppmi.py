"""Provide utilies to work with the PPMI dataset."""
import datetime
import logging
import os.path
import re
from pathlib import Path
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


def disease_duration(
    study_data_dir: str,
    *,
    force: bool = False,
    _minimal=True,
) -> pd.DataFrame:
    """Return a DataFrame containing disease durations.

    Parameters
    ----------
    study_data_dir: str
        Directory path containing the study files.
    force: bool
        Whether the download for PD history and UPDRS III should be forced,
        by default False.
    _minimal: bool
        Whether the extras dataframe columns are dropped, by default True.

    Returns
    -------
    pd.DataFrame
        DataFrame containing disease durations for each (patient,event) pair found
        in "MDS_UPDRS_Part_III.csv".
    """

    def abs_month_diff(x: datetime.datetime, y: datetime.datetime, /) -> int:
        """Return the absolute month difference between two dates.

        Parameters
        ----------
        x : datetime.datetime
            First date.
        y : datetime.datetime
            Second date.

        Returns
        -------
        int
            Absolute month difference.
        """
        delta = relativedelta(x, y)
        return abs(delta.years * 12) + abs(delta.months)

    ppmi_downloader = ppmi.Downloader(study_data_dir)
    required_files = ["MDS-UPDRS_Part_III.csv", "PD_Diagnosis_History.csv"]

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
        os.path.join(study_data_dir, "MDS-UPDRS_Part_III.csv"),
        low_memory=False,
    )[["PATNO", "EVENT_ID", "INFODT"]]

    PDDXDT_map = dict(zip(pddxdt["PATNO"].values, pddxdt["PDDXDT"].values))
    pdxdur["PDDXDT"] = pdxdur["PATNO"].map(PDDXDT_map)

    pdxdur["PDXDUR"] = pdxdur.apply(
        lambda row: abs_month_diff(parse(row["INFODT"]), parse(row["PDDXDT"]))
        if row["PDDXDT"] is not np.nan
        else np.nan,
        axis=1,
    )
    if _minimal:
        pdxdur.drop(labels=["INFODT", "PDDXDT"], inplace=True, axis=1)

    return pdxdur


def clean_protocol_description(desc: str) -> str:
    """Create valid protocol description for file names (as done by PPMI).

    Parameters
    ----------
    str
        Protocol description. Example: "MPRAGE GRAPPA"
    """
    return re.sub(r"[\s()/]", "_", desc)


def find_nifti_file_in_cache(
    subject_id: str,
    event_id: str,
    description: str,
    *,
    cache_dir: str = ".cache",
    base_dir: str = "inputs",
    debug: bool = True,
) -> str:
    """Return cached nifti files, if any.

    Search for nifti file matching `subject_id`, `event_id` and
    `description` in the cache directory.

    Parameters
    ----------
    subject_id: str
        Subject ID
    event_id: str
        Event ID. Example: BL
    description: str
        Protocol description. Example: "MPRAGE GRAPPA"

    Returns
    -------
    str:
        File name matching the `subject_id`, `event_id`, and if possible
        `description`. Empty string if no matching file is found.
    """
    expression = f"PPMI_{subject_id}_{clean_protocol_description(description)}.nii.gz"
    filenames = list(
        Path(cache_dir, base_dir, f"sub-{subject_id}", f"ses-{event_id}", "anat").rglob(
            expression
        )
    )

    match len(filenames):
        case 0:
            if debug:
                logging.warning(
                    f"""No Nifti file matched by {expression}
{subject_id=}
{event_id=}
description={clean_protocol_description(description)}
"""
                )
        case 1:
            return filenames[0].as_posix()
        case _:
            if debug:
                logging.warning(
                    f"""More than 1 Nifti file matched by {expression}
{subject_id=}
{event_id=}
description={clean_protocol_description(description)}
"""
                )

    return ""
