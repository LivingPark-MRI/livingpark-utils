"""Provide utility function for the LivingPark notebook for Mak et al papers."""
from collections import defaultdict
from itertools import combinations

import pandas as pd

visit2month = {
    "BL": 0,
    "V01": 3,
    "V02": 6,
    "V03": 9,
    "V04": 12,
    "V05": 18,
    "V06": 24,
    "V07": 30,
    "V08": 36,
    "V09": 42,
    "V10": 48,
    "V11": 54,
    "V12": 60,
    "V13": 72,
    "V14": 84,
    "V15": 96,
    "V16": 108,
    "V17": 120,
    "V18": 132,
    "V19": 144,
    "V20": 156,
}


def find_visit_pairs(months: int) -> dict:
    """Return the pairs of visits closest to each other.

    Visit pairs are found given a target time difference in months.

    Parameters
    ----------
    months: int
        number of months between baseline and
        follow-up

    Returns
    -------
    dict
        pairs of visits (EVENT_ID in PPMI dataset)
        closest to each other, given the number of
        months between baseline and follow-up
    """
    diff = float("inf")
    diff_hist: dict = defaultdict(dict)

    for (k, v), (k_, v_) in combinations(visit2month.items(), 2):
        if (diff_ := abs(abs(v - v_) - months)) <= diff:
            diff = diff_
            diff_hist[diff][k] = k_

    return diff_hist[diff]


def find_acceptable_visit_pairs(pd_df: pd.DataFrame, hc_df: pd.DataFrame) -> None:
    """Print the number of unique PD-MCI, PD-NC and HC subjects per visit pairs.

    Parameters
    ----------
    pd_df: pd.DataFrame
        DataFrame containing data ingested
        for PD subjects
    hc_df: pd.DataFrame
        DataFrame containing data ingested
        for healthy controls

    Returns
    -------
    None
    """
    events = ["BL", "V04", "V06", "V08", "V10"]

    print("Unique PD-MCI subjects per visit pairs:")
    for c in combinations(events, 2):
        v0 = set(
            pd_df[(pd_df["EVENT_ID"] == c[0]) & (pd_df["COGSTATE"] == 2)][
                "PATNO"
            ].values
        )
        v1 = set(pd_df[(pd_df["EVENT_ID"] == c[1])]["PATNO"].values)
        if len(v0 & v1):
            print(
                f"{c[0]:3} & {c[1]:3} = {len(v0 & v1):>3}"
                f" | Month difference: \
                {visit2month[c[1]] - visit2month[c[0]]}"
            )

    print("\nUnique PD-NC subjects per visit pairs:")
    for c in combinations(events, 2):
        v0 = set(
            pd_df[(pd_df["EVENT_ID"] == c[0]) & (pd_df["COGSTATE"] == 1)][
                "PATNO"
            ].values
        )
        v1 = set(pd_df[(pd_df["EVENT_ID"] == c[1])]["PATNO"].values)
        if len(v0 & v1):
            print(
                f"{c[0]:3} & {c[1]:3} = {len(v0 & v1):>3}"
                f" | Month difference: \
                {visit2month[c[1]] - visit2month[c[0]]}"
            )

    print("\nUnique HC subjects per visit pairs:")
    for c in combinations(events, 2):
        v0 = set(hc_df[(hc_df["EVENT_ID"] == c[0])]["PATNO"].values)
        v1 = set(hc_df[(hc_df["EVENT_ID"] == c[1])]["PATNO"].values)
        if len(v0 & v1):
            print(
                f"{c[0]:3} & {c[1]:3} = {len(v0 & v1):>3}"
                f" | Month difference: \
                {visit2month[c[1]] - visit2month[c[0]]}"
            )


def create_cohort(*, pd_df: pd.DataFrame, hc_df: pd.DataFrame, months: int) -> tuple:
    """Print the number of unique PD-MCI, PD-NC and HC subjects per visit pairs.

    Parameters
    ----------
    pd_df: pd.DataFrame
        DataFrame containing data ingested
        for PD subjects
    hc_df: pd.DataFrame
        DataFrame containing data ingested
        for healthy controls
    months: int
        number of months between baseline and
        follow-up

    Returns
    -------
    tuple
        three replication cohorts for PD-MCI, PD-NC
        and HC subjects given the number of months
        between baseline and follow-up
    """

    def sample_cohort(df, /, *, n):
        _df = df.drop_duplicates(subset=["PATNO"])
        n = min(_df.index.size, n)
        return _df.sample(n=n, replace=False, random_state=1)

    visit_pairs = find_visit_pairs(months)
    visit_df = pd_df.copy()
    visit_df["NEXT_VISIT"] = visit_df["EVENT_ID"].map(visit_pairs)

    visit_df = visit_df.merge(
        visit_df.drop(
            ["AGE_AT_VISIT", "SEX", "NEXT_VISIT", "EDUCYRS"],
            axis=1,
        ),
        left_on=[
            "PATNO",
            "NEXT_VISIT",
        ],
        right_on=[
            "PATNO",
            "EVENT_ID",
        ],
        suffixes=(None, "_NX"),
    ).drop_duplicates()

    mci = sample_cohort(visit_df[visit_df["COGSTATE"] == 2], n=36)
    nc = sample_cohort(
        visit_df[
            (visit_df["COGSTATE"] == 1) & ~visit_df["PATNO"].isin(mci["PATNO"].unique())
        ],
        n=64,
    )

    visit_df_ = hc_df.copy()
    visit_df_["NEXT_VISIT"] = visit_df_["EVENT_ID"].map(visit_pairs)
    visit_df_ = visit_df_.merge(
        visit_df_.drop(
            ["AGE_AT_VISIT", "SEX", "NEXT_VISIT", "EDUCYRS"],
            axis=1,
        ),
        left_on=[
            "PATNO",
            "NEXT_VISIT",
        ],
        right_on=[
            "PATNO",
            "EVENT_ID",
        ],
        suffixes=(None, "_NX"),
    ).drop_duplicates()
    hc = sample_cohort(visit_df_, n=37)

    return mci, nc, hc
