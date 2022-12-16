"""Utilities to ignore patient visits without leaking their identities."""
import re
from pathlib import Path

import pandas as pd


def insert_hash(df: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    """Insert an "HASH" column to the given dataframe.

    The "HASH" column is generated based on the `columns` list.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to insert hash identifier.
    columns : list[str]
        Columns used to create the hash identifier.

    Returns
    -------
    pd.DataFrame
        New dataframe with an "HASH" identifier column.
    """
    df = df.copy()
    df["HASH"] = df.apply(
        lambda x: hash("_".join([str(x[column]) for column in columns])),
        axis=1,
    ).astype(str)
    return df


def remove_ignored(df: pd.DataFrame, *, ignore_file: str) -> pd.DataFrame:
    """Remove ignored subjects from the dataframe, specified in `ignore_file`.

    The `ignore_file` should contain hash identifier for the subject.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to remove ignored subject from. It must have an "HASH" column.
    ignore_file : str
        File containing the hash identifier to remove.

    Returns
    -------
    pd.DataFrame
        New dataframe with ignored subjects removed.
    """
    is_hash = re.compile(r"^-?\d+")
    with Path(ignore_file).open() as fin:
        ignored = [line for line in fin.read().splitlines() if is_hash.match(line)]

    df = df.copy()
    return df[~df["HASH"].isin(ignored)]
