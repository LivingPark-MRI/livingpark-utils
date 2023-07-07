"""Utilities to ignore patient visits without leaking their identities."""
import hashlib
import os
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
        lambda x: hashlib.sha256(
            "_".join([str(x[column]) for column in columns]).encode()
        ).hexdigest(),
        axis=1,
    ).astype(str)
    return df


def add_ignored(df: pd.DataFrame, *, ignore_file: str, mode: str = "a") -> None:
    """Add new subject to the ignore file.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with subject to ignore. It must have an "HASH" column.
    ignore_file : str
        File containing the hash identifier to remove.
    mode : str, optional
        mode to open file. Either "a", "a+" or "w", by default "a"

    Raises
    ------
    ValueError
        Invalid `mode` to open file was given.
    """
    match mode:
        case "w":
            with Path(ignore_file).open("w") as fout:
                fout.write(os.linesep.join(df["HASH"].values) + os.linesep)
        case "a" | "a+":
            # Prevent appending existing subjects.
            with Path(".ppmiignore").open("a+") as f:
                f.seek(0)
                diff = set(df["HASH"].values) - set(f.read().split())
                if diff:
                    f.write(os.linesep.join(diff) + os.linesep)
        case _:
            raise ValueError(f"invalid mode: '{mode}'")


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
    with Path(ignore_file).open() as fin:
        ignored = [line for line in fin.read().splitlines()]

    df = df.copy()
    return df[~df["HASH"].isin(ignored)]
