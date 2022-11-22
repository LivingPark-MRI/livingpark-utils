"""Abstraction to download dataset from different locations."""
import os.path
from abc import ABC
from abc import abstractmethod

import pandas as pd


class DownloaderABC(ABC):
    """Abstract class to handle download of dataset.

    Parameters
    ----------
    ABC : ABC
        Base for abstract class.
    """

    def __init__(self, out_dir: str, cache_dir: str = ".cache") -> None:
        """Initialize a download handler.

        During initialization, the output and cache directories are created.

        Parameters
        ----------
        out_dir : str
            Path of the output directory.
        cache_dir : str, optional
            Path of the cache directory., by default ".cache"
        """
        self.out_dir = out_dir
        self.cache_dir = cache_dir
        os.makedirs(os.path.join(os.getcwd(), self.out_dir), mode=755, exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), self.cache_dir), mode=755, exist_ok=True)

    @abstractmethod
    def get_study_files(
        self, query: list[str], *, timeout: int = 600
    ) -> tuple[list[str], list[str]]:
        """Download the study files of a dataset.

        Parameters
        ----------
        query : list[str]
            Study files to download.
        timeout : int, optional
            Number of second before the download times out., by default 600

        Returns
        -------
        tuple[list[str], list[str]]
            Tuple with the successful and missing study files, respectlively.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def missing_study_files(
        self, query: list[str], *, force: bool = False
    ) -> list[str]:
        """Determine the study files missing locally.

        Parameters
        ----------
        query : list[str]
            Study files to verify.
        force : bool, optional
            When `True`, all study files are reported missing locally., by default False

        Returns
        -------
        list[str]
            Missing study files locally.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def get_T1_nifti_files(
        self, query: pd.DataFrame, *, symlink: bool = False, timeout: int = 120
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Download the T1 NIfTI files of a dataset.

        Parameters
        ----------
        query : pd.DataFrame
            Cohort to download.
        symlink : bool, optional
            When `True`, symlinks are created from the caching directory., by default
            False
        timeout : int, optional
             Number of second before the download times out., by default 120

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            Tuple with the successful and missing T1 NIfTI file identifiers,
            respectlively.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def missing_T1_nifti_files(
        self, query: pd.DataFrame, *, force: bool = False
    ) -> pd.DataFrame:
        """Determine the missing T1 NIfTI files locally.

        Parameters
        ----------
        query : pd.DataFrame
            Cohort to verify.
        force : bool, optional
            When `True`, all study data are reported missing locally., by default False

        Returns
        -------
        pd.DataFrame
            Missing T1 NIfTI files locally.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError
