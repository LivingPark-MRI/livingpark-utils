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
    def get_metadata(
        self, query: list[str], *, timeout: int = 600
    ) -> tuple[list[str], list[str]]:
        """Download the metadata of a dataset.

        Parameters
        ----------
        query : list[str]
            Metadata file(s) to download.
        timeout : int, optional
            Number of second before the download times out., by default 600

        Returns
        -------
        tuple[list[str], list[str]]
            Tuple with the successful and missing files, respectlively.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def missing_metadata(self, query: list[str], *, force: bool = False) -> list[str]:
        """Determine the metadata missing locally.

        Parameters
        ----------
        query : list[str]
            Metadata file(s) to verify.
        force : bool, optional
            When `True`, all metadata are reported missing locally., by default False

        Returns
        -------
        list[str]
            Missing metadata file(s) locally.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def get_raw_data(
        self, query: pd.DataFrame, *, symlink: bool = False, timeout: int = 120
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Download the raw data of a dataset.

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
            Tuple with the successful and missing raw data identifiers, respectlively.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def missing_raw_data(
        self, query: pd.DataFrame, *, force: bool = False
    ) -> pd.DataFrame:
        """Determine the missing raw data locally.

        Parameters
        ----------
        query : pd.DataFrame
            Cohort to verify.
        force : bool, optional
            When `True`, all metadata are reported missing locally., by default False

        Returns
        -------
        pd.DataFrame
            Missing raw data locally.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def get_derivative(
        self, query: pd.DataFrame, *, symlink: bool = False, timeout: int = 120
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Download the derivative of a dataset.

        Parameters
        ----------
        query : pd.DataFrame
            Derivative(s) to download.
        symlink : bool, optional
            When `True`, symlinks are created from the caching directory., by default
            False
        timeout : int, optional
            Number of second before the download times out., by default 120

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            Tuple with the successful and missing data derivative identifiers,
            respectlively.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def missing_derivative(self, query: pd.DataFrame, *, force: bool) -> pd.DataFrame:
        """Determine the missing data derivatives of a dataset.

        Parameters
        ----------
        query : pd.DataFrame
            Data derivative(s) to verify.
        force : bool, optional
            When `True`, all metadata are reported missing locally., by default False

        Returns
        -------
        pd.DataFrame
            Missing data derivative(s) locally.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError
