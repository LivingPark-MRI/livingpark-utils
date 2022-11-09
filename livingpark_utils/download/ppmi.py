"""Downloader for the ppmi dataset."""
import os.path

import pandas as pd
import ppmi_downloader

from .DownloaderABC import DownloaderABC
from livingpark_utils.dataset import ppmi


class Downloader(DownloaderABC):
    """Handle the download of PPMI dataset.

    Parameters
    ----------
    DownloaderABC : DownloaderABC
        Abstract class to handle dataset download.
    """

    def __init__(
        self, out_dir: str, *, cache_dir: str = ".cache", headless=True
    ) -> None:
        """Initialize a download handler.

        During initialization, the output and cache directories are created.

        Parameters
        ----------
        out_dir : str
            Path of the output directory.
        cache_dir : str, optional
            Path of the cache directory., by default ".cache"
        """
        super().__init__(out_dir, cache_dir)
        self.headless = headless

    def get_study_files(
        self,
        query: list[str],
        *,
        timeout: int = 600,
    ) -> tuple[list[str], list[str]]:
        """Download required PPMI study files, if not available.

        Parameters
        ----------
        query : list
            Required PPMI study files (cvs files) supported by ppmi_downloader.
        timeout : int, default 600
            Number of second before the download times out.

        Raises
        ------
        Exception:
            If failure occurs during download.
        """
        try:
            downloader = ppmi_downloader.PPMIDownloader()
            downloader.download_metadata(
                query,
                destination_dir=self.out_dir,
                headless=self.headless,
                timeout=timeout,
            )
        except Exception:
            missing = self.missing_study_files(query)
            success = list(set(query) - set(missing))
            return success, missing

        return query, []

    def missing_study_files(self, query, *, force: bool = False) -> list[str]:
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
        """
        if force:
            return query
        else:
            return [
                filepath
                for filepath in query
                if not os.path.exists(os.path.join(self.out_dir, filepath))
            ]

    def get_T1_nifti_files(
        self,
        query: pd.DataFrame,
        *,
        symlink: bool = False,
        timeout: int = 120,  # Per subject
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
        """
        cohort = query
        missing_patno = cohort["PATNO"]
        try:
            ppmi_dl = ppmi_downloader.PPMIDownloader()
            print(f"Downloading image data of {missing_patno.nunique()} subjects")
            ppmi_dl.download_imaging_data(
                missing_patno.unique(),
                type="nifti",
                timeout=timeout * missing_patno.nunique(),
                headless=self.headless,
            )
        except Exception:
            missing = self.missing_T1_nifti_files(query)
            success = query[~query["PATNO"].isin(missing["PATNO"])]
            return success, missing

        # Find cohort file names among downloaded files
        results_path = "outputs"
        ppmi_fd = ppmi_downloader.PPMINiftiFileFinder()
        for _, row in cohort.iterrows():
            if not row["File name"] or row["File name"] is None:
                filename = ppmi_fd.find_nifti(
                    row["PATNO"], row["EVENT_ID"], row["Description"]
                )
                if filename is None:
                    print(
                        "Not found: "
                        + f"{row['PATNO'], row['EVENT_ID'], row['Description']}"
                    )
                else:  # copy file to dataset
                    dest_dir = os.path.join(
                        "inputs",
                        f'sub-{row["PATNO"]}',
                        f'ses-{row["EVENT_ID"]}',
                        "anat",
                    )
                    os.makedirs(dest_dir, exist_ok=True)
                    dest_file = os.path.join(dest_dir, os.path.basename(filename))
                    os.rename(filename, dest_file)
                    row["File name"] = dest_file

        # Update file names in cohort
        cohort["File name"] = cohort.apply(
            lambda x: ppmi.find_nifti_file_in_cache(
                x["PATNO"], x["EVENT_ID"], x["Description"]
            ),
            axis=1,
        )

        # Create symlinks to inputs if necessary
        if symlink:
            for file_name in cohort["File name"].values:
                dest_dir = os.path.dirname(file_name).replace(
                    os.path.join(self.cache_dir, "inputs"),
                    os.path.join(results_path, "pre_processing"),
                )
                dest_file = os.path.join(
                    dest_dir,
                    os.path.basename(file_name.replace(self.cache_dir, "")),
                )
                if not os.path.exists(dest_file):
                    os.makedirs(dest_dir, exist_ok=True)
                    os.symlink(
                        os.path.relpath(os.path.abspath(file_name), start=dest_file),
                        dest_file,
                    )

        return query, pd.DataFrame()

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
        """
        if force:
            return query
        else:
            cohort = query
            cohort["File name"] = cohort.apply(
                lambda x: ppmi.find_nifti_file_in_cache(
                    x["PATNO"], x["EVENT_ID"], x["Description"]
                ),
                axis=1,
            )
            return cohort[cohort["File name"] == ""]
