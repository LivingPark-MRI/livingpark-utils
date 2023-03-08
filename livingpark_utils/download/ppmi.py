"""Downloader for the ppmi dataset."""
import logging
import os.path
import traceback
from typing import Iterator
from typing import Sequence
from typing import TypeVar

import pandas as pd
import ppmi_downloader
from ppmi_downloader import fileMatchingError
from urllib3.connectionpool import ReadTimeoutError

from .DownloaderABC import DownloaderABC
from livingpark_utils.dataset import ppmi


log_file = "livingpark_utils-ppmiDownloader.log"
logging.Formatter(fmt="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")
fh = logging.FileHandler(log_file)
fh.setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.addHandler(fh)


T = TypeVar("T")


def batched(iterable: Sequence[T], *, n: int) -> Iterator[Sequence[T]]:
    """Segment the `iterable` into `n` batches.

    Parameters
    ----------
    iterable : _type_
        _description_
    n : int
        Number of batches.

    Yields
    ------
    _type_
        _description_
    """
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


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
            downloader = ppmi_downloader.PPMIDownloader(headless=self.headless)
            downloader.download_metadata(
                query,
                destination_dir=self.out_dir,
                timeout=timeout,
            )
        except Exception:
            print(traceback.format_exc())
            missing = self.missing_study_files(query)
            success = list(set(query) - set(missing))
            return success, missing
        finally:
            if "downloader" in locals():
                downloader.quit()

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
        batch_size: int = 50,
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
        batch_size : int, optional
            Number of subjects to download in each batch, by default 100.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            Tuple with the successful and missing T1 NIfTI file identifiers,
            respectively.
        """
        missing_patno = query["PATNO"]
        print(f"Downloading image data of {missing_patno.nunique()} subjects")
        for batch in batched(missing_patno.unique(), n=batch_size):
            try:
                ppmi_dl = ppmi_downloader.PPMIDownloader(headless=self.headless)
                ppmi_dl.download_imaging_data(
                    batch,
                    type="nifti",
                    timeout=timeout * len(batch),
                )
                # We map the files in each batch to limit re-download on failures.
                query = self._map_nifti_from_cache(query, symlink=symlink)

            except (TimeoutError, ReadTimeoutError):
                logger.error(traceback.format_exc())

                query = self._map_nifti_from_cache(query, symlink=symlink)
                missing = self.missing_T1_nifti_files(query)
                success = query[~query["PATNO"].isin(missing["PATNO"])].copy()
                return success, missing

            except Exception:
                logger.error(traceback.format_exc())

            finally:
                if "ppmi_dl" in locals():
                    ppmi_dl.quit()

        query = self._map_nifti_from_cache(query, symlink=symlink, debug=True)
        missing = self.missing_T1_nifti_files(query)
        success = query[~query["PATNO"].isin(missing["PATNO"])].copy()
        return success, missing

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
            query["File name"] = query.apply(
                lambda x: ppmi.find_nifti_file_in_cache(
                    x["PATNO"], x["EVENT_ID"], x["Description"]
                ),
                axis=1,
            )

            return query[query["File name"] == ""].copy()

    def _map_nifti_from_cache(
        self, cohort: pd.DataFrame, *, symlink: bool, debug: bool = False
    ) -> pd.DataFrame:
        # Find cohort file names among downloaded files
        results_path = "outputs"
        ppmi_fd = ppmi_downloader.PPMINiftiFileFinder()
        failures = 0
        for _, row in cohort.iterrows():
            if not row["File name"] or row["File name"] is None:
                try:
                    filename = ppmi_fd.find_nifti(
                        row["PATNO"], row["EVENT_ID"], row["Description"]
                    )
                except fileMatchingError:
                    failures += 1
                    logger.error(traceback.format_exc())

                    continue
                except Exception as e:
                    raise e

                if filename is None:
                    continue

                elif not os.path.exists(filename):
                    if debug:
                        print(
                            "Error: File not found."
                            f"Possibly due to a failed download: {filename}"
                        )
                else:  # copy file to dataset
                    dest_dir = os.path.join(
                        "inputs",
                        f'sub-{row["PATNO"]}',
                        f'ses-{row["EVENT_ID"]}',
                        "anat",
                    )
                    os.makedirs(dest_dir, exist_ok=True)
                    dest_file = os.path.join(
                        dest_dir,
                        os.path.basename(filename).replace(
                            "__", "_"
                        ),  # Edge-case for some desc.
                    )
                    os.rename(filename, dest_file)
                    row["File name"] = dest_file

        if debug and failures > 0:
            print(
                f"Failed to downloaded {failures} files."
                f"See {log_file} for more details."
            )

        # Update file names in cohort
        cohort["File name"] = cohort.apply(
            lambda x: ppmi.find_nifti_file_in_cache(
                x["PATNO"], x["EVENT_ID"], x["Description"]
            ),
            axis=1,
        )

        # Create symlinks to inputs if necessary
        if symlink:
            for filename in cohort["File name"].values:
                if filename is None or filename == "":
                    continue

                dest_dir = os.path.dirname(filename).replace(
                    os.path.join(self.cache_dir, "inputs"),
                    os.path.join(results_path, "pre_processing"),
                )
                dest_file = os.path.join(
                    dest_dir,
                    os.path.basename(filename.replace(self.cache_dir, "")),
                )
                if not os.path.exists(dest_file):
                    os.makedirs(dest_dir, exist_ok=True)
                    os.symlink(
                        os.path.relpath(os.path.abspath(filename), start=dest_file),
                        dest_file,
                    )

        return cohort
