"""Downloader for the ppmi dataset."""
import logging
import os.path
import re
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator
from typing import Sequence
from typing import TypeVar

import pandas as pd
import ppmi_downloader
from ppmi_downloader import fileMatchingError
from urllib3.connectionpool import ReadTimeoutError

from .DownloaderABC import DownloaderABC
from livingpark_utils.dataset import ppmi
from livingpark_utils.dataset.convert import dcm2niix
from livingpark_utils.dataset.convert import fileConversionError


logger = logging.getLogger(__name__)
log_file = Path("logs") / "livingpark_utils-ppmiDownloader.log"
log_file.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
fh = logging.FileHandler(log_file)
f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(f_format)
logger.addHandler(fh)
logger.setLevel(logging.INFO)


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


def __parse_xml_metadata(xml_file: Path) -> dict:
    """Return visit metadata.

    Parameters
    ----------
    xml_file: str
        PPMI XML image metadata file. Such files come with PPMI image collections.

    Returns
    -------
    metadata : dict
        Contains the following metadata: `subject_id`, `visit_id`, `series_id`,
        `image_id`, `description`, and `n_files`.
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()  # type: ignore

    def _get_text(r: ET.Element, xpath: str) -> str | None:
        """Return the `elm` text if available, otherwise None.

        Parameters
        ----------
        r : ET.Element
            root of the XML tree.
        xpath : str
            XPath to the `elm`.

        Returns
        -------
        str | None
            `elm` text if available, otherwise None.
        """
        elm = r.find(xpath)
        if isinstance(elm, ET.Element):
            return elm.text
        return None

    metadata = {}
    metadata["subject_id"] = _get_text(root, ".//subjectIdentifier")
    metadata["visit_id"] = _get_text(root, ".//visitIdentifier")
    metadata["series_id"] = _get_text(root, ".//seriesIdentifier")
    metadata["image_id"] = _get_text(root, ".//imageUID")
    metadata["description"] = _get_text(root, ".//imagingProtocol/description")

    n_files = _get_text(root, ".//imagingProtocol//protocol[@term='Matrix Z']")
    if isinstance(n_files, str):
        metadata["n_files"] = int(float(n_files))  # type: ignore
    else:
        metadata["n_files"] = None

    return metadata


def find_dicom(patno: str | int, event_id: str, desc: str) -> list[Path]:
    """Find DICOM files in the PPMI download folder.

    Parameters
    ----------
    patno : str | int
        Subject ID.
    event_id : str
        Visit ID. e.g. "Baseline"
    desc : str
        Protocol description.

    Returns
    -------
    list[Path]
        List of path to the DICOM files.

    Raises
    ------
    fileMatchingError
        When no XML files match the given `patno`, `event_id`, and `desc`.
    """
    visit_map = {
        "SC": "Screening",
        "BL": "Baseline",
        "V01": "Month 3",
        "V02": "Month 6",
        "V03": "Month 9",
        "V04": "Month 12",
        "V05": "Month 18",
        "V06": "Month 24",
        "V07": "Month 30",
        "V08": "Month 36",
        "V09": "Month 42",
        "V10": "Month 48",
        "V11": "Month 54",
        "V12": "Month 60",
        "V13": "Month 72",
        "V14": "Month 84",
        "V15": "Month 96",
        "V16": "Month 108",
        "V17": "Month 120",
        "V18": "Month 132",
        "V19": "Month 144",
        "V20": "Month 156",
        "ST": "Symptomatic Therapy",
        "U01": "Unscheduled Visit 01",
        "U02": "Unscheduled Visit 02",
        "PW": "Premature Withdrawal",
    }

    patno = str(patno)

    xml_files = Path("PPMI").rglob(
        f"PPMI_{patno}_{ppmi.clean_protocol_description(desc)}_*.xml"
    )
    for xml_file in xml_files:
        print(f"Parsing file: {xml_file.as_posix()}")
        metadata = __parse_xml_metadata(xml_file)

        print(patno, visit_map[event_id], desc)
        print(
            metadata["subject_id"], metadata["visit_id"], metadata["description"], "\n"
        )
        if (
            (patno == metadata["subject_id"])
            and (visit_map[event_id] == metadata["visit_id"])
            and (desc == metadata["description"])
        ):
            # Retrieve all the DICOM files for a subject
            # Then only keep the ones matching the regex.
            # We do this process in two steps because the `wildcard` accepted by the
            # rglob` is less versatile than regex from the`re` module.
            subject_dir = Path("PPMI", patno)
            wildcard = f"*_S{metadata['series_id']}_I{metadata['image_id']}.dcm"
            regex = r"PPMI_{}_MR_{}_*br_raw.*_S{}_I{}\.dcm".format(
                metadata["subject_id"],
                ppmi.clean_protocol_description(metadata["description"]),
                metadata["series_id"],
                metadata["image_id"],
            )
            filenames = [
                f for f in subject_dir.rglob(wildcard) if re.match(regex, f.name)
            ]

            if len(filenames) == 0:
                raise fileMatchingError(
                    f"Found no files matching {subject_dir}/**/{regex}\n"
                )
            elif len(filenames) == metadata["n_files"]:
                logger.info(
                    "Found all files matching "
                    f"{subject_dir}/**/{regex}: {len(filenames)}"
                )
            else:
                logger.warning(
                    f"Found {len(filenames)} files matching {subject_dir}/**/{regex} "
                    f"while exactly {metadata['n_files']} was expected"
                )
            return filenames

    raise fileMatchingError(f"No XML file found: {patno=}, {event_id=}, {desc=}\n")


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
            # Remove suffixed date from PPMI filename.
            for filename in Path(self.out_dir).iterdir():
                filename.rename(re.sub(r"_\d+[a-zA-Z]+\d+", "", filename.as_posix()))

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
        timeout: int = 120,
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
            batched_query = query[query["PATNO"].isin(batch)]
            try:
                ppmi_dl = ppmi_downloader.PPMIDownloader(headless=self.headless)
                ppmi_dl.download_imaging_data(
                    batched_query,
                    timeout=timeout * len(batch),
                )
                # We map the files in each batch to limit re-download on failures.
                self._map_nifti_from_cache(batched_query, symlink=symlink)

            except (TimeoutError, ReadTimeoutError):
                logger.error(traceback.format_exc())

                self._map_nifti_from_cache(batched_query, symlink=symlink)
                missing = self.missing_T1_nifti_files(query)
                success = query[~query["PATNO"].isin(missing["PATNO"])].copy()
                return success, missing

            except Exception:
                logger.error(traceback.format_exc())

            finally:
                if "ppmi_dl" in locals():
                    ppmi_dl.quit()

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
            When `True`, all study data are reported missing locally, by default False.

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
                    x["PATNO"],
                    x["EVENT_ID"],
                    x["Description"],
                    debug=False,
                ),
                axis=1,
            )
            return query[query["File name"] == ""].copy()

    def _map_nifti_from_cache(
        self,
        cohort: pd.DataFrame,
        *,
        symlink: bool = False,
    ) -> pd.DataFrame:
        # Find cohort file names among downloaded files
        failures = 0
        for _, row in cohort.iterrows():
            if (
                "File name" not in row
                or not row["File name"]
                or row["File name"] is None
            ):
                try:
                    filenames = find_dicom(
                        row["PATNO"], row["EVENT_ID"], row["Description"]
                    )
                    if filenames is None:
                        continue

                    inputs_dir = Path(
                        "inputs",
                        f"sub-{row['PATNO']}",
                        f"ses-{row['EVENT_ID']}",
                        "anat",
                    )
                    dcm2niix_nifti = Path(
                        dcm2niix(filenames, inputs_dir).outputs.converted_files
                    )
                    nifti_basename = "PPMI_{}_{}.nii.gz".format(
                        row["PATNO"],
                        ppmi.clean_protocol_description(row["Description"]),
                    )
                    logger.info(
                        f"Renaming {dcm2niix_nifti=} "
                        f"to {inputs_dir.joinpath(nifti_basename)}"
                    )
                    input_nifti = dcm2niix_nifti.rename(
                        inputs_dir.joinpath(nifti_basename)
                    )

                    if symlink:
                        outputs_dir = Path(
                            "outputs",
                            "pre_processing",
                            f"sub-{row['PATNO']}",
                            f"ses-{row['EVENT_ID']}",
                            "anat",
                        )
                        output_nifti = outputs_dir.joinpath(nifti_basename)
                        output_nifti.parent.mkdir(
                            parents=True, exist_ok=True, mode=0o755
                        )
                        relative_path = os.path.relpath(input_nifti, start=output_nifti)

                        logger.info(
                            f"Creating symlink: {output_nifti} --> {input_nifti=}\n"
                        )
                        if output_nifti.is_symlink():
                            output_nifti.unlink()
                        output_nifti.symlink_to(relative_path)

                except fileMatchingError:
                    failures += 1
                    logger.error(traceback.format_exc())
                    continue
                except fileConversionError:
                    logger.error(traceback.format_exc())
                except Exception as e:
                    raise e

        if failures > 0:
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

        return cohort
