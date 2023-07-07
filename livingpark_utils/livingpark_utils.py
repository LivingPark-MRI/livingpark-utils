"""Provide utility function for the LivingPark notebook for paper replication."""
import datetime
import os.path
import warnings
from pprint import pprint

import pandas as pd
import pytz  # type: ignore
from IPython.display import HTML

from .deprecation import deprecated
from .download.DownloaderABC import DownloaderABC


class FailedDownloadError(Exception):
    """Exception to report a failed download."""

    pass


class LivingParkUtils:
    """Contain functions to be reused across LivingPark notebooks."""

    def __init__(
        self,
        data_cache_path: str = ".cache",
    ) -> None:
        """Initialize a LivingPark notebook.

        Parameters
        ----------
        data_cache_path: str, default ".cache"
            Local path where to store the dataset cache.
            Keep default value unless you know what you're doing.
        """
        self.data_cache_path = data_cache_path
        self.study_files_dir = os.path.abspath(os.path.join("inputs", "study_files"))
        self.code_dir = os.path.abspath("code")

        self.setup_notebook_cache()
        os.makedirs(self.study_files_dir, mode=0o755, exist_ok=True)
        os.makedirs(self.code_dir, mode=0o755, exist_ok=True)

    def setup_notebook_cache(self) -> None:
        """Create, install, and update the cache directory, if needed.

        Notes
        -----
        Aggregate the inputs and outputs into a single dataset by creating symlinks.
        """
        for x in ("", "inputs", "outputs"):
            os.makedirs(os.path.join(self.data_cache_path, x), exist_ok=True)

        # Make or update links to cache
        for x in ["inputs", "outputs"]:
            if os.path.islink(x):
                os.remove(x)
            elif os.path.exists(x):
                raise Exception(f"Directory {x} exists and is not a symlink.")
            else:
                print(f"{x} doesnt exist")
            os.symlink(os.path.join(self.data_cache_path, x), x)

    def notebook_init(self) -> HTML:
        """Initialize a paper replication notebook.

        It ignores cell warnings, install dependencies, show execution time, and create
        a toggle button for displaying/hiding code cells.

        Returns
        -------
        HTML
            An HTML button to hide/show code cells in the notebooks.
        """
        warnings.filterwarnings("ignore")

        now = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S %Z %z")
        print(f"This notebook was run on {now}")

        return HTML(
            filename=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "toggle_button.html",
            )
        )

    def get_study_files(
        self,
        query: list[str],
        default: DownloaderABC,
        force: bool = False,
        timeout: int = 600,
        **kwargs,
    ) -> None:
        """Download the required study files, using a given downloader.

        Parameters
        ----------
        query : list[str]
            Required study files.
        default : DownloaderABC
            Download handler.
        force : bool, optional, default
            When `True`, the study files are always downloaded. Otherwise, only the
            missing study files are downloaded., by default False
        timeout : int, optional
            Number of second before the download times out., by default 600
        """
        missing = default.missing_study_files(query, force=force)
        if len(missing) == 0:
            print("Download skipped: No missing files!")
        else:
            pprint(f"Downloading files: {missing}")
            _, missing = default.get_study_files(missing, timeout=timeout, **kwargs)

            if len(missing) > 0:
                raise FailedDownloadError(f"Missing files: {missing}")
            else:
                print("Download completed")

    def get_T1_nifti_files(
        self,
        query: pd.DataFrame,
        default: DownloaderABC,
        *,
        symlink: bool = False,
        force: bool = False,
        timeout: int = 120,
        fallback: DownloaderABC = None,
        **kwargs,
    ) -> None:
        """Download the required T1 NIfTI files, using a given downloader.

        Parameters
        ----------
        query : pd.DataFrame
            Cohort to download.
        default : DownloaderABC
            Download handler.
        symlink : bool, optional
            When `True`, symlinks are created from the caching directory., by default
            False
        force : bool, optional
            When `True`, the files are always downloaded. Otherwise, only the missing
            files are downloaded., by default False
        timeout : int, optional
            Number of second before the download times out., by default 120
        fallback : DownloaderABC, optional
            When some subject fail to download, use this alternative downloader., by
            default None
        """
        missing = default.missing_T1_nifti_files(query, force=force)
        if len(missing) == 0:
            return print("Download skipped: No missing files!")
        else:
            _, missing = default.get_T1_nifti_files(
                missing, symlink=symlink, timeout=timeout, **kwargs
            )

            if len(missing) > 0 and fallback:
                _, missing = fallback.get_T1_nifti_files(
                    missing, symlink=symlink, timeout=timeout, **kwargs
                )
                if len(missing) > 0:
                    with open("install_nifti.log") as fout:
                        fout.write(missing)
                    raise FailedDownloadError(
                        "Some files could not be downloaded."
                        "\nSee `install_nifti.log` file for more information."
                    )

        print("Download completed")

    # Methods to deprecate
    @deprecated(extra="Moved to function `pipeline.spm::SPM::write_spm_batch_files`.")
    def write_spm_batch_files(
        self,
        template_job_filename: str,
        replaced_keys: dict,
        executable_job_file_name: str,
    ) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.write_spm_batch_files(
            template_job_filename,
            replaced_keys,
            executable_job_file_name,
        )

    import boutiques

    @deprecated(extra="Moved to function `pipeline.spm::SPM::run_spm_batch_file`.")
    def run_spm_batch_file(
        self,
        executable_job_file_name: str,
        boutiques_descriptor: str = "zenodo.6881412",
        force: bool = False,
    ) -> boutiques.ExecutorOutput:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.run_spm_batch_file(
            executable_job_file_name,
            boutiques_descriptor,
            force,
        )

    @deprecated(
        extra="Moved to function `pipeline.spm::SPM::find_tissue_image_in_cache`."
    )
    def find_tissue_image_in_cache(
        self,
        tissue_class: int,
        patno: int,
        visit: str,
        image_prefix: str = "",
        pre_processing_dir: str = "pre_processing",
    ) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.find_tissue_image_in_cache(
            tissue_class,
            patno,
            visit,
            image_prefix,
            pre_processing_dir,
        )

    @deprecated(
        extra="Moved to function `pipeline.spm::SPM::export_spm_segmentations`."
    )
    def export_spm_segmentations(
        self,
        cohort: pd.DataFrame,
        folder: str,
        mni_space: bool = True,
        show_wm: bool = True,
        show_gm: bool = True,
        cut_coords: tuple = (-28, -7, 17),
        force: bool = False,
        extension: str = "png",
    ) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.export_spm_segmentations(
            cohort,
            folder,
            mni_space,
            show_wm,
            show_gm,
            cut_coords,
            force,
            extension,
        )

    @deprecated(extra="Moved to function `pipeline.spm::SPM::qc_spm_segmentations`.")
    def qc_spm_segmentations(
        self,
        cohort,
        mni_space=True,
        show_gm=True,
        show_wm=True,
        cut_coords=(-28, -7, 17),
    ) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.qc_spm_segmentations(
            cohort,
            mni_space,
            show_gm,
            show_wm,
            cut_coords,
        )

    @deprecated(
        extra=(
            "Moved to function"
            "`pipeline.spm::SPM::spm_compute_missing_segmentations`."
        )
    )
    def spm_compute_missing_segmentations(self, cohort: pd.DataFrame) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.spm_compute_missing_segmentations(cohort)

    @deprecated(
        extra="Moved to function `pipeline.spm::SPM::spm_compute_dartel_normalization`."
    )
    def spm_compute_dartel_normalization(self, cohort: pd.DataFrame) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.spm_compute_dartel_normalization(cohort)

    @deprecated(
        extra=(
            "Moved to function"
            "`pipeline.spm::SPM::spm_compute_intra_cranial_volumes`."
        )
    )
    def spm_compute_intra_cranial_volumes(self, cohort: pd.DataFrame) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.qc_spmspm_compute_intra_cranial_volumes_segmentations(cohort)

    @deprecated(extra="Moved to function `pipeline.spm::SPM::spm_compute_vbm_stats`.")
    def spm_compute_vbm_stats(
        self,
        cohort: pd.DataFrame,
        tissue_class: int,
        group1_patnos: list,
        group2_patnos: list,
        icvs: dict,
    ) -> None:
        # noqa
        from pipeline.spm import SPM

        spm = SPM(code_dir=self.code_dir, cache=self.data_cache_path)
        spm.spm_compute_vbm_stats(
            cohort,
            tissue_class,
            group1_patnos,
            group2_patnos,
            icvs,
        )

    @deprecated(
        extra="Moved to function `livinpark_utils::LivingParkUtils::get_study_files`."
    )
    def download_ppmi_metadata(
        self,
        required_files: list,
        force: bool = False,
        headless: bool = True,
        timeout: int = 600,
    ) -> None:
        """Download PPMI required study files, if not available.

        Parameters
        ----------
        required_files : list
            Required PPMI study files (cvs files) supported by ppmi_downloader.
        force : bool, optional
        headless : bool, default True
            If True, prevent broswer window to open during download.
        timeout : int, default 600
            Number of second before the download times out.

        Raises
        ------
        Exception:
            If failure occurs during download.
        """
        from livingpark_utils.download import ppmi

        ppmi_downloader = ppmi.Downloader(self.study_files_dir, headless=headless)
        return self.get_study_files(
            query=required_files, default=ppmi_downloader, force=force, timeout=timeout
        )

    @deprecated(extra="Moved to module `livingpark_utils.dataset.ppmi`.")
    def clean_protocol_description(self, desc: str) -> str:
        """Create valid protocol description for file names (as done by PPMI).

        Parameters
        ----------
        str
            Protocol description. Example: "MPRAGE GRAPPA"
        """
        from livingpark_utils.dataset.ppmi import clean_protocol_description

        return clean_protocol_description(desc=desc)

    @deprecated(extra="Moved to module `livingpark_utils.dataset.ppmi`.")
    def find_nifti_file_in_cache(
        self,
        subject_id: str,
        event_id: str,
        protocol_description: str,
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
        base_dir: str, default "inputs"
            The base directory where to look for in the cache.
            Example: 'outputs/pre_processing'. This is useful when the nifti files
            are present in more than a single location. For instance, when using SPM
            pipelines, it is usually reasonable to link the nifti files from the inputs
            directory to the outputs directory, as SPM write its outputs next to the
            inputs by default.

        Returns
        -------
        str:
            File name matching the `subject_id`, `event_id`, and if possible
            `protocol_description`. Empty string if no matching file is found.
        """
        from livingpark_utils.dataset.ppmi import find_nifti_file_in_cache

        return find_nifti_file_in_cache(
            subject_id=subject_id,
            event_id=event_id,
            description=protocol_description,
            cache_dir=self.data_cache_path,
            base_dir=base_dir,
        )

    @deprecated(extra="Moved to module `livingpark_utils.dataset.ppmi`.")
    def disease_duration(self) -> pd.DataFrame:
        """Return a DataFrame containing disease durations.

        Returns
        -------
        pd.DataFrame
            DataFrame containing disease durations for each (patient,event) pair found
            in "MDS_UPDRS_Part_III.csv".
        """
        from livingpark_utils.dataset.ppmi import disease_duration

        return disease_duration(study_data_dir=self.study_files_dir, force=False)

    @deprecated(extra="Moved to module `livingpark_utils.clinical`.")
    def moca2mmse(self, moca_score: int) -> int:
        """Return a MMSE score given a MoCA score.

        Parameters
        ----------
        moca_score: int
            MoCA score

        Returns
        -------
        int
            MMSE score corresponding to the MoCA score
            Conversion made using Table 2 in
            https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4371590
        """
        from livingpark_utils.clinical import moca2mmse

        return moca2mmse(moca_score=moca_score)

    from matplotlib import axes

    @deprecated(extra="Moved to module `livingpark_utils.visualization`.")
    def reformat_plot_labels(self, dist: pd.Series, ax: axes.Axes, freq: int) -> None:
        """Reformat tick locations and labels of the x-axis on a plot.

        Parameters
        ----------
        dist: pd.Series
            Series representing the number of elements
            for each distinct values of a column
        ax: axes.Axes
            Matplotlib's Axes class to access figure
            elements and set the coordinate system
        freq: int
            interval between labels

        Returns
        -------
        None
        """
        from livingpark_utils.visualization import reformat_plot_labels

        return reformat_plot_labels(dist=dist, ax=ax, freq=freq)

    @deprecated(
        extra=(
            "Moved to function `livinpark_utils::LivingParkUtils::get_T1_nifti_files`."
        )
    )
    def download_missing_nifti_files(
        self, cohort: pd.DataFrame, link_in_outputs=False
    ) -> None:
        """Download missing nifti files required by cohort.

        For each subject in cohort, look for T1-weighted nifti image file in
        notebook cache. Download all the missing files from PPMI, move them
        to notebook cache (inputs directory), and add their names to cohort.

        Parameters
        ----------
        cohort: pd.DataFrame
            A Pandas DataFrame containing columns PATNO (PPMI patient id), EVENT_ID
            (MRI visit, for instance 'V06'), and Description (for instance
            'MPRAGE GRAPPA'). Can be built from the file produced by
            'MRI metadata.ipynb'. A column 'File name' will be added to the DataFrame
             if not already present. This column
            will contain the paths of the T1-weighted nifti files associated with the
             patient, MRI visit, and protocol description.
        link_in_outputs: bool
            If True, create symbolic links to input nifti files in
            outputs/pre-processing. Useful for processing tools that
            write next to input files, such as SPM.

        Returns
        -------
        None
        """
        from livingpark_utils.download import ppmi

        ppmi_downloader = ppmi.Downloader(self.study_files_dir, headless=True)
        return self.get_T1_nifti_files(
            query=cohort, default=ppmi_downloader, symlink=link_in_outputs
        )

    @deprecated(extra="Moved to module `livingpark_utils.dataset.ppmi`.")
    def cohort_id(self, cohort: pd.DataFrame) -> str:
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
        from livingpark_utils.dataset.ppmi import cohort_id

        return cohort_id(cohort=cohort)

    @deprecated(extra="Moved to module `livingpark_utils.pipeline.qc`.")
    def make_gif(self, frame_folder: str, output_name: str = "animation.gif") -> None:
        """Make gifs from a set of images located in the same folder.

        Parameters
        ----------
        frame_folder : str
            Folder where frames are stored. Frames must be in a format supported by PIL.
        output_name : str
            Base name of the gif file. Will be written in frame_folder.
        """
        from livingpark_utils.pipeline import qc

        return qc.make_gif(frame_folder=frame_folder, output_name=output_name)
