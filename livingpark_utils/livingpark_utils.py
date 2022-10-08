"""Provide utility function for the LivingPark notebook for paper replication."""
import csv
import datetime
import glob
import math
import os.path
import pkgutil
import subprocess
import sys
import warnings
from pprint import pprint

import boutiques
import nilearn.plotting as nplt
import numpy as np
import pandas as pd
import ppmi_downloader
import pytz  # type: ignore
from boutiques.descriptor2func import function as descriptor2func
from dateutil.parser import parse  # type: ignore
from dateutil.relativedelta import relativedelta  # type: ignore
from IPython.display import HTML
from IPython.display import Image as ImageDisplay
from matplotlib import axes
from matplotlib import pyplot as plt
from PIL import Image


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

        self.setup_notebook_cache()
        os.makedirs(self.study_files_dir, exist_ok=True)

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
                print(f"removing link {x}")
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

        print("Installing notebook dependencies (see log in install.log)... ")
        with open("install.log", "wb") as fout:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "-U",
                    "-r",
                    "requirements.txt",
                ],
                stdout=fout,
                stderr=fout,
            )

        now = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S %Z %z")
        print(f"This notebook was run on {now}")

        return HTML(
            filename=os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "toggle_button.html",
            )
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
        if force:
            missing_files = required_files
        else:
            missing_files = [
                x
                for x in required_files
                if not os.path.exists(os.path.join(self.study_files_dir, x))
            ]

        if len(missing_files) > 0:
            pprint(f"Downloading files: {missing_files}")
            try:
                ppmi = ppmi_downloader.PPMIDownloader()
                ppmi.download_metadata(
                    missing_files,
                    destination_dir=self.study_files_dir,
                    headless=headless,
                    timeout=timeout,
                )
            except Exception as e:
                print("Download failed!")
                raise (e)

            print("Download completed!")

        else:
            print("Download skipped: No missing files!")

    # def __install_datalad_cache(self) -> None:
    #     """Install the DataLad dataset.

    #     Notes
    #     -----
    #     Requires a functional ssh connection to `self.ssh_username`@`self.host`.
    #     Located at `self.host_dir`/`self.notebook_name`/`self.data_cache_path`.
    #     """
    #     if os.path.exists(self.data_cache_path):
    #         # noqa: TODO check if path is a valid DataLad dataset without doing d.status because it's too long.
    #         d = datalad.api.Dataset(self.data_cache_path)
    #         d.update(how="merge")
    #     else:
    #         datalad.api.install(
    #             source=(
    #                 f"{self.ssh_username}@{self.ssh_host}:"
    #                 f"{self.ssh_host_dir}/{self.notebook_name}"
    #             ),
    #             path=self.data_cache_path,
    #         )

    def clean_protocol_description(self, desc: str) -> str:
        """Create valid protocol description for file names (as done by PPMI).

        Parameters
        ----------
        str
            Protocol description. Example: "MPRAGE GRAPPA"
        """
        return (
            desc.replace(" ", "_").replace("(", "_").replace(")", "_").replace("/", "_")
        )

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
        expression = os.path.join(
            self.data_cache_path,
            base_dir,
            f"sub-{subject_id}",
            f"ses-{event_id}",
            "anat",
            f"PPMI_*{self.clean_protocol_description(protocol_description)}*.nii",
        )
        files = glob.glob(expression)
        assert len(files) <= 1, f"More than 1 Nifti file matched by {expression}"
        if len(files) == 1:
            return files[0]
        # print(
        #     "Warning: no nifti file found for: "
        #     f"{(subject_id, event_id, protocol_description)} with strict glob "
        #     "expression. Trying with lenient glob expression."
        # )
        expression = os.path.join(
            self.data_cache_path,
            base_dir,
            f"sub-{subject_id}",
            f"ses-{event_id}",
            "anat",
            "PPMI_*.nii",
        )
        files = glob.glob(expression)
        assert len(files) <= 1, f"More than 1 Nifti file matched by {expression}"
        if len(files) == 1:
            return files[0]
        # print(
        #     f"Warning: no nifti file found for: "
        #     f"{(subject_id, event_id, protocol_description)} "
        #     "with lenient expression, returning None"
        # )
        return ""

    def disease_duration(self) -> pd.DataFrame:
        """Return a DataFrame containing disease durations.

        Returns
        -------
        pd.DataFrame
            DataFrame containing disease durations for each (patient,event) pair found
            in "MDS_UPDRS_Part_III.csv".
        """
        # Download required files
        self.download_ppmi_metadata(
            ["MDS_UPDRS_Part_III.csv", "PD_Diagnosis_History.csv"]
        )

        pddxdt = pd.read_csv(
            os.path.join(self.study_files_dir, "PD_Diagnosis_History.csv")
        )[["PATNO", "EVENT_ID", "PDDXDT"]]
        pddxdt = pddxdt[(pddxdt["EVENT_ID"] == "SC") & pddxdt["PDDXDT"].notna()]
        pdxdur = pd.read_csv(
            os.path.join(self.study_files_dir, "MDS_UPDRS_Part_III.csv"),
            low_memory=False,
        )[["PATNO", "EVENT_ID", "INFODT"]]

        PDDXDT_map = dict(zip(pddxdt["PATNO"].values, pddxdt["PDDXDT"].values))
        pdxdur["PDDXDT"] = pdxdur["PATNO"].map(PDDXDT_map)

        pdxdur["PDXDUR"] = pdxdur.apply(
            lambda row: relativedelta(parse(row["INFODT"]), parse(row["PDDXDT"])).months
            if row["PDDXDT"] is not np.nan
            else np.nan,
            axis=1,
        )
        pdxdur.drop(labels=["INFODT", "PDDXDT"], inplace=True, axis=1)

        return pdxdur

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
        mapping = {
            1: 6,
            2: 9,
            3: 11,
            4: 12,
            5: 13,
            6: 14,
            7: 15,
            8: 15,
            9: 16,
            10: 17,
            11: 18,
            12: 18,
            13: 19,
            14: 20,
            15: 21,
            16: 22,
            17: 22,
            18: 23,
            19: 24,
            20: 25,
            21: 26,
            22: 26,
            23: 27,
            24: 28,
            25: 28,
            26: 29,
            27: 29,
            28: 30,
            29: 30,
            30: 30,
        }

        try:
            if math.isnan(moca_score):
                return np.nan
            else:
                return mapping[moca_score]
        except Exception as e:
            print(e)
            return moca_score

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
        ax.set_xticklabels([x.removesuffix(".0") for x in dist.index.astype(str)])
        for label in ax.xaxis.get_ticklabels():
            try:
                if int(label.get_text()) % freq != 0:
                    label.set_visible(False)
            except Exception:
                pass

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
        # Find nifti file names in cohort
        cohort["File name"] = cohort.apply(
            lambda x: self.find_nifti_file_in_cache(
                x["PATNO"], x["EVENT_ID"], x["Description"]
            ),
            axis=1,
        )
        print(f"Number of available subjects: {len(cohort[cohort['File name'] != ''])}")
        print(f"Number of missing subjects: {len(cohort[cohort['File name'] == ''])}")

        # Download missing file names
        try:
            ppmi_dl = ppmi_downloader.PPMIDownloader()
            missing_subject_ids = cohort[cohort["File name"] == ""]["PATNO"]
            print(f"Downloading image data of {len(missing_subject_ids)} subjects")
            ppmi_dl.download_imaging_data(
                missing_subject_ids,
                type="nifti",
                timeout=120 * len(missing_subject_ids),
                headless=False,
            )
        except Exception as e:
            print("Download failed!")
            raise (e)

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
            lambda x: self.find_nifti_file_in_cache(
                x["PATNO"], x["EVENT_ID"], x["Description"]
            ),
            axis=1,
        )

        # Create symlinks to inputs if necessary
        if link_in_outputs:
            for file_name in cohort["File name"].values:
                dest_dir = os.path.dirname(file_name).replace(
                    os.path.join(self.data_cache_path, "inputs"),
                    os.path.join(results_path, "pre_processing"),
                )
                dest_file = os.path.join(
                    dest_dir,
                    os.path.basename(file_name.replace(self.data_cache_path, "")),
                )
                if not os.path.exists(dest_file):
                    os.makedirs(dest_dir, exist_ok=True)
                    os.symlink(
                        os.path.relpath(os.path.abspath(file_name), start=dest_file),
                        dest_file,
                    )

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
        return str(hash(tuple(sorted(cohort["PATNO"])))).replace("-", "_")

    def write_spm_batch_files(
        self,
        template_job_filename: str,
        replaced_keys: dict,
        executable_job_file_name: str,
    ) -> None:
        """Write SPM batch files from a template by replacing placeholder keys in it.

        Open the SPM batch file in template_job_filename, search and replace keys found
        in replaced_keys, and write the result in two files, a "batch" file and a "job"
        file. Output file names are built from job_file_name. Job file names must end
        with '_job.m'

        Parameters
        ----------
        template_job_filename: str
            File name of template SPM job. Contains placeholder keys to be replaced to
            create an executable batch. No format is specified for the keys, make sure
            that they are uniquely identified in the template file!

        replaced_keys: dict
            Dictionary containing keys to be replaced by values in the template job
            file. Example: {'[IMAGE]': 'inputs/sub-1234/ses-1/anat/image.nii'}. Make
            sure that the keys are present in the template job file name!

        executable_job_file_name: str
            File name where to write the executable job file. Must end in '_job.m'.
            An SPM batch file calling this job file will also be written with a
            '_batch.m' ending.

        Returns
        -------
        None
        """

        def replace_keys(string, replace_keys):
            for k in replace_keys:
                string = string.replace(k, replace_keys[k])
            return string

        # Read template file
        with open(template_job_filename) as f:
            content = f.read()

        assert template_job_filename.endswith("_job.m")
        assert executable_job_file_name.endswith("_job.m")

        with open(executable_job_file_name, "w") as f:
            f.write(replace_keys(content, replaced_keys))

        print(f"Job batch file written in {os.path.basename(executable_job_file_name)}")

        # Batch file
        content_batch = pkgutil.get_data(
            __name__, os.path.join("templates", "call_batch.m")
        )
        assert content_batch is not None, "Cannot read batch template file."
        content_batch_str = content_batch.decode("utf-8")
        tempfile_name_batch = executable_job_file_name.replace("_job", "_batch")

        with open(tempfile_name_batch, "w") as f:
            job_dir = os.path.dirname(os.path.abspath(executable_job_file_name))
            f.write(
                replace_keys(
                    content_batch_str,
                    {
                        "[BATCH]": f"addpath('{job_dir}')"
                        + os.linesep
                        + os.path.basename(executable_job_file_name.replace(".m", ""))
                    },
                )
            )

        print(f"Batch file written in {os.path.basename(tempfile_name_batch)}")

    def run_spm_batch_file(
        self,
        executable_job_file_name: str,
        boutiques_descriptor: str = "zenodo.6881412",
        force: bool = False,
    ) -> boutiques.ExecutorOutput:
        """Run an SPM batch file using Boutiques.

        Requires Docker or Singularity container engines (Singularity untested yet in
        this context). Download the Boutiques descriptor from Zenodo or use the local
        file passed as argument. Download the Docker container, create a Boutiques
        invocation and run it. Write logs in log file created from
        executable_job_file_name (example: pre_processing_1234.log). If log file
        already exists, skip execution unless force is set to True.

        Parameters
        ----------
        executable_job_file_name: str
            An SPM job file ready to be executed.
            Example: 'code/batches/pre_processing_1234_job.m'.
            See self.write_spm_batch_files for a possible way to create such a file.


        boutiques_descriptor: str
            A Boutiques descriptor in the form of a Zenodo id, local file name, or
            JSON string. Don't modify the default value unless you know what you are
            doing.

        force: bool
            Force execution even if log file already exists for this execution.
            Default: False.

        Returns
        -------
        execution_output: boutiques.ExecutorOutput
            Boutiques execution output object containing exit code and various logs.
        """
        log_dir = os.path.join("outputs", "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_file_name = os.path.abspath(
            os.path.join(
                log_dir,
                os.path.basename(executable_job_file_name.replace("_job.m", ".log")),
            )
        )
        spm_batch_file = executable_job_file_name.replace("_job", "_batch")

        if not force:
            if os.path.exists(log_file_name):
                print(
                    f"Log file {os.path.basename(log_file_name)} exists, "
                    + "skipping batch execution (remove file or use force=True "
                    + "to force execution)"
                )
                return
            else:
                print(
                    f"Log file {os.path.basename(log_file_name)} does not exist, "
                    + "running batch"
                )

        # Initialize Boutiques Python function for descriptor
        spm_batch = descriptor2func(boutiques_descriptor)

        output = spm_batch(
            "launch",
            "-s",
            "-u",
            spm_batch_file=spm_batch_file,
            log_file_name=log_file_name,
        )

        assert (
            output.exit_code == 0
        ), f"Execution error, inspect output object for logs: {output}"

        print("Execution was successful.")

        return output

    def find_tissue_image_in_cache(
        self,
        tissue_class: int,
        patno: int,
        visit: str,
        image_prefix: str = "",
        pre_processing_dir: str = "pre_processing",
    ) -> str:
        """Find SPM tissue class image of patient at visit with given protocol.

        Scans the outputs directory for an SPM tissue class file obtained from nifti
        file of patient at visit using protocol description. To find a tissue file,
        matches glob expression
        outputs/{pre_processing_dir}sub-{patno}/ses-{visit}/anat/smwc{tissue_class}PPMI*.nii
        Returns an error if more than one file is found that matches this expression.

        Paramters
        ---------
        tissue_class: int
            1 (grey matter) or 2 (white matter)

        patno: int
            PPMI patient identifier

        visit: str
            PPMI visit name. Example: 'V04'.

        pre_processing_dir: str
            Directory in 'outputs' where pre-processing results are stored.

        image_prefix: str
            Prefix to use in addition to 'c' in the image name. Examples: 'r', 'smw'.

        Returns
        -------
        tissue_class_file: str
            path of a tissue class file. Empty string if no tissue class file is found.
        """
        if tissue_class not in (1, 2, 3, 4, 5, 6):
            raise Exception(f"Unrecognized tissue class: {tissue_class}")
        dirname = os.path.join("outputs", pre_processing_dir)
        expression = os.path.join(
            f"{dirname}",
            f"sub-{patno}",
            f"ses-{visit}",
            "anat",
            f"{image_prefix}c{tissue_class}PPMI*.nii",
        )
        files = glob.glob(expression)
        assert (
            len(files) <= 1
        ), f"More than 1 files were matched by expression: {expression}"
        if len(files) == 0:
            print(f"No file matched by expression: {expression}")
            return ""
        else:
            return os.path.abspath(files[0])

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
        """Export segmentation images as 2D image files.

        Meant to be used for quality control. make_gifs can assemble these images
        in an animate gif.

        Parameters
        ----------
        cohort: pd.DataFrame
            LivingPark cohort to export. Must have a column called 'File name'.

        folder: str
            Folder where to export the segmentation images.

        cut_coords: tuple
            Passed to Nilearn viewer. The MNI coordinates of the cutting plane.

        force: bool
            If True, force export to existing folder. Removes all the files in folder
            before writing new ones.

        extension: str
            Image file extension supported by Matplotlib. Example: 'png'.
        """
        alpha = 0.5

        if (not force) and os.path.exists(folder):
            print(
                f"Folder {folder} already exists, skipping image export "
                + " (remove folder or use --force to force)."
            )
            return

        if os.path.exists(folder):  # force is True
            print(f"Folder {folder} already exists, removing its content")
            for f in glob.glob(os.path.join(folder, "*")):
                os.remove(f)

        os.makedirs(folder, exist_ok=True)

        for i in range(len(cohort)):

            input_file = cohort["File name"].values[i]
            subj_id = cohort["PATNO"].values[i]

            output_file_name = input_file.replace(
                os.path.join(self.data_cache_path, "inputs"),
                os.path.join("outputs", "pre_processing"),
            )
            output_file_c1 = (
                output_file_name.replace("PPMI", "smwc1PPMI")
                if mni_space
                else output_file_name.replace("PPMI", "c1PPMI", 1)
            )
            output_file_c2 = (
                output_file_name.replace("PPMI", "smwc2PPMI")
                if mni_space
                else output_file_name.replace("PPMI", "c1PPMI", 1)
            )

            fig = plt.figure()
            display = nplt.plot_anat(
                anat_img=input_file if mni_space else None,
                cut_coords=list(cut_coords) if cut_coords else None,
                figure=fig,
                title=f"#{i}/{len(cohort)}",
            )

            if show_gm:
                display.add_overlay(
                    output_file_c1, cmap="Reds", threshold=0.1, alpha=alpha
                )

            if show_wm:
                display.add_overlay(
                    output_file_c2, cmap="Blues", threshold=0.1, alpha=alpha
                )

            os.makedirs(folder, exist_ok=True)
            plt.savefig(
                os.path.join(
                    folder, f"qc_{self.cohort_id(cohort)}_{subj_id}.{extension}"
                )
            )
            plt.close(fig)  # so as to not display the figure

    def make_gif(self, frame_folder: str, output_name: str = "animation.gif") -> None:
        """Make gifs from a set of images located in the same folder.

        Parameters
        ----------
        frame_folder : str
            Folder where frames are stored. Frames must be in a format supported by PIL.

        output_name : str
            Base name of the gif file. Will be written in frame_folder.
        """
        frames = [
            Image.open(image)
            for image in glob.glob(os.path.join(f"{frame_folder}", "*.png"))
        ]
        frame_one = frames[0]
        frame_one.save(
            os.path.join(frame_folder, output_name),
            format="GIF",
            append_images=frames,
            save_all=True,
            duration=1000,
            loop=0,
        )
        print(f"Wrote {os.path.join(frame_folder, output_name)}")

    def qc_spm_segmentations(
        self,
        cohort,
        mni_space=True,
        show_gm=True,
        show_wm=True,
        cut_coords=(-28, -7, 17),
    ) -> None:
        """Display a gif file with SPM segmentation results from the cohort.

        Parameters
        ----------
        cohort: pd.DataFrame
            LivingPark cohort to QC. Must have a column called 'File name'.
        """
        qc_dir = f"qc_{self.cohort_id(cohort)}"

        self.export_spm_segmentations(
            cohort,
            qc_dir,
            mni_space=mni_space,
            show_gm=show_gm,
            show_wm=show_wm,
            cut_coords=cut_coords,
        )
        animation_file = "animation.gif"
        self.make_gif(qc_dir, output_name=animation_file)
        gif_content = open(os.path.join(qc_dir, animation_file), "rb").read()
        image = ImageDisplay(data=gif_content, format="png")
        return image

    def spm_compute_missing_segmentations(
        self, cohort: pd.DataFrame
    ) -> boutiques.ExecutorOutput | None:
        """Run SPM segmentation batch for missing segmentations in cohort.

        For each (patient, event_id) pair in cohort, use self.find_tissue_image_in_cache
        to find segmentation result (tissue probability map). If segmentation result
        is not found, use SPM segmentation batch to create it.

        Parameters
        ----------
        cohort: pd.DataFrame
            A LivingPark cohort. Must have columns PATNO, EVENT_ID and Description.

        Returns
        -------
        execution_output: boutiques.ExecutorOutput:
            Boutiques execution output of SPM batch. None if no
        segmentation was missing.
        """
        # Segmentation batch template
        segmentation_job_template = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            "segmentation_job.m",
        )
        segmentation_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"segmentation_{self.cohort_id(cohort)}_job.m"
            )
        )

        image_files = sorted(
            os.path.abspath(
                self.find_nifti_file_in_cache(
                    row["PATNO"],
                    row["EVENT_ID"],
                    row["Description"],
                    base_dir=os.path.join("outputs", "pre_processing"),
                )
            )
            for index, row in cohort.iterrows()
            if (  # segmentation doesn't exist
                self.find_tissue_image_in_cache(1, row["PATNO"], row["EVENT_ID"]) == ""
                and self.find_tissue_image_in_cache(2, row["PATNO"], row["EVENT_ID"])
                == ""
            )
        )

        print(f"Missing segmentations: {len(image_files)}")
        if len(image_files) == 0:
            return None

        image_files_quote = [f"'{x},1'" for x in image_files]
        self.write_spm_batch_files(
            segmentation_job_template,
            {"[IMAGES]": os.linesep.join(image_files_quote)},
            segmentation_job_name,
        )

        # Force execution since we know there are missing files
        output = self.run_spm_batch_file(segmentation_job_name, force=True)
        return output

    def spm_compute_dartel_normalization(
        self, cohort: pd.DataFrame
    ) -> boutiques.ExecutorOutput:
        """Run DARTEL and normalization segmentation batch for subjects in cohort.

        All the subjects in the cohort must already be segmented.
        Run spm_compute_missing_segmentations to compute segmentations.

        Parameters
        ----------
         cohort: pd.DataFrame
            A LivingPark cohort. Must have columns PATNO and EVENT_ID

        Returns
        -------
        execution_output: boutiques.ExecutorOutput
            Boutiques execution output of SPM batch.
        """
        # DARTEL and normalization batch
        dartel_norm_job_template = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            "dartel_normalization_job.m",
        )
        dartel_norm_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"dartel_norm_{self.cohort_id(cohort)}_job.m"
            )
        )

        c1_image_files = sorted(
            os.path.abspath(
                self.find_tissue_image_in_cache(
                    1,
                    row["PATNO"],
                    row["EVENT_ID"],
                )
            )
            for index, row in cohort.iterrows()
        )

        c2_image_files = sorted(
            os.path.abspath(
                self.find_tissue_image_in_cache(
                    2,
                    row["PATNO"],
                    row["EVENT_ID"],
                )
            )
            for index, row in cohort.iterrows()
        )

        rc1_image_files = sorted(
            os.path.abspath(
                self.find_tissue_image_in_cache(
                    1, row["PATNO"], row["EVENT_ID"], image_prefix="r"
                )
            )
            for index, row in cohort.iterrows()
        )

        rc2_image_files = sorted(
            os.path.abspath(
                self.find_tissue_image_in_cache(
                    2, row["PATNO"], row["EVENT_ID"], image_prefix="r"
                )
            )
            for index, row in cohort.iterrows()
        )

        rc1_files_quote = [f"'{x},1'" for x in rc1_image_files]
        rc2_files_quote = [f"'{x},1'" for x in rc2_image_files]
        c1_files_quote = [f"'{x}'" for x in c1_image_files]
        c2_files_quote = [f"'{x}'" for x in c2_image_files]
        self.write_spm_batch_files(
            dartel_norm_job_template,
            {
                "[RC1_IMAGES]": os.linesep.join(rc1_files_quote),
                "[RC2_IMAGES]": os.linesep.join(rc2_files_quote),
                "[C1_IMAGES]": os.linesep.join(c1_files_quote),
                "[C2_IMAGES]": os.linesep.join(c2_files_quote),
                "[NORM_FWHM]": "4 4 4",
            },
            dartel_norm_job_name,
        )

        output = self.run_spm_batch_file(dartel_norm_job_name)
        return output

    def spm_compute_intra_cranial_volumes(self, cohort: pd.DataFrame) -> dict:
        """Compute intra-cranial volume for all subjects in cohort.

        All the subjects in the cohort must already be segmented and normalized to
        MNI space.
        Run spm_compute_missing_segmentations to compute segmentations and
        spm_compute_dartel_normalization to normalize them.

        Parameters
        ----------
         cohort: pd.DataFrame
            A LivingPark cohort. Must have columns PATNO, EVENT_ID and Description

        Returns
        -------
        volumes: dict
            Dictionary where keys are segmentation files and values are intra-cranial
        volumes.
        """
        # Tissue volumes batch
        tissue_volumes_job_template = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            "tissue_volumes_job.m",
        )
        volumes_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"tissue_volumes_{self.cohort_id(cohort)}_job.m"
            )
        )

        image_files = sorted(
            os.path.abspath(
                self.find_nifti_file_in_cache(
                    row["PATNO"],
                    row["EVENT_ID"],
                    row["Description"],
                    base_dir=os.path.join("outputs", "pre_processing"),
                )
            )
            for index, row in cohort.iterrows()
        )

        segmentation_files = sorted(
            x.replace(".nii", "_seg8.mat").replace(",1", "") for x in image_files
        )
        volumes_file = os.path.abspath(
            os.path.join(
                "outputs", os.path.basename(volumes_job_name.replace(".m", ".txt"))
            )
        )  # output file containing brain volumes
        self.write_spm_batch_files(
            tissue_volumes_job_template,
            {
                "[SEGMENTATION_FILES]": os.linesep.join(
                    [f"'{x}'" for x in segmentation_files]
                ),
                "[VOLUMES_FILE]": volumes_file,
            },
            volumes_job_name,
        )
        self.run_spm_batch_file(volumes_job_name)

        icvs = {}  # intra-cranial volumes per segmentation file

        def subject_id(segmentation_filename):
            """Return subject id from segmentation file name."""
            sub_id = segmentation_filename.split(os.path.sep)[-4].replace("sub-", "")
            assert int(sub_id)
            return sub_id

        with open(volumes_file) as csvfile:
            reader = csv.DictReader(csvfile, quotechar="'")
            for row in reader:
                assert len(row) == 4, f"Malformed row: {row}"
                icvs[subject_id(row["File"])] = (
                    float(row["Volume1"])
                    + float(row["Volume2"])
                    + float(row["Volume3"])
                )

        return icvs

    def spm_compute_vbm_stats(
        self,
        cohort: pd.DataFrame,
        tissue_class: int,
        group1_patnos: list,
        group2_patnos: list,
        icvs: dict,
    ) -> dict:
        """
        Compute VBM stats for cohort.

        Cohort must already be segmented into GM and WM using
        self.spm_compute_missing_segmentations and normalized to common space using
        self.spm_compute_dartel_normalization. Inter-cranial volumes must be
        pre-computed using self.spm_compute_intra_cranial_volumes.

        Parameters
        ----------
         cohort: pd.DataFrame
            A LivingPark cohort. Must have columns PATNO and EVENT_ID
         tissue_class: int
            "gm" for grey matter, "wm" for white matter
         group1/2_patnos: list of PPMI patient numbers in group1/2. Important: a patno
            must appear in exactly one group and must appear excatly once in this group.
        icys: dict
            inter-cranial volumes by patno, as returned by
            self.spm_compute_intra_cranial_volumes
        """
        # Stats batch (grey matter)
        stats_job_template = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "templates",
            "stats_job.m",
        )

        cohort_id = self.cohort_id(cohort)

        stats_job_name = os.path.abspath(
            os.path.join("code", "batches", f"stats_{tissue_class}_{cohort_id}_job.m")
        )

        design_dir = os.path.join("outputs", f"results-{tissue_class}-{cohort_id}")
        os.makedirs(design_dir, exist_ok=True)

        # Don't mess up with ordering, it's critical
        tissue_numbers = {"gm": 1, "wm": 2}

        def find_tissue_image(patno):
            tissue = tissue_numbers[tissue_class]
            visit = cohort[cohort["PATNO"] == patno]["EVENT_ID"].values[0]
            return self.find_tissue_image_in_cache(tissue, patno, visit, "smw")

        group1_smwc = [
            f"'{find_tissue_image(patno)},1'" for patno in sorted(group1_patnos)
        ]
        group2_smwc = [
            f"'{find_tissue_image(patno)},1'" for patno in sorted(group2_patnos)
        ]

        groups_patnos = [x for x in sorted(group1_patnos) + sorted(group2_patnos)]

        replace_keys = {
            "[DESIGN_DIR]": os.path.abspath(design_dir),
            "[GROUP1_SMWC_SCANS]": os.linesep.join(group1_smwc),
            "[GROUP2_SMWC_SCANS]": os.linesep.join(group2_smwc),
            "[ICVS]": os.linesep.join(
                [str(icvs[str(x)]) for x in groups_patnos]
            ),  # don't mess up ordering
            "[AGES]": os.linesep.join(
                [
                    str(cohort[cohort["PATNO"] == x]["Age"].values[0])
                    for x in groups_patnos
                ]
            ),
        }

        # Stats batch

        self.write_spm_batch_files(stats_job_template, replace_keys, stats_job_name)
        output = self.run_spm_batch_file(stats_job_name)
        return output
