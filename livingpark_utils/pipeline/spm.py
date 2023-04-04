"""Utility function to run SPM pipelines."""
import csv
import glob
import os.path
import pkgutil
from pathlib import Path
from shutil import copyfile

import boutiques
import nilearn.plotting as nplt
import pandas as pd
from boutiques.descriptor2func import function as descriptor2func
from IPython.display import Image as ImageDisplay
from matplotlib import pyplot as plt

from . import qc
from ..dataset import ppmi
from ..dataset.ppmi import clean_protocol_description
from .exceptions import PipelineExecutionError
from .PipelineABC import PipelineABC


pkg_root = Path(pkgutil.resolve_name("livingpark_utils").__path__[0])


class SPM(PipelineABC):
    """SPM functions and QC.

    Parameters
    ----------
    PipelineABC
        Abstract class for pipelines.
    """

    def __init__(self, code_dir: str = "code", cache: str = ".cache") -> None:
        """Initialize the `SPM` object.

        Parameters
        ----------
        code_dir : str, optional
            Path to directory to write SPM batch and job files , by default "code".
        cache : str, optional
            Path to directory containing the cached MRI images, by default ".cache".
        """
        super().__init__()
        self.code_dir = code_dir
        self.cache = cache

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

        os.makedirs(os.path.join(self.code_dir, "batches"), exist_ok=True)
        with open(executable_job_file_name, "w") as f:
            f.write(replace_keys(content, replaced_keys))

        print(f"Job batch file written in {os.path.basename(executable_job_file_name)}")

        # Batch file
        content_batch = pkgutil.get_data(
            "livingpark_utils", os.path.join("templates", "call_batch.m")
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

        if output.exit_code != 0:
            raise PipelineExecutionError(
                f"Execution error, inspect output object for logs: {output}"
            )

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

        mni_space: bool
            Set to True if segmentations are in MNI space. If set to False, use subject
            T1 as background, otherwise use MNI template.

        show_wm: bool
            Set to True to show white matter segmentations.

        show_gm: bool
            Set to True to show grey matter segmentations.

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
                os.path.join(self.cache, "inputs"),
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
            if not mni_space:
                display = nplt.plot_anat(
                    anat_img=input_file,
                    cut_coords=list(cut_coords) if cut_coords else None,
                    figure=fig,
                    title=f"#{i}/{len(cohort)}",
                )
            else:
                display = nplt.plot_anat(
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
                    folder, f"qc_{ppmi.cohort_id(cohort)}_{subj_id}.{extension}"
                )
            )
            plt.close(fig)  # so as to not display the figure

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
        qc_dir = f"qc_{ppmi.cohort_id(cohort)}"

        self.export_spm_segmentations(
            cohort,
            qc_dir,
            mni_space=mni_space,
            show_gm=show_gm,
            show_wm=show_wm,
            cut_coords=cut_coords,
        )
        animation_file = "animation.gif"
        qc.make_gif(qc_dir, output_name=animation_file)
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
        segmentation_job_template = pkg_root.joinpath(
            "templates",
            "segmentation_job.m",
        ).as_posix()
        segmentation_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"segmentation_{ppmi.cohort_id(cohort)}_job.m"
            )
        )

        cohort["nifti_cache"] = cohort.apply(
            lambda row: ppmi.find_nifti_file_in_cache(
                row["PATNO"],
                row["EVENT_ID"],
                row["Description"],
                base_dir=Path("outputs", "pre_processing").as_posix(),
            ),
            axis=1,
        )

        if any(cohort["nifti_cache"] == ""):
            raise ValueError(
                "Some visit data is missing for pre-processing."
                '\nSee "nifti_cache" column for missing data.'
            )

        image_files = sorted(
            os.path.abspath(row["nifti_cache"])
            for _, row in cohort.iterrows()
            if (  # segmentation doesn't exist
                self.find_tissue_image_in_cache(1, row["PATNO"], row["EVENT_ID"]) == ""
                or self.find_tissue_image_in_cache(2, row["PATNO"], row["EVENT_ID"])
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

        # If execution was successful, create a read-only copy of mat files
        # so that they can be restored when other SPM processes (e.g., tissue volumes)
        # corrupt them
        if output.exit_code == 0:
            mat_files = [
                self.find_tissue_image_in_cache(1, row["PATNO"], row["EVENT_ID"])
                .replace("c1", "")
                .replace(".nii", "_seg8.mat")
                for index, row in cohort.iterrows()
            ]
            for mat_file in mat_files:
                assert os.path.exists(mat_file), f"{mat_file} doesn't exist"
                copyfile(mat_file, mat_file + ".bak")

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
        dartel_norm_job_template = pkg_root.joinpath(
            "templates",
            "dartel_normalization_job.m",
        ).as_posix()
        dartel_norm_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"dartel_norm_{ppmi.cohort_id(cohort)}_job.m"
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
        tissue_volumes_job_template = pkg_root.joinpath(
            "templates",
            "tissue_volumes_job.m",
        ).as_posix()
        volumes_job_name = os.path.abspath(
            os.path.join(
                "code", "batches", f"tissue_volumes_{ppmi.cohort_id(cohort)}_job.m"
            )
        )

        cohort["nifti_cache"] = cohort.apply(
            lambda row: ppmi.find_nifti_file_in_cache(
                row["PATNO"],
                row["EVENT_ID"],
                row["Description"],
                base_dir=Path("outputs", "pre_processing").as_posix(),
            ),
            axis=1,
        )

        if any(cohort["nifti_cache"] == ""):
            raise ValueError(
                "Some visit data is missing for pre-processing."
                '\nSee "nifti_cache" column for missing data.'
            )

        image_files = sorted(cohort["nifti_cache"].apply(os.path.abspath).values)

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
        stats_job_template = pkg_root.joinpath(
            "templates",
            "stats_job.m",
        ).as_posix()

        cohort_id = ppmi.cohort_id(cohort)

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

        self.write_spm_batch_files(
            stats_job_template,
            replace_keys,
            stats_job_name,
        )
        output = self.run_spm_batch_file(stats_job_name)
        return output

    def find_prefixed_img(
        self,
        folder: str,
        *,
        patno: int,
        visit: str,
        desc: str,
        prefix: str = "",
    ) -> Path | None:
        """Find the NifTi image for a subject visit, with the given filename `prefix`.

        Parameters
        ----------
        folder : str
            Folder to search.
        patno : int
            Patient ID.
        visit : str
            Visit ID.
        desc: str
            Scan description.
        prefix : str, optional
            Prefix for the NifTi filename, by default "".

        Returns
        -------
        Path | None
            Return the `Path` to the prefixed filename, if it exist.
            Otherwise, return `None`.
        """
        root_dir = Path(folder, f"sub-{patno}", f"ses-{visit}", "anat")
        file_pattern = f"{prefix}PPMI_*{clean_protocol_description(desc)}_br_raw_*.nii"
        filename = glob.glob(
            file_pattern,
            root_dir=root_dir.as_posix(),
        )

        if len(filename) > 1:
            raise PipelineExecutionError(
                f"Error: Expected at most one file with pattern: {file_pattern}."
            )
        elif len(filename) == 0:
            return None

        return Path(root_dir.joinpath(filename[0]))

    def pairwise_registration(
        self,
        cohort: pd.DataFrame,
        time_diff: float,
        force: bool = False,
    ) -> boutiques.ExecutorOutput | None:
        """Perform SPM pairwise registration on the `cohort`.

        Parameters
        ----------
        cohort : pd.DataFrame
            A LivingPark cohort. Must have columns PATNO, EVENT_ID and Description.
        time_diff : float
            Number of years between the baseline and follow-up visit.
        force: bool
            Whether the preprocessing should be rerun when results already exist.

        Returns
        -------
        boutiques.ExecutorOutput | None
            Boutiques execution output of SPM batch.
            None if no computation was required.

        Raises
        ------
        ValueError
            When required files for preprocessing are missing.
        """
        job_template = pkg_root.joinpath(
            "templates",
            "pairwise_registration_job.m",
        ).as_posix()
        job_name = (
            Path()
            .cwd()
            .joinpath(
                "code",
                "batches",
                f"pairwise_registration{ppmi.cohort_id(cohort)}_job.m",
            )
            .as_posix()
        )

        cohort["nifti_cache"] = cohort.apply(
            lambda row: ppmi.find_nifti_file_in_cache(
                row["PATNO"],
                row["EVENT_ID"],
                row["Description"],
                base_dir=Path("outputs", "pre_processing").as_posix(),
            ),
            axis=1,
        )

        if any(cohort["nifti_cache"] == ""):
            raise ValueError(
                "Some visit data is missing for pre-processing."
                '\nSee "nifti_cache" column for missing data.'
            )

        cohort["SPM_VOL"] = cohort["nifti_cache"].map(
            lambda x: f"'{Path(x).absolute().as_posix()},1'"
        )
        baseline = cohort[cohort["MRI_ID"].str.endswith("_Baseline")].sort_values(
            by=["PATNO"]
        )
        follow_up = cohort[cohort["MRI_ID"].str.endswith("_Follow-up")].sort_values(
            by=["PATNO"]
        )

        dv_img = baseline.apply(
            lambda row: self.find_prefixed_img(
                Path.cwd().joinpath("outputs", "pre_processing").as_posix(),
                patno=row["PATNO"],
                visit=row["EVENT_ID"],
                desc=row["Description"],
                prefix="dv_",
            ),
            axis=1,
        ).values

        # Check for existing divergence rate images.
        if force or not all(dv_img):
            print(
                "[INFO] New subjects detected.\n"
                "       Recomputing SPM longitudinal pairwise registration."
            )

            # baseline and follow_up subjects must be in the same order.
            if any(baseline["PATNO"].values != follow_up["PATNO"].values):
                raise ValueError(
                    "Mismatching order between baseline and follow-up subjects."
                    "\nThis is most likely due to a missing subject visit."
                )

            VOLS_1 = os.linesep.join(baseline["SPM_VOL"].values)
            VOLS_2 = os.linesep.join(follow_up["SPM_VOL"].values)

            self.write_spm_batch_files(
                job_template,
                {"[VOLS_1]": VOLS_1, "[VOLS_2]": VOLS_2, "[TDIF]": str(time_diff)},
                job_name,
            )

            output = self.run_spm_batch_file(
                job_name, force=True, boutiques_descriptor="zenodo.7659044"
            )

            return output

        else:
            print("No new subject detected.")

        return None

    def spatial_normalization(
        self,
        subject: pd.Series,  # TODO confirm type
        /,
        align_img_prefix: str,
        write_img_prefix: str,
        force: bool = False,
    ) -> boutiques.ExecutorOutput | None:
        """Execute SPM spatial normalization on a subject.

        Parameters
        ----------
        subject : pd.Series
            Subject to normalize.
        align_img_prefix : str
            File prefix of the image to align with the template.
        write_img_prefix : str
            File prefix of the image to normalize.
        force : bool, optional
            Whether the preprocessing should be rerun when results already exist,
            by default False.

        Returns
        -------
        boutiques.ExecutorOutput | None
            Boutiques execution output of SPM batch.
            None if no computation was required.

        Raises
        ------
        ValueError
            When required files for preprocessing are missing.
        """
        job_template = pkg_root.joinpath(
            "templates",
            "spatial_normalization_job.m",
        ).as_posix()
        job_name = (
            Path()
            .cwd()
            .joinpath(
                "code",
                "batches",
                f"spatial_normalization_{hash(subject['PATNO'])}_job.m",
            )
            .as_posix()
        )

        align_img = self.find_prefixed_img(
            Path.cwd().joinpath("outputs", "pre_processing").as_posix(),
            patno=subject["PATNO"],
            visit=subject["EVENT_ID"],
            desc=subject["Description"],
            prefix=align_img_prefix,
        )

        write_img = self.find_prefixed_img(
            Path.cwd().joinpath("outputs", "pre_processing").as_posix(),
            patno=subject["PATNO"],
            visit=subject["EVENT_ID"],
            desc=subject["Description"],
            prefix=write_img_prefix,
        )

        if align_img is None or write_img is None:
            print(align_img, write_img)
            raise ValueError("Some visit images are missing for pre-processing.")

        norm_img = self.find_prefixed_img(
            Path.cwd().joinpath("outputs", "pre_processing").as_posix(),
            patno=subject["PATNO"],
            visit=subject["EVENT_ID"],
            desc=subject["Description"],
            prefix=f"w{write_img_prefix}",
        )
        # Check for existing normalized images.
        if force or not norm_img:
            ALIGN_VOL = f"'{align_img.absolute().as_posix()},1'"
            WRITE_VOL = f"'{write_img.absolute().as_posix()},1'"

            self.write_spm_batch_files(
                job_template,
                {"[ALIGN_VOL]": ALIGN_VOL, "[WRITE_VOL]": WRITE_VOL},
                job_name,
            )

            output = self.run_spm_batch_file(
                job_name, force=True, boutiques_descriptor="zenodo.7659044"
            )
            return output

        else:
            print("No new subject detected.")

        return None

    def spatial_smoothing(
        self,
        cohort: pd.DataFrame,
        /,
        img_prefix: str,
        force: bool = False,
    ) -> boutiques.ExecutorOutput | None:
        """Execute SPM spatial smoothing for a cohort.

        Parameters
        ----------
        cohort : pd.DataFrame
            Cohort to perform spatial smoothing on.
        img_prefix : str
            File prefix for the image to smooth.
        force : bool, optional
            _description_, by default False

        force : bool, optional
            Whether the preprocessing should be rerun when results already exist,
            by default False.

        Returns
        -------
        boutiques.ExecutorOutput | None
            Boutiques execution output of SPM batch.
            None if no computation was required.

        Raises
        ------
        ValueError
            When required files for preprocessing are missing.
        """
        job_template = pkg_root.joinpath(
            "templates",
            "spatial_smoothing_job.m",
        ).as_posix()
        job_name = (
            Path()
            .cwd()
            .joinpath(
                "code",
                "batches",
                f"spatial_smoothing_{ppmi.cohort_id(cohort)}_job.m",
            )
            .as_posix()
        )

        imgs = cohort.apply(
            lambda row: self.find_prefixed_img(
                Path("outputs", "pre_processing").as_posix(),
                patno=row["PATNO"],
                visit=row["EVENT_ID"],
                desc=row["Description"],
                prefix=img_prefix,
            ),
            axis=1,
        ).values

        if any([img is None for img in imgs]):
            raise ValueError("Some visit images are missing for pre-processing.")

        smooth_imgs = cohort.apply(
            lambda row: self.find_prefixed_img(
                Path("outputs", "pre_processing").as_posix(),
                patno=row["PATNO"],
                visit=row["EVENT_ID"],
                desc=row["Description"],
                prefix=f"s{img_prefix}",
            ),
            axis=1,
        ).values

        # Check for existing normalized images.
        if force or not all(smooth_imgs):
            pass
            VOLS = os.linesep.join(
                [f"'{Path(img).absolute().as_posix()},1'" for img in imgs]
            )

            self.write_spm_batch_files(
                job_template,
                {"[VOLS]": VOLS},
                job_name,
            )

            output = self.run_spm_batch_file(
                job_name, force=True, boutiques_descriptor="zenodo.7659044"
            )
            return output

        else:
            print("No new subject detected.")

        return None
