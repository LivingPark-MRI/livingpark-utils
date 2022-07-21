"""Provide utility function for the LivingPark notebook for paper replication."""
import datetime
import glob
import math
import os
import pkgutil
import subprocess
import sys
import warnings
from configparser import SafeConfigParser
from pprint import pprint

import datalad
import numpy as np
import pandas as pd
import ppmi_downloader
import pytz  # type: ignore
from dateutil.parser import parse  # type: ignore
from dateutil.relativedelta import relativedelta  # type: ignore
from IPython.display import HTML


class LivingParkUtils:
    """Contain functions to be reused across LivingPark notebooks."""

    def __init__(
        self,
        notebook_name: str,
        config_file: str = ".livingpark_config",
        data_cache_path: str = ".cache",
        use_bic_server: bool = None,
        ssh_username: str = None,
        ssh_host: str = "login.bic.mni.mcgill.ca",  # TODO: call this cache server
        ssh_host_dir: str = "/data/pd/ppmi/livingpark-papers",
    ) -> None:
        """Initialize a LivingPark notebook.

        When undefined, the parameters of the LivingPark configuration are set by
            #. Looking for configuration file
            #. Looking for environment variables
            #. Prompting the user

        Notes
        -----
        The configuration parameters for SSH are only used when Datalad is.

        Parameters
        ----------
        notebook_name: str
            Name of the notebook. Used as DataLad dataset name when DataLad is used.
            Example: "scherfler-etal".
        config_file: str, default ".livingpark_config"
            File path of LivingPark configuration.
        data_cache_path: str, default ".cache"
            Local path where to store the dataset cache.
            Keep default value unless you know what you're doing.
        use_bic_server: TODO check type
            TODO add description.
        ssh_host: str, default "login.bic.mni.mcgill.ca"
            SSH host where DataLad dataset is stored.
        ssh_host_dir: str, default "/data/pd/ppmi/livingpark-papers"
            Absolute path to host directory where DataLad dataset is stored.
        """
        self.notebook_name = notebook_name
        self.config_file = os.path.abspath(config_file)
        self.ssh_host = ssh_host
        self.ssh_host_dir = ssh_host_dir
        self.data_cache_path = data_cache_path
        self.study_files_dir = os.path.abspath(os.path.join("inputs", "study_files"))

        os.makedirs(self.study_files_dir, exist_ok=True)

        # These variables will be set by the configuration
        self.use_bic_server = use_bic_server
        self.ssh_username = ssh_username

        save_config = True

        # look in config file
        if os.path.exists(self.config_file):
            config = SafeConfigParser()
            config.read(self.config_file)
            self.use_bic_server = bool(config.get("livingpark", "use_bic_server"))
            if self.use_bic_server == "True":
                self.ssh_username = config.get("livingpark", "ssh_username")
            save_config = False

        if self.use_bic_server is None:
            # read environment variable
            var = os.environ.get("LIVINGPARK_USE_BIC_SERVER")
            if var is not None:
                self.use_bic_server = bool(var)
                save_config = False
            if self.use_bic_server:
                self.ssh_username = os.environ.get("LIVINGPARK_SSH_USERNAME")
                if self.ssh_username is None:
                    raise Exception(
                        "Environment variable LIVINGPARK_SSH_USERNAME must be defined"
                        " since LIVINGPARK_USE_BIC_SERVER is set."
                    )

        if self.use_bic_server is None:
            # prompt user
            answer = input(f"Do you have an account on {self.ssh_host}? (y/n) ")
            if any(answer.lower() == f for f in ["yes", "y", "1", "ye"]):
                self.use_bic_server = True
                self.ssh_username = input(f"What's your username on {self.ssh_host}? ")
            else:
                self.use_bic_server = False
            # TODO: attempt ssh connection / check git-annex config

        if save_config:
            print("write config file")
            # write config file
            config = SafeConfigParser()
            config.read(self.config_file)
            config.add_section("livingpark")
            config.set("livingpark", "use_bic_server", str(self.use_bic_server))
            if self.use_bic_server:
                config.set("livingpark", "ssh_username", self.ssh_username)
            with open(self.config_file, "w") as f:
                config.write(f)

    def setup_notebook_cache(self) -> None:
        """Create, install, and update the cache directory, if needed.

        Notes
        -----
        Aggregate the inputs and outputs into a single dataset by creating symlinks.
        """
        # TODO: enable DataLad synchro with BIC server
        # Create or update cache
        # if self.use_bic_server:
        #     self.__install_datalad_cache()
        # else:

        os.makedirs(self.data_cache_path, exist_ok=True)

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
        f = open("install.log", "wb")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            stdout=f,
            stderr=f,
        )

        now = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S %Z %z")
        print(f"This notebook was run on {now}")

        return pkgutil.get_data(__name__, "toggle_button.html")

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

    def __install_datalad_cache(self) -> None:
        """Install the DataLad dataset.

        Notes
        -----
        Requires a functional ssh connection to `self.ssh_username`@`self.host`.
        Located at `self.host_dir`/`self.notebook_name`/`self.data_cache_path`.
        """
        if os.path.exists(self.data_cache_path):
            # noqa: TODO check if path is a valid DataLad dataset without doing d.status because it's too long.
            d = datalad.api.Dataset(self.data_cache_path)
            d.update(how="merge")
        else:
            datalad.api.install(
                source=(
                    f"{self.ssh_username}@{self.ssh_host}:"
                    f"{self.ssh_host_dir}/{self.notebook_name}"
                ),
                path=self.data_cache_path,
            )

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
    ) -> str | None:
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
            TODO Describe this. Not sure what it is exactly.

        Returns
        -------
        str or None
            File name matching the `subject_id`, `event_id`, and if possible
            `protocol_description`. None if no matching file is found.
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
        print(
            "Warning: no nifti file found for: "
            f"{(subject_id, event_id, protocol_description)} with strict glob "
            "expression. Trying with lenient glob expression."
        )
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
        print(
            f"Warning: no nifti file found for: "
            f"{(subject_id, event_id, protocol_description)} "
            "with lenient expression, returning None"
        )
        return None

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
        print(
            f"Number of available subjects: {len(cohort[cohort['File name'].notna()])}"
        )
        print(f"Number of missing subjects: {len(cohort[cohort['File name'].isna()])}")

        # Download missing file names
        try:
            ppmi_dl = ppmi_downloader.PPMIDownloader()
            missing_subject_ids = cohort[cohort["File name"].isna()]["PATNO"]
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
            if row["File name"] is None:
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
                    # print(dest_dir, file_name, dest_file)
                    os.makedirs(dest_dir, exist_ok=True)
                    os.symlink(
                        os.path.relpath(os.path.abspath(file_name), start=dest_file),
                        dest_file,
                    )
