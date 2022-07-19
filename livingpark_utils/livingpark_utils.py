import datalad
import datalad.api as dat
import datetime
import warnings
import os
import os.path as op
import ppmi_downloader
import subprocess
import sys
from configparser import SafeConfigParser
import glob
from IPython.display import HTML


class LivingParkUtils:
    """
    Contains functions to be reused across LivingPark notebooks
    """

    def __init__(
        self,
        notebook_name,
        config_file=".livingpark_config",
        data_cache_path=".cache",
        use_bic_server=None,
        ssh_username=None,
        ssh_host="login.bic.mni.mcgill.ca", # TODO: call this cache server
        ssh_host_dir="/data/pd/ppmi/livingpark-papers",
    ):
        """
        Initializes a LivingPark notebook.

        Parameters:
        * notebook_name: name of the notebook. Used as DataLad dataset name when DataLad is used. Example: 'scherfler-etal'.
        * config_file: LivingPark configuration file path. Default: .livingpark_config in current working directoy. If not passed to the constructor, parameters are set by (1) looking
          into configuration file, (2) looking in environment variables, (3) prompting the user.
        * data_cache_path: local path where to store the dataset cache. Keep default value unless you know what you're doing.
        * ssh_host: ssh host where DataLad dataset is stored. Not used when DataLad is not used.
        * ssh_host_dir: directory on host where DataLad dataset is stored (absolute path). Not used when DataLad is not used.
        """

        self.notebook_name = notebook_name
        self.config_file = op.abspath(config_file)
        self.ssh_host = ssh_host
        self.ssh_host_dir = ssh_host_dir
        self.data_cache_path = data_cache_path
        self.study_files_dir = op.join("inputs", "study_files")

        os.makedirs(self.study_files_dir, exist_ok=True)

        # These variables will be set by the configuration
        self.use_bic_server = use_bic_server
        self.ssh_username = ssh_username

        save_config = True

        # look in config file
        if op.exists(self.config_file):
            config = SafeConfigParser()
            config.read(self.config_file)
            self.use_bic_server = bool(config.get("livingpark", "use_bic_server"))
            if self.use_bic_server == 'True':
                self.ssh_username = config.get("livingpark", "ssh_username")
            save_config = False

        if self.use_bic_server is None:
            # read environment variable
            var = os.environ.get("LIVINGPARK_USE_BIC_SERVER")
            if not var is None:
                self.use_bic_server = bool(var)
                save_config = False
            if self.use_bic_server:
                self.ssh_username = os.environ.get("LIVINGPARK_SSH_USERNAME")
                assert (
                    not self.ssh_username is None
                ), "Environment variable LIVINGPARK_SSH_USERNAME must be defined since LIVINGPARK_USE_BIC_SERVER is set."

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

    def setup_notebook_cache(self):
        '''
        Depending on configuration, create cache directory, datalad-install it from cache server, or datalad-update it. 
        Create symlinks for 'inputs' and 'outputs' to cache directory.
        '''

        # Create or update cache
        if self.use_bic_server:
            self.__install_datalad_cache()
        else:
            os.makedirs(self.data_cache_path, exist_ok=True)

        # Make or update links to cache
        for x in ["inputs", "outputs"]:
            if op.islink(x):
                print(f"removing link {x}")
                os.remove(x)
            elif op.exists(x):
                raise Exception(
                    f"Directory {x} exists and is not a symlink. This should not have happened."
                )
            else:
                print(f"{x} doesnt exist")
            os.symlink(op.join(self.data_cache_path, x), x)

    def prologue(self):

        """
        Function to be used as prolog in a notebook.
        """

        # Don't print warnings in notebook
        warnings.filterwarnings("ignore")

        # Install notebook dependencies
        print("Installing notebook dependencies (see log in install.log)... ")
        f = open("install.log", "wb")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            stdout=f,
            stderr=f,
        )

        # Notebook execution timestamp
        print(f"This notebook was run on {datetime.datetime.now()}")

        # Button to toggle code on/off
        on_off_button = HTML(
            """<script>
            code_show=true; 
            function code_toggle() {
                 if (code_show){
                 $('div.input').hide();
                 } else {
                 $('div.input').show();
                 }
                 code_show = !code_show
            } 
            $( document ).ready(code_toggle);
            </script>
            <form action="javascript:code_toggle()"><input type="submit" value="Click here to toggle on/off the Python code."></form>"""
        )
        return on_off_button

    def install_ppmi_study_files(self, required_files, force=False):
        """
        Download PPMI study files in required_files if they are not already available in data_dir.

        Positional parameters:
        * required_files: list of required PPMI study files (cvs files) supported by ppmi_downloader.
        * force: if True, download the files even if they are already present in self.study_files_dir.
        """

        if not force:
            missing_files = [
                x
                for x in required_files
                if not op.exists(os.path.join(self.study_files_dir, x))
            ]
        else:
            missing_files = required_files

        if len(missing_files) > 0:
            print(f"Downloading file: {missing_files}")
            ppmi = ppmi_downloader.PPMIDownloader()
            ppmi.download_metadata(
                missing_files,
                destination_dir=self.study_files_dir,
                headless=False,
                timeout=600,
            )

        print(f"The following files are now available: {required_files}")

    def __install_datalad_cache(self):
        """
        Installs the DataLad dataset located at {self.ssh_username}@{self.host}:{self.host_dir}/{self.notebook_name} into {self.data_cache_path}.
        Requires a functional ssh connection to {self.ssh_username}@{self.host}.
        """

        if op.exists(self.data_cache_path):
            # TODO: check if path is a valid DataLad dataset without doing d.status because it's too long
            d = dat.Dataset(self.data_cache_path)
            d.update(how="merge")
        else:

            dat.install(
                source=f"{self.ssh_username}@{self.ssh_host}:{self.ssh_host_dir}/{self.notebook_name}",
                path=self.data_cache_path,
            )

    def clean_protocol_description(self, desc):
        """
        Replace whitespaces and parentheses in protocol descriptions to use
        them in file names (as done by PPMI)

        Parameters:
        * desc: Protocol description. Example: 'MPRAGE GRAPPA'
        """
        return desc.replace(" ", "_").replace("(", "_").replace(")", "_").replace("/", "_")

    def find_nifti_file_in_cache(
        self, subject_id, event_id, protocol_description, base_dir="inputs"
    ):
        '''
      In cache directory, search for nifti file matching subject_id, event_id and protocol_description. If not found, 
      search for nifti file matching subject_id and event_id only, and return it if a single file is found.

      Parameters:
      * subject_id: Subject id
      * event_id: Event id. Example: BL
      * protocol_description: Protocol description. Example: 'MPRAGE GRAPPA'

      Return value:
      * File name matching the subject_id, event_id, and if possible protocol_description. None if no matching file is found.
      '''

        expression = op.join(
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
        print(f'Warning: no nifti file found for: {(subject_id, event_id, protocol_description)}, removing protocol description from glob expression')
        expression = op.join(
            self.data_cache_path,
            base_dir,
            f"sub-{subject_id}",
            f"ses-{event_id}",
            "anat",
            f"PPMI_*.nii",
        )
        files = glob.glob(expression)
        assert len(files) <= 1, f"More than 1 Nifti file matched by {expression}"
        if len(files) == 1:
            return files[0]
        print(f'Warning: no nifti file found for: {(subject_id, event_id, protocol_description)}, using lenient expression, returning None')
        return None
