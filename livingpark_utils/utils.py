import datalad
import datalad.api as dat
import datetime
import warnings
import os
import os.path as op
import ppmi_downloader
import subprocess
import sys
from IPython.display import HTML

def prologue():
    '''
    Function to be used as prolog in a notebook.
    '''

    # Don't print warnings in notebook
    warnings.filterwarnings("ignore")

    # Install notebook dependencies
    print('Installing notebook dependencies (see log in install.log)... ')
    f = open('install.log', 'wb')
    subprocess.check_call([sys.executable, "-m", "pip", "install", '-r', 'requirements.txt'], stdout=f, stderr=f)

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


def install_ppmi_study_files(required_files, data_dir='data', force=False):
    '''
    Download PPMI study files in required_files if they are not already available in data_dir.

    Positional parameters:
    * required_files: list of required PPMI study files (cvs files) supported by ppmi_downloader.
    * data_dir: directory where to download the files. 
    * force: if True, download the files even if they are already present in data_dir.
    '''

    if not force:
        missing_files = [x for x in required_files if not op.exists(os.path.join(data_dir, x))]
    else:
        missing_files = required_files

    if len(missing_files) > 0:
        print(f"Downloading file: {missing_files}")
        ppmi = ppmi_downloader.PPMIDownloader()
        ppmi.download_metadata(
            missing_files, destination_dir=data_dir, headless=False, timeout=600
        )

    print(f"The following files are now available: {required_files}")

def install_datalad_repo(username, repo_name, host='login.bic.mni.mcgill.ca', host_dir='/data/pd/ppmi/livingpark-papers', local_datalad_path='.datalad'):
    '''
    Installs the DataLad dataset located at {username}@{host}:{host_dir}/{repo_name} into {local_datalad_path}. Requires a functional ssh connection to {username}@{host}.

    * username: user name on host.
    * repo_name: name of DataLad dataset located in host_dir on host.
    * host: ssh host where DataLad dataset is stored.
    * host_dir: directory on host where DataLad dataset is stored (absolute path).
    * local_datalad_path: local path where to download the dataset. Use default unless you know what you're doing.
    '''
    
    if op.exists(local_datalad_path):
        # TODO: check if path is a valid DataLad dataset without doing d.status because it's too long
        d = dat.Dataset(local_datalad_path)
        d.update(how="merge")
    else:
        dat.install(
            source=f"{username}@{host}:{host_dir}/{repo_name}",
            path=local_datalad_path,
    )