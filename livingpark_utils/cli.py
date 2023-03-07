"""CLI entrypoint for livingpark utilities."""
from typing import Any

import click
import pandas as pd

import livingpark_utils


def import_from(module: str, name: str) -> Any:
    """Import an object from a given module.

    Parameters
    ----------
    module : str
        Import path to the module containing the object.
    name : str
        Object to import withing the module.

    Returns
    -------
    Any
        Object to import.
    """
    module_obj = __import__(module, fromlist=[name])
    return getattr(module_obj, name)


@click.command()
@click.option("--downloader", type=click.Choice(["ppmi"]))
@click.option(
    "--symlink",
    type=bool,
    default=True,
    help="Create symlink between the cache and the `input`/`output` directories.",
)
@click.option(
    "--force",
    type=bool,
    default=False,
    help="Download all the subject files regardless of local cache.",
)
@click.option(
    "--timeout",
    type=int,
    default=120,
    help="Maximum duration before interrupting the download (per subject).",
)
@click.option(
    "--batch-size",
    type=int,
    default=50,
    help="Number of file to download in each batch.",
)
@click.argument(
    "cohort_file", type=click.Path(exists=True, file_okay=True, readable=True)
)
def get_T1_nifti_files(
    downloader: str,
    symlink: bool,
    force: bool,
    timeout: int,
    batch_size: int,
    cohort_file: str,
):
    r"""Download T1 nifti files from cohort csv file.

    The csv file is generated in he Jupyter notebook for cohort definition.
    \f

    Parameters
    ----------
    downloader : str
        Type of downloader to use. Choices: ["ppmi"].
    symlink : bool
        When `True`, symlinks are created from the caching directory., by default
            True.
    force : bool
        When `True`, all study files are reported missing locally., by default False.
    timeout : int
        Number of second before the download times out., by default 120.
    batch_size : int
        Number of files to download in each batch.
    cohort_file : str
        Path to the csv file containing the cohort definition.
    """
    utils = livingpark_utils.LivingParkUtils()
    download_handler = import_from(
        f"livingpark_utils.download.{downloader}", "Downloader"
    )(utils.study_files_dir)

    utils.get_T1_nifti_files(
        pd.read_csv(cohort_file),
        default=download_handler,
        symlink=symlink,
        force=force,
        timeout=timeout,
        batch_size=batch_size,
    )
