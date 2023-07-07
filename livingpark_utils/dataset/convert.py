"""Utility functions to convert datasets between different file formats."""
from pathlib import Path
from typing import Any

from nipype import config
from nipype import logging
from nipype.interfaces.dcm2nii import Dcm2niix

log_dir = Path("logs").absolute()
log_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
config.update_config({"logging": {"log_directory": log_dir, "log_to_file": True}})
config.enable_debug_mode()
logging.update_logging(config)


class fileConversionError(Exception):
    """Exception to report a failed file conversion."""

    pass


def dcm2niix(filenames: list[Path] | Path, output_dir: Path) -> Any:
    """Convert DICOM files to NIfTI using dcm2niix.

    Parameters
    ----------
    filenames : list[Path] | Path
        File to convert. Can either be a list of files or a directory.
    output_dir : Path, optional
        Path to the output folder

    Returns
    -------
    Any
        If conversion is successful, returns the converter. Otherwise, None.

    Raises
    ------
    fileConversionError
        When a file conversion fails.
    """
    converter = Dcm2niix()
    if isinstance(filenames, Path) and filenames.is_dir():
        converter.inputs.source_dir = filenames
    else:
        converter.inputs.source_names = filenames

    output_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    converter.inputs.output_dir = output_dir

    try:
        return converter.run()
    except Exception as e:
        raise fileConversionError(e)
