"""Utility functions to convert datasets between different file formats."""
from pathlib import Path
from typing import Any

from nipype.interfaces.dcm2nii import Dcm2niix


class fileConversionError(Exception):
    """Exception to report a failed file conversion."""

    pass


def dcm2niix(filenames: list[Path] | Path, output_dir: Path = None) -> Any:
    """Convert DICOM files to NIfTI using dcm2niix.

    Parameters
    ----------
    filenames : list[Path] | Path
        File to convert. Can either be a list of files or a directory.
    output_dir : Path, optional
        Path to the output folder, by default None

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

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
        converter.inputs.output_dir = output_dir

    try:
        return converter.run()
    except Exception as e:
        raise fileConversionError(e)
