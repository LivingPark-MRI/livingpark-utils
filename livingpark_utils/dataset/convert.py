import logging
from pathlib import Path

from nipype.interfaces.dcm2nii import Dcm2niix


class fileConversionError(Exception):
    pass


def dcm2niix(filenames: list[Path] | Path, output_dir: Path = None):
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
    except:
        raise fileConversionError
