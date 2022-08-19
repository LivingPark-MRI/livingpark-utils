"""Convinience methods to run the auto-generated scripts.

Notes
-----
The methods below need to be created manually when a new notebook is added.
"""
import importlib


def mri_metadata():
    """Execute auto-generated script for `../notebooks/mri_metadata.ipynb`."""
    importlib.import_module("mri_metadata", "livingpatk_utils.scripts")


def pd_status():
    """Execute auto-generated script for `../notebooks/pd_status.ipynb`."""
    importlib.import_module("pd_status", "livingpatk_utils.scripts")
