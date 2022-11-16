# noqa: D100
from setuptools import setup
import codecs
import os

install_requires = [
    "datalad",
    "IPython",
    "ppmi_downloader",
    "nilearn",
    "boutiques",
    "pillow",
    "matplotlib",
    "numpy",
    "pandas",
    "pytz",
]

here = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\\n" + fh.read()

setup(
    name="livingpark_utils",
    version="0.7",
    description="Utility functions to write LivingPark notebooks.",
    author="Tristan Glatard",
    author_email="tristan.glatard@concordia.ca",
    license="MIT",
    packages=["livingpark_utils"],
    python_requires=">=3.10",
    install_requires=install_requires,
    include_package_data=True,
    long_description_content_type="text/markdown",
    long_description=long_description,
)
