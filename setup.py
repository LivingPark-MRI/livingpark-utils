# noqa: D100
from setuptools import setup


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
    long_description=README,
)
