# noqa: D100
from setuptools import setup


install_requires = [
    "datalad",
    "IPython",
    "ppmi_downloader",
]

setup(
    name="livingpark_utils",
    version="0.3.1",
    description="Utility functions to write LivingPark notebooks.",
    author="Tristan Glatard",
    author_email="tristan.glatard@concordia.ca",
    license="MIT",
    packages=["livingpark_utils"],
    python_requires=">=3.10",
    install_requires=install_requires,
)
