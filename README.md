# LivingPark utils

A collection of utility functions to write LivingPark notebooks.

Usage examples:

```python
import livingpark_utils
from livingpark_utils import download
from livingpark_utils.clinical import moca2mmse
from livingpark_utils.dataset import ppmi

utils = livingpark_utils.LivingParkUtils()
downloader = download.ppmi.Downloader(utils.study_files_dir)

utils.notebook_init()
utils.get_study_files(["Demographics.csv"], default=downloader)
utils.get_T1_nifti_files(
    cohort, default=downloader
)  # `cohort` is of type: pd.DataFrame

ppmi.find_nifti_file_in_cache(x["PATNO"], x["EVENT_ID"], x["Description"])
ppmi.disease_duration()

moca2mmse(2)
```

Exclude subjects from a cohort without leaking patient information.

```python
from livingpark_utils.ignore import (
    insert_hash,
    remove_ignored,
)

# Assuming a cohort definition defined as `cohort`.
cohort = insert_hash(cohort, columns=["PATNO", "EVENT_ID", "Description"])
remove_ignored(cohort, ignore_file=".ppmiignore")
```

Usage to execute utility notebooks:

```python
from livingpark_utils.scripts import run

run.mri_metadata()
run.pd_status()
```

Note: Optionally use the `%%capture` cell magic to further hide notebook outputs.

# CLI commands

Download T1 nifti files using a cohort definition file.

```
$ get_T1_nifti_files <cohort_file> --downloader (ppmi) [--symlink=<bool>]
[--force=<bool>] [--timeout=<int>]
```

The `cohort_file` is a csv file created beforehand. Respectively to the chosen downloader, it must have the following columns:

- PPMI: `PATNO`, `EVENT_ID`, and `Description`.

## Troubleshooting

### Permission issues on Windows

We use symbolic links when creating the folder for cached data.
Unfortunately, by default, Windows does not authorize users to create symbolic links.
To fix this issue on your machine, please follow the guide from this [blog post](https://www.scivision.dev/windows-symbolic-link-permission-enable/).

## Contributing guidelines

We welcome contributions of any kind in the form of Pull-Request to this repository.
See also [LivingPark contributing guidelines](https://github.com/LivingPark-MRI/documentation).

Make sure to:

- Use Python type annotations
- Include Python docstrings using [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) format for all functions
- Format docstrings
- Run `psf/black` on the files you modify
- Run `pre-commit run --all` before committing, this will be checked in your PR
