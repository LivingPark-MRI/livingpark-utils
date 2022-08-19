# LivingPark utils

A collection of utility functions to write LivingPark notebooks.

Usage examples:

```
import livingpark_utils

utils = livingpark_utils.LivingParkUtils('scherfler-etal')
utils.notebook_init()
utils.download_ppmi_metadata(['Demographics.csv'])
utils.find_nifti_file_in_cache(x['PATNO'], x['EVENT_ID'], x['Description'])
utils.disease_duration()
utils.moca2mmse(2)
```

Usage to execute utility notebooks:

```python
from livingpark_utils.scripts import (
    mri_metadata,
    pd_status,
)
```

Note: This will execute the notebooks directly.

## Troubleshooting

### Permission issues on Windows

We use symbolic links when creating the folder for cached data.
Unfortunately, by default, Windows does not authorize users to create symbolic links.
To fix this issue on your machine, please follow the guide from this [blog post](https://www.scivision.dev/windows-symbolic-link-permission-enable/).

## Contributing guidelines

We welcome contributions of any kind in the form of Pull-Request to this repository.
See also [LivingPark contributing guidelines](https://github.com/LivingPark-MRI/documentation).

### Code formatting

Before committing:

- Run `psf/black` on the modified file(s)
- Run `pre-commit run --all`
