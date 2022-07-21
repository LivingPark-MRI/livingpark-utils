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

## Contributing guidelines

We welcome contributions of any kind in the form of Pull-Request to this repository.
See also [LivingPark contributing guidelines](https://github.com/LivingPark-MRI/documentation).


### Code formatting

Before committing:
* Run `psf/black` on the modified file(s)
* Run `pre-commit run --all`
