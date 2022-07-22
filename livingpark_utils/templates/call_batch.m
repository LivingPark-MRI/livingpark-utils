addpath('/spm12-r7771')
spm('defaults', 'PET');
spm_jobman('initcfg');
[BATCH]
spm_jobman('run', matlabbatch);
