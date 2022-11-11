#!/usr/bin/env python
# coding: utf-8

# # Introduction
# 
# Many imaging studies require T1-weighted images with specific acquisition parameters, usually sagittal acquisition. In PPMI, metadata on MRI acquisition is available in file "Magnetic_Resonance_Imaging__MRI_.csv", however, this file does not contain detailed information about acquistion parameters.
# 
# This notebook downloads acquisition parameters of 3D T1-weighted images from the PPMI imaging database, filters sagittal acquisition scans, and converts visit names used in the imaging database to the ones used in other PPMI metadata. 
# 
# The resulting file can be used to build imaging cohorts based on PPMI.

# In[1]:


from IPython.display import HTML

HTML('''<script>
code_show=true; 
function code_toggle() {
 if (code_show){
 $('div.input').hide();
 } else {
 $('div.input').show();
 }
 code_show = !code_show
} 
$( document ).ready(code_toggle);
</script>
<form action="javascript:code_toggle()"><input type="submit" value="Click here to toggle on/off the Python code."></form>''')


# # Data download
# 
# Let's download information about PPMI 3D T1-weighted scans:

# In[2]:


import os
import os.path as op
import pandas as pd
import ppmi_downloader

data_dir = op.join('inputs', 'study_files')

if not op.exists(data_dir):
    os.makedirs(data_dir)

mri_file_name = '3D_mri_info.csv'
required_files = [mri_file_name]
missing_files = [x for x in required_files if not op.exists(os.path.join(data_dir, x))]

if len(missing_files) > 0:
    ppmi = ppmi_downloader.PPMIDownloader()
    file_name = ppmi.download_3D_T1_info(destination_dir=data_dir, headless=False)
    assert(op.exists(op.join(data_dir, file_name)))
    os.rename(op.join(data_dir, file_name), op.join(data_dir, mri_file_name))

print('File downloaded')


# # Filter sagittal acquisitions
# 
# PPMI scans were acquired using the following protocols:

# In[3]:


pd.set_option('display.max_rows', 500)
mri_info = pd.read_csv(op.join(data_dir, mri_file_name))
mri_info.groupby('Description').count()


# To keep only the sagittal acquisitions, we will remove the following protocols:

# In[4]:


# Remove sequences that exactly match the following
removed_sequences=['COR', # coronal acquisitions
                   'Coronal', 
                   'Cal Head 24',  # not sure what this is
                   'Transverse',  # transverse (axial) acquisitions
                   'tra_T1_MPRAGE',
                   'TRA'
                  ]
print(removed_sequences)


# We will also remove the protocol names containing the following strings:

# In[5]:


# Remove sequences containing the following strings
removed_sequences_contain = ['AX', 'axial', 'Phantom']
print(removed_sequences_contain)


# We obtain the following list of protocols:

# In[6]:


mri_info = mri_info[~mri_info['Description'].isin(removed_sequences)]
for s in removed_sequences_contain:
    mri_info = mri_info[~mri_info['Description'].str.contains(s)]
mri_info.groupby('Description').count()


# # Convert visit names

# Let's use the following map to convert visit names to the codes used in PPMI metadata:

# In[7]:


visit_map = {
    'Screening': 'SC',
    'Baseline': 'BL',
    'Month 12': 'V04',
    'Month 24': 'V06',
    'Month 36': 'V08',
    'Month 48': 'V10',
    'Symptomatic Therapy': 'ST',
    'Unscheduled Visit 01': 'U01',
    'Unscheduled Visit 02': 'U02',
    'Premature Withdrawal': 'PW'
}
print(visit_map)


# We obtain the following distribution by visit code:

# In[8]:


mri_info['Visit code'] = mri_info['Visit'].apply(lambda x: visit_map[x])
mri_info.groupby('Visit code').count()


# Finally, let's save our table as csv file:

# In[9]:


filename = 'MRI_info.csv'
mri_info.to_csv(op.join(data_dir, filename), index=False)
print(f'Saved in {filename}')


# In[ ]:




