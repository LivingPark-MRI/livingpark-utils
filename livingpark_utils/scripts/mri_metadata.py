#!/usr/bin/env python
# coding: utf-8

# # Introduction
# 
# Many imaging studies require T1-weighted images with specific acquisition parameters, usually sagittal acquisition. In PPMI, metadata on MRI acquisition is available in file "Magnetic_Resonance_Imaging__MRI_.csv", however, this file does not contain detailed information about acquistion parameters.
# 
# This notebook downloads acquisition parameters of 3D T1-weighted images from the PPMI imaging database, filters sagittal acquisition scans, and converts visit names used in the imaging database to the ones used in other PPMI metadata. 
# 
# The resulting file can be used to build imaging cohorts based on PPMI.

# # Data download
# 
# Let's download information about PPMI 3D T1-weighted scans:

# In[1]:


import livingpark_utils
import ppmi_downloader
import os


utils = livingpark_utils.LivingParkUtils()
utils.notebook_init()

mri_file_name = "3D_mri_info.csv"
if not os.path.exists(os.path.join(utils.study_files_dir, mri_file_name)):
    ppmi = ppmi_downloader.PPMIDownloader()
    file_name = ppmi.download_3D_T1_info(destination_dir=utils.study_files_dir)
    os.rename(
        os.path.join(utils.study_files_dir, file_name),
        os.path.join(utils.study_files_dir, mri_file_name),
    )


# In[2]:


# import pandas as pd

# mri_file_name_all = '3D_mri_info_all.csv'
# all_images = pd.read_csv(os.path.join(utils.study_files_dir, mri_file_name_all))
# pd.unique(all_images['Description'])


# # Filter non-T1 acquisitions
# 
# The "Weighting" parameter in the Imaging Protocol field is not fully reliable as some T1 images have "Weighting=PD". Therefore we extract T1 images as the ones with "Weighting=T1" in their Imaging Protocol OR "T1" in their protocol description. We obtain the following list of protocol descriptions.

# In[3]:


import pandas as pd

pd.set_option("display.max_rows", 500)
mri_info = pd.read_csv(os.path.join(utils.study_files_dir, mri_file_name))


# In[4]:


# Keep only T1 images
mri_info = mri_info[
    mri_info["Imaging Protocol"].str.contains("Weighting=T1")
    | mri_info["Description"].str.contains("t1")
    | mri_info["Description"].str.contains("T1")
]
mri_info.groupby("Description").count()


# # Filter sagittal acquisitions
# 

# To keep only the sagittal acquisitions, we will remove the following protocols:

# In[5]:


# Remove sequences that exactly match the following
removed_sequences = [
    "COR",  # coronal acquisitions
    "Coronal",
    "Cal Head 24",  # not sure what this is
    "Transverse",  # transverse (axial) acquisitions
    "tra_T1_MPRAGE",
    "TRA",
]
print(removed_sequences)


# We will also remove the protocol names containing the following strings:

# In[6]:


# Remove sequences containing the following strings
removed_sequences_contain = ["AX", "Ax", "axial", "Phantom", "T2"]
print(removed_sequences_contain)


# We obtain the following list of protocols:

# In[7]:


mri_info = mri_info[~mri_info["Description"].isin(removed_sequences)]
for s in removed_sequences_contain:
    mri_info = mri_info[~mri_info["Description"].str.contains(s)]
mri_info.groupby("Description").count()


# # Convert visit names

# Let's use the following map to convert visit names to the codes used in PPMI metadata:

# In[8]:


visit_map = {
    "Screening": "SC",
    "Baseline": "BL",
    "Month 6": "V02",
    "Month 12": "V04",
    "Month 24": "V06",
    "Month 36": "V08",
    "Month 48": "V10",
    "Symptomatic Therapy": "ST",
    "Unscheduled Visit 01": "U01",
    "Unscheduled Visit 02": "U02",
    "Premature Withdrawal": "PW",
}
print(visit_map)


# We obtain the following distribution by visit code:

# In[9]:


mri_info["Visit code"] = mri_info["Visit"].apply(lambda x: visit_map[x])
mri_info.groupby("Visit code").count()


# Finally, let's save our table as csv file:

# In[10]:


filename = "MRI_info.csv"
mri_info.to_csv(os.path.join(utils.study_files_dir, filename), index=False)
print(f"Saved in {filename}")


# In[ ]:




