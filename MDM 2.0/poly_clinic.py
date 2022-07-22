# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 12:36:18 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os

os.chdir(r'D:\\SVAAS_2\\svaas_doctors_21_07_22')
clinic_add= pd.read_csv('tbl_doc_clinic_add.csv',error_bad_lines=False)


#clinic_add.columns
clinic=clinic_add.copy()

clinic=clinic[['name','opening_time','closing_time','clinic_phone_no','clinic_email','address','clinic_pincode','longitute','latitude','city_id','branch_id','status','id','drl_clinic_id']]

clinic.columns=['name','openTimings','closeTimings','phone','email','address','pincode','lon','lat','city','erxClinicId','isActive','svaasId','svaasClinicId']

clinic['highlights']=None
clinic['helpline']=None
clinic['locationUrl']=None
clinic['state']=None
clinic['image']=None
clinic['logo']=None
clinic['photos']=None
clinic['ourBestDoctor']=None
clinic['servicePincode']=None
clinic['serviceState']=None
clinic['serviceCity']=None

clinic1 = clinic[['name','openTimings','closeTimings','phone','email','highlights','helpline','locationUrl','address','pincode','lon','lat','state','city','image','erxClinicId','logo','photos','ourBestDoctor','servicePincode','serviceState','serviceCity','isActive','svaasId','svaasClinicId']]

clinic1.to_csv('poly_clinic.csv', index=False)
