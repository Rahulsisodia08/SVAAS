# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 22:29:54 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\SVAAS_data_17_06_22')
# uPLOADING DATA
df_pres= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\prescribed_test_mapping_17_06_22.csv')

#Uploading brdg_appt_conslt_mapping data
df= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\brdg_appt_conslt_mapping_17_06_22.csv')
# Creating new df by taking doc_id and appt_id
doc_id=df[['brdg_appt_conslt_sid','doctor_id']]
# Removing duplicates from doc_id and appt_id
doc_id=doc_id.drop_duplicates(['brdg_appt_conslt_sid','doctor_id'])
# Merging two data frame 
df1_new=pd.merge(df_pres,doc_id, how='left', on='brdg_appt_conslt_sid')
# prescribed medicine cnt
doctor_wise_pres_med_cnt=df1_new.groupby('doctor_id').agg({'prescribed_test':'count'}).rename(columns={"prescribed_test":"pres_test_cnt"}).sort_values(by='pres_test_cnt',ascending=False)
doctor_wise_map_med_cnt=df1_new.groupby('doctor_id').agg({'mapped_test_name':'count'}).rename(columns={"mapped_test_name":"mapped_test_name"}).sort_values(by='mapped_test_name',ascending=False)

#brdg_appt_id count by doctor wise in prescribed medicine
doctor_wise_pres_med_brdg_cnt=df1_new[['doctor_id','brdg_appt_conslt_sid']].groupby('doctor_id').nunique()
#brdg_appt_id count by doctor wise in mapped medicine
doctor_wise_map_med_brdg_cnt=df1_new[df1_new.mapped_test_name.notnull()][['doctor_id','brdg_appt_conslt_sid']].groupby('doctor_id').nunique()

pres_med_all=pd.merge(doctor_wise_pres_med_cnt,doctor_wise_map_med_cnt, how='outer', on='doctor_id')
pres_med_all=pd.merge(pres_med_all,doctor_wise_pres_med_brdg_cnt, how='outer', on='doctor_id')
pres_med_all=pd.merge(pres_med_all,doctor_wise_map_med_brdg_cnt, how='outer', on='doctor_id')
#sending file to csv
#changing columns
pres_med_all.columns=['pres_test_count','mapped_test_count','pres_med_visit_cnt','maped_test_visit_cnt']

pres_med_all.to_csv('pres_test_all.csv')
 
#doctor_wise_pres_med_cnt.columns=['doctor_id','pres_med_cnt','mapped_medicine_name','pres_visit_cnt','maped_visit_cnt']