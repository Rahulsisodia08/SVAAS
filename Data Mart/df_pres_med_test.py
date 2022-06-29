# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 14:56:24 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\SVAAS_data_17_06_22')
# uPLOADING DATA
df= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\brdg_appt_conslt_mapping_17_06_22.csv')
df.columns
df1=df[['brdg_appt_conslt_sid','appointment_dt_sid','appointment_datetime','appointment_status','appointment_type',
        'doctor_sid','prescription','next_visit','complaints','diagnosis', 'advice', 'bp_dia', 'bp_sys', 'pulse', 
        'weight', 'height','bmi']]

df1['appointment_complete_status']=df1['appointment_status'].replace({'completed':1,'missed':0,'cancelled':0,'declined':0,'no_show':0,'confirmed':0,
                                   'in progress':0,'booked':0})

# =============================================================================
# medicine mapping
# =============================================================================

#Setting directory of data
os.chdir(r'D:\SVAAS_2\SVAAS_data_17_06_22')
# uPLOADING DATA
df_pres= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\prescribed_medicine_mapping_17_06_22.csv')

#Uploading brdg_appt_conslt_mapping data
df_parent= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\brdg_appt_conslt_mapping_17_06_22.csv')
# Creating new df by taking doc_id and appt_id
doc_id=df[['brdg_appt_conslt_sid','doctor_sid']]
# Removing duplicates from doc_id and appt_id
doc_id=doc_id.drop_duplicates(['brdg_appt_conslt_sid','doctor_sid'])
# Merging two data frame 
df1_new=pd.merge(df_pres,doc_id, how='left', on='brdg_appt_conslt_sid')
# prescribed medicine cnt
doctor_wise_pres_med_cnt=df1_new.groupby(['doctor_sid','brdg_appt_conslt_sid']).agg({'prescribed_medicine':'count'}).rename(columns={"prescribed_medicine":"pres_med_cnt"}).sort_values(by='pres_med_cnt',ascending=False)
# Mapped medicine count
doctor_wise_map_med_cnt=df1_new.groupby(['doctor_sid','brdg_appt_conslt_sid']).agg({'mapped_medicine_name':'count'}).rename(columns={"prescribed_medicine":"mapped_medicine_name"}).sort_values(by='mapped_medicine_name',ascending=False)

#brdg_appt_id count by doctor id wise in prescribed medicine
doctor_wise_pres_med_brdg_cnt=df1_new[['doctor_sid','brdg_appt_conslt_sid']].groupby(['doctor_sid','brdg_appt_conslt_sid']).nunique()
#brdg_appt_id count by doctor id wise in mapped medicine
doctor_wise_map_med_brdg_cnt=df1_new[df1_new.mapped_medicine_name.notnull()][['doctor_sid','brdg_appt_conslt_sid']].groupby(['doctor_sid','brdg_appt_conslt_sid']).nunique()

pres_med_all=pd.merge(doctor_wise_pres_med_cnt,doctor_wise_map_med_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
pres_med_all=pd.merge(pres_med_all,doctor_wise_pres_med_brdg_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
pres_med_all=pd.merge(pres_med_all,doctor_wise_map_med_brdg_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
#sending file to csv
#changing columns
pres_med_all.columns=['pres_med_cnt','mapped_medicine_cnt']
pres_med_all=pres_med_all.reset_index()
pres_med_all2=pd.merge(pres_med_all,df1,how='outer',on=['brdg_appt_conslt_sid','doctor_sid'])
#pres_med_all3=pd.merge(df1.set_index('brdg_appt_conslt_sid', drop=True),pres_med_all.set_index('brdg_appt_conslt_sid', drop=True), how='outer',left_on=['doctor_sid'],right_on=['doctor_id'])



pres_med_all.to_csv('final_report2.csv')
df1.to_csv('final_report1.csv')

#doctor_wise_pres_med_cnt.columns=['doctor_id','pres_med_cnt','mapped_medicine_name','pres_visit_cnt','maped_visit_cnt']

# =============================================================================
# Prescribed test mapping
# =============================================================================
df_pres= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\prescribed_test_mapping_17_06_22.csv')

#Uploading brdg_appt_conslt_mapping data
df_parent= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\brdg_appt_conslt_mapping_17_06_22.csv')
# Creating new df by taking doc_id and appt_id
doc_id=df[['brdg_appt_conslt_sid','doctor_sid']]
# Removing duplicates from doc_id and appt_id
doc_id=doc_id.drop_duplicates(['brdg_appt_conslt_sid','doctor_sid'])
# Merging two data frame 
df1_new=pd.merge(df_pres,doc_id, how='left', on='brdg_appt_conslt_sid')
# prescribed medicine cnt
doctor_wise_pres_test_cnt=df1_new.groupby(['doctor_sid','brdg_appt_conslt_sid']).agg({'prescribed_test':'count'}).rename(columns={"prescribed_test":"pres_test_cnt"}).sort_values(by='pres_test_cnt',ascending=False)
doctor_wise_map_test_cnt=df1_new.groupby(['doctor_sid','brdg_appt_conslt_sid']).agg({'mapped_test_name':'count'}).rename(columns={"mapped_test_name":"mapped_test_name"}).sort_values(by='mapped_test_name',ascending=False)

#brdg_appt_id count by doctor wise in prescribed medicine
doctor_wise_pres_test_brdg_cnt=df1_new[['doctor_sid','brdg_appt_conslt_sid']].groupby(['doctor_sid','brdg_appt_conslt_sid']).nunique()
#brdg_appt_id count by doctor wise in mapped medicine
doctor_wise_map_test_brdg_cnt=df1_new[df1_new.mapped_test_name.notnull()][['doctor_sid','brdg_appt_conslt_sid']].groupby(['doctor_sid','brdg_appt_conslt_sid']).nunique()

pres_test_all=pd.merge(doctor_wise_pres_test_cnt,doctor_wise_map_test_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
pres_test_all=pd.merge(pres_test_all,doctor_wise_pres_test_brdg_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
pres_test_all=pd.merge(pres_test_all,doctor_wise_map_test_brdg_cnt, how='outer', on=['doctor_sid','brdg_appt_conslt_sid'])
pres_test_all.reset_index()
#merging
#pres_test_all2=pd.merge(pres_test_all,df1,how='outer',on=['doctor_sid','brdg_appt_conslt_sid'])

pres_test_all2=pd.merge(df1,pres_test_all, how='outer',on=['brdg_appt_conslt_sid','doctor_sid'])

#merging pres_med_all and pre_test_all
pres_med_test_all=pd.merge(pres_med_all,pres_test_all, how='outer',on=['brdg_appt_conslt_sid','doctor_sid'])

#merging with df1 and pres_med_test_all
df_med_test=pd.merge(df1,pres_med_test_all, how='outer',on=['brdg_appt_conslt_sid','doctor_sid'])
#sending to csv
df_med_test.to_csv('df_med_test.csv',index=False)


# =============================================================================
# loading bill summary
# =============================================================================





