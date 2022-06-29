# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 22:44:50 2022

@author: RahulKumarSisodia
"""
import pandas as pd
import numpy as np
import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\SVAAS_data_17_06_22')
# uPLOADING DATA
df_bill= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\fact_bill_summary_17_06_22.csv')
# total unique patient
df_bill2=df_bill[['patient_sid','doctor_sid','transaction_type','total_transaction_amount','bill_sid']]
df_bill2.drop_duplicates('bill_sid',inplace=True)
df_bill2=df_bill2[df_bill2['transaction_type']=='Doctor consultation']

df_bill['patient_sid'].nunique()
#df_bill=df_bill[['patient_sid','doctor_sid','transaction_type','total_transaction_amount']]
#patients sid count greater than 1
df_bill=df_bill[df_bill['transaction_type']=='Doctor consultation']
#repeated patients df
repeated_visit=df_bill2[df_bill2['patient_sid'].map(df_bill2['patient_sid'].value_counts()) > 1]
#unique patients df
unique_visit=df_bill2[df_bill2['patient_sid'].map(df_bill2['patient_sid'].value_counts())==1]
#groupby repeated visit counts
repeat_patient_doc_paid_visit_count=df_bill2[df_bill2['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','patient_sid']].groupby('doctor_sid').count()
#repeated patient count counts
repeat_patient_doc_paid_patient_count=df_bill2[df_bill2['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','patient_sid']].groupby('doctor_sid').nunique()

#single patient visit count
single_v_patient_doc_paid_visit_count=df_bill2[~df_bill2['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','patient_sid']].groupby('doctor_sid').count()
#single_v_patient_doc_paid_patient_count=df_bill[~df_bill['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','patient_sid']].groupby('doctor_sid').nunique()
# Total patients counts
total_patients=df_bill2[['doctor_sid','patient_sid']].groupby('doctor_sid').nunique()
#merging repeated single and total 
visit_repeat_counts=pd.merge(repeat_patient_doc_paid_visit_count,repeat_patient_doc_paid_patient_count, how='outer', on='doctor_sid')
visit_repeat_counts=pd.merge(visit_repeat_counts,single_v_patient_doc_paid_visit_count, how='outer', on='doctor_sid')
visit_repeat_counts=pd.merge(visit_repeat_counts,total_patients, how='outer', on='doctor_sid')
visit_repeat_counts.columns=['repeat_patient_visit_counts','repeat_patient_patient_count','single_patient_visit_count','total_patient_count']
#trancation type
repeat_patient_doc_paid_visit_amt=df_bill[df_bill['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','total_transaction_amount']].groupby('doctor_sid').sum()
single_v_patient_doc_paid_visit_amt=df_bill[~df_bill['patient_sid'].isin (repeated_visit['patient_sid'])][['doctor_sid','total_transaction_amount']].groupby('doctor_sid').sum()
total_patients_amt=df_bill[['doctor_sid','total_transaction_amount']].groupby('doctor_sid').sum()
#merge
visit_repeat_counts=pd.merge(visit_repeat_counts,repeat_patient_doc_paid_visit_amt, how='outer', on='doctor_sid')
visit_repeat_counts=pd.merge(visit_repeat_counts,single_v_patient_doc_paid_visit_amt, how='outer', on='doctor_sid')
visit_repeat_counts=pd.merge(visit_repeat_counts,total_patients_amt, how='outer', on='doctor_sid')
#changing columns name
visit_repeat_counts.columns=['repeat_patient_visit_counts','repeat_patient_patient_count','single_patient_visit_count','total_patient_count','repeat_patient_total_amt','single_patient_total_amt','total_amount']
#sending to csv
visit_repeat_counts.to_csv('visit_repeat_counts_amt.csv')

# =============================================================================
# 
# =============================================================================
