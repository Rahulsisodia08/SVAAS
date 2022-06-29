# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 15:20:44 2022

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
df_bill['patient_sid'].nunique()
df_bill=df_bill[['patient_sid','doctor_sid','transaction_type','total_transaction_amount','bill_sid']]
df_bill=df_bill.sort_values(by=['patient_sid','bill_sid'])
#sort by bill sid
#pat
doc_pat_ids=df_bill[df_bill['transaction_type']=='Doctor consultation'][['patient_sid','doctor_sid']]
#dropping duplicate patient id
doc_pat_ids=doc_pat_ids.drop_duplicates(['patient_sid'],keep='first')
#creating lab bill df
df_bill_lab=df_bill[df_bill['transaction_type']=='Lab bill']
#Merging lab bill 
df_bill_lab=pd.merge(df_bill_lab,doc_pat_ids,how='left',on='patient_sid')
total_patients_lab_amt=df_bill_lab[['doctor_sid_y','total_transaction_amount']].groupby('doctor_sid_y').sum()

#### Pharmacy 
df_bill_pharmacy=df_bill[df_bill['transaction_type']=='Pharmacy purchase']
df_bill_pharmacy=pd.merge(df_bill_pharmacy,doc_pat_ids,how='left',on='patient_sid')
total_patients_phamacy_amt=df_bill_pharmacy[['doctor_sid_y','total_transaction_amount']].groupby('doctor_sid_y').sum()

# Merging
lab_pharmacy_total_amt=pd.merge(total_patients_lab_amt,total_patients_phamacy_amt,how='outer',on='doctor_sid_y')
# changing columns name
lab_pharmacy_total_amt.columns=['lab_total_amt','pharmacy_total_amt']
# sending to csv
lab_pharmacy_total_amt.to_csv('lab_pharmacy_total_amt.csv')

