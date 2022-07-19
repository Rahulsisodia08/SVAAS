# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 14:51:48 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os


df=pd.read_csv(r'D:\SVAAS_2\tbl_doctor_availability_catalogue_price_mapping.csv')
#df_order=pd.read_excel(r'D:\SVAAS_2\spec_order.xlsx')
#df=pd.merge(df,df_order,on='short_code',how='left')
#df.head()
df.columns

df=df[['catalogue_price_consultation','insurance_partner_id','type','doctor_availability_id']]

pivoted= df.pivot(index=['type','doctor_availability_id'], 
                            columns='insurance_partner_id', values="catalogue_price_consultation").reset_index()

pivoted.to_csv('doctor_price_updated.csv')



