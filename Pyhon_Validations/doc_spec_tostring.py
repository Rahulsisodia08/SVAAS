# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 14:51:48 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os


df=pd.read_excel(r'D:\SVAAS_2\tbl_doctor_qualification.xlsx')
df_order=pd.read_excel(r'D:\SVAAS_2\spec_order.xlsx')

df['short_code']=df['short_code'].str.lower()
df_order['short_code']=df_order['short_code'].str.lower()

df_order.loc[df_order['short_code'].str.contains('mbbs'),'order_no']= 1
df_order.loc[df_order['short_code'].str.contains('md'),'order_no']= 2
df_order.loc[df_order['short_code'].str.contains('ms'),'order_no']= 3
df_order.loc[df_order['short_code'].str.contains('dnb'),'order_no']= 4
df_order.loc[df_order['short_code'].str.contains('mch'),'order_no']= 5
df_order.loc[df_order['short_code'].str.contains('dm'),'order_no']= 6


df=pd.merge(df,df_order,on='short_code',how='left')
df.head()

df.loc[(df['order_no'].isnull()), 'order_no'] = 100

df=df.sort_values(['order_no'],ascending=True)
df=df[['created_by','created_date','status','updated_by','updated_date','short_code','doctor_details_id']]


df1 = df.groupby(['doctor_details_id','created_by','created_date','status','updated_by','updated_date'])['short_code'].agg(lambda x: ', '.join(x.dropna())).reset_index()


df1.to_excel('tlb_doctor_qualification_updates.xlsx')




