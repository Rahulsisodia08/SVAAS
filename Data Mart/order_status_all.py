# -*- coding: utf-8 -*-

# =============================================================================
# clt+4 -- comment in spider
# f9-- run the cose
# =============================================================================
import pandas as pd
import numpy as np



import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\SVAAS_data_17_06_22')
#uplodaing dataframe
df1= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\fact_appt_status_hist_17_06_22.csv')
df1.head()
#Changing to date time
df1['created_date']=pd.to_datetime(df1['created_date'])
df1['schedule_date']=pd.to_datetime(df1['schedule_date'])
#Uploading brdg_appt_conslt_mapping data
df= pd.read_csv(r'D:\SVAAS_2\SVAAS_data_17_06_22\brdg_appt_conslt_mapping_17_06_22.csv')
# Creating new df by taking doc_id and appt_id
doc_id=df[['brdg_appt_conslt_sid','doctor_id']]
# Removing duplicates from doc_id and appt_id
doc_id=doc_id.drop_duplicates(['brdg_appt_conslt_sid','doctor_id'])
# Merging two data frame 
df1_new=pd.merge(df1,doc_id, how='left', on='brdg_appt_conslt_sid')
# Sorting the data
df1_new.sort_values(by=['created_date'], ascending=True, inplace=True)
# Dropping duplicates
df2=df1_new.drop_duplicates(subset=['appointment_id','schedule_status'], keep='first')
#Groupby doctor_id
schedule_status_cnt=df2.groupby(['doctor_id','schedule_status']).agg({'schedule_status':'count'}).rename(columns={'schedule_status':'visit_cnt'}).reset_index()
sc=schedule_status_cnt.pivot(index='doctor_id',columns='schedule_status', values='visit_cnt').reset_index()
#sending to csv
#sc.to_csv('order_status_hist_visit_cnt.csv',index=False)
total_order_cnt=df2.groupby(['doctor_id']).nunique().reset_index()
total_order_cnt=total_order_cnt[['brdg_appt_conslt_sid','doctor_id']].reset_index()
total_order_cnt.drop('index',axis=1,inplace=True)
total_order_cnt.to_csv('total_order_cnt.csv',index=False)
#merging
status_visit_all_cnt=pd.merge(sc,total_order_cnt,how='outer',on='doctor_id')
status_visit_all_cnt.to_csv('status_visit_all_cnt.csv',index=False)


# =============================================================================
# 
# =============================================================================

pivoted1= df2.pivot(index=["appointment_id",'doctor_id'], columns='schedule_status', values="created_date").reset_index()
# Adding columns complted, and consulation time
pivoted1['complted_time']=pivoted1['confirmed']-pivoted1['booked']
pivoted1['conslutation_time']=pivoted1['completed']-pivoted1['in progress']
# copy the data
pivoted2=pivoted1.copy()
# Extracting hours from complted_time
pivoted2['complted_time_hours']=pivoted2['complted_time'].dt.total_seconds()/3600
# Extracting hours from complted_time
pivoted2['conslutation_time_hours']=pivoted2['conslutation_time'].dt.total_seconds()/3600
## making dataframe 
final_doc_hrs=pivoted2[['doctor_id','complted_time_hours','conslutation_time_hours']]
final_doc_hrs = final_doc_hrs[final_doc_hrs['doctor_id'].notna()]
#groupby doctor_id
final_doc_hrs_v2=final_doc_hrs.groupby('doctor_id').agg({'complted_time_hours':'mean','conslutation_time_hours':'mean'})

final_doc_hrs_v3=final_doc_hrs.groupby('doctor_id').agg({'complted_time_hours':'count'})
final_doc_hrs_v4=final_doc_hrs.groupby('doctor_id').agg({'conslutation_time_hours':'count'})
#merge
final_doc_hrs_v5=pd.merge(final_doc_hrs_v3,final_doc_hrs_v4,how='outer',on='doctor_id')

#final merge
final_doc_hrs_v6=pd.merge(final_doc_hrs_v2,final_doc_hrs_v5,how='outer',on='doctor_id')
final_doc_hrs_v7=final_doc_hrs.groupby('doctor_id').agg({'complted_time_hours':'min','conslutation_time_hours':'min'})
final_doc_hrs_v8=final_doc_hrs.groupby('doctor_id').agg({'complted_time_hours':'max','conslutation_time_hours':'max'})
#median
final_doc_hrs_v10=final_doc_hrs.groupby('doctor_id').agg({'complted_time_hours':'median','conslutation_time_hours':'median'}).rename(columns={'complted_time_hours':'complted_time_hours_median','conslutation_time_hours':'conslutation_time_hours_median'})



final_doc_hrs_v9=pd.merge(final_doc_hrs_v6,pd.merge(final_doc_hrs_v7,final_doc_hrs_v8,how='outer',on='doctor_id'),how='outer',on='doctor_id')
final_doc_hrs_v11=pd.merge(final_doc_hrs_v9,final_doc_hrs_v10,how='outer',on='doctor_id')

final_doc_hrs_v9.columns=['confirm_avg_time','consut_avg_time','confirm_count_time','consut_count_time','confirm_min_time','consut_min_time','confirm_max_time','consut_max_time','confirm_median_time','consut_median_time']

#changing columns
final_doc_hrs_v11.columns=['confirm_avg_time','consut_avg_time','confirm_count_time','consut_count_time','confirm_min_time','consut_min_time','confirm_max_time','consut_max_time','confirm_median_time','consut_median_time']


final_doc_hrs_v11.to_csv('final_doc_hrs_v11.csv',index=True)







