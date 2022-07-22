# -*- coding: utf-8 -*-
"""
Created on Thu Jul 14 11:22:33 2022

@author: RahulKumarSisodia
"""

# =============================================================================
# Insights file
# =============================================================================
import pandas as pd
import numpy as np
import os

os.chdir(r'D:\SVAAS_2\No_show_analysis\svaas_04_07_2022')
df=pd.read_csv('final_first_File_py.csv')
df.columns

doct_appt=df.groupby('doctor_sid_y').agg({'brdg_appt_conslt_sid':'count'}).rename(columns={'brdg_appt_conslt_sid':'appt_cnt'}).sort_values('appt_cnt',ascending=False)

doct_avg=df.groupby('doctor_sid_y').agg({'complted_time_hours':'mean'}).rename(columns={'complted_time_hours':'avg_waiting_time'}).sort_values('avg_waiting_time',ascending=True)

doct_min=df.groupby('doctor_sid_y').agg({'complted_time_hours':'min'}).rename(columns={'complted_time_hours':'min_time'}).sort_values('min_time',ascending=True)
doct_max=df.groupby('doctor_sid_y').agg({'complted_time_hours':'max'}).rename(columns={'complted_time_hours':'max_time'}).sort_values('max_time',ascending=True)
#df1 = pd.concat([doct_appt, doct_avg, doct_min,doct_max], axis=1, join='outer')


doc_conslt_avg=df.groupby('doctor_sid_y').agg({'conslutation_time_hours':'mean'}).rename(columns={'conslutation_time_hours':'avg_conslt_time'}).sort_values('avg_conslt_time',ascending=True)

doct_conslt_min=df.groupby('doctor_sid_y').agg({'conslutation_time_hours':'min'}).rename(columns={'conslutation_time_hours':'min_conslt_time'}).sort_values('min_conslt_time',ascending=True)
doct_conslt_max=df.groupby('doctor_sid_y').agg({'conslutation_time_hours':'max'}).rename(columns={'conslutation_time_hours':'max_conslt_time'}).sort_values('max_conslt_time',ascending=True)
df1 = pd.concat([doct_appt, doct_avg, doct_min,doct_max,doc_conslt_avg,doct_conslt_min,doct_conslt_max], axis=1, join='outer')



# Cosidering atleast 10 appointemnets
#df1=df1[df1['appt_cnt']>10]
#df1.to_csv('Top_doctor_waiting_time_insight.csv')
df.columns

###################################################################################################
df2=pd.read_csv('final_first_File_py.csv')
#df2=df[df['final_status']=='completed']
df2=df[['brdg_appt_conslt_sid','doctor_sid_y','pres_med_cnt','mapped_medicine_count','pres_test_cnt','mapped_test_count','bp_dia','pulse','complaints','diagnosis','weight','height','bmi']]

#med_appt=df2.groupby('doctor_id').agg({'brdg_appt_conslt_sid':'count'}).rename(columns={'brdg_appt_conslt_sid':'appt_cnt'}).sort_values('appt_cnt',ascending=False)
med_pres=df2.groupby('doctor_sid_y').agg({'pres_med_cnt':'sum'}).rename(columns={'pres_med_cnt':'pres_med_cnt'}).sort_values('pres_med_cnt',ascending=False)
med_mapp=df2.groupby('doctor_sid_y').agg({'mapped_medicine_count':'sum'}).rename(columns={'mapped_medicine_count':'mapped_medicine_count'}).sort_values('mapped_medicine_count',ascending=False)

test_pres=df2.groupby('doctor_sid_y').agg({'pres_test_cnt':'sum'}).rename(columns={'pres_test_cnt':'pres_test_cnt'}).sort_values('pres_test_cnt',ascending=False)
test_mapp=df2.groupby('doctor_sid_y').agg({'mapped_test_count':'sum'}).rename(columns={'mapped_test_count':'mapped_test_count'}).sort_values('mapped_test_count',ascending=False)



df2_nan=df2[df2['pres_med_cnt'].isnull()]
df2_nan['pres_med_cnt'] = df2_nan['pres_med_cnt'].replace(np.nan, 0)
med_pres_nan=df2_nan.groupby('doctor_sid_y').agg({'pres_med_cnt':'count'}).rename(columns={'pres_med_cnt':'pres_med_nan_cnt'}).sort_values('pres_med_nan_cnt',ascending=False)

df2_nan1=df2[df2['pres_test_cnt'].isnull()]
df2_nan1['pres_test_cnt'] = df2_nan1['pres_test_cnt'].replace(np.nan, 0)
test_pres_nan=df2_nan1.groupby('doctor_sid_y').agg({'pres_test_cnt':'count'}).rename(columns={'pres_test_cnt':'pres_test_nan_cnt'}).sort_values('pres_test_nan_cnt',ascending=False)




complaints=df2.groupby('doctor_sid_y').agg({'complaints':'count'})
diagnosis=df2.groupby('doctor_sid_y').agg({'diagnosis':'count'})
weight=df2.groupby('doctor_sid_y').agg({'weight':'count'})
height=df2.groupby('doctor_sid_y').agg({'height':'count'})
bmi=df2.groupby('doctor_sid_y').agg({'bmi':'count'})
pulse=df2.groupby('doctor_sid_y').agg({'pulse':'count'})


df_concat=pd.concat([df1, med_pres, med_mapp,med_pres_nan,test_pres,test_mapp,test_pres_nan,complaints,diagnosis,weight,height,bmi,pulse], axis=1, join='outer')

#######################################################################################################

traffic=df[df['completed'].notnull()][['patient_sid_y_doc','doctor_sid_y','completed']]
traffic['completed']=pd.to_datetime(traffic['completed'])
traffic['last_date']=traffic['completed'].max()
traffic['diff']=traffic['last_date']-traffic['completed']
traffic['last_week']=traffic['diff']<='7 days'
traffic['last_week']=traffic.last_week.replace({True: 1, False: 0})
traffic['last_week2']=np.where((traffic['last_week']==0) &  (traffic['diff']<='14 days'),1,0)
traffic['last_week3']=np.where((traffic['last_week']==0) & (traffic['last_week2']==0) &(traffic['diff']<='21 days'),1,0)
traffic['last_week4']=np.where((traffic['last_week']==0) & (traffic['last_week2']==0)&(traffic['last_week3']==0) &(traffic['diff']<='28 days'),1,0)


t1=traffic.groupby('doctor_sid_y').agg({'last_week':'sum'})
t2=traffic.groupby('doctor_sid_y').agg({'last_week2':'sum'})
t3=traffic.groupby('doctor_sid_y').agg({'last_week3':'sum'})
t4=traffic.groupby('doctor_sid_y').agg({'last_week4':'sum'})


####################### Least active doctor################
traffic['last_month']=traffic['diff']<='28 days'
traffic['last_month']=traffic.last_month.replace({True: 1, False: 0})

t5=traffic.groupby('doctor_sid_y').agg({'last_month':'sum'}).rename(columns={'last_month':'atleast1_cnslt'})
t7=t5[t5['atleast1_cnslt']==1]

t6=df[['doctor_sid_y','booked']]
t6['booked']=pd.to_datetime(t6['booked'])
t6=t6.sort_values(['doctor_sid_y','booked'])
t6.info()

t7=t6.groupby(['doctor_sid_y']).agg(Minimum_Date=('booked', np.min), Maximum_Date=('booked', np.max))

t8=pd.concat([t7,t5],axis=1, join='outer')



df_concat1=pd.concat([df_concat, t1, t2,t3,t4], axis=1, join='outer')

##############################patient and repeated patients#############################################################
df3=df[['patient_sid_y_doc','doctor_sid_y','booked']]
df3.drop_duplicates('patient_sid_y_doc',inplace=True)

p1=df3.groupby('doctor_sid_y').agg({'patient_sid_y_doc':'count'}).rename(columns={'patient_sid_y_doc':'patients_cnt'})
p2=df3.sort_values(['patient_sid_y_doc','booked'])
p2=df3.groupby('patient_sid_y_doc').agg({'booked':'min'}).rename(columns={'booked':'start_booked'})
p2=pd.merge(df3,p2,how="outer",on='patient_sid_y_doc')

df_concat1=pd.concat([df_concat1,p1,t7], axis=1, join='outer')

#################Repeated patients


repeated_pat=pd.read_csv('is_new_patient_py.csv')
#dropping last columns
del repeated_pat[repeated_pat.columns[-1]]
#filter the repeatede patients
repeated_pat=repeated_pat[repeated_pat['is_new_patient']==0]
repeated_pat.columns=['brdg_appt_conslt_sid','repeated_patients']
df_1=df[['brdg_appt_conslt_sid','doctor_sid_y']]

rep_p2=pd.merge(repeated_pat,df_1,how='outer',on='brdg_appt_conslt_sid')
rep_p2=rep_p2.reset_index(drop=True)
rep_p3=rep_p2.groupby('doctor_sid_y').agg({'repeated_patients':'count'}).rename(columns={'repeated_patients':'repeated_patients_cnt'})

df_concat1=pd.concat([df_concat1,rep_p3], axis=1, join='outer')

################################################################################################




df_concat1.to_csv('Doctor_insights.csv')













