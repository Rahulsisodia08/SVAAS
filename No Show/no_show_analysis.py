# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 18:51:09 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\No_show_analysis\svaas_04_07_2022')
# uPLOADING DATA
df1= pd.read_excel(r'D:\SVAAS_2\No_show_analysis\svaas_04_07_2022\SVAAS_Doctor_Appointment_Journey_vw (1).xlsx')
df=df1.copy()
df.info()
df.columns
#Removing extra space from doctor name
#df['dr_Name']= df['dr_Name'].str.strip()
#removing data of doctor Divya Kavri and Dr Mamatha R
df=df[df['dr_Name'] !='Divya Kavuri']
df=df[df['dr_Name'] !='Dr mamatha R']
##filter by  Date: 14/09/2021 to latest data
df=df[df['Booked_schedule']>='01-01-2022']

#Changing to date time
df['Booked_schedule']=pd.to_datetime(df['Booked_schedule'])
df['re_scheduled']=pd.to_datetime(df['re_scheduled'])
df['in_progress_schedule']=pd.to_datetime(df['in_progress_schedule'])
df['declined_schedule']=pd.to_datetime(df['declined_schedule'])
df['noshow_schedule']=pd.to_datetime(df['noshow_schedule'])
df['missed_scheduled']=pd.to_datetime(df['missed_scheduled'])
df['completed_scheduled']=pd.to_datetime(df['completed_scheduled'])
#df['appointment_start_scheduled']=pd.to_datetime(df['appointment_start_scheduled'])
#df['appointment_end_scheduled']=pd.to_datetime(df['appointment_end_scheduled'])

#find the total no_shows #800
df['Final_Status'].value_counts()

#conslutation type
service_type_total=df[['Service_Type','Final_Status']].groupby('Service_Type').agg({'Service_Type':'count'}).rename(columns={"Service_Type":"total_final_status_cnt"}).reset_index()
service_type_no_show=df[df['Final_Status']=='no_show'][['Service_Type','Final_Status']].groupby('Service_Type').agg({'Service_Type':'count'}).rename(columns={"Service_Type":"no_show_cnt"}).reset_index()
#merge
service_type=pd.merge(service_type_no_show,service_type_total,how='inner',on='Service_Type')
service_type['%_no_show']=(service_type['no_show_cnt']*100)/service_type['total_final_status_cnt']
service_type['%_no_show']=service_type['%_no_show'].round(decimals=2)
service_type.to_csv('service_type_no_show.csv',index=False)

#By insurance type

insurance_total=df[['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"total_final_status_cnt"}).reset_index()
insurance_type_no_show=df[df['Final_Status']=='no_show'][['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"no_show_cnt"}).reset_index()
#merge
insurance_type=pd.merge(insurance_type_no_show,insurance_total,how='inner',on='tenant_id')
insurance_type['%_no_show']=(insurance_type['no_show_cnt']*100)/insurance_type['total_final_status_cnt']
insurance_type['%_no_show']=insurance_type['%_no_show'].round(decimals=2)
insurance_type.to_csv('Insurance_type_no_show.csv',index=False)

################## Service type=Inperson
Inperson=df[df['Service_Type']=='InPerson']
insurance_total1=Inperson[['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"total_final_status_cnt"}).reset_index()
insurance_type_no_show1=Inperson[Inperson['Final_Status']=='no_show'][['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"no_show_cnt"}).reset_index()
#merge
insurance_type1=pd.merge(insurance_type_no_show1,insurance_total1,how='inner',on='tenant_id')
insurance_type1['%_no_show']=(insurance_type1['no_show_cnt']*100)/insurance_type1['total_final_status_cnt']
insurance_type1['%_no_show']=insurance_type1['%_no_show'].round(decimals=2)
insurance_type1.to_csv('Inperson_Insurance_type_no_show.csv',index=False)

################## Service type=OnlineConsultation
Online_consultation=df[df['Service_Type']=='OnlineConsultation']
insurance_total2=Online_consultation[['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"total_final_status_cnt"}).reset_index()
insurance_type_no_show2=Online_consultation[Online_consultation['Final_Status']=='no_show'][['tenant_id','Final_Status']].groupby('tenant_id').agg({'tenant_id':'count'}).rename(columns={"tenant_id":"no_show_cnt"}).reset_index()
#merge
insurance_type2=pd.merge(insurance_type_no_show2,insurance_total2,how='inner',on='tenant_id')
insurance_type2['%_no_show']=(insurance_type2['no_show_cnt']*100)/insurance_type2['total_final_status_cnt']
insurance_type2['%_no_show']=insurance_type2['%_no_show'].round(decimals=2)
insurance_type2.to_csv('Online_consultation_Insurance_type_no_show.csv',index=False)






#by hc_name
hc_total=df[['hc_Name','Final_Status']].groupby('hc_Name').count()
hc_total.columns=['total_final_status_cnt']
hc_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
hc_total=hc_total.reset_index()
hc_total=hc_total.dropna(axis=0)

HC_name_no_show_analysis=df[df['Final_Status']=='no_show'][['hc_Name','Final_Status']].groupby('hc_Name').count()
HC_name_no_show_analysis.columns=['no_show_cnt']
HC_name_no_show_analysis.sort_values(by='no_show_cnt',ascending=False,inplace=True)
HC_name_no_show_analysis=HC_name_no_show_analysis.reset_index()
#merge
hc_name=pd.merge(HC_name_no_show_analysis,hc_total,how='inner',on='hc_Name')
hc_name['%_no_show']=(hc_name['no_show_cnt']*100)/hc_name['total_final_status_cnt']
hc_name['%_no_show']=hc_name['%_no_show'].round(decimals=2)
hc_name.to_csv('HC_name_no_show_analysis.csv',index=False)

#by doctor name
doc_total=df[['dr_Name','Final_Status']].groupby('dr_Name').count()
doc_total.columns=['total_final_status_cnt']
doc_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
doc_total=doc_total.reset_index()

doc_name_no_show=df[df['Final_Status']=='no_show'][['dr_Name','Final_Status']].groupby('dr_Name').count()
doc_name_no_show.columns=['doc_no_show_cnt']
doc_name_no_show.sort_values(by='doc_no_show_cnt',ascending=False,inplace=True)
doc_name_no_show=doc_name_no_show.reset_index()
#merge
doctor_name=pd.merge(doc_name_no_show,doc_total,how='inner',on='dr_Name')
doctor_name['%_no_show']=(doctor_name['doc_no_show_cnt']*100)/doctor_name['total_final_status_cnt']
doctor_name['%_no_show']=doctor_name['%_no_show'].round(decimals=2)
doctor_name.to_csv('doc_name_no_show.csv',index=False)


#by patient name

patient_total=df[['P_Name','Final_Status','uh_opd_id']].groupby(['P_Name','uh_opd_id']).count()
patient_total.columns=['total_final_status_cnt']
patient_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
patient_total=patient_total.reset_index()

p_name_no_show=df[df['Final_Status']=='no_show'][['P_Name','Final_Status']].groupby(['P_Name']).count()
p_name_no_show.columns=['patient_no_show_cnt']
p_name_no_show.sort_values(by='patient_no_show_cnt',ascending=False,inplace=True)
p_name_no_show=p_name_no_show.reset_index()
#merge
patient_name=pd.merge(p_name_no_show,patient_total,how='inner',on='P_Name')
patient_name['%_no_show']=(patient_name['patient_no_show_cnt']*100)/patient_name['total_final_status_cnt']
patient_name['%_no_show']=patient_name['%_no_show'].round(decimals=2)

patient_name=patient_name[['uh_opd_id','P_Name', 'patient_no_show_cnt', 'total_final_status_cnt','%_no_show']]
patient_name.to_csv('p_name_no_show.csv',index=False)

#weekly data analysis
df['Year-Week'] = df['Booked_schedule'].dt.strftime('%Y-%U')

year_weekly_total=df[['Year-Week','Final_Status']].groupby('Year-Week').count()
year_weekly_total.columns=['total_final_status_cnt']
year_weekly_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
year_weekly_total=year_weekly_total.reset_index()

year_weekly_no_show_anlysis=df[df['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
year_weekly_no_show_anlysis.columns=['no_show_cnt_year_weekly']
year_weekly_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
year_weekly_no_show_anlysis=year_weekly_no_show_anlysis.reset_index()
#merge
year_weekly=pd.merge(year_weekly_no_show_anlysis,year_weekly_total,how='inner',on='Year-Week')
year_weekly['%_no_show']=(year_weekly['no_show_cnt_year_weekly']*100)/year_weekly['total_final_status_cnt']
year_weekly['%_no_show']=year_weekly['%_no_show'].round(decimals=2)
year_weekly.to_csv('year_weekly_no_show.csv',index=False)

#Patient age wise
df['P_Age'].nunique()
df['P_Age'].value_counts()
df['P_Name'].nunique()
df['']


bins= [0,12,24,43,60,110]
labels = ['0-12 yrs','13-24 yrs','25-43 yrs','44-60 yrs','61-110 yrs']
df['AgeGroup'] = pd.cut(df['P_Age'], bins=bins, labels=labels, right=False)
df_AgeGroup_total=df[['AgeGroup','Final_Status']].groupby('AgeGroup').count()
df_AgeGroup_total.columns=['total_final_status_cnt']
df_AgeGroup_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
df_AgeGroup_total=df_AgeGroup_total.reset_index()

df_AgeGroup=df[df['Final_Status']=='no_show'][['AgeGroup','Final_Status']].groupby('AgeGroup').count()
df_AgeGroup.columns=['no_show_AgeGroup']
df_AgeGroup.sort_values(by='no_show_AgeGroup',ascending=False,inplace=True)
df_AgeGroup=df_AgeGroup.reset_index()
#merge
AgeGroup=pd.merge(df_AgeGroup,df_AgeGroup_total,how='inner',on='AgeGroup')
AgeGroup['%_no_show']=(AgeGroup['no_show_AgeGroup']*100)/AgeGroup['total_final_status_cnt']
AgeGroup['%_no_show']=AgeGroup['%_no_show'].round(decimals=2)
AgeGroup.to_csv('AgeGroup_no_show.csv',index=False)


######################################################################################################################
#Trends of top 3 doctors whos no_show is greater than or equal 50

df['Year-Week'] = df['Booked_schedule'].dt.strftime('%Y-%U')
doc1=df[df['dr_Name']=="Prasanna B"]

doc_top1_total=doc1[['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top1_total.columns=['total_final_status_cnt']
doc_top1_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
doc_top1_total=doc_top1_total.reset_index()

doc_top1_no_show_anlysis=doc1[doc1['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top1_no_show_anlysis.columns=['no_show_cnt_year_weekly']
doc_top1_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
doc_top1_no_show_anlysis=doc_top1_no_show_anlysis.reset_index()
#merge
doc_top1=pd.merge(doc_top1_no_show_anlysis,doc_top1_total,how='inner',on='Year-Week')
doc_top1['%_no_show']=(doc_top1['no_show_cnt_year_weekly']*100)/doc_top1['total_final_status_cnt']
doc_top1['%_no_show']=doc_top1['%_no_show'].round(decimals=2)
doc_top1.to_csv('doctor_top1.csv',index=False)

#2nd doctor trend analysis
doc2=df[df['dr_Name']=="Ella reddy Chinthala"]
doc_top2_total=doc2[['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top2_total.columns=['total_final_status_cnt']
doc_top2_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
doc_top2_total=doc_top2_total.reset_index()

doc_top2_no_show_anlysis=doc2[doc2['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top2_no_show_anlysis.columns=['no_show_cnt_year_weekly']
doc_top2_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
doc_top2_no_show_anlysis=doc_top2_no_show_anlysis.reset_index()
#merge
doc_top2=pd.merge(doc_top2_no_show_anlysis,doc_top2_total,how='inner',on='Year-Week')
doc_top2['%_no_show']=(doc_top2['no_show_cnt_year_weekly']*100)/doc_top2['total_final_status_cnt']
doc_top2['%_no_show']=doc_top2['%_no_show'].round(decimals=2)
doc_top2.to_csv('doctor_top2.csv',index=False)

#3rd doctor
doc3=df[df['dr_Name']=="Nalini Tellakula"]
doc_top3_total=doc3[['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top3_total.columns=['total_final_status_cnt']
doc_top3_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
doc_top3_total=doc_top3_total.reset_index()

doc_top3_no_show_anlysis=doc3[doc3['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
doc_top3_no_show_anlysis.columns=['no_show_cnt_year_weekly']
doc_top3_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
doc_top3_no_show_anlysis=doc_top3_no_show_anlysis.reset_index()
#merge
doc_top3=pd.merge(doc_top3_no_show_anlysis,doc_top3_total,how='inner',on='Year-Week')
doc_top3['%_no_show']=(doc_top3['no_show_cnt_year_weekly']*100)/doc_top3['total_final_status_cnt']
doc_top3['%_no_show']=doc_top3['%_no_show'].round(decimals=2)
doc_top3.to_csv('doctor_top3.csv',index=False)

#######################################################################################################################
#Top 3 health coaches by analysis
#for First Health coach
df['Year-Week'] = df['Booked_schedule'].dt.strftime('%Y-%U')
hc1=df[df['hc_Name']=="Vasundhara  Arora"]

hc_top1_total=hc1[['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_top1_total.columns=['total_final_status_cnt']
hc_top1_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
hc_top1_total=hc_top1_total.reset_index()

hc_top1_no_show_anlysis=hc1[hc1['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_top1_no_show_anlysis.columns=['no_show_cnt_year_weekly']
hc_top1_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
hc_top1_no_show_anlysis=hc_top1_no_show_anlysis.reset_index()
#merge
hc_top1=pd.merge(hc_top1_no_show_anlysis,hc_top1_total,how='inner',on='Year-Week')
hc_top1['%_no_show']=(hc_top1['no_show_cnt_year_weekly']*100)/hc_top1['total_final_status_cnt']
hc_top1['%_no_show']=hc_top1['%_no_show'].round(decimals=2)
hc_top1.to_csv('Vasundhara_hc_top1.csv',index=False)

#2nd health coach trend analysis
hc2=df[df['hc_Name']=="Sayli  Deshpande"]

hc_to2_total=hc2[['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_to2_total.columns=['total_final_status_cnt']
hc_to2_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
hc_to2_total=hc_to2_total.reset_index()

hc_top2_no_show_anlysis=hc2[hc2['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_top2_no_show_anlysis.columns=['no_show_cnt_year_weekly']
hc_top2_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
hc_top2_no_show_anlysis=hc_top2_no_show_anlysis.reset_index()
#merge
hc_top2=pd.merge(hc_top2_no_show_anlysis,hc_to2_total,how='inner',on='Year-Week')
hc_top2['%_no_show']=(hc_top2['no_show_cnt_year_weekly']*100)/hc_top2['total_final_status_cnt']
hc_top2['%_no_show']=hc_top2['%_no_show'].round(decimals=2)
hc_top2.to_csv('Sayli_hc_top2.csv',index=False)

#3rd health coach trend analysis
hc3=df[df['hc_Name']=="Aloka  Agranayak"]

hc_top3_total=hc3[['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_top3_total.columns=['total_final_status_cnt']
hc_top3_total.sort_values(by='total_final_status_cnt',ascending=False,inplace=True)
hc_top3_total=hc_top3_total.reset_index()

hc_top3_no_show_anlysis=hc3[hc3['Final_Status']=='no_show'][['Year-Week','Final_Status']].groupby('Year-Week').count()
hc_top3_no_show_anlysis.columns=['no_show_cnt_year_weekly']
hc_top3_no_show_anlysis.sort_values(by='no_show_cnt_year_weekly',ascending=False,inplace=True)
hc_top3_no_show_anlysis=hc_top3_no_show_anlysis.reset_index()
#merge
hc_top3=pd.merge(hc_top3_no_show_anlysis,hc_top3_total,how='inner',on='Year-Week')
hc_top3['%_no_show']=(hc_top3['no_show_cnt_year_weekly']*100)/hc_top3['total_final_status_cnt']
hc_top3['%_no_show']=hc_top3['%_no_show'].round(decimals=2)
hc_top3.to_csv('Aloka_hc_top3.csv',index=False)

#################################################################################################################

####################################################################################################################
#appointment id wise counts (visit wise)
df2=df.copy()
df2=df2[df2['dr_Name']!='Divya Kavuri']
df2=df2[df2['dr_Name']!='Dr mamatha R']


#Find number of patients who has showed no_shows on previous day and tried booking on next day and same day.
df2 = df2.sort_values(['Booked_schedule','P_Name'], ascending=True)

#date diff column
df2['diff'] = df2['Booked_schedule'].dt.date.diff()
#shift patient name 
df2['p_name2']=df2['P_Name'].shift()
# checking same patients               
df2['patient_flag']=np.where(df2['p_name2']==df2['P_Name'],1,0)                     
 #appointments on next day or same day with same patients                    
df3=df2[(df2['Booked_schedule'].dt.date.diff()<='1 days') &(df2['patient_flag']==1)]
#hc coaches
hc_coaches=df3.groupby('hc_Name').agg({'hc_Name':'count'}).rename(columns={"hc_Name":"hc_Name_cnt"}).sort_values(by='hc_Name_cnt',ascending=False)
hc_coaches.to_csv('hc_coaches.csv')

dr_name=df3.groupby('dr_Name').agg({'dr_Name':'count'}).rename(columns={"dr_Name":"dr_Name_cnt"}).sort_values(by='dr_Name_cnt',ascending=False)
dr_name.to_csv('dr_name.csv')

p_name=df3.groupby(['P_Name','uh_opd_id']).agg({'P_Name':'count'}).rename(columns={"P_Name":"p_Name_cnt"}).sort_values(by='p_Name_cnt',ascending=False)
p_name.to_csv('p_name.csv')

######################################################################################################################

#other appointments on next day or same day with same doctor(yes/no)
df4 = df.sort_values(['Booked_schedule','dr_Name','P_Name'], ascending=True)

#date diff column
df4['diff'] = df4['Booked_schedule'].dt.date.diff() 
#shifting doctor name
df4['doc_name2']=df4['dr_Name'].shift()
# checking same doctor                
df4['doctor_flag']=np.where(df4['doc_name2']==df4['dr_Name'],1,0) 
#shift patient name 
df4['p_name2']=df4['P_Name'].shift()
# checking same patients               
df4['patient_flag']=np.where(df4['p_name2']==df4['P_Name'],1,0) 

df5=df4[(df4['Booked_schedule'].dt.date.diff()<='1 days') &(df4['doctor_flag']==1)&(df4['patient_flag']==1)]

#Other Appointment on next or same day with different doctor (Yes/No)
df6=df4[(df4['Booked_schedule'].dt.date.diff()<='1 days') &(df4['doctor_flag']==0)&(df4['patient_flag']==1)]
#doctor not the same the previous rows
df4['dr_Name'].ne(df4['dr_Name'].shift())
df7=df4[(df4['Booked_schedule'].dt.date.diff()<='1 days') &(df4['dr_Name'].ne(df4['dr_Name'].shift()))]


# =============================================================================
# Monthly Trend analysis
# =============================================================================
df['Mon_Year'] = df['Booked_schedule'].dt.strftime('%b-%Y')


mon_yr_total=df[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
mon_yr_no_show=df[df['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()
#merge
montly_year=pd.merge(mon_yr_no_show,mon_yr_total,how='inner',on='Mon_Year')
montly_year['%_no_show']=(montly_year['no_show_cnt']*100)/montly_year['total_final_status_cnt']
montly_year['%_no_show']=montly_year['%_no_show'].round(decimals=2)
montly_year.to_csv('monthly_year_no_show.csv',index=False)

#################################################################################################################
# first Doctor wise monthly trend analysis
doc1=df[df['dr_Name']=="Prasanna B"]

doc_m1_total=doc1[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
doc_m1_no_show=doc1[doc1['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
doc_m1=pd.merge(doc_m1_no_show,doc_m1_total,how='inner',on='Mon_Year')
doc_m1['%_no_show']=(doc_m1['no_show_cnt']*100)/doc_m1['total_final_status_cnt']
doc_m1['%_no_show']=doc_top1['%_no_show'].round(decimals=2)
doc_m1.to_csv('doctor_m1.csv',index=False)


doc2=df[df['dr_Name']=="Ella reddy Chinthala"]

doc_m2_total=doc2[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
doc_m2_no_show=doc2[doc2['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
doc_m2=pd.merge(doc_m2_no_show,doc_m2_total,how='inner',on='Mon_Year')
doc_m2['%_no_show']=(doc_m2['no_show_cnt']*100)/doc_m2['total_final_status_cnt']
doc_m2['%_no_show']=doc_m2['%_no_show'].round(decimals=2)
doc_m2.to_csv('doctor_m2.csv',index=False)


doc3=df[df['dr_Name']=="Nalini Tellakula"]

doc_m3_total=doc3[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
doc_m3_no_show=doc3[doc3['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
doc_m3=pd.merge(doc_m3_no_show,doc_m3_total,how='inner',on='Mon_Year')
doc_m3['%_no_show']=(doc_m3['no_show_cnt']*100)/doc_m3['total_final_status_cnt']
doc_m3['%_no_show']=doc_m3['%_no_show'].round(decimals=2)
doc_m3.to_csv('doctor_m3.csv',index=False)

#######################################################################################################################
#Trend Analysis by health coaches
hc1=df[df['hc_Name']=="Vasundhara  Arora"]

hc_m1_total=hc1[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
hc_m1_no_show=hc1[hc1['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
hc_m1=pd.merge(hc_m1_no_show,hc_m1_total,how='inner',on='Mon_Year')
hc_m1['%_no_show']=(hc_m1['no_show_cnt']*100)/hc_m1['total_final_status_cnt']
hc_m1['%_no_show']=hc_m1['%_no_show'].round(decimals=2)
hc_m1.to_csv('hc_m1.csv',index=False)


hc2=df[df['hc_Name']=="Sayli  Deshpande"]

hc_m2_total=hc2[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
hc_m2_no_show=hc2[hc2['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
hc_m2=pd.merge(hc_m2_no_show,hc_m2_total,how='inner',on='Mon_Year')
hc_m2['%_no_show']=(hc_m2['no_show_cnt']*100)/hc_m2['total_final_status_cnt']
hc_m2['%_no_show']=hc_m2['%_no_show'].round(decimals=2)
hc_m2.to_csv('hc_m2.csv',index=False)


hc3=df[df['hc_Name']=="Aloka  Agranayak"]

hc_m3_total=hc3[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
hc_m3_no_show=hc3[hc3['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
hc_m3=pd.merge(hc_m3_no_show,hc_m3_total,how='inner',on='Mon_Year')
hc_m3['%_no_show']=(hc_m3['no_show_cnt']*100)/hc_m3['total_final_status_cnt']
hc_m3['%_no_show']=hc_m3['%_no_show'].round(decimals=2)
hc_m3.to_csv('hc_m3.csv',index=False)

#####################################################################################################################
# Monthly trend of patients

p1=df[df['P_Name']=="Bandanadam  "]

p_m1_total=p1[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
p_m1_no_show=p1[p1['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
p_m1=pd.merge(p_m1_no_show,p_m1_total,how='inner',on='Mon_Year')
p_m1['%_no_show']=(p_m1['no_show_cnt']*100)/p_m1['total_final_status_cnt']
p_m1['%_no_show']=p_m1['%_no_show'].round(decimals=2)
p_m1.to_csv('p_m1.csv',index=False)


p2=df[df['P_Name']=="Pranshul  "]

p_m2_total=p2[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
p_m2_no_show=p2[p2['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
p_m2=pd.merge(p_m2_no_show,p_m2_total,how='inner',on='Mon_Year')
p_m2['%_no_show']=(p_m2['no_show_cnt']*100)/p_m2['total_final_status_cnt']
p_m2['%_no_show']=p_m2['%_no_show'].round(decimals=2)
p_m2.to_csv('p_m2.csv',index=False)

p3=df[df['P_Name']=="Akhil  Panigrahi"]

p_m3_total=p3[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
p_m3_no_show=p3[p3['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
p_m3=pd.merge(p_m3_no_show,p_m3_total,how='inner',on='Mon_Year')
p_m3['%_no_show']=(p_m3['no_show_cnt']*100)/p_m3['total_final_status_cnt']
p_m3['%_no_show']=p_m3['%_no_show'].round(decimals=2)
p_m3.to_csv('p_m3.csv',index=False)

p4=df[df['P_Name']=="Ms Jyoti Bharat pallan"]

p_m4_total=p4[['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"total_final_status_cnt"}).sort_values(by='total_final_status_cnt',ascending=False).reset_index()
p_m4_no_show=p4[p4['Final_Status']=='no_show'][['Mon_Year','Final_Status']].groupby('Mon_Year').agg({'Final_Status':'count'}).rename(columns={"Final_Status":"no_show_cnt"}).sort_values(by='no_show_cnt',ascending=False).reset_index()

#merge
p_m4=pd.merge(p_m4_no_show,p_m4_total,how='inner',on='Mon_Year')
p_m4['%_no_show']=(p_m4['no_show_cnt']*100)/p_m4['total_final_status_cnt']
p_m4['%_no_show']=p_m4['%_no_show'].round(decimals=2)
p_m4.to_csv('p_m4.csv',index=False)


