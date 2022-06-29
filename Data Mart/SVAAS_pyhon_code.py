# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 17:07:37 2022

@author: RahulKumarSisodia
"""

import pandas as pd
import numpy as np
import os

#Setting directory of data
os.chdir(r'D:\SVAAS_2\No_show_analysis\svaas data_27_06_202')
appt= pd.read_csv('brdg_appt_conslt_mapping.csv')
appt_status=pd.read_excel(r'D:\SVAAS_2\No_show_analysis\svaas data_27_06_202\fact_appt_status_hist.xlsx')
doct_jo=pd.read_csv('SVAAS_Doctor_Appointment_Journey_vw.csv')

doct_jo2=doct_jo[['dr_id','dr_Name','dr_gender','dr_specialization','dr_exp','tenant_id','consultation_fee','Service_Type','payable_by_user','payable_by_insurance','brdg_appt_conslt_sid','Booked_schedule','re_scheduled','in_progress_schedule','declined_schedule','noshow_schedule','missed_scheduled','completed_scheduled','appointment_start_scheduled','appointment_end_scheduled','Final_Status']]

#Changing to date time
appt_status.info()
appt_status['created_date']=pd.to_datetime(appt_status['created_date'])
appt_status['schedule_date']=pd.to_datetime(appt_status['schedule_date'])

appt_status['created_date']=pd.to_datetime(appt_status['created_date'])
appt_status.columns

# Sorting the data
appt_status.sort_values(by=['created_date'], ascending=True, inplace=True)
# Dropping duplicates
appt_status2=appt_status.drop_duplicates(subset=['brdg_appt_conslt_sid','schedule_status'], keep='last')
appt_status2.drop('row_insert_dt',axis=1,inplace=True)

# Here we are considering only the newest time.

#Pivoting
pivoted= appt_status2.pivot(index=['brdg_appt_conslt_sid',"appointment_id",'start_time', 'end_time'], 
                            columns='schedule_status', values="created_date").reset_index()

#for final status
appt_status3=appt_status2[['brdg_appt_conslt_sid','schedule_status']]
appt_status3=appt_status3.drop_duplicates(subset=['brdg_appt_conslt_sid'], keep='last')

pivoted1= pd.merge(pivoted,appt_status3,how='left',on='brdg_appt_conslt_sid')
# Adding columns complted, and consulation time
pivoted1['complted_time']=pivoted1['confirmed']-pivoted1['booked']
pivoted1['conslutation_time']=pivoted1['completed']-pivoted1['in progress']
# copy the data

# Extracting hours from complted_time
pivoted1['complted_time_hours']=pivoted1['complted_time'].dt.total_seconds()/3600
# Extracting hours from complted_time
pivoted1['conslutation_time_hours']=pivoted1['conslutation_time'].dt.total_seconds()/3600

appt_status_final=pivoted1.drop(['complted_time','conslutation_time'],axis=1)
# merging with brdg appt conslut mapping
appt.rename(columns = {'appointment_status':'final_status'}, inplace = True)

appt2=appt.drop(['row_update_dt','row_insert_dt','clinic_id','policy_id','policy_sid','insurance_partner_sid',
                 'cancellation_reason','svaas_appointment_id','prescription','next_visit','appointment_dt_sid',
                 'appointment_id','appointment_datetime','consultation_id'],axis=1)

dt=pd.merge(appt2,appt_status_final,how='outer',on='brdg_appt_conslt_sid')

# =============================================================================
# Prescribed medicine and prescribe test
# =============================================================================
med=pd.read_csv(r'D:\SVAAS_2\No_show_analysis\svaas data_27_06_202\prescribed_medicine_mapping.csv')
test=pd.read_csv(r'D:\SVAAS_2\No_show_analysis\svaas data_27_06_202\prescribed_test_mapping.csv')

pres_med=med.groupby('brdg_appt_conslt_sid').agg({'prescribed_medicine':'count'}).rename(columns={"prescribed_medicine":"pres_med_cnt"}).sort_values(by='pres_med_cnt',ascending=False)
# Mapped medicine count
mapped_med=med.groupby('brdg_appt_conslt_sid').agg({'mapped_medicine_name':'count'}).rename(columns={"mapped_medicine_name":"mapped_medicine_count"}).sort_values(by='mapped_medicine_count',ascending=False)

pres_med=pd.merge(pres_med,mapped_med,how='outer',on='brdg_appt_conslt_sid')

#test
pres_test=test.groupby('brdg_appt_conslt_sid').agg({'prescribed_test':'count'}).rename(columns={"prescribed_test":"pres_test_cnt"}).sort_values(by='pres_test_cnt',ascending=False)
mapped_test=test.groupby('brdg_appt_conslt_sid').agg({'mapped_test_name':'count'}).rename(columns={"mapped_test_name":"mapped_test_count"}).sort_values(by='mapped_test_count',ascending=False)

pres_test=pd.merge(pres_test,mapped_test,how='outer',on='brdg_appt_conslt_sid')

#merging medicine and test
med_test=pd.merge(pres_med,pres_test,how='outer',on='brdg_appt_conslt_sid')

#merging with dt and med_test
dt2=pd.merge(dt,med_test,how='outer',on='brdg_appt_conslt_sid')

# =============================================================================
# Bill summary
# =============================================================================
bill=pd.read_csv(r'D:\SVAAS_2\No_show_analysis\svaas data_27_06_202\fact_bill_summary.csv')
doc_bill=bill[bill['vendor_type']=="Doctor"]
pharmacy_bill=bill[bill['vendor_type']=="Pharmacy services"]
lab_bill=bill[bill['vendor_type']=="Diagnostic service process"]

doc_bill=doc_bill[['service_type','brdg_appt_conslt_sid','bill_sid','doctor_sid','doctor_clinic_addr_sid',
                   'patient_sid','vendor_sid','transaction_dt','consultation_amount','paid_by_customer',
                   'paid_by_insurer','pg_charges','transaction_amount','payment_type','overall_transaction_status',
                   'refund_status','total_gst_amount','convenience_charge','delivery_charge','total_transaction_amount',
                   'platform_fee_amount','is_pay_out','is_settle_vendor','is_settle_vendor_datetime','payout_datetime','source_payment_type',
                   'marketing_fees_amount_settle','platform_fee_amount_settle','source_pg_charges']]

pharmacy_bill=pharmacy_bill[['service_type','pharmacy_sid','pharmacy_branch_id','pharmacy_order_type','pharmacy_flag','bill_sid','doctor_sid','doctor_clinic_addr_sid','brdg_appt_conslt_sid',
                   'patient_sid','vendor_sid','transaction_dt','consultation_amount','paid_by_customer',
                   'paid_by_insurer','pg_charges','transaction_amount','payment_type','overall_transaction_status',
                   'refund_status','total_gst_amount','convenience_charge','delivery_charge','total_transaction_amount',
                   'platform_fee_amount','is_pay_out','is_settle_vendor','is_settle_vendor_datetime','payout_datetime','source_payment_type',
                   'marketing_fees_amount_settle','platform_fee_amount_settle','source_pg_charges']]

lab_bill=lab_bill[['service_type','test_type','lab_id','diagnostic_lab_sid','brdg_appt_conslt_sid','bill_sid','doctor_sid','doctor_clinic_addr_sid',
                   'patient_sid','vendor_sid','transaction_dt','consultation_amount','paid_by_customer',
                   'paid_by_insurer','pg_charges','transaction_amount','payment_type','overall_transaction_status',
                   'refund_status','total_gst_amount','convenience_charge','delivery_charge','total_transaction_amount',
                   'platform_fee_amount','is_pay_out','is_settle_vendor','is_settle_vendor_datetime','payout_datetime','source_payment_type',
                   'marketing_fees_amount_settle','platform_fee_amount_settle','source_pg_charges']]

#Merging doc_bill with dt2
dt3=pd.merge(dt2,doc_bill,how='outer',on='brdg_appt_conslt_sid')

dt3.to_csv('first_file_py.csv',index=False)
pharmacy_bill.to_csv('pharmacy_bill.csv',index=False)
lab_bill.to_csv('lab_bill.csv',index=False)
