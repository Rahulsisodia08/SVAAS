library(data.table)
#install.packages('readxl')
library(readxl)
#install.packages(('dplyr'))
library(dplyr)
#install.packages('nortest')
library(nortest)#For Normality Test (Heavy Data)
#install.packages('ggstatsplot')
library(ggstatsplot) #For Correlation Matrix
#install.packages('ggcorrplot')
library(ggcorrplot) #For Correlation Plot
#install.packages('ltm')
library(ltm) #Biserial Test

library(readxl)
#install.packages('writexl')
library(writexl)
#install.packages('DescTools')
library(DescTools)
#install.packages('ineq')
library(ineq)
#install.packages('caTools')
library(caTools)
#install.packages('ROCR')
library(ROCR)
#install.packages('lubridate')
library(lubridate)

`%ni%`<-Negate(`%in%`)

setwd("D:\\SVAAS_2\\No_show_analysis\\svaas data_27_06_202")

appt<-fread('brdg_appt_conslt_mapping.csv')
appt_status<-fread('fact_appt_status_hist.csv')
doc_j<-fread('SVAAS_Doctor_Appointment_Journey_vw.csv')

doc_j[brdg_appt_conslt_sid==1335]
colnames(doc_j)
doc_j2<-doc_j[,.(dr_id,dr_Name,dr_gender,dr_specialization,dr_exp,tenant_id,consultation_fee,Service_Type,payable_by_user,payable_by_insurance,brdg_appt_conslt_sid,Booked_schedule,re_scheduled,in_progress_schedule,declined_schedule,noshow_schedule,missed_scheduled,completed_scheduled,appointment_start_scheduled,appointment_end_scheduled,Final_Status)]

appt_status2<-appt_status[order(brdg_appt_conslt_sid,created_date)][,head(.SD,1),.(brdg_appt_conslt_sid,schedule_status)]
appt_status2<-dcast.data.table(appt_status2,brdg_appt_conslt_sid+appointment_id+schedule_date+start_time+end_time+tenant_id~schedule_status,value.var = 'created_date')

dt<-copy(appt_status2)
dt_final<-appt_status[order(brdg_appt_conslt_sid,-created_date)][,head(.SD,1),.(brdg_appt_conslt_sid)][,.(brdg_appt_conslt_sid,schedule_status)]
dt<-merge(dt,dt_final,by='brdg_appt_conslt_sid',all = T)
setnames(dt,'schedule_status','final_status')


final_status_appt<-appt[,.(brdg_appt_conslt_sid,appointment_status,appointment_type,doctor_id,doctor_sid,insurance_company_name,vendor_sid,patient_id,patient_sid,complaints,diagnosis,advice,bp_dia,bp_sys,pulse,bmi,weight,height)]

both_status<-merge(dt[,.(brdg_appt_conslt_sid,final_status)],final_status_appt[,.(brdg_appt_conslt_sid,appointment_status)],by='brdg_appt_conslt_sid',all = T)

dt2<-merge(final_status_appt,dt,by='brdg_appt_conslt_sid',all = T)
all_status<-merge(both_status,unique(doc_j2[,.(brdg_appt_conslt_sid,doc_journey_Final_Status=Final_Status)]),by='brdg_appt_conslt_sid',all = T)
dt2<-merge(dt2,unique(doc_j2[,.(brdg_appt_conslt_sid,doc_journey_Final_Status=Final_Status)]),by='brdg_appt_conslt_sid',all = T)


colnames(dt2)

write.csv(dt2,'first_file.csv',row.names = F,na='')

#######################################################

med<-fread('prescribed_medicine_mapping.csv')
med<-med[,.(brdg_appt_conslt_sid,consultation_datetime,prescribed_medicine,mapped_medicine_name,medicine_sid,doctor_clinic_addr_sid,quantity,dosage)]
      

test<-fread('prescribed_test_mapping.csv')
test<-test[,.(brdg_appt_conslt_sid,consultation_datetime,prescribed_test,mapped_test_name,diagnostic_test_sid,doctor_clinic_addr_sid,quantity)]

dt2<-merge(dt2,merge(merge(med[nchar(prescribed_medicine)>0,.N,.(brdg_appt_conslt_sid,prescribed_medicine)][,.N,brdg_appt_conslt_sid][,.(brdg_appt_conslt_sid,pres_med_count=N)],
      med[nchar(prescribed_medicine)>0,.N,.(brdg_appt_conslt_sid,mapped_medicine_name)][,.N,brdg_appt_conslt_sid][,.(brdg_appt_conslt_sid,mapped_med_count=N)],by='brdg_appt_conslt_sid',all = T),
merge(test[nchar(prescribed_test)>0,.N,.(brdg_appt_conslt_sid,prescribed_test)][,.N,brdg_appt_conslt_sid][,.(brdg_appt_conslt_sid,pres_test_count=N)],
      test[nchar(prescribed_test)>0,.N,.(brdg_appt_conslt_sid,mapped_test_name)][,.N,brdg_appt_conslt_sid][,.(brdg_appt_conslt_sid,mapped_test_count=N)],by='brdg_appt_conslt_sid',all = T),by='brdg_appt_conslt_sid',all = T),by='brdg_appt_conslt_sid',all = T)


write.csv(dt2,'first_file.csv',row.names = F,na='')
colnames(dt2)
library(lubridate)
dt2[,confirm_time:=as.POSIXlt(confirmed,format="%d-%m-%Y %H:%M:%S",tz=Sys.timezone())-as.POSIXlt(booked,format="%d-%m-%Y %H:%M:%S",tz=Sys.timezone())]
dt2[,confirm_time:=as.numeric(confirm_time/3600)]

dt2[,consult_time:=as.POSIXlt(completed,format="%d-%m-%Y %H:%M:%S",tz=Sys.timezone())-as.POSIXlt(`in progress`,format="%d-%m-%Y %H:%M:%S",tz=Sys.timezone())]
dt2[,consult_time:=as.numeric(consult_time/3600)]
write.csv(dt2,'first_file.csv',row.names = F,na='')

############ fact_bill_summary ###########################

fact_b_hist<-fread('fact_bill_order_status_hist.csv')


bill<-fread('fact_bill_summary.csv')
colnames(bill)
bill_doc<-bill[vendor_type=='Doctor',.(service_type,brdg_appt_conslt_sid,bill_sid,doctor_sid,doctor_clinic_addr_sid,patient_sid,vendor_sid,transaction_dt,consultation_amount,paid_by_customer,paid_by_insurer,pg_charges,transaction_amount,payment_type,
                                       overall_transaction_status,refund_status,total_gst_amount,convenience_charge,delivery_charge,total_transaction_amount,platform_fee_amount,is_pay_out,is_settle_vendor,is_settle_vendor_datetime,payout_datetime,source_payment_type,marketing_fees_amount_settle,platform_fee_amount_settle,source_pg_charges)]


bill_doc[,.N,brdg_appt_conslt_sid]

bill_med<-bill[vendor_type=='Pharmacy services',.(service_type,pharmacy_sid,pharmacy_branch_id,pharmacy_order_type,pharmacy_flag,brdg_appt_conslt_sid,bill_sid,doctor_sid,doctor_clinic_addr_sid,patient_sid,vendor_sid,transaction_dt,consultation_amount,paid_by_customer,paid_by_insurer,pg_charges,transaction_amount,payment_type,medicine_order_type,
                                       overall_transaction_status,refund_status,total_gst_amount,convenience_charge,delivery_charge,total_transaction_amount,platform_fee_amount,is_pay_out,is_settle_vendor,is_settle_vendor_datetime,payout_datetime,source_payment_type,marketing_fees_amount_settle,platform_fee_amount_settle,source_pg_charges)]
bill_med[,brdg_appt_conslt_sid:=NULL]
bill_med<-merge(bill_med,bill_doc[,.(bill_sid,brdg_appt_conslt_sid)],by='bill_sid',all.x = T)



bill_lab<-bill[vendor_type=='Diagnostic service process',.(service_type,test_type,lab_id,diagnostic_lab_sid,brdg_appt_conslt_sid,bill_sid,doctor_sid,doctor_clinic_addr_sid,patient_sid,vendor_sid,transaction_dt,consultation_amount,paid_by_customer,paid_by_insurer,pg_charges,transaction_amount,payment_type,
                                                  overall_transaction_status,refund_status,total_gst_amount,convenience_charge,delivery_charge,total_transaction_amount,platform_fee_amount,is_pay_out,is_settle_vendor,is_settle_vendor_datetime,payout_datetime,source_payment_type,marketing_fees_amount_settle,platform_fee_amount_settle,source_pg_charges)]
bill_lab[,brdg_appt_conslt_sid:=NULL]

dt2<-merge(dt2,bill_doc,by='brdg_appt_conslt_sid',all = T)

write.csv(dt2,'first_file.csv',row.names = F,na='')
write.csv(bill_med,'bill_med.csv',row.names = F,na='')
write.csv(bill_lab,'bill_lab.csv',row.names = F,na='')



