# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 17:31:52 2022

@author: RahulKumarSisodia
"""


import pandas as pd
import numpy as np
import os

os.chdir(r'D:\SVAAS_2\FW__doctor_dump')
df_user=pd.read_csv('tbl_user.csv')
df_doc_details=pd.read_csv('tbl_doctor_details.csv')
df_doc_avaibility=pd.read_csv('tbl_doctor_availability.csv')
df_clinic_details=pd.read_csv('tbl_doctor_clinic_address.csv',error_bad_lines=False)
df_specilization=pd.read_csv('tbl_doctors_specialization.csv')
df_cat_price=pd.read_csv('tbl_doctor_availability_catalogue_price_mapping.csv')
df_qualification=pd.read_csv('tbl_doctor_qualification.csv')
df_spec_map=pd.read_csv('tbl_doctor_specialization_mapping.csv')
df_tr_spec_map=pd.read_csv('tbl_doctor_tertiary_specialization_mapping.csv')

######################################### Merge ##################################################
df_user.columns
dt=df_user[['id','email','status']]
dt.rename(columns = {'id':'user_id'}, inplace = True)
df_doc_details.columns

temp=df_doc_details[['user_id','first_name','middle_name','last_name','contact_no','dob','tan','ifsc_code','gender','year_of_experience','pan_number','language_known','bank_name','branch_name','account_no','account_holder_name','kam_id','kam_name','kam_phone_no','device_id','profile_picture','prescription_service_provider','uprn_no','id','drl_doctor_id']]
temp.rename(columns = {'id':'doctor_details_id'}, inplace = True)
temp.rename(columns = {'drl_doctor_id':'svaasDoctorId'}, inplace = True)
#temp.rename(columns = {'user_id':'id'}, inplace = True)

dt=pd.merge(dt,temp,how='outer',on='user_id')

################################# city_id ########################################################
temp2=df_clinic_details[['city_id','doctor_details_id','id']]
temp2.rename(columns = {'id':'doctor_clinic_table_id'}, inplace = True)

dt=pd.merge(dt,temp2,how='outer',on='doctor_details_id')

################################## Specilization ##################################################
temp6=df_specilization[['id','name']]
temp7=df_spec_map[['specialization_id','doctor_details_id']]
temp8=df_tr_spec_map[['specialization_id','doctor_details_id']]
temp6.rename(columns = {'id':'specialization_id'}, inplace = True)

temp7=pd.merge(temp6,temp7,on='specialization_id')
temp8=pd.merge(temp6,temp8,on='specialization_id')
temp7=temp7.drop_duplicates(subset=(['name','doctor_details_id']))
temp8=temp8.drop_duplicates(subset=(['name','doctor_details_id']))
temp7.rename(columns = {'name':'Primary_Specialization'}, inplace = True)
temp8.rename(columns = {'name':'Secondary_Specialization'}, inplace = True)

temp9=pd.merge(temp7,temp8,how='outer',on='doctor_details_id')
temp9=temp9.drop(['specialization_id_x', 'specialization_id_y'], axis=1)
#temp9.rename(columns = {'doctor_details_id':'id'}, inplace = True)
temp9=temp9.drop_duplicates(subset=(['doctor_details_id']))
dt=pd.merge(dt,temp9,how='outer',on='doctor_details_id')

######################### qualification ##############################################################
df_qualification.columns
temp3=df_qualification[['doctor_details_id','short_code']]
df_order=pd.read_excel(r'D:\SVAAS_2\spec_order.xlsx')

temp3['short_code']=temp3['short_code'].str.lower()
df_order['short_code']=df_order['short_code'].str.lower()

df_order.loc[df_order['short_code'].str.contains('mbbs'),'order_no']= 1
df_order.loc[df_order['short_code'].str.contains('md'),'order_no']= 2
df_order.loc[df_order['short_code'].str.contains('ms'),'order_no']= 3
df_order.loc[df_order['short_code'].str.contains('dnb'),'order_no']= 4
df_order.loc[df_order['short_code'].str.contains('mch'),'order_no']= 5
df_order.loc[df_order['short_code'].str.contains('dm'),'order_no']= 6


temp5_qualif=pd.merge(temp3,df_order,on='short_code',how='left')
temp5_qualif.loc[(temp5_qualif['order_no'].isnull()), 'order_no'] = 100
temp5_qualif=temp5_qualif.sort_values(['order_no'],ascending=True)
temp5_qualif = temp5_qualif.groupby(['doctor_details_id'])['short_code'].agg(lambda x: ', '.join(x.dropna())).reset_index()
#temp5_qualif.rename(columns = {'doctor_details_id':'id'}, inplace = True)

dt=pd.merge(dt,temp5_qualif,how='outer',on='doctor_details_id')

####################################### Price ########################################################
temp1=df_doc_avaibility[['type','first_consultation_fee','follow_up_consultation_fee','drl_consultation_fee','price_to_drl_followup','price_to_drl_consultation','doctor_clinic_table_id','price_to_drl_percentage','id']]

temp1.rename(columns = {'id':'doctor_availability_id'}, inplace = True)
t1=pd.melt(temp1, id_vars =['type','doctor_clinic_table_id'])
#t1.drop_duplicates(['type', 'doctor_clinic_table_id', 'variable', 'value'],keep=False,inpalce=True)         
#t1.drop_duplicates(['type','doctor_clinic_table_id'],keep=False,inplace=True)

t1['variable']=t1['variable'] + '_' + t1['type'].astype(str)
t1.drop('type',axis=1,inplace=True)
#pivoting
pivoted= t1.pivot(index=['doctor_clinic_table_id'],columns='variable', values="value").reset_index()

#pivoted.drop_duplicates(subset =['doctor_clinic_table_id', 'drl_consultation_fee_inPerson','drl_consultation_fee_online', 'follow_up_consultation_fee_inPerson','follow_up_consultation_fee_online','price_to_drl_consultation_inPerson','price_to_drl_consultation_online', 'price_to_drl_followup_inPerson','price_to_drl_followup_online', 'price_to_drl_percentage_inPerson','price_to_drl_percentage_online'],keep = False, inplace = True)


#################### catalogue_price_consultations ##################################################

temp4=df_cat_price[['insurance_partner_id','type','catalogue_price_consultation','catalogue_price_followup','doctor_availability_id','catalogue_price_percentage']]
#melt
temp4_melt=pd.melt(temp4, id_vars =['type','insurance_partner_id','doctor_availability_id'])
temp4_melt.columns
temp4_melt.drop_duplicates(subset =['type', 'insurance_partner_id', 'doctor_availability_id', 'variable','value'], keep = False, inplace = True)

temp4_melt['variable']=temp4_melt['variable'] + '_' + temp4_melt['type'] + '_' + temp4_melt['insurance_partner_id'].astype(str)
#temp4_melt['variable1']=temp4_melt["variable"].str.cat(temp4_melt[["type", "insurance_partner_id"]].astype(str), sep="_")

temp4_melt.drop(['type','insurance_partner_id'],axis=1,inplace=True)
#dropping duplicates
temp4_melt.drop_duplicates(subset =['doctor_availability_id', 'variable','value'], keep = False, inplace = True)
#pivoting
temp4_pivoted= temp4_melt.pivot(index=['doctor_availability_id'],columns='variable', values="value").reset_index()

#################################################################################################
#dropping nan
n1=temp4_pivoted[temp4_pivoted['catalogue_price_consultation_InPerson_271'].notnull()]
c_inp_not=n1[(n1['catalogue_price_consultation_InPerson_271'] != n1['catalogue_price_consultation_InPerson_4568'])]
c_inp_not.to_csv('conslt_inperson_price_ins_part_not_match_py.csv',index=False)

n2=temp4_pivoted[temp4_pivoted['catalogue_price_followup_InPerson_271'].notnull()]
c_fw_inp_not=n2[(n2['catalogue_price_followup_InPerson_271']!=n2['catalogue_price_followup_InPerson_4568'])]
c_fw_inp_not.to_csv('conslt_follow_up_inperson_price_ins_part_not_match_py.csv',index=False)

n3=temp4_pivoted[temp4_pivoted['catalogue_price_consultation_OnlineConsultation_271'].notnull()]
n3[(n3['catalogue_price_consultation_OnlineConsultation_271']!=n3['catalogue_price_consultation_OnlineConsultation_4568'])]

n4=temp4_pivoted[temp4_pivoted['catalogue_price_followup_OnlineConsultation_271'].notnull()]
n4[(n4['catalogue_price_followup_OnlineConsultation_271']!=n4['catalogue_price_followup_OnlineConsultation_4568'])]

n5=temp4_pivoted[temp4_pivoted['catalogue_price_percentage_OnlineConsultation_271'].notnull()]
n5[(n5['catalogue_price_percentage_OnlineConsultation_271']!=n5['catalogue_price_percentage_OnlineConsultation_4568'])]

n6=temp4_pivoted[temp4_pivoted['catalogue_price_percentage_InPerson_271'].notnull()]
n6[(n6['catalogue_price_percentage_InPerson_271']!=n6['catalogue_price_percentage_InPerson_4568'])]

#making list 
doc_avab_id=list(c_fw_inp_not['doctor_availability_id'].unique())

temp4_pivoted=temp4_pivoted[~temp4_pivoted['doctor_availability_id'].isin(doc_avab_id)]

doc_avab_id1=list(c_fw_inp_not['doctor_availability_id'].unique())
temp4_pivoted=temp4_pivoted[~temp4_pivoted['doctor_availability_id'].isin(doc_avab_id1)]

temp4_pivoted=temp4_pivoted[['doctor_availability_id','catalogue_price_consultation_InPerson_271','catalogue_price_followup_InPerson_271','catalogue_price_consultation_OnlineConsultation_271','catalogue_price_followup_OnlineConsultation_271','catalogue_price_percentage_InPerson_271','catalogue_price_percentage_OnlineConsultation_271']]
temp4_pivoted.columns=['doctor_availability_id','catalogue_price_consultation_InPerson','catalogue_price_followup_InPerson','catalogue_price_consultation_OnlineConsultation','catalogue_price_followup_OnlineConsultation','catalogue_price_percentage_InPerson','catalogue_price_percentage_OnlineConsultation']

#################################################################################################

temp1_cat=temp1[['doctor_availability_id','doctor_clinic_table_id']]
temp4_pivoted_cat=pd.merge(temp4_pivoted,temp1_cat,how='inner',on='doctor_availability_id')
temp4_pivoted_cat.drop('doctor_availability_id',axis=1,inplace=True)

temp4_pivoted_cat=pd.melt(temp4_pivoted_cat, id_vars =['doctor_clinic_table_id'])
temp4_pivoted_cat=temp4_pivoted_cat.dropna()
temp4_pivoted_cat.columns

temp4_pivoted_cat.drop_duplicates(['doctor_clinic_table_id', 'variable', 'value'],keep=False,inplace=True)
#pivoting
temp4_pivoted_cat= temp4_pivoted_cat.pivot(index=['doctor_clinic_table_id'],columns='variable', values="value").reset_index()
################################################# Done #####################################################
#merging with dt
dt=pd.merge(dt,pivoted,how='outer',on='doctor_clinic_table_id')
dt=pd.merge(dt,temp4_pivoted_cat,how='outer',on='doctor_clinic_table_id')

################################################################################################
typ=df_doc_avaibility[['type','doctor_clinic_table_id']]
typ.drop_duplicates(['type','doctor_clinic_table_id'],keep=False,inplace=True)
typ=typ.sort_values(by='doctor_clinic_table_id',ascending=True)
typ =typ.groupby(['doctor_clinic_table_id'])['type'].agg(lambda x: '|'.join(x.dropna())).reset_index()
typ.rename(columns={'type':'consultationType'},inplace=True)
typ.drop_duplicates(['consultationType','doctor_clinic_table_id'],keep=False,inplace=True)

######################################################################################################
inp=df_cat_price[['insurance_partner_id','doctor_availability_id']]

inp.drop_duplicates(['insurance_partner_id','doctor_availability_id'],keep=False,inplace=True)
inp=pd.merge(inp,temp1_cat, on='doctor_availability_id')
#inp.drop_duplicates(['doctor_clinic_table_id','insurance_partner_id',],keep=False,inplace=True)
inp=inp.sort_values(by='insurance_partner_id',ascending=True)
inp['insurance_partner_id']=inp['insurance_partner_id'].astype(str)
inp =inp.groupby(['doctor_clinic_table_id'])['insurance_partner_id'].agg(lambda x: '|'.join(x.dropna())).reset_index()
inp.rename(columns={'insurance_partner_id':'insuranceProviderId'},inplace=True)


################################## adding columns ################################################
dt['description']= None
dt['primeDoctor']= None
dt['googleReview']= None
dt['familyDoctor']= None
dt['numberOfCustomerAllowedPerFamily']= None

dt=pd.merge(dt,typ,how='outer',on='doctor_clinic_table_id')

dt['numberFollowUp']= None
dt['followUpDuration']= None

dt=pd.merge(dt,inp,how='outer',on='doctor_clinic_table_id')

dt['ABDM_Healthcare_Professional_ID']= None


############################### final csv file ###############################
dt=dt[['first_name', 'middle_name', 'last_name','email','contact_no','description','pan_number','uprn_no','tan','dob', 'gender', 'Primary_Specialization', 'Secondary_Specialization', 'short_code','year_of_experience','language_known','prescription_service_provider','profile_picture','googleReview', 'familyDoctor','numberOfCustomerAllowedPerFamily','device_id',
       'bank_name', 'branch_name','account_holder_name','account_no','ifsc_code','city_id','kam_id', 'kam_name','kam_phone_no','consultationType','first_consultation_fee_OnlineConsultation',"follow_up_consultation_fee_OnlineConsultation","drl_consultation_fee_OnlineConsultation",'price_to_drl_followup_OnlineConsultation','catalogue_price_consultation_OnlineConsultation','catalogue_price_followup_OnlineConsultation','price_to_drl_percentage_OnlineConsultation','catalogue_price_percentage_OnlineConsultation',
       'numberFollowUp',"followUpDuration","insuranceProviderId",'status','ABDM_Healthcare_Professional_ID','doctor_details_id','svaasDoctorId']]
       
       
dt.column=['firstName', 'middleName', 'lastName','email','phone','description','pan','mci','tan','dob', 'gender', 'PrimarySpecialization', 'SecondarySpecialization', 'qualifications','yearOfExperience','language','erxServiceProvider','profilePicture','googleReview', 'familyDoctor','numberOfCustomerAllowedPerFamily','erxDoctorId',
       'bankName', 'branchName','accountholderName','accountNumber','ifsccode','baseCity','kamId', 'kamName','kamPhone','consultationType','consultationFee',"followUpFee","drlConsultationFee",'drlFollowUpFee','catalogueConsultationFee','catalogueFollowUpFee','drlPricePercent','drlCataloguePricePercent',
       'numberFollowUp',"followUpDuration","insuranceProviderId",'isActive','abdmProfessionalId','svaasId','svaasDoctorId']     

dt.to_csv('first_merge_file_py.csv',index=False)







