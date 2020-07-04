import json
import pandas as pd
import numpy as np
import geoip2.database
from attribute_pro_0614.proc_attr_func_1 import *
from attribute_pro_0614.model_attr_func import *
from attribute_pro_0614.read_json_xh import *
from attribute_pro_0614.model_rank_extra import *
# df=pd.read_csv('df1.csv',encoding = 'utf-8')
# data_df = pd.read_csv(r'C:\Users\WFM\python\wool_data_analysis\data_df.csv')
reader = geoip2.database.Reader(r'C:\Users\WFM\python\wool_data_analysis\attribute_pro_0614\GeoLite2-City.mmdb')

# photo attribute preprocess
dic_photo,dic_msg,dic_call,dic_cont,dic_runpac=select_max_time(data_df_3)
ud_df=merge_udf(dic_photo,dic_msg,dic_call,dic_cont,dic_runpac)
dic_photo_num,dic_photo_diff,dic_msg_num,dic_msg_diff,dic_call_num,dic_call_diff,\
dic_call_add,dic_cont_num,dic_cont_diff,dic_cont_add=Process_Photo_Message_call_contact(ud_df)

# main attributes process based on device_id
dic_wifi_state,dic_list,dic_conn_wifi,dic_ip,dic_address,dic_mean,dic_var,dic_count,\
dic_toal_count, dic_appkey, dic_carr, dic_adb, dic_usb, dic_alarm, dic_cmd, dic_imsi, \
dic_ped_mean, dic_ped_var,dic_ped_count, dic_ped_times, dic_pn, dic_loc, dic_net, dic_sto_mean, \
dic_sto_var,dic_sto_count,dic_lbs_max,dic_lbs_count=Process_Simple_Attr(data_df)

final_df=pd.DataFrame([dic_wifi_state,dic_list,dic_conn_wifi,dic_ip,dic_address,dic_mean,dic_var,dic_count,dic_toal_count, dic_appkey, dic_carr, dic_adb, dic_usb, dic_alarm, dic_cmd, dic_imsi, dic_ped_mean, dic_ped_var,dic_ped_count, dic_ped_times, dic_pn, dic_loc, dic_net, dic_sto_mean, dic_sto_var,dic_sto_count,dic_lbs_max,dic_lbs_count,dic_photo_num,dic_photo_diff,dic_msg_num,dic_msg_diff,dic_call_num,dic_call_diff,dic_call_add,dic_cont_num,dic_cont_diff,dic_cont_add],
                      index=['wifi_state_num','wifi_list_num','connected_wifi_num','address_num','address_name','battery_mean','battery_var','battery_count', 'device_count', 'app_key', 'carrier', 'adb', 'usb', 'alarm', 'cmd', 'imsi', 'ped_mean', 'ped_var', 'ped_count', 'ped_times', 'pn', 'location', 'net','storage_mean', 'storage_var', 'storage_count', 'lbs_max', 'lbs_count',
                             'photo_num','photo_diff','msg_num','msg_diff','call_num','call_diff','call_duration','contact_num','contact_diff','contact_count']).transpose().reset_index()
final_df=final_df.rename(columns={'index':'@device_id'})

second_df=pd.merge(final_df,ud_df,on='@device_id')

# process installed_packages
dic_brand,dic_installpac=Process_Brand_InstallPackage(data_df_1)
bran_inst_df = pd.DataFrame([dic_brand,dic_installpac],index=['@content@_cp@brand','@content@_ep@attribute@installed_packages']).transpose().reset_index().rename(columns={'index': '@device_id'})

# process attributes - 'photo','message','call','contact','address'
sdf=second_df
dic_photo,dic_msg,dic_call,dic_cont,dic_address=Process_Match_Dic(sdf)
match_df=pd.DataFrame([dic_photo,dic_msg,dic_call,dic_cont,dic_address],index=['photo_match_num','msg_match_num','call_match_num','contact_match_num','address_match_num']).transpose().reset_index()
match_df=match_df.rename(columns={'index':'@device_id'})

p_df=bran_inst_df.sort_values(by='@content@_cp@brand')
after_installed_pck_dic = Process_Install_Packages(p_df, attribute='@content@_ep@attribute@installed_packages')
after_installed_pck_df = pd.DataFrame(after_installed_pck_dic, index=[0]).transpose().reset_index()
after_installed_pck_df = after_installed_pck_df.rename(columns={'index': '@device_id', 0: 'absent_frequency'})

Final_DF=pd.merge(final_df,match_df,on='@device_id')
Final_DF=pd.merge(Final_DF,after_installed_pck_df,on='@device_id')
#Final_DF.to_csv('Final_DF.csv',encoding='utf-8')

# preprocess - extracting attribtues 'brand/model/rp/sensor/build_prop'
bran_num, bran_value, mode_num, mode_value, rp_num, rp_val, dic_num, dic_sen, dic_buic, dic_buiv = model_key(data_df_2)
att_list = [bran_num, bran_value, mode_num, mode_value, rp_num, rp_val, dic_num, dic_sen, dic_buic, dic_buiv]
model_df = pd.DataFrame(att_list).transpose().reset_index()
model_df.columns = ['@device_id', 'brand_num', 'brand_value', 'model_num', 'model_value',
                    'rp_num', 'rp_value', 'sensor_num', 'sensor_value', 'build_num', 'build_value']

# process attributes based on model sorted - brand model rp sensor build_prop
data_2 = data_sorted(model_df)
id_list, count_list, new_list, same_brand, same_model, \
prop_brand, prop_model, sensor_value, build_value = fm_attr_2(data_2)
# attr_list_2 = [id_list, count_list, new_list, same_brand, same_model, prop_brand, prop_model, sensor_value, build_value]
fm_df_2 = pd.DataFrame([id_list, count_list, new_list, same_brand, same_model, prop_brand, prop_model, sensor_value, build_value]).transpose()
fm_df_2.columns = (['@device_id', 'rp_counts', 'rp_match', 'same_brand', 'same_model', 'prop_brand', 'prop_model', 'sensor_same', 'build_same'])

# merge all the dataframes
data_df = pd.merge(second_df, bran_inst_df, on='@device_id')
data_df = pd.merge(data_df, match_df, on='@device_id')
data_df = pd.merge(data_df, after_installed_pck_df, on='@device_id')
data_df = pd.merge(data_df, model_df, on='@device_id')
data_df = pd.merge(data_df, fm_df_2, on='@device_id')
data_df.to_csv('data_df_final_0624.csv')