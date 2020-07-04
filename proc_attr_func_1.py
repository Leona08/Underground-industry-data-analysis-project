# -*- coding: utf-8 -*-
# coding: unicode_escape
import json
import pandas as pd
import operator
import numpy as np
import geoip2.database
import time
import datetime
from attribute_pro_0614.proc_attr_func_pro import *

reader = geoip2.database.Reader(r'C:\Users\WFM\python\wool_data_analysis\attribute_pro_0614\GeoLite2-City.mmdb')
#读出数据库中IP地址对应的省份名称
def ip_print_AddrInfo(ip):
    try:
        response = reader.city(ip)
        province = response.subdivisions.most_specific.name
    #ProvinceIsoCode = response.subdivisions.most_specific.iso_code
    #City_Name = response.city.names.get('zh-CN')
    except:
        province=np.NaN
    return province
#df=pd.read_csv('df1.csv',encoding = 'utf-8')
#挑选出photo、call、msg、contacts、running_package属性的最新的时间戳对应的有值的记录
def select_max_time(df):
    i = 0
    time_photo = {}
    dic_photo = {}

    time_msg = {}
    dic_msg = {}

    time_cont = {}
    dic_cont = {}

    time_call = {}
    dic_call = {}

    time_runpac={}
    dic_runpac={}
    while i <= df.shape[0]:
        if i == 0:
            if pd.isna(df.iloc[i]['@content@_ep@attribute@photo']) == False:
                time_photo.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
            else:
                time_photo.setdefault(i, np.NaN)
            if pd.isna(df.iloc[i]['@content@_ep@attribute@message']) == False:
                time_msg.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
            else:
                time_msg.setdefault(i, np.NaN)
            if pd.isna(df.iloc[i]['@content@_ep@attribute@call']) == False:
                time_call.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
            else:
                time_call.setdefault(i, np.NaN)
            if pd.isna(df.iloc[i]['@content@_ep@attribute@contacts']) == False:
                time_cont.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
            else:
                time_cont.setdefault(i, np.NaN)
            if pd.isna(df.iloc[i]['@content@_ep@attribute@running_packages']) == False:
                time_runpac.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
            else:
                time_runpac.setdefault(i, np.NaN)
        elif i == df.shape[0]:
            if np.all(pd.isna(list(time_photo.values())) == True):
                dic_photo.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
            else:
                photo_max_index = (sorted(time_photo.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                photo_max_time = (sorted(time_photo.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                photo_value = df.iloc[photo_max_index]['@content@_ep@attribute@photo']
                dic_photo.setdefault(df.iloc[i - 1]['@device_id'], [photo_max_time,photo_value])

            if np.all(pd.isna(list(time_msg.values())) == True):
                dic_msg.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
            else:
                msg_max_index = (sorted(time_msg.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                msg_max_time = (sorted(time_msg.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                msg_value = df.iloc[msg_max_index]['@content@_ep@attribute@message']
                dic_msg.setdefault(df.iloc[i - 1]['@device_id'], [msg_max_time,msg_value])

            if np.all(pd.isna(list(time_call.values())) == True):
                dic_call.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
            else:
                call_max_index = (sorted(time_call.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                call_max_time = (sorted(time_call.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                call_value = df.iloc[call_max_index]['@content@_ep@attribute@call']
                dic_call.setdefault(df.iloc[i - 1]['@device_id'], [call_max_time,call_value])

            if np.all(pd.isna(list(time_cont.values())) == True):
                dic_cont.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
            else:
                cont_max_index = (sorted(time_cont.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                cont_max_time = (sorted(time_cont.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                cont_value = df.iloc[cont_max_index]['@content@_ep@attribute@contacts']
                dic_cont.setdefault(df.iloc[i - 1]['@device_id'], [cont_max_time,cont_value])

            if np.all(pd.isna(list(time_runpac.values())) == True):
                dic_runpac.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
            else:
                runpac_max_index = (sorted(time_runpac.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                runpac_max_time = (sorted(time_runpac.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                runpac_value = df.iloc[runpac_max_index]['@content@_ep@attribute@running_packages']
                dic_runpac.setdefault(df.iloc[i - 1]['@device_id'], [runpac_max_time,runpac_value])


        else:
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if pd.isna(df.iloc[i]['@content@_ep@attribute@photo']) == False:
                    time_photo.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_photo.setdefault(i, np.NaN)
            else:
                if np.all(pd.isna(list(time_photo.values())) == True):
                    dic_photo.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
                else:
                    photo_max_index = (sorted(time_photo.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                    photo_max_time = (sorted(time_photo.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                    photo_value = df.loc[photo_max_index, '@content@_ep@attribute@photo']
                    dic_photo.setdefault(df.iloc[i - 1]['@device_id'], [photo_max_time,photo_value])
                time_photo = {}
                if pd.isna(df.loc[i, '@content@_ep@attribute@photo']) == False:
                    time_photo.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_photo.setdefault(i, np.NaN)

            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if pd.isna(df.iloc[i]['@content@_ep@attribute@message']) == False:
                    time_msg.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_msg.setdefault(i, np.NaN)
            else:
                if np.all(pd.isna(list(time_msg.values())) == True):
                    dic_msg.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
                else:
                    msg_max_index = (sorted(time_msg.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                    msg_max_time = (sorted(time_msg.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                    msg_value = df.iloc[msg_max_index]['@content@_ep@attribute@message']
                    dic_msg.setdefault(df.iloc[i - 1]['@device_id'], [msg_max_time, msg_value])
                time_msg = {}
                if pd.isna(df.loc[i, '@content@_ep@attribute@message']) == False:
                    time_msg.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_msg.setdefault(i, np.NaN)
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if pd.isna(df.iloc[i]['@content@_ep@attribute@call']) == False:
                    time_call.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_call.setdefault(i, np.NaN)
            else:
                if np.all(pd.isna(list(time_call.values())) == True):
                    dic_call.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
                else:
                    call_max_index = (sorted(time_call.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                    call_max_time = (sorted(time_call.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                    call_value = df.iloc[call_max_index]['@content@_ep@attribute@call']
                    dic_call.setdefault(df.iloc[i - 1]['@device_id'], [call_max_time, call_value])
                time_call = {}
                if pd.isna(df.loc[i, '@content@_ep@attribute@call']) == False:
                    time_call.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_call.setdefault(i, np.NaN)
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if pd.isna(df.iloc[i]['@content@_ep@attribute@contacts']) == False:
                    time_cont.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_cont.setdefault(i, np.NaN)
            else:
                if np.all(pd.isna(list(time_cont.values())) == True):
                    dic_cont.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
                else:
                    cont_max_index = (sorted(time_cont.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                    cont_max_time = (sorted(time_cont.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                    cont_value = df.iloc[cont_max_index]['@content@_ep@attribute@contacts']
                    dic_cont.setdefault(df.iloc[i - 1]['@device_id'], [cont_max_time, cont_value])
                time_cont = {}
                if pd.isna(df.loc[i, '@content@_ep@attribute@contacts']) == False:
                    time_cont.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_cont.setdefault(i, np.NaN)
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if pd.isna(df.iloc[i]['@content@_ep@attribute@running_packages']) == False:
                    time_runpac.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_runpac.setdefault(i, np.NaN)
            else:
                if np.all(pd.isna(list(time_runpac.values())) == True):
                    dic_runpac.setdefault(df.iloc[i - 1]['@device_id'], [np.NaN,np.NaN])
                else:
                    runpac_max_index = (sorted(time_runpac.items(), key=operator.itemgetter(1), reverse=True))[0][0]
                    runpac_max_time = (sorted(time_runpac.items(), key=operator.itemgetter(1), reverse=True))[0][1]
                    runpac_value = df.iloc[runpac_max_index]['@content@_ep@attribute@running_packages']
                    dic_runpac.setdefault(df.iloc[i - 1]['@device_id'], [runpac_max_time, runpac_value])
                time_runpac = {}
                if pd.isna(df.loc[i, '@content@_ep@attribute@running_packages']) == False:
                    time_runpac.setdefault(i, int(df.iloc[i]['@content@_ep@timestamp']))
                else:
                    time_runpac.setdefault(i, np.NaN)

        i = i + 1
    return dic_photo,dic_msg,dic_call,dic_cont,dic_runpac
def merge_udf(dic_photo,dic_msg,dic_call,dic_cont,dic_runpac):
    p_df = pd.DataFrame(dic_photo, index=['photo_time','@content@_ep@attribute@photo']).transpose().reset_index()
    p_df=p_df.rename(columns={'index': '@device_id'})
    m_df=pd.DataFrame(dic_msg,index=['msg_time','@content@_ep@attribute@message']).transpose().reset_index()
    m_df=m_df.rename(columns={'index': '@device_id'})
    call_df=pd.DataFrame(dic_call,index=['call_time','@content@_ep@attribute@call']).transpose().reset_index()
    call_df=call_df.rename(columns={'index': '@device_id'})
    con_df=pd.DataFrame(dic_cont,index=['contact_time','@content@_ep@attribute@contacts']).transpose().reset_index()
    con_df=con_df.rename(columns={'index': '@device_id'})
    runpac_df=pd.DataFrame(dic_runpac,index=['runpac_time','@content@_ep@attribute@running_packages']).transpose().reset_index()
    runpac_df=runpac_df.rename(columns={'index': '@device_id'})
    ud_df=pd.merge(p_df,m_df,on='@device_id')
    ud_df=pd.merge(ud_df,call_df,on='@device_id')
    ud_df=pd.merge(ud_df,con_df,on='@device_id')
    ud_df=pd.merge(ud_df,runpac_df,on='@device_id')
    return ud_df

def process_battery(df,data,i,attribute):
    if pd.isna(df.iloc[i][attribute]) == False and \
            df.iloc[i][attribute] != '-1':
        battery = df.iloc[i][attribute].split('/')[0]
        if battery.isdigit() == True:
            data.append(int(battery))
        else:
            data.append(np.NaN)
    else:
        data.append(np.NaN)
    return data

def process_wifi_state(df,w_set,i,attribute):
    if np.all(pd.isna(df.iloc[i][attribute]) == True) or df.iloc[i][attribute] == '':
        w_set=w_set
    else:
        wifi_list = json.loads(df.iloc[i][attribute],strict=False)
        if len(wifi_list) != 0:
            for j in range(len(wifi_list)):
                dic = wifi_list[j]
                w_set.add(dic['ssid'])
    return w_set

def process_wifi_list(df,wifi_set,i,attribute):
    if np.all(pd.isna(df.iloc[i][attribute]) == True):
        wifi_set = wifi_set
    else:
        w_set = set(df.iloc[i][attribute].split('|'))
        wifi_set = wifi_set | w_set
    return wifi_set

def process_conn_wifi(df,wifi_set,i,attribute):
    if np.all(pd.isna(df.iloc[i][attribute]) == True) or pd.isna(df.iloc[i][attribute]) == '<unknown ssid>':
        wifi_set=wifi_set
    else:
        wifi_set.add(df.iloc[i][attribute])
    return wifi_set

def process_ip(df,ip_set,i,attribute):
    if np.all(pd.isna(df.iloc[i][attribute]) == True):
        ip_set=ip_set
    else:
        province=ip_print_AddrInfo(df.iloc[i][attribute])
        if province!=None:
            ip_set.add(province)
    return ip_set
#dic_diff:返回msg和photo属性中的最近一次动作（存储/发送）与采集时间的差值
#dic_num:返回存储或发送的数量
def process_msg_photo(ud_df,j,dic_num,dic_diff,date_max,attribute,attr_time_name,uf_time_name):
    if np.all(pd.isna(ud_df.iloc[j][attribute]) == True) or ud_df.iloc[j][attribute] == '':
        dic_num.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
        dic_diff.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
    else:
        attr_list = json.loads(ud_df.iloc[j][attribute],strict = False)
        if len(attr_list) != 0:
            dic_num.setdefault(ud_df.iloc[j]['@device_id'], len(attr_list))
            for k in range(len(attr_list)):
                dic = attr_list[k]
                attr_time = dic.get(attr_time_name, '0')
                if attr_time.isdigit() == True:
                    attr_date = int(attr_time)
                    if attr_date > date_max:
                        date_max = attr_date
                else:
                    date_max = date_max
            date_time = ud_df.iloc[j][uf_time_name]
            date_diff = date_time - date_max
            dic_diff.setdefault(ud_df.iloc[j]['@device_id'], date_diff)
        else:
            dic_num.setdefault(ud_df.iloc[j]['@device_id'], 0)
            dic_diff.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)


    return dic_num,dic_diff

#dic_num:联系人或通话次数的总和
#dic_diff:最新的发送动作的时间与采集时间的时间差
#dic_add:通话总时长或联系总次数

def process_contacts_call(ud_df,j,add,dic_num,dic_diff,dic_add,attribute,date_max,add_name,attr_time_name,uf_time_name):
    if np.all(pd.isna(ud_df.iloc[j][attribute]) == True) or ud_df.iloc[j][attribute] == '':
        dic_num.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
        dic_diff.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
        dic_add.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
    else:
        attr_list = json.loads(ud_df.iloc[j][attribute],strict = False)
        if len(attr_list) != 0:
            dic_num.setdefault(ud_df.iloc[j]['@device_id'], len(attr_list))
            for k in range(len(attr_list)):
                dic = attr_list[k]
                ad = dic.get(add_name, '0')
                if ad.isdigit() == True:
                    add = add + int(ad)
                else:
                    add = add
                attr_time = dic.get(attr_time_name, '0')
                if attr_time.isdigit() == True:
                    attr_date = int(attr_time)
                    if attr_date > date_max:
                        date_max = attr_date
                else:
                    date_max = date_max
            date_time = ud_df.iloc[j][uf_time_name]
            date_diff = date_time - date_max
            dic_diff.setdefault(ud_df.iloc[j]['@device_id'], date_diff)
            dic_add.setdefault(ud_df.iloc[j]['@device_id'], add)
        else:
            dic_num.setdefault(ud_df.iloc[j]['@device_id'], 0)
            dic_diff.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
            dic_add.setdefault(ud_df.iloc[j]['@device_id'], np.NaN)
    return dic_num,dic_diff,dic_add

def Process_Photo_Message_call_contact(ud_df):
    dic_photo_num = {}
    dic_photo_diff = {}
    dic_msg_num={}
    dic_msg_diff={}
    dic_call_num = {}
    dic_call_diff = {}
    dic_call_add = {}
    dic_cont_num = {}
    dic_cont_diff = {}
    dic_cont_add = {}
    for j in range(len(ud_df)):
        pdate_max = 0
        mdate_max = 0
        date_call_max = 0
        add_call = 0
        date_cont_max = 0
        add_cont = 0
        dic_photo_num,dic_photo_diff=process_msg_photo(ud_df,j,dic_photo_num,dic_photo_diff,date_max=pdate_max,attribute='@content@_ep@attribute@photo',attr_time_name='datetaken',uf_time_name='photo_time')
        dic_msg_num,dic_msg_diff=process_msg_photo(ud_df,j,dic_msg_num,dic_msg_diff,date_max=mdate_max,attribute='@content@_ep@attribute@message',attr_time_name='date',uf_time_name='msg_time')
        dic_call_num,dic_call_diff,dic_call_add=process_contacts_call(ud_df,j,add_call,dic_call_num,dic_call_diff,dic_call_add,attribute='@content@_ep@attribute@call',date_max=date_call_max,add_name='duration',attr_time_name='date',uf_time_name='call_time')
        dic_cont_num,dic_cont_diff,dic_cont_add=process_contacts_call(ud_df,j,add_cont,dic_cont_num,dic_cont_diff,dic_cont_add,attribute='@content@_ep@attribute@contacts',date_max=date_cont_max,add_name='times_contacted',attr_time_name='last_time_contacted',uf_time_name='contact_time')
    return dic_photo_num,dic_photo_diff,dic_msg_num,dic_msg_diff,dic_call_num,dic_call_diff,dic_call_add,dic_cont_num,dic_cont_diff,dic_cont_add

def Process_Simple_Attr(df):
    dic_wifi_state = {}
    dic_list = {}
    dic_conn_wifi={}
    dic_ip={}
    dic_mean = {}
    dic_var = {}
    dic_count = {}
    dic_address = {}
    count1 = 0
    count2 = 0
    i = 0
    data_df=df
    while i <= df.shape[0]:
        if i == 0:
            wstate_set = set([])
            wstate_set = process_wifi_state(df, wstate_set, i,attribute= '@content@_ep@attribute@wifi_state')
            wlist_set = set([])
            wlist_set = process_wifi_list(df, wlist_set, i,attribute='@content@_ep@attribute@wifi_list')
            wconn_set = set([])
            wconn_set = process_conn_wifi(df, wconn_set, i,attribute='@content@_ep@attribute@connect_wifi@ssid')
            ip_set = set([])
            ip_set = process_ip(df,ip_set,i,attribute='@remote_address')
            add_set = set([])
            add_set = process_ip(df, add_set, i, attribute='@remote_address')
            data = []
            data = process_battery(df, data, i, attribute='@content@_ep@attribute@battery')
            count1 = 1


            app_key = []
            app_key.append(appKey(df,i))

            net_list = []
            net_list.append(netUni(df,i))

            carr_list = list()
            carr_list.append(Carrier(df,i))

            alarm_list = []
            alarm_list.append(alarms(df,i))

            cmd_list = list()
            cmd_list.append(cmdline(df,i))

            loc_list = list()
            loc_list.append(location(df,i))

            pn_list = list()
            pn_list.append(pn(df,i))

            imsi_list = list()
            imsi_list.append(imsi(df,i))

            sto_list = list()
            sto_value, storage_count = storage(df,i)
            sto_list.append(sto_value)
            sto_count = list()
            sto_count.append(storage_count)

            adb_list = list()
            adb_usb = list()
            adb_list.append(adb(df,i))
            adb_usb.append(usb(df,i))

            pedme_list = list()
            pedme_count = list()
            pedme_time = list()
            ped_value, ped_time, ped_count = pedmet(df,i)
            pedme_list.append(ped_value)
            pedme_count.append(ped_count)
            pedme_time.append(ped_time)

            lbs_list = []
            lbs_list.append(lbs(df,i))

            count2 = 1
        elif i == df.shape[0]:
            dic_wifi_state.setdefault(df.iloc[i - 1]['@device_id'], len(wstate_set))
            dic_list.setdefault(df.iloc[i - 1]['@device_id'], len(wlist_set))
            dic_conn_wifi.setdefault(df.iloc[i - 1]['@device_id'], len(wconn_set))
            dic_ip.setdefault(df.iloc[i-1]['@device_id'],len(ip_set))
            if len(list(add_set)) != 0:
                dic_address.setdefault(df.iloc[i - 1]['@device_id'], str(list(add_set)))
            else:
                dic_address.setdefault(df.iloc[i - 1]['@device_id'], np.NaN)
            dic_mean.setdefault(df.iloc[i - 1]['@device_id'], np.nanmean(data))
            dic_var.setdefault(df.iloc[i - 1]['@device_id'], np.nanvar(data))
            dic_count.setdefault(df.iloc[i - 1]['@device_id'], int(count1))


            dic_toal_count.setdefault(data_df.loc[i - 1, '@device_id'], count2)
            dic_appkey.setdefault(data_df.loc[i - 1, '@device_id'], uniapp(app_key))
            dic_net.setdefault(data_df.loc[i - 1, '@device_id'], netValue(net_list,df))
            dic_carr.setdefault(data_df.loc[i - 1, '@device_id'], carValue(carr_list,df))
            dic_alarm.setdefault(data_df.loc[i - 1, '@device_id'], ala_value(alarm_list))
            dic_cmd.setdefault(data_df.loc[i - 1, '@device_id'], cmd_value(cmd_list))
            dic_loc.setdefault(data_df.loc[i - 1, '@device_id'], loc_value(loc_list))
            dic_pn.setdefault(data_df.loc[i - 1, '@device_id'], pn_value(pn_list))
            dic_imsi.setdefault(data_df.loc[i - 1, '@device_id'], imsi_value(imsi_list))
            dic_sto_mean.setdefault(data_df.loc[i - 1, '@device_id'], error(sto_list, np.nanmean))
            dic_sto_var.setdefault(data_df.loc[i - 1, '@device_id'], error(sto_list, np.nanvar))
            dic_sto_count.setdefault(data_df.loc[i - 1, '@device_id'], np.sum(sto_count))
            dic_adb.setdefault(data_df.loc[i - 1, '@device_id'], adb_value(adb_list))
            dic_usb.setdefault(data_df.loc[i - 1, '@device_id'], adb_value(adb_usb))
            dic_ped_mean.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_list, np.nanmean))
            dic_ped_var.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_list, np.nanvar))
            dic_ped_count.setdefault(data_df.loc[i - 1, '@device_id'], np.sum(pedme_count))
            dic_ped_times.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_time, np.nanmax))
            lbs_max, lbs_count = lbs_value(lbs_list)
            dic_lbs_max.setdefault(data_df.loc[i - 1, '@device_id'], lbs_max)
            dic_lbs_count.setdefault(data_df.loc[i - 1, '@device_id'], lbs_count)
        else:
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                wstate_set = process_wifi_state(df, wstate_set, i,attribute='@content@_ep@attribute@wifi_state')
                wlist_set = process_wifi_list(df, wlist_set, i,attribute='@content@_ep@attribute@wifi_list')
                wconn_set = process_conn_wifi(df, wconn_set, i, attribute='@content@_ep@attribute@connect_wifi@ssid')
                ip_set = process_ip(df, ip_set, i, attribute='@remote_address')
                add_set = process_ip(df, add_set, i, attribute='@remote_address')

                data = process_battery(df, data, i, attribute='@content@_ep@attribute@battery')
                count1 = count1 + 1

                app_key.append(appKey(df,i))

                net_list.append(netUni(df,i))

                carr_list.append(Carrier(df,i))

                alarm_list.append(alarms(df,i))

                cmd_list.append(cmdline(df,i))

                loc_list.append(location(df,i))

                pn_list.append(pn(df,i))

                imsi_list.append(imsi(df,i))

                sto_value, storage_count = storage(df,i)
                sto_list.append(sto_value)
                sto_count.append(storage_count)

                adb_list.append(adb(df,i))
                adb_usb.append(usb(df,i))

                ped_value, ped_time, ped_count = pedmet(df,i)
                pedme_list.append(ped_value)
                pedme_count.append(ped_count)
                pedme_time.append(ped_time)

                lbs_list.append(lbs(df,i))
                count2 = count2 + 1

            else:
                dic_wifi_state.setdefault(df.iloc[i - 1]['@device_id'], len(wstate_set))
                dic_list.setdefault(df.iloc[i - 1]['@device_id'], len(wlist_set))
                dic_conn_wifi.setdefault(df.iloc[i - 1]['@device_id'], len(wconn_set))
                dic_ip.setdefault(df.iloc[i - 1]['@device_id'], len(ip_set))
                if len(list(add_set))!=0:
                    dic_address.setdefault(df.iloc[i - 1]['@device_id'],str(list(add_set)))
                else:
                    dic_address.setdefault(df.iloc[i - 1]['@device_id'],np.NaN)
                dic_mean.setdefault(df.iloc[i - 1]['@device_id'], np.nanmean(data))
                dic_var.setdefault(df.iloc[i - 1]['@device_id'], np.nanvar(data))
                dic_count.setdefault(df.iloc[i - 1]['@device_id'], int(count1))
                wstate_set = set([])
                wstate_set = process_wifi_state(df, wstate_set, i, attribute='@content@_ep@attribute@wifi_state')
                wlist_set = set([])
                wlist_set = process_wifi_list(df, wlist_set, i, attribute='@content@_ep@attribute@wifi_list')
                wconn_set = set([])
                wconn_set = process_conn_wifi(df, wconn_set, i, attribute='@content@_ep@attribute@connect_wifi@ssid')
                ip_set = set([])
                ip_set = process_ip(df, ip_set, i, attribute='@remote_address')
                add_set = set([])
                add_set = process_ip(df, add_set, i, attribute='@remote_address')
                data = []
                data = process_battery(df, data, i, attribute='@content@_ep@attribute@battery')
                count1 = 1

                dic_toal_count.setdefault(data_df.loc[i - 1, '@device_id'], count2)
                count2 = 1
                dic_appkey.setdefault(data_df.loc[i - 1, '@device_id'], uniapp(app_key))
                app_key = []
                app_key.append(appKey(df,i))

                dic_net.setdefault(data_df.loc[i - 1, '@device_id'], netValue(net_list,df))
                net_list = []
                net_list.append(netUni(df,i))

                dic_carr.setdefault(data_df.loc[i - 1, '@device_id'], carValue(carr_list,df))
                carr_list = []
                carr_list.append(Carrier(df,i))

                dic_alarm.setdefault(data_df.loc[i - 1, '@device_id'], ala_value(alarm_list))
                alarm_list = []
                alarm_list.append(alarms(df,i))

                dic_cmd.setdefault(data_df.loc[i - 1, '@device_id'], cmd_value(cmd_list))
                cmd_list = list()
                cmd_list.append(cmdline(df,i))

                dic_loc.setdefault(data_df.loc[i - 1, '@device_id'], loc_value(loc_list))
                loc_list = list()
                loc_list.append(location(df,i))

                dic_pn.setdefault(data_df.loc[i - 1, '@device_id'], pn_value(pn_list))
                pn_list = list()
                pn_list.append(pn(df,i))

                dic_imsi.setdefault(data_df.loc[i - 1, '@device_id'], imsi_value(imsi_list))
                imsi_list = list()
                imsi_list.append(imsi(df, i))

                dic_sto_mean.setdefault(data_df.loc[i - 1, '@device_id'], error(sto_list, np.nanmean))
                dic_sto_var.setdefault(data_df.loc[i - 1, '@device_id'], error(sto_list, np.nanvar))
                dic_sto_count.setdefault(data_df.loc[i - 1, '@device_id'], np.sum(sto_count))
                sto_list = list()
                sto_value, storage_count = storage(df,i)
                sto_list.append(sto_value)
                sto_count = list()
                sto_count.append(storage_count)

                dic_adb.setdefault(data_df.loc[i - 1, '@device_id'], adb_value(adb_list))
                dic_usb.setdefault(data_df.loc[i - 1, '@device_id'], adb_value(adb_usb))
                adb_list = list()
                adb_usb = list()
                adb_list.append(adb(df,i))
                adb_usb.append(usb(df,i))

                dic_ped_mean.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_list, np.nanmean))
                dic_ped_var.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_list, np.nanvar))
                dic_ped_count.setdefault(data_df.loc[i - 1, '@device_id'], np.sum(pedme_count))
                dic_ped_times.setdefault(data_df.loc[i - 1, '@device_id'], error(pedme_time, np.nanmax))
                pedme_list = list()
                pedme_count = list()
                pedme_time = list()
                ped_value, ped_time, ped_count = pedmet(df,i)
                pedme_list.append(ped_value)
                pedme_count.append(ped_count)
                pedme_time.append(ped_time)

                lbs_max, lbs_count = lbs_value(lbs_list)
                dic_lbs_max.setdefault(data_df.loc[i - 1, '@device_id'], lbs_max)
                dic_lbs_count.setdefault(data_df.loc[i - 1, '@device_id'], lbs_count)

                lbs_list = []
                lbs_list.append(lbs(df,i))
        i = i + 1
    return dic_wifi_state,dic_list,dic_conn_wifi,dic_ip,dic_address,dic_mean,dic_var,dic_count,dic_toal_count, dic_appkey, dic_carr, dic_adb, dic_usb, dic_alarm, dic_cmd, dic_imsi, dic_ped_mean, dic_ped_var,dic_ped_count, dic_ped_times, dic_pn, dic_loc, dic_net, dic_sto_mean, dic_sto_var,dic_sto_count,dic_lbs_max,dic_lbs_count

#把同一个device_id的brand和install_package的所有值合并为一条记录
def Process_Brand_InstallPackage(df):
    dic_brand={}
    dic_installpac={}
    i=0
    while i <= df.shape[0]:
        if i == 0:
            brand_set=set([])
            installpac_set=set([])
            #判断属性值是否为nan，若为nan，则"=="为False
            if df.iloc[i]['@content@_cp@brand']== df.iloc[i]['@content@_cp@brand']:
                brand_set.add(df.iloc[i]['@content@_cp@brand'].upper())
            else:
                brand_set.add('0')
            if df.iloc[i]['@content@_ep@attribute@installed_packages']== df.iloc[i]['@content@_ep@attribute@installed_packages']:
                installpac_set.add(df.iloc[i]['@content@_ep@attribute@installed_packages'])
            else:
                installpac_set.add('0')
        elif i == df.shape[0]:
            if len(list(brand_set)) == 1 and list(brand_set)[0]!='0':
                dic_brand.setdefault(df.iloc[i - 1]['@device_id'], list(brand_set)[0])
            else:
                dic_brand.setdefault(df.iloc[i - 1]['@device_id'], np.NaN)

            if len(list(installpac_set))==1 and list(installpac_set)[0] == '0':
                dic_installpac.setdefault(df.iloc[i - 1]['@device_id'], '0')
            else:
                dic_installpac.setdefault(df.iloc[i - 1]['@device_id'],str(list(installpac_set)))
        else:
            if df.iloc[i]['@device_id'] == df.iloc[i - 1]['@device_id']:
                if df.iloc[i]['@content@_cp@brand'] == df.iloc[i]['@content@_cp@brand']:
                    brand_set.add(df.iloc[i]['@content@_cp@brand'].upper())
                else:
                    brand_set.add('0')

                if df.iloc[i]['@content@_ep@attribute@installed_packages'] == df.iloc[i]['@content@_ep@attribute@installed_packages']:
                    installpac_set.add(df.iloc[i]['@content@_ep@attribute@installed_packages'])
                else:
                    installpac_set.add('0')
            else:
                if len(list(brand_set)) == 1 and list(brand_set)[0] != '0':
                    dic_brand.setdefault(df.iloc[i - 1]['@device_id'], list(brand_set)[0])
                else:
                    dic_brand.setdefault(df.iloc[i - 1]['@device_id'], np.NaN)

                if len(list(installpac_set)) == 1 and list(installpac_set)[0] == '0':
                    dic_installpac.setdefault(df.iloc[i - 1]['@device_id'], '0')
                else:
                    dic_installpac.setdefault(df.iloc[i - 1]['@device_id'], str(list(installpac_set)))

                brand_set=set([])
                installpac_set=set([])

                if df.iloc[i]['@content@_cp@brand'] == df.iloc[i]['@content@_cp@brand']:
                    brand_set.add(df.iloc[i]['@content@_cp@brand'].upper())
                else:
                    brand_set.add('0')
                if df.iloc[i]['@content@_ep@attribute@installed_packages'] == df.iloc[i]['@content@_ep@attribute@installed_packages']:
                    installpac_set.add(df.iloc[i]['@content@_ep@attribute@installed_packages'])
                else:
                    installpac_set.add('0')
        i=i+1

    return dic_brand,dic_installpac

def generate_match_set(sdf,i,attribute,match_set_name):
    attr_set=set([])
    if attribute == 'address_name' :
        if sdf.iloc[i][attribute] != '[nan]':
        #str(list(add_set))
            attr_list=eval(sdf.iloc[i][attribute])
            for k in range(len(attr_list)):
                attr_set.add(attr_list[k])
    else:
        if np.all(pd.isna(sdf.iloc[i][attribute])==True) or sdf.iloc[i][attribute] == ''\
                or sdf.iloc[i][attribute] == 'nan':
            attr_set=attr_set
        else:
            attr_list = json.loads(sdf.iloc[i][attribute],strict = False)
            for k in range(len(attr_list)):
                dic = attr_list[k]
                attr_set.add(dic.get(match_set_name, ''))
    return attr_set

def compute_match_num(sdf,index,attribute,match_set_name):
    #tuple可作为索引（可哈希），list和set都不可以
    set_dic = {}
    value_list = []
    match_dic = {}
    for i in index:
        attr_set = generate_match_set(sdf, i, attribute, match_set_name)
        set_dic.setdefault(i, attr_set)
    for value in set_dic.values():
        value_list.append(value)
    value_list = np.unique(value_list)
    for v in value_list:
        match_dic.setdefault(tuple(v), [])
    for k, v in set_dic.items():
        v = tuple(v)
        if v in match_dic.keys():
            match_dic[v].append(k)
        else:
            match_dic[v] = []
    return match_dic
#对属性photo、msg、call、contacts进行设备之间的比较；先对属性_num排序，然后找到相同数量且相同内容的匹配数量
def Process_Match_Dic(sdf):
    dic_photo = {}
    dic_msg = {}
    dic_call = {}
    dic_cont = {}
    dic_address = {}

    index_photo = []
    index_msg = []
    index_call = []
    index_cont = []
    index_address = []

    sdf_photo = sdf.sort_values(by='photo_num')
    sdf_msg = sdf.sort_values(by='msg_num')
    sdf_call = sdf.sort_values(by='call_num')
    sdf_cont = sdf.sort_values(by='contact_num')
    sdf_address = sdf.sort_values(by='address_num')
    L=sdf.shape[0]
    j=0
    while j <= L:
        if j == 0:
            if sdf_photo.iloc[j]['photo_num'] == 0:
                dic_photo.setdefault(sdf_photo.iloc[j]['@device_id'], np.NaN)
            elif sdf_photo.iloc[j]['photo_num'] != 0 and pd.isna(sdf_photo.iloc[j]['photo_num']) == False:
                index_photo.append(j)
            else:
                dic_photo.setdefault(sdf_photo.iloc[j]['@device_id'], np.NaN)

            if sdf_msg.iloc[j]['msg_num'] == 0:
                dic_msg.setdefault(sdf_msg.iloc[j]['@device_id'], np.NaN)
            elif sdf_msg.iloc[j]['msg_num'] != 0 and pd.isna(sdf_msg.iloc[j]['msg_num']) == False:
                index_msg.append(j)
            else:
                dic_msg.setdefault(sdf_msg.iloc[j]['@device_id'], np.NaN)

            if sdf_call.iloc[j]['call_num'] == 0:
                dic_call.setdefault(sdf_call.iloc[j]['@device_id'], np.NaN)
            elif sdf_call.iloc[j]['call_num'] != 0 and pd.isna(sdf_call.iloc[j]['call_num']) == False:
                index_call.append(j)
            else:
                dic_call.setdefault(sdf_call.iloc[j]['@device_id'], np.NaN)

            if sdf_cont.iloc[j]['contact_num'] == 0:
                dic_cont.setdefault(sdf_call.iloc[j]['@device_id'], np.NaN)
            elif sdf_cont.iloc[j]['contact_num'] != 0 and pd.isna(sdf_cont.iloc[j]['contact_num']) == False:
                index_cont.append(j)
            else:
                dic_cont.setdefault(sdf_cont.iloc[j]['@device_id'], np.NaN)

            if sdf_address.iloc[j]['address_num'] == 0:
                dic_address.setdefault(sdf_address.iloc[j]['@device_id'], np.NaN)
            elif sdf_address.iloc[j]['address_num'] != 0 and pd.isna(sdf_address.iloc[j]['address_num']) == False:
                index_address.append(j)
            else:
                dic_address.setdefault(sdf_address.iloc[j]['@device_id'], np.NaN)
        elif j == L:
            if len(index_photo) != 0:
                match_dic_photo = compute_match_num(sdf_photo, index_photo,'@content@_ep@attribute@photo','_display_name')
                for v_list in match_dic_photo.values():
                    if len(v_list) != 0:
                        for i in v_list:
                            dic_photo.setdefault(sdf.iloc[i]['@device_id'], len(v_list) - 1)
            else:
                dic_photo.setdefault(sdf_photo.iloc[j - 1]['@device_id'], np.NaN)

            if len(index_msg) != 0:
                match_dic_msg = compute_match_num(sdf_msg, index_msg,'@content@_ep@attribute@message','address')
                for v_list in match_dic_msg.values():
                    if len(v_list) != 0:
                        for i in v_list:
                            dic_msg.setdefault(sdf_msg.iloc[i]['@device_id'], len(v_list) - 1)
            else:
                dic_msg.setdefault(sdf_msg.iloc[j - 1]['@device_id'], np.NaN)

            if len(index_call) != 0:
                match_dic_call = compute_match_num(sdf_call, index_call,'@content@_ep@attribute@call','number')
                for v_list in match_dic_call.values():
                    if len(v_list) != 0:
                        for i in v_list:
                            dic_call.setdefault(sdf_call.iloc[i]['@device_id'], len(v_list) - 1)
            else:
                dic_call.setdefault(sdf_call.iloc[j - 1]['@device_id'], np.NaN)

            if len(index_cont) != 0:
                match_dic_cont= compute_match_num(sdf_cont, index_cont,'@content@_ep@attribute@contacts','display_name')
                for v_list in match_dic_cont.values():
                    if len(v_list) != 0:
                        for i in v_list:
                            dic_cont.setdefault(sdf_cont.iloc[i]['@device_id'], len(v_list) - 1)
            else:
                dic_cont.setdefault(sdf_cont.iloc[j - 1]['@device_id'], np.NaN)

            if len(index_address) != 0:
                match_dic_address= compute_match_num(sdf_address, index_address,'address_name',None)
                for v_list in match_dic_address.values():
                    if len(v_list) != 0:
                        for i in v_list:
                            dic_address.setdefault(sdf_address.iloc[i]['@device_id'], len(v_list) - 1)
            else:
                dic_address.setdefault(sdf_address.iloc[j - 1]['@device_id'], np.NaN)

        else:
            if sdf_photo.iloc[j]['photo_num'] == 0:
                dic_photo.setdefault(sdf_photo.iloc[j]['@device_id'], np.NaN)
            elif sdf_photo.iloc[j]['photo_num'] != 0 and pd.isna(sdf_photo.iloc[j]['photo_num']) == False:
                if sdf_photo.iloc[j]['photo_num'] == sdf_photo.iloc[j - 1]['photo_num']:
                    index_photo.append(j)
                else:
                    if len(index_photo) != 0:
                        match_dic_photo = compute_match_num(sdf_photo, index_photo,'@content@_ep@attribute@photo','_display_name')
                        for v_list in match_dic_photo.values():
                            if len(v_list) != 0:
                                for i in v_list:
                                    dic_photo.setdefault(sdf_photo.iloc[i]['@device_id'], len(v_list) - 1)
                    index_photo = []
                    index_photo.append(j)
            else:
                if len(index_photo) != 0:
                    match_dic_photo = compute_match_num(sdf_photo, index_photo, '@content@_ep@attribute@photo', '_display_name')
                    for v_list in match_dic_photo.values():
                        if len(v_list) != 0:
                            for i in v_list:
                                dic_photo.setdefault(sdf_photo.iloc[i]['@device_id'], len(v_list) - 1)
                index_photo = []
                dic_photo.setdefault(sdf_photo.iloc[j]['@device_id'], np.NaN)

            if sdf_msg.iloc[j]['msg_num'] == 0:
                dic_msg.setdefault(sdf_msg.iloc[j]['@device_id'], np.NaN)
            elif sdf_msg.iloc[j]['msg_num'] != 0 and pd.isna(sdf_msg.iloc[j]['msg_num']) == False:
                if sdf_msg.iloc[j]['msg_num'] == sdf_msg.iloc[j - 1]['msg_num']:
                    index_msg.append(j)
                else:
                    if len(index_msg) != 0:
                        match_dic_msg = compute_match_num(sdf_msg, index_msg,'@content@_ep@attribute@message','address')
                        for v_list in match_dic_msg.values():
                            if len(v_list) != 0:
                                for i in v_list:
                                    dic_msg.setdefault(sdf_msg.iloc[i]['@device_id'], len(v_list) - 1)
                    index_msg = []
                    index_msg.append(j)
            else:
                if len(index_msg) != 0:
                    match_dic_msg = compute_match_num(sdf_msg, index_msg,'@content@_ep@attribute@message','address')
                    for v_list in match_dic_msg.values():
                        if len(v_list) != 0:
                            for i in v_list:
                                dic_msg.setdefault(sdf_msg.iloc[i]['@device_id'], len(v_list) - 1)
                index_msg = []
                dic_msg.setdefault(sdf_msg.iloc[j]['@device_id'], np.NaN)

            if sdf_call.iloc[j]['call_num'] == 0:
                dic_call.setdefault(sdf_call.iloc[j]['@device_id'], np.NaN)
            elif sdf_call.iloc[j]['call_num'] != 0 and pd.isna(sdf_call.iloc[j]['call_num']) == False:
                if sdf_call.iloc[j]['call_num'] == sdf_call.iloc[j - 1]['call_num']:
                    index_call.append(j)
                else:
                    if len(index_call) != 0:
                        match_dic_call = compute_match_num(sdf_call, index_call,'@content@_ep@attribute@call','number')
                        for v_list in match_dic_call.values():
                            if len(v_list) != 0:
                                for i in v_list:
                                    dic_call.setdefault(sdf_call.iloc[i]['@device_id'], len(v_list) - 1)
                    index_call = []
                    index_call.append(j)
            else:
                if len(index_call) != 0:
                    match_dic_call = compute_match_num(sdf_call, index_call,'@content@_ep@attribute@call','number')
                    for v_list in match_dic_call.values():
                        if len(v_list) != 0:
                            for i in v_list:
                                dic_call.setdefault(sdf_call.iloc[i]['@device_id'], len(v_list) - 1)
                index_call = []
                dic_call.setdefault(sdf_call.iloc[j]['@device_id'], np.NaN)

            if sdf_cont.iloc[j]['contact_num'] == 0:
                dic_cont.setdefault(sdf_cont.iloc[j]['@device_id'], np.NaN)
            elif sdf_cont.iloc[j]['contact_num'] != 0 and pd.isna(sdf_cont.iloc[j]['contact_num']) == False:
                if sdf_cont.iloc[j]['contact_num'] == sdf_cont.iloc[j - 1]['contact_num']:
                    index_cont.append(j)
                else:
                    if len(index_call) != 0:
                        match_dic_cont = compute_match_num(sdf_cont, index_cont,'@content@_ep@attribute@contacts','display_name')
                        for v_list in match_dic_cont.values():
                            if len(v_list) != 0:
                                for i in v_list:
                                    dic_cont.setdefault(sdf_cont.iloc[i]['@device_id'], len(v_list) - 1)
                    index_cont = []
                    index_cont.append(j)
            else:
                if len(index_cont) != 0:
                    match_dic_cont = compute_match_num(sdf_cont, index_cont,'@content@_ep@attribute@contacts','display_name')
                    for v_list in match_dic_cont.values():
                        if len(v_list) != 0:
                            for i in v_list:
                                dic_cont.setdefault(sdf_cont.iloc[i]['@device_id'], len(v_list) - 1)
                index_cont = []
                dic_cont.setdefault(sdf_cont.iloc[j]['@device_id'], np.NaN)

            if sdf_address.iloc[j]['address_num'] == 0:
                dic_address.setdefault(sdf_address.iloc[j]['@device_id'], np.NaN)
            elif sdf_address.iloc[j]['address_num'] != 0 and pd.isna(sdf_address.iloc[j]['address_num']) == False:
                if sdf_address.iloc[j]['address_num'] == sdf_address.iloc[j - 1]['address_num']:
                    index_address.append(j)
                else:
                    if len(index_address) != 0:
                        match_dic_address = compute_match_num(sdf_address, index_address,'address_name',None)
                        for v_list in match_dic_address.values():
                            if len(v_list) != 0:
                                for i in v_list:
                                    dic_address.setdefault(sdf_address.iloc[i]['@device_id'], len(v_list) - 1)
                    index_address= []
                    index_address.append(j)
            else:
                if len(index_address) != 0:
                    match_dic_address = compute_match_num(sdf_address, index_address,'address_name',None)
                    for v_list in match_dic_address.values():
                        if len(v_list) != 0:
                            for i in v_list:
                                dic_address.setdefault(sdf_address.iloc[i]['@device_id'], len(v_list) - 1)
                index_address = []
                dic_address.setdefault(sdf_address.iloc[j]['@device_id'], np.NaN)
        j = j + 1
    return dic_photo,dic_msg,dic_call,dic_cont,dic_address

#处理对brand排序之后的p_df中的install_package的值：'['0','com.pi.al|com.yun.lo']'

def process_list(p_df,i,attribute):
    attr_list1=[]
    if p_df.iloc[i][attribute] != '0':
        value_list = eval(p_df.iloc[i][attribute])
        for j in range(len(value_list)):
            if value_list[j] != '0':
                attr_list1 = attr_list1 + value_list[j].split('|')
    return attr_list1
#求相同的brand的install_package的并集中的包出现的频率比例（0-1）
def compute_frequency(attr_list):
    dic_freq={}

    if len(attr_list)!=0:
        for str1 in attr_list:
            dic_freq.setdefault(str1, attr_list.count(str1))
        sum = 0
        for value in dic_freq.values():
            sum = sum + value
        for key in dic_freq.keys():
            dic_freq[key] = dic_freq[key] / sum
    return dic_freq
#计算每个device_id的包的缺失值
def compute_absent(dic_freq,dic):

    dic_absent = {}
    if dic_freq:
        for key in dic.keys():
            absent_value=0
            total_pack_list = list(dic_freq.keys())
            id_pack_list = eval(dic[key])
            ret_list = [item for item in total_pack_list if item not in id_pack_list]
            for pck in ret_list:
                absent_value = absent_value + dic_freq[pck]
            dic_absent.setdefault(key, absent_value)

    else:
        for key in dic.keys():
            dic_absent.setdefault(key, np.NaN)
    return dic_absent

def Process_Install_Packages(p_df,attribute):
    dic_total={}
    i=0
    while i <= p_df.shape[0]:
        if i==0:
            attr_list=[]
            dic={}
            attr_list1=process_list(p_df,i,attribute)
            dic.setdefault(p_df.iloc[i]['@device_id'],str(attr_list1))
            attr_list=attr_list+attr_list1
        elif i == p_df.shape[0]:
            dic_freq = compute_frequency(attr_list)
            dic_absent = compute_absent(dic_freq, dic)
            dic_total.update(dic_absent)
        else:
            if p_df.iloc[i]['@content@_cp@brand']== p_df.iloc[i - 1]['@content@_cp@brand']:
                attr_list1=process_list(p_df,i,attribute)
                dic.setdefault(p_df.iloc[i]['@device_id'], str(attr_list1))
                #求得所有相同的brand的install_package的包名的并集
                attr_list=attr_list+attr_list1
            else:
                dic_freq=compute_frequency(attr_list)
                dic_absent=compute_absent(dic_freq,dic)
                dic_total.update(dic_absent)
                attr_list=[]
                dic={}
                attr_list1 = process_list(p_df, i, attribute)
                dic.setdefault(p_df.iloc[i]['@device_id'], str(attr_list1))
                attr_list = attr_list + attr_list1
        i=i+1
    return dic_total
def time_stamp(time_df,i):
    if pd.isna(time_df(df.iloc[i]['@content@_ep@timestamp'])) == False:
        timeStamp = int(time_df(df.iloc[i]['@content@_ep@timestamp']))
        date_time = time.localtime(float(timeStamp / 1000))  # 毫秒时间戳
        date1 = time.strftime("%Y-%m-%d %H:%M:%S", date_time)
        date1 = datetime.datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
    else:
        date1=np.NaN
    return date1
def compute_overlapping(dic_frequency,dic):
    dic_overlap = {}
    if dic_frequency:
        for key in dic.keys():
            overlap_value = 0
            total_pack_list = list(dic_frequency.keys())
            index_pack_list = eval(dic[key])
            ret_list = [item for item in total_pack_list if item in index_pack_list]
            for pck in ret_list:
                overlap_value = overlap_value + dic_frequency[pck]
            dic_overlap.setdefault(key, overlap_value)

    else:
        for key in dic.keys():
            dic_overlap.setdefault(key, np.NaN)
    return dic_overlap


def Process_Running_Package(df):
    time_df=df.sort_values(by='@content@_ep@timestamp')
    time_df=time_df.drop(['Unnamed: 0'],axis=1,inplace=False)
    i=0
    dic={}
    dic_total={}
    while i <= time_df.shape[0]:
        if i ==0:
            RunPac_list=[]
            date1=time_stamp(time_df,i)
            initial_time=date1
            if pd.isna(time_df.iloc[i]['@content@_ep@attribute@running_packages'])==False:
                runpac_list=time_df.iloc[i]['@content@_ep@attribute@running_packages'].split('|')
                dic.setdefault(i,str(runpac_list))
                RunPac_list=RunPac_list+runpac_list
        elif i == time_df.shape[0]:
            None
        else:
            date2=time_stamp(time_df,i)
            interval_time=(date2-initial_time).total_seconds()
            #5个小时时间段
            if interval_time<18000:
                if pd.isna(time_df.iloc[i]['@content@_ep@attribute@running_packages']) == False:
                    runpac_list = time_df.iloc[i]['@content@_ep@attribute@running_packages'].split('|')
                    dic.setdefault(i, str(runpac_list))
                    RunPac_list = RunPac_list + runpac_list
            else:
                #计算重合度
                dic_frequency=compute_frequency(RunPac_list)
                dic_overlap=compute_overlapping(dic_frequency,dic)
                dic_total.update(dic_overlap)
                initial_time=date2
                RunPac_list=[]
                if pd.isna(time_df.iloc[i]['@content@_ep@attribute@running_packages']) == False:
                    runpac_list = time_df.iloc[i]['@content@_ep@attribute@running_packages'].split('|')
                    dic.setdefault(i, str(runpac_list))
                    RunPac_list = RunPac_list + runpac_list
        i=i+1
    return dic_total
