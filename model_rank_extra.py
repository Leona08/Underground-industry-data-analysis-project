import numpy as np
import pandas as pd
# from attribute_pro_0614.read_json_xh import *
# sensor
def sensor(data_df_2,i):
    if pd.isna(data_df_2.loc[i, '@content@_ep@attribute@sensor']):
        value = 0
    else:
        value = data_df_2.loc[i, '@content@_ep@attribute@sensor']
    return value

def sen_value(sen_list):
    if len(list(set(sen_list))) == 1 and sen_list[0] == 0:
        val = np.NaN
        num = 0
    elif len(list(set(sen_list))) == 1 and sen_list[0] != 0:
        val = list(set(sen_list))[0]
        num = 1
    elif len(list(set(sen_list))) != 1:
        value = [i for i in list(set(sen_list)) if i !=0]
        val = value = [i for i in list(set(sen_list)) if i !=0][0]
        num = len(value)
    return val, num

def build(data_df_2, i):
    if pd.isna(data_df_2.loc[i, '@content@_ep@attribute@build_prop']):
        value = 0
    elif data_df_2.loc[i, '@content@_ep@attribute@build_prop'] == '':
        value = 0
    else:
        value = data_df_2.loc[i, '@content@_ep@attribute@build_prop']
    return value

def buid_value(build_list):
    li = list(set(build_list))
    if len(li) == 1 and li[0] == 0:
        v = np.NaN
        c = 0
    elif len(li) == 1 and li[0] != 0:
        v = li[0]
        c = 1
    elif len(li) != 1:
        val = [i for i in li if i != 0]
        if len(val) == 1:
            v = val[0]
            c = 1
        else:
            v = val
            c = len(val)
    return v, c

def model(data_df_2, i):
    if pd.isna(data_df_2.loc[i, '@content@_cp@model']):
        value = 0
    elif data_df_2.loc[i, '@content@_cp@model'] == '':
        value = 0
    else:
        value = data_df_2.loc[i, '@content@_cp@model']
    return value

def brand(data_df_2, i):
    if pd.isna(data_df_2.loc[i, '@content@_cp@brand']):
        value = 0
    elif data_df_2.loc[i, '@content@_cp@brand'] == '':
        value = 0
    else:
        value = data_df_2.loc[i, '@content@_cp@brand']
    return value

def model_value(model_list):
    s = list(set(model_list))
    if len(s) == 1 and s[0] == 0:
        v_model = np.NaN
        c_model = 0
    elif len(s) == 1 and s[0] != 0:
        v_model = s[0]
        c_model = 1
    elif len(s) != 1:
        val = [i for i in s if i != 0]
        if len(val) == 1:
            v_mdoel = val[0]
            c_model = 1
        else:
            v_model = val
            c_model = len(val)
    return v_model, c_model

def brand_value(brand_list):
    s = list(set(brand_list))
    if len(s) == 1 and s[0] == 0:
        v_brand = np.NaN
        c_brand = 0
    elif len(s) == 1 and s[0] != 0:
        v_brand = s[0]
        c_brand = 1
    elif len(s) != 1:
        val = [i for i in s if i != 0]
        if len(val) == 1:
            v_brand = val[0]
            c_brand = 1
        else:
            v_brand = val
            c_brand = len(val)
    return v_brand, c_brand

def rp(data_df_2, i):
    if pd.isna(data_df_2.loc[i, '@content@_cp@rp']):
        value = 0
    elif data_df_2.loc[i, '@content@_cp@rp'] == '':
        value = 0
    else:
        value = data_df_2.loc[i, '@content@_cp@rp']
    return value

def rp_value(rp_list):
    s = list(set(rp_list))
    if len(s) == 1 and s[0] == 0:
        v_rp = np.NaN
        c_rp = 0
    elif len(s) == 1 and s[0] != 0:
        v_rp = s[0]
        c_rp = 1
    elif len(s) != 1:
        val = [i for i in s if i != 0]
        if len(val) == 1:
            v_rp = val[0]
            c_rp = 1
        else:
            v_rp = val
            c_rp = len(val)
    return v_rp, c_rp

dic_sen = dict()
dic_num = dict()

dic_buiv =  dict()
dic_buic = dict()

mode_num = dict()
mode_value = dict()

bran_num = dict()
bran_value = dict()

rp_num = dict()
rp_val = dict()
def model_key(data_df_2):
    i = 0
    while i <= data_df_2.shape[0]:
        if i == 0:
            brand_list = []
            brand_list.append(brand(data_df_2,i))

            model_list = []
            model_list.append(model(data_df_2,i))

            rp_list = []
            rp_list.append(rp(data_df_2,i))

            sen_list = []
            sen_list.append(sensor(data_df_2,i))

            build_list = []
            build_list.append(build(data_df_2,i))

        elif i == data_df_2.shape[0]:
            v_brand, c_brand = brand_value(brand_list)
            bran_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_brand)
            bran_value.setdefault(data_df_2.loc[i - 1, '@device_id'], v_brand)

            v_model, c_model = model_value(model_list)
            mode_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_model)
            mode_value.setdefault(data_df_2.loc[i - 1, '@device_id'], v_model)

            v_rp, c_rp = rp_value(rp_list)
            rp_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_rp)
            rp_val.setdefault(data_df_2.loc[i - 1, '@device_id'], v_rp)

            val, num = sen_value(sen_list)
            dic_sen.setdefault(data_df_2.loc[i - 1, '@device_id'], val)
            dic_num.setdefault(data_df_2.loc[i - 1, '@device_id'], num)

            v, c = buid_value(build_list)
            dic_buiv.setdefault(data_df_2.loc[i - 1, '@device_id'], v)
            dic_buic.setdefault(data_df_2.loc[i - 1, '@device_id'], c)
        else:
            if data_df_2.loc[i, '@device_id'] == data_df_2.loc[i - 1, '@device_id']:
                brand_list.append(brand(data_df_2,i))

                model_list.append(model(data_df_2,i))

                rp_list.append(rp(data_df_2,i))

                sen_list.append(sensor(data_df_2,i))

                build_list.append(build(data_df_2,i))

            else:
                v_brand, c_brand = brand_value(brand_list)
                bran_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_brand)
                bran_value.setdefault(data_df_2.loc[i - 1, '@device_id'], v_brand)

                v_model, c_model = model_value(model_list)
                mode_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_model)
                mode_value.setdefault(data_df_2.loc[i - 1, '@device_id'], v_model)

                v_rp, c_rp = rp_value(rp_list)
                rp_num.setdefault(data_df_2.loc[i - 1, '@device_id'], c_rp)
                rp_val.setdefault(data_df_2.loc[i - 1, '@device_id'], v_rp)

                val, num = sen_value(sen_list)
                dic_sen.setdefault(data_df_2.loc[i - 1, '@device_id'], val)
                dic_num.setdefault(data_df_2.loc[i - 1, '@device_id'], num)
                v, c = buid_value(build_list)
                dic_buiv.setdefault(data_df_2.loc[i - 1, '@device_id'], v)
                dic_buic.setdefault(data_df_2.loc[i - 1, '@device_id'], c)

                brand_list = []
                brand_list.append(brand(data_df_2,i))

                model_list = []
                model_list.append(model(data_df_2,i))

                rp_list = []
                rp_list.append(rp(data_df_2,i))

                sen_list = []
                build_list = []

                sen_list.append(sensor(data_df_2,i))
                build_list.append(build(data_df_2,i))

        i = i+1
    return bran_num, bran_value, mode_num, mode_value, rp_num, rp_val, dic_num, dic_sen, dic_buic, dic_buiv

# print(final_df.head(5))
# final_df.to_csv('compare_0530_1827.csv')