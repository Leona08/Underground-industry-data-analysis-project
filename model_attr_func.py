# change attribute names
# 2019-5-31 20:19
import numpy as np
import pandas as pd
import re
from attribute_pro_0614.model_rank_extra import *

# data = pd.read_csv(r'C:\Users\WFM\python\wool_data_analysis\compare_0530_1827.csv')
def data_sorted(model_df):
    data_1 = model_df.sort_values(by = 'model_value')
    # data_2.columns.tolist
    order = ['model_value','model_num', '@device_id', 'brand_num', 'brand_value',
             'rp_num', 'rp_value', 'sensor_num', 'sensor_value', 'build_num', 'build_value']
    data_1 = data_1[order]

    data_2 = data_1.reset_index()
    return data_2

# rp_list 相同model的rp值
# rp_list = [1, 1, 1, 3, 3, 4]
def rp_proce(rp_list):
    uni = set(rp_list)
    ite = []
    count = []
    rp_count = []
    for items in uni:
        ite.append(items) # 取出rp_list中unique值
        a = (rp_list.count(items))/len(rp_list)
        b = round(a, 4) #保留四位小数
        count.append(b) #rp_list中每个unique出现的次数
    for i in range(len(rp_list)):
        for j in range(len(ite)):
            if rp_list[i] == ite[j]:
                rp_count.append(count[j])
    return rp_count

# rp_count中取值最小的则取值1（质疑）， 如果只有一个值，则为0
def rp_p2(rp_count):
    L = list(set(rp_count))
    n_L = []
    if len(L) == 1:
        n_L = [0] * len(rp_count)
    else:
        min = np.min(rp_count)
        for i in range(len(rp_count)):
            if rp_count[i] == min:
                n_L.append(1)
            else:
                n_L.append(0)
    return n_L

# 将build_prop中 brand和model属性取出来
def build(data_2,i):
    # brand_list = list()
    if pd.isna(data_2.loc[i, 'build_value']):
        model = ''
        brand = ''
    else:
        build_ = data_2.loc[i, 'build_value']
        L = re.split('[|=]', build_)
        # print(L)
        if len(L) != 0:
            for index, value in enumerate(L):
                if 'ro.product.brand' in L and value == 'ro.product.brand':
                    brand_index = index
                    brand = str(L[index+1])
                    # print(brand)
                if 'ro.product.model' in L and value == 'ro.product.model':
                    model_index = index
                    model = str(L[index+1])
                    # print(model)
                elif 'ro.product.brand' not in L:
                    brand = ''
                elif 'ro.product.model' not in L:
                    model = ''
        else:
            brand = ''
            model = ''
    return brand, model

def sensor(data_2,i):
    if pd.isna(data_2.loc[i, 'sensor_value']):
        value = 0
    else:
        value = data_2.loc[i, 'sensor_value']
    return value

# sens_list = [0, 0, 0, 4, 4, 5, 5, 5, 1, 2]
def sens_fun(sens_list):
    ex_nan = [i for i in sens_list if i !=0] #exclude nan
    uni = set(ex_nan)
    ite = []
    count = []
    sens_count = []
    for items in uni:
        ite.append(items)  # unique value
        a = (ex_nan.count(items)) / len(ex_nan) # 不含有nan的list中，每个元素占比
        b = round(a, 4)  # 保留四位小数
        count.append(b)  # ex_count中每个unique出现的次数
    ite.append(0)
    count.append(np.NaN)
    for i in range(len(sens_list)):
        for j in range(len(ite)):
            if sens_list[i] == ite[j]:
                sens_count.append(count[j])
    return sens_count

def build_pro(data_2, i):
    if pd.isna(data_2.loc[i, 'build_value']):
        value = 0
    else:
        value = data_2.loc[i, 'build_value']
    return value

# sens_list = [0, 0, 0, 4, 4, 5, 5, 5, 1, 2]
def build_fun(build_list):
    ex_nan = [i for i in list(build_list) if i !=0] #exclude nan
    uni = set(ex_nan)
    ite = []
    count = []
    build_count = []
    for items in uni:
        ite.append(items)  # unique value
        a = (ex_nan.count(items)) / len(ex_nan) # 不含有nan的list中，每个元素占比
        b = round(a, 4)  # 保留四位小数
        count.append(b)  # ex_count中每个unique出现的次数
    ite.append(0)
    count.append(np.NaN)
    for i in range(len(build_list)):
        for j in range(len(ite)):
            if build_list[i] == ite[j]:
                build_count.append(count[j])
    return build_count

id_list = []
count_list = []
new_list = []
same_brand = list()
same_model = list()
prop_model = list()
prop_brand = list()
sensor_value = list()
build_value = list()

def fm_attr_2(data_2):
    i = 0
    while i <= data_2.shape[0]:
        if i == 0:
            id_list.append(data_2.loc[i, '@device_id'])
            rp_list = []
            rp_list.append(data_2.loc[i, 'rp_value'])

            sens_list = []
            sens_list.append(sensor(data_2,i))

            build_list = []
            build_list.append(build_pro(data_2,i))

            brand, model = build(data_2,i)
            prop_brand.append(brand)
            prop_model.append(model)

            if brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                    brand == data_2.loc[i, 'brand_value']:
                same_brand.append(1)
            elif brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                    brand != data_2.loc[i, 'brand_value']:
                same_brand.append(0)
            else:
                same_brand.append(np.NaN)

            if model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                    model ==data_2.loc[i, 'model_value']:
                same_model.append(1)
            elif model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                    model != data_2.loc[i, 'model_value']:
                same_model.append(0)
            else:
                same_model.append(np.NaN)

        elif i == data_2.shape[0]:

            rp_count = rp_proce(rp_list)
            n_L = rp_p2(rp_count)
            count_list.extend(rp_count)
            new_list.extend(n_L)
            sensor_value.extend(sens_fun(sens_list))
            build_value.extend(build_fun(build_list))

        else:
            if data_2.loc[i, 'model_value'] == data_2.loc[i-1, 'model_value']:
                id_list.append(data_2.loc[i, '@device_id'])
                rp_list.append(data_2.loc[i, 'rp_value'])
                # rp_count = rp_proce(rp_list)
                # n_L = rp_p2(rp_count)
                sens_list.append(sensor(data_2,i))

                build_list.append(build_pro(data_2,i))

                brand, model = build(data_2,i)
                prop_brand.append(brand)
                prop_model.append(model)

                if brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                        brand == data_2.loc[i, 'brand_value']:
                    same_brand.append(1)
                elif brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                        brand != data_2.loc[i, 'brand_value']:
                    same_brand.append(0)
                else:
                    same_brand.append(np.NaN)

                if model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                        model == data_2.loc[i, 'model_value']:
                    same_model.append(1)
                elif model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                        model != data_2.loc[i, 'model_value']:
                    same_model.append(0)
                else:
                    same_model.append(np.NaN)

            else:
                id_list.append(data_2.loc[i, '@device_id'])
                # print(rp_list)
                rp_count = rp_proce(rp_list)
                n_L = rp_p2(rp_count)
                count_list.extend(rp_count)
                new_list.extend(n_L)

                sensor_value.extend(sens_fun(sens_list))
                build_value.extend(build_fun(build_list))

                rp_list = []
                rp_list.append(data_2.loc[i, 'rp_value'])

                sens_list = []
                sens_list.append(sensor(data_2,i))

                build_list = []
                build_list.append(build_pro(data_2,i))

                brand, model = build(data_2,i)
                prop_brand.append(brand)
                prop_model.append(model)

                if brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                        brand == data_2.loc[i, 'brand_value']:
                    same_brand.append(1)
                elif brand != '' and pd.isna(data_2.loc[i, 'brand_value']) == False and \
                        brand != data_2.loc[i, 'brand_value']:
                    same_brand.append(0)
                else:
                    same_brand.append(np.NaN)

                if model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                        model == data_2.loc[i, 'model_value']:
                    same_model.append(1)
                elif model != '' and pd.isna(data_2.loc[i, 'model_value']) == False and \
                        model != data_2.loc[i, 'model_value']:
                    same_model.append(0)
                else:
                    same_model.append(np.NaN)

        i = i + 1
    return id_list, count_list, new_list, same_brand, same_model, prop_brand,prop_model, sensor_value, build_value

# print(len(id_list))
# print(len(count_list))
# print(len(new_list))
# df = pd.DataFrame([id_list, count_list, new_list, same_brand, same_model, prop_brand, prop_model, sensor_value, build_value]).transpose()
# df.columns = (['@device_id', 'rp_counts', 'rp_match', 'same_brand', 'same_model', 'prop_brand', 'prop_model', 'sensor_same', 'build_same'])
# # print(df.head(5))
# data_df = pd.merge(data_2, df, on='@device_id')
# print(data_df.head(5))
# data_df.to_csv('rp_merge_update_0531_2108.csv')

# attr_list = [dic_brand, set_brand, dic_model, set_model, dic_rp, set_rp]
# final_df = pd.DataFrame(attr_list).transpose().reset_index()
# final_df = final_df.rename(columns={'index': '@device_id'})
# print(final_df)
# final_df.to_csv('brand_rp_0515.csv')