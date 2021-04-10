#!/usr/bin/env python
# coding: utf-8

#imports
import pandas as pd
from datetime import datetime, timedelta 
import numpy as np


str_st = "2019-12-28"
str_en = "2021-01-30"
start_date = datetime.strptime(str_st,"%Y-%m-%d")
end_date = datetime.strptime(str_en,"%Y-%m-%d")
def date_range_str(start_date, end_date):
    return [(start_date + timedelta(n)).strftime('%Y%m%d') for n in range(int ((end_date - start_date).days) + 1)]

date_list = date_range_str(start_date, end_date)


column_names = ['timestamp','allocation_id','count_hostname','count_node_name','job_sch_node_num','count_inp','size_inp',
                'sum_inp','count_top','size_top','sum_top','mean_inp','std_inp','max_inp','min_inp','mean_top','std_top',
                'max_top','min_top','q25_inp','q50_inp','q75_inp','q25_top','q50_top','q75_top']

for i,date_ins in enumerate(date_list):
    if date_ins == 20210130:
        #for last day
        df_today = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i]}.csv')
        try:
            df_next = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i+1]}.csv')            
        except:
            print('Missing next',date_ins)
            df_next = pd.DataFrame(columns=column_names)
        try:
            df_prev = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i-1]}.csv')            
            print(date_ins)
        except:
            print('Missing prev',date_ins)
            df_prev = pd.DataFrame(columns=column_names)            

    else: #for other days
        #try and except to skip for missing days
        try:
            df_today = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i]}.csv')
            df_next = pd.DataFrame(columns=column_names)
            print(date_ins)
        except:
            print('Missing today',date_ins)
            continue

        try:
            df_next = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i+1]}.csv')            
        except:
            print('Missing next',date_ins)
            df_next = pd.DataFrame(columns=column_names)
        try:
            df_prev = pd.read_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s/{date_list[i-1]}.csv')            
            print(date_ins)
        except:
            print('Missing prev',date_ins)
            df_prev = pd.DataFrame(columns=column_names)            
        
        df_today = df_today[~df_today['allocation_id'].isin(df_prev['allocation_id'])]
        df_common = df_next[df_next['allocation_id'].isin(df_today['allocation_id'])]
        df_concat = pd.concat([df_today,df_common],axis=0)
        df_concat['timestamp'] = pd.to_datetime(df_today['timestamp'])
        
    df_concat = df_concat.loc[:,['timestamp','allocation_id','sum_top','sum_inp','count_inp','size_inp','max_inp','min_inp']]
    df_concat = df_concat.groupby(['allocation_id']).agg(max_sum_top=('sum_top','max'),max_sum_inp=('sum_inp','max'),
                                                         mean_sum_inp=('sum_inp','mean'),max_max_inp=('max_inp','max'),
                                                         min_min_inp=('min_inp','min'),
                                              max_hosts_count=('count_inp','max'),max_hosts_size=('size_inp','max'),
                                                        begin_time=('timestamp','min'),end_time=('timestamp','max')).reset_index()
    df_concat.to_csv(f'/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_jobwise_10s/{date_ins}.csv')
    
