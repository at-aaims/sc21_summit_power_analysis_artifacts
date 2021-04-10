#!/usr/bin/env python
# coding: utf-8

#imports
import pandas as pd
from datetime import datetime, timedelta 
import numpy as np


# date range
str_st = "2019-12-28"
str_en = "2021-01-30"
start_date = datetime.strptime(str_st,"%Y-%m-%d")
end_date = datetime.strptime(str_en,"%Y-%m-%d")
def date_range_str(start_date, end_date):
    return [(start_date + timedelta(n)).strftime('%Y%m%d') for n in range(int ((end_date - start_date).days) + 1)]

date_list = date_range_str(start_date, end_date)

column_names = ['timestamp', 'allocation_id', 'count_hostname', 'count_node_name','job_sch_node_num', 
                'mean_cpu_power', 'std_cpu_power', 'min_cpu_power', 'max_cpu_power', 'q25_cpu_power', 
                'q50_cpu_power', 'q75_cpu_power', 'count_cpu_power', 'size_cpu_power', 'cpu_nans', 
                'mean_gpu_power', 'std_gpu_power', 'min_gpu_power', 'max_gpu_power', 'q25_gpu_power',
                'q50_gpu_power', 'q75_gpu_power', 'count_gpu_power', 'size_gpu_power', 'gpu_nans']

SOURCE_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s_components'
DEST_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_jobwise_10s_components'
for i,date_ins in enumerate(date_list):
    if date_ins == 20210130:
        #for last day
        df_today = pd.read_csv(f'{SOURCE_DIR}/{date_list[i]}.csv')
        df_prev = pd.read_csv(f'{SOURCE_DIR}/{date_list[i-1]}.csv')
        df_next = pd.DataFrame(columns=column_names)
    else: #for other days
        #try and except to skip for missing days
        try:
            df_today = pd.read_csv(f'{SOURCE_DIR}/{date_list[i]}.csv')
            df_next = pd.DataFrame(columns=column_names)
            print(date_ins)
        except:
            print('Missing today',date_ins)
            continue
        try:
            df_next = pd.read_csv(f'{SOURCE_DIR}/{date_list[i+1]}.csv')            
        except:
            print('Missing next',date_ins)
            df_next = pd.DataFrame(columns=column_names)
        try:
            df_prev = pd.read_csv(f'{SOURCE_DIR}/{date_list[i-1]}.csv')            
            print(date_ins)
        except:
            print('Missing prev',date_ins)
            df_prev = pd.DataFrame(columns=column_names)            
        # concat df to get common jobs today and next days        
        df_today = df_today[~df_today['allocation_id'].isin(df_prev['allocation_id'])]
        df_common = df_next[df_next['allocation_id'].isin(df_today['allocation_id'])]
        df_concat = pd.concat([df_today,df_common],axis=0)
        df_concat['timestamp'] = pd.to_datetime(df_today['timestamp'])
        
    df_concat = df_concat.groupby(['allocation_id']).agg(mean_mean_cpu_pwr=('mean_cpu_power','mean'),max_mean_cpu_pwr=('mean_cpu_power','max'),
                                            min_mean_cpu_pwr=('mean_cpu_power','min'),max_max_cpu_pwr=('max_cpu_power','max'),
                                            count_cpu_power=('count_cpu_power','first'),mean_mean_gpu_pwr=('mean_gpu_power','mean'), 
                                            max_mean_gpu_pwr=('mean_gpu_power','max'),min_mean_gpu_pwr=('mean_gpu_power','min'),
                                            max_max_gpu_pwr=('max_gpu_power','max'),count_gpu_power=('count_gpu_power','first'),
                                                         begin_time=('timestamp','min'),end_time=('timestamp','max')).reset_index()
    df_concat.to_csv(f'{DEST_DIR}/{date_ins}.csv')
    


