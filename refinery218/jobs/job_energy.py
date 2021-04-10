#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import dask.bag as db
import dask.dataframe as dd
import numpy as np
import pyarrow
from datetime import datetime, timedelta


def date_range(start_date, end_date):
    return [(start_date + timedelta(n))for n in range(int ((end_date - start_date).days) + 1)]

str_st = "2019-12-27 00:00:00"
str_en = "2021-01-31 00:00:00"
start_date = pd.Timestamp(str_st, tz = 'UTC')
end_date = pd.Timestamp(str_en, tz = 'UTC')
date_list = date_range(start_date, end_date)


df_l= []
for date_td in date_list[0:2]:
    dt_str = datetime.strftime(date_td,'%Y%m%d')
    dt_str_lst = list([dt_str])
    for cur_dt in dt_str_lst:
        fn = f"/gpfs/alpine/proj-shared/stf218/data/lake.dev/summit_jobs_full_perdate_parquet/{cur_dt}.parquet/part.0.parquet"
        cols = ['allocation_id','primary_job_id','account','energy','gpu_energy','num_nodes','num_gpus','num_processors','begin_time','end_time']
        df = pd.read_parquet(fn,columns=cols)
        df['job_domain'] = df['account'].str[0:3]
        
        grp_df = df.groupby(['allocation_id','primary_job_id'],as_index=False).agg({'energy':'sum',
                                                            'gpu_energy':'sum',
                                                            'num_nodes':'first','num_gpus':'first',
                                                            'begin_time':'first', 'end_time':'first',
                                                            'job_domain':'first','account':'first'})
        df_l.append(grp_df)
        complete_df = pd.concat(df_l,axis=0)


complete_df.to_csv("/gpfs/alpine/proj-shared/stf218/data/lake.dev/summit_perhost_jobs_full/jobwise_combine.csv",index=False)

