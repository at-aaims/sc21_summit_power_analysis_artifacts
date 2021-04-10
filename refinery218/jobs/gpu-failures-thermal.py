#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 4
#SBATCH -J gpu-failures-thermal
#SBATCH -t 48:00:00
#SBATCH -A stf218
#SBATCH -o ../logs/gpu-failures-thermal-%J.out
import os
import sys
import time
import glob
from itertools import product
from functools import partial
from loguru import logger
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
pd.options.mode.chained_assignment = None

COMPUTE_SCRIPT = 'gpu-failures-thermal.py'
WATCHDOG_INTERVAL_SEC = 3600*48

FAILURES = '/gpfs/alpine/stf218/proj-shared/data/lake.dev/summit_gpu_failures/gpu_failures_spatial.csv'
THERMAL_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/10s_agg/'
THERMAL_PERJOB_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_thermal_perjob_comptype_cep/'
RES = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_gpu_failures/gpu_failures_zscores_ref218.csv'
JOBS = '/gpfs/alpine/stf218/proj-shared/data/lake.dev/summit_jobs/jobs.csv'
PERNODE_JOBS = '/gpfs/alpine/stf218/proj-shared/data/lake.dev/summit_perhost_jobs/pernode_jobs.csv'

JOB_ID = 'allocation_id'
NODE_OLD = 'node_name'
START = 'begin_time'
END = 'end_time'

NODE = 'hostname'
PROJ = 'account'
EXACT_TIME = 'exact'
TIME = 'timestamp'
XID = 'xid'
TEMP = 'temp'
Z = 'z_score'
MEAN = 'mean'
STD = 'std'
PROC = 'processed'
OG_IDX = 'orig_idx'
GPU_TEMPS = [f'gpu{gpu}_core_temp.mean' for gpu in range(6)]
GPU_MEAN = 'gpu_core.mean'
GPU_STD = 'gpu_core.std'
GPU_COUNT = 'gpu_core.count'
GPU_SIZE = 'gpu_core.size'
GPU = 'GPU'

# XIDS = [63, 74, 79, 48] # page retirement event, NVLINK error, off-the-bus, double-bit
XIDS = [64, 48, 45, 61, 44, 63, 74, 79, 62, 38, 32, 12, 69]


def save(df, i=0):
    if i % 100 == 0:
        df.to_csv(RES)
    
@logger.catch
def compute(client):
    failures = pd.read_csv(FAILURES, usecols=[TIME, NODE, XID, GPU])

    # Remove data for login and batch nodes.
    failures = failures[~failures[NODE].str.startswith('login') & ~failures[NODE].str.startswith('batch')]
    failures[failures[NODE].str.startswith('login') | failures[NODE].str.startswith('batch')][NODE].unique()

    # Round time to the nearest 10s.
    failures[EXACT_TIME] = pd.to_datetime(failures[TIME], utc=True)
    failures[TIME] = failures[EXACT_TIME].dt.round('10s')

    if os.path.exists(RES):
        df = pd.read_csv(RES, parse_dates=[TIME, EXACT_TIME], index_col=OG_IDX)
    else:
        df = failures.sort_values(EXACT_TIME)
        df.index.name = OG_IDX
        df[TEMP] = np.nan
        df[Z] = np.nan
        df[MEAN] = np.nan
        df[STD] = np.nan
        df[PROC] = 0
        
    pernode_jobs = pd.read_csv(PERNODE_JOBS, usecols=[JOB_ID, NODE_OLD]).rename(columns={NODE_OLD: NODE}) 
    pernode_jobs = pernode_jobs.set_index(NODE).sort_index()
    logger.info('sorted pernode jobs')

    # Find the job associated with each failure.
    jobs = pd.read_csv(JOBS, usecols=[JOB_ID, START, END], parse_dates=[START, END])
    jobs = jobs[jobs[START] <= jobs[END]]
    jobs = jobs.set_index(pd.IntervalIndex.from_arrays(jobs[START], jobs[END])).drop(columns=[START, END]).sort_index()
    logger.info('sorted jobs')

    
    # Find z-score (across job nodes) of GPU core temperature at the moment of failure.
    prev_date_key = prev_date_jobs_key = ''
    prev_date_bad = prev_date_jobs_bad = False
    for i, (idx, vals) in enumerate(df[[PROC, XID, GPU, EXACT_TIME, TIME, NODE, TEMP, MEAN, STD, Z]].iterrows()):

        is_processed, xid, gpu, exact, time, node = vals[:6]
        
        if is_processed:
            logger.info(f'{i + 1}/{len(df)} ({idx}) already processed')
            continue
        else:
            df.loc[idx, PROC] = 1

        # Get GPU core temperature at the time of failure.
        col = f'gpu{gpu}_core_temp.mean'
        date_key = time.strftime('%Y%m%d')
        if date_key != prev_date_key:
            prev_date_key = date_key
            date_thermal_path = THERMAL_DIR + f'{date_key}.parquet'
            if os.path.exists(date_thermal_path):
                date_thermal = dd.read_parquet(
                    date_thermal_path, columns=[NODE] + GPU_TEMPS, split_row_groups=True, engine='pyarrow')
                prev_date_bad = False
            else:
                logger.info(f'{i + 1}/{len(df)} ({idx}) no openbmc datafile {date_key}.parquet')
                prev_date_bad = True
                continue
        elif prev_date_bad:
            logger.info(f'{i + 1}/{len(df)} ({idx}) no openbmc datafile {date_key}.parquet')
            continue
        try:
            if date_thermal.npartitions == 1440:
                time_thermal = date_thermal.get_partition(time.hour * 60 + time.minute).loc[time].compute()
            else:
                time_thermal = date_thermal.loc[time].compute()
        except KeyError:
            logger.info(f'{i + 1}/{len(df)} ({idx}) time_thermal does not have index {time}')
            save(df, i)
            continue
        failure_thermal = time_thermal[time_thermal[NODE] == node]
        if len(failure_thermal) != 1:
            logger.info(f'{i + 1}/{len(df)} ({idx}) failure_thermal has {len(failure_thermal)} rows')
            save(df, i)
            continue
        temp = failure_thermal.iloc[0][GPU_TEMPS[gpu]]
        
        if pd.isna(temp):
            continue
        else:
            df.loc[idx, TEMP] = temp

        # Get id of the job associated with the failure.
        try:
            time_jobs = jobs.loc[exact]
        except KeyError:
            logger.info(f'{i + 1}/{len(df)} ({idx}) no job matches failure time {exact}')
            save(df, i)
            continue
            
        failure_job = time_jobs.merge(pernode_jobs.loc[node])
        if len(failure_job) != 1:
            logger.info(f'{i + 1}/{len(df)} ({idx}) failure_job has {len(failure_job)} rows')
            save(df, i)
            continue
        job_id = int(failure_job.iloc[0][JOB_ID])

        # Find temperature distribution across the job at the timestep associated with the failure.
        if date_key != prev_date_jobs_key:
            prev_date_jobs_key = date_key
            date_thermal_jobs_path = THERMAL_PERJOB_DIR + f'{date_key}.csv'
            if os.path.exists(date_thermal_jobs_path):
                date_thermal_jobs = pd.read_csv(
                    date_thermal_jobs_path, usecols=[TIME, JOB_ID, GPU_MEAN, GPU_STD, GPU_COUNT, GPU_SIZE], parse_dates=[TIME])
                prev_date_jobs_bad = False
            else:
                logger.info(f'{i + 1}/{len(df)} ({idx}) no pernode-jobs datafile {date_key}.csv')
                prev_date_jobs_bad = True
                continue
        elif prev_date_jobs_bad:
            logger.info(f'{i + 1}/{len(df)} ({idx}) no pernode-jobs datafile {date_key}.csv')
            continue
            
        failure_thermal_job = date_thermal_jobs[(date_thermal_jobs[JOB_ID] == job_id) & (date_thermal_jobs[TIME] == time)]
        if len(failure_thermal_job) != 1:
            logger.info(f'{i + 1}/{len(df)} ({idx}) failure_thermal_job has {len(failure_thermal_job)} rows')
            save(df, i)
            continue
        mu, std = failure_thermal_job[[GPU_MEAN, GPU_STD]].values[0]
        df.loc[idx, MEAN], df.loc[idx, STD] = mu, std
        na_ratio = 1 - failure_thermal_job[GPU_COUNT].values[0] / failure_thermal_job[GPU_SIZE].values[0]
        if na_ratio > 0.2:
            logger.info(f'{i + 1}/{len(df)} >20% missing thermal data.')
            continue
        
        df.loc[idx, Z] = (temp - mu) / std
        logger.info(f'{i + 1}/{len(df)} finished, z-score {df.loc[idx, Z]}.')

        save(df, i)
    
    save(df)
    
    
if __name__ == "__main__":
    andes_dask_batch(compute, script=COMPUTE_SCRIPT, watchdog=WATCHDOG_INTERVAL_SEC)
