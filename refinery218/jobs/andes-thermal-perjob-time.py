#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 32
#SBATCH -J andes-thermal-perjob-time
#SBATCH -t 8:00:00
#SBATCH -A stf218
#SBATCH -o ../logs/andes-thermal-perjob-time-%J.out
import os
import sys
import time
import glob
from itertools import product
from datetime import datetime, timedelta
from loguru import logger
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
from refinery218.filestream import DayCursor


COMPUTE_SCRIPT = "andes-thermal-perjob-time.py"

DATA_DEV_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake.dev/'
JOBS = os.path.join(DATA_DEV_DIR, 'summit_jobs/jobs.csv')
PERNODE_JOBS = os.path.join(DATA_DEV_DIR, 'summit_perhost_jobs/pernode_jobs.csv')
CEP = os.path.join(DATA_DEV_DIR, 'summit_cooling_plant/interp_cep_data.parquet')
PERNODE_JOBS_TS = os.path.join(DATA_DEV_DIR, 'summit_perhost_jobs_timeseries/data')
DATA_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/'
SRC_DIR = os.path.join(DATA_DIR, 'summit_power_temp_openbmc/10s_agg')
DST_DIR = os.path.join(DATA_DIR, 'summit_thermal_perjob_time_cep')

AGG_FREQ = '10s'
JOB_ID = 'allocation_id'
BEGIN = 'begin_time'
END = 'end_time'
TIME = 'timestamp'
NODE_OLD = 'node_name'
NODE = 'hostname'
TEMP_SENSORS = ['core', 'mem']
CORE_TEMPS, MEM_TEMPS = [[f'gpu{gpu}_{sensor}_temp.mean' for gpu in range(6)] for sensor in TEMP_SENSORS]
ALL_TEMPS = CORE_TEMPS + MEM_TEMPS
BANDS = [-np.inf, 80, 83, 87, 90, np.inf]
N_BANDS = len(BANDS) - 1
N_CORES_IN_BANDS, N_MEMS_IN_BANDS = [[f'n_{sensor}s_band{band}' for band in range(N_BANDS)] for sensor in TEMP_SENSORS]
HOT_GPUS = 'hot_gpus'
ANY_NAN = 'any_nan'
EMPTY_MAPPED_PARTITION = pd.DataFrame(columns=[NODE, ANY_NAN, HOT_GPUS] + N_CORES_IN_BANDS + N_MEMS_IN_BANDS)
# If not None, cursor stops at the offset from the beginning
# Useful for initial development & debugging
CURSOR_STOP_OFFSET = None

# andes_dask_batch has a watchdog that restarts the dask cluster
# when no heartbeat (calling watchdog_heartbeat()) is seen within
# WATCHDOG_INTERVAL_SEC
WATCHDOG_INTERVAL_SEC = 450

# Pre-commit tasks to overlap scheduling delay & computation
OVERLAP_DAY_COUNT = 1


def skip_if(cursor, offset, date_key):
    """Return True if we want to skip a particular date"""
    # Useful to skip work that already has been done
    return False


def compute_partition(df):
    if not isinstance(df, pd.DataFrame) and not isinstance(df, dd.DataFrame):
        return EMPTY_MAPPED_PARTITION

    pernode_jobs_partition_path = os.path.join(PERNODE_JOBS_TS, f'part={df.index.floor("min").max()}')
    if os.path.exists(pernode_jobs_partition_path):
        pernode_jobs_partition = pd.read_parquet(pernode_jobs_partition_path, columns=[TIME, NODE, JOB_ID])
    else:
        return EMPTY_MAPPED_PARTITION
    
    if pernode_jobs_partition.empty:
        return EMPTY_MAPPED_PARTITION
    
    # Join with per-node job time-series data.
    df = pernode_jobs_partition.merge(df.reset_index(), on=[TIME, NODE])

    # Detect NaNs in each row.
    df[ANY_NAN] = df[ALL_TEMPS].isna().any(axis=1)
    # Replace temperature with bands.
    df[ALL_TEMPS] = df[ALL_TEMPS].apply(pd.cut, bins=BANDS, right=False, labels=False)
    # Count bands for each row.
    for n_sensors_in_bands, temps in zip([N_CORES_IN_BANDS, N_MEMS_IN_BANDS], [CORE_TEMPS, MEM_TEMPS]):
        for band, n_sensors_in_band in enumerate(n_sensors_in_bands):
            df[n_sensors_in_band] = (df[temps] == band).sum(axis=1)
    # Encode hot GPUs for each node.
    are_hot_gpus = df[ALL_TEMPS] > 1
    df[HOT_GPUS] = df[NODE] + ':' + are_hot_gpus.fillna('_').astype(int).astype(str).agg(''.join, axis=1)
    df[HOT_GPUS] = df[HOT_GPUS].mask(~are_hot_gpus.any(axis=1))

    agg = df.groupby([TIME, JOB_ID]).agg({NODE: 'size', ANY_NAN: 'sum', HOT_GPUS: lambda x: list(x.dropna()),
                                          **{n: 'sum' for n in N_CORES_IN_BANDS + N_MEMS_IN_BANDS}})

    return agg


def compute_day(offset, date_key, ddf):
    """Computation of a day worth of data"""
    
    res = ddf.map_partitions(compute_partition).compute().reset_index(level=JOB_ID)

    return offset, date_key, res


def handle_result(context, res):
    """Handle results (sink)"""

    _, date_key, df = res

    try:
        df = df.join(context['cep'])
    except TypeError:
        context['cep'].index = context['cep'].index.tz_localize(df.index.tz)
        df = df.join(context['cep'])
        
    df.to_csv(os.path.join(DST_DIR, f'{date_key}.csv'))

    logger.info(f'processed {date_key}')
    
    

@logger.catch
def compute(client):
    """Main computation loop

    Computation is done using futures instead of delays to reduce the impact of
    the scheduler delays
    """
    cursor = DayCursor(
        client, basedir=SRC_DIR, cursor_stop_offset=CURSOR_STOP_OFFSET,
        # Data we read (if column is not None and list, it will read that column
        index=TIME,
        columns=[NODE] + ALL_TEMPS,
        # Days to attach prior or after the current date
        prev_days=0, next_days=1,
        # Skip condition
        skip_fn=skip_if,
        # Whether we load the per day into memory when iterating
        persist=True,
    )

    # Read job data.
    jobs = pd.read_csv(JOBS, usecols=[JOB_ID, BEGIN, END], parse_dates=[BEGIN, END]).set_index(JOB_ID)
    jobs[BEGIN] = jobs[BEGIN].dt.round(AGG_FREQ)
    jobs[END] = jobs[END].dt.round(AGG_FREQ)
    pernode_jobs = pd.read_csv(PERNODE_JOBS, usecols=[JOB_ID, NODE_OLD]).set_index(JOB_ID)
    pernode_jobs = pernode_jobs.rename(columns={NODE_OLD: NODE})
    
    # Read interpolated CEP data.
    context = {'cep': pd.read_parquet(CEP, engine='pyarrow')}
        
    logger.info("Beginning iteration")
    futures = as_completed()
    for offset, date_key, ddf in cursor.iter():
        # Create per-node level job data for the jobs that started on this day.
        jobs_day = jobs[jobs[BEGIN].dt.strftime('%Y%m%d') == date_key]
        pernode_jobs_day = pernode_jobs.merge(jobs_day, left_index=True, right_index=True).reset_index()

        futures.add(client.submit(compute_day, offset, date_key, ddf))

        # Also, skip a beat so that we have at least two or more futures submitted
        # Then, dequeue and block on exactly one future at a time
        if offset < OVERLAP_DAY_COUNT:
            continue
        for future in futures:
            handle_result(context, future.result())
            watchdog_heartbeat()
            break

    # We wait for the rest
    for future in futures:
        handle_result(context, future.result())
        watchdog_heartbeat()
    logger.info("Iteration finished")
    return 0

#
# Submission block that takes care of the entrypoints
#

if __name__ == "__main__":
    andes_dask_batch(compute, script=COMPUTE_SCRIPT, watchdog=WATCHDOG_INTERVAL_SEC)
