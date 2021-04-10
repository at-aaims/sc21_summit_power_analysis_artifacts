#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 16
#SBATCH -J power_ts_job_ignorant
#SBATCH -t 4:00:00
#SBATCH -A gen150
#SBATCH -o ../logs/power_ts_job_ignorant_component-%J.out
import os
import sys
import time
import glob
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.distributed import as_completed
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
from refinery218.filestream import DayCursor


COMPUTE_SCRIPT = "power_ts_job_ignorant_component.py"

start_time = datetime.now()

# Source dataset base location
SOURCE_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/10s_agg'
DEST_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_10s_components'
# If not None, cursor stops at the offset from the beginning
# Useful for initial development & debugging
CURSOR_STOP_OFFSET = None

# andes_dask_batch has a watchdog that restarts the dask cluster
# when no heartbeat (calling watchdog_heartbeat()) is seen within
# WATCHDOG_INTERVAL_SEC
WATCHDOG_INTERVAL_SEC = 450

# Pre-commit tasks to overlap scheduling delay & computation
OVERLAP_DAY_COUNT = 1

#column names to be computed on each day of dataframe
COLUMN_NAMES = ['timestamp','mean_cpu_power','std_cpu_power','min_cpu_power','max_cpu_power','q25_cpu_power',
        'q50_cpu_power','q75_cpu_power','count_cpu_power','size_cpu_power','cpu_nans', 'mean_gpu_power','std_gpu_power',
        'min_gpu_power','max_gpu_power','q25_gpu_power','q50_gpu_power','q75_gpu_power','count_gpu_power',
        'size_gpu_power','gpu_nans'] 


def skip_if(cursor, offset, date_key):
    """Return True if we want to skip a particular date"""
    # Useful to skip work that already has been done
    return int(date_key) < 20191227 

#functions to calculate quantile
def q50(x):
    return x.quantile(0.5)
def q25(x):
    return x.quantile(0.25)
def q75(x):
    return x.quantile(0.75)

def compute_partition(df):
    """Perform compute on a partition

    Partitions are perfectly aligned along 1 minute and the timestamp index is
    sorted.

    Below is a reasonably costly operation that aggregates data from
    each timestep.  That dataframe would aggregate what the timestep had (i.e.,
    measurements from all the hosts)  Also, this is done with *all* of the
    columns.
    """
    
    logger.info(f'Dataframe  ignorant={type(df)}')
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns = COLUMN_NAMES)
    #reset index to make timestamp a column if it is index
    df.reset_index(inplace=True)

    df = df.loc[:,['timestamp','p0_power.mean', 'p1_power.mean', 'p0_gpu0_power.mean', 'p0_gpu1_power.mean', 
        'p0_gpu2_power.mean', 'p1_gpu0_power.mean', 'p1_gpu1_power.mean', 'p1_gpu2_power.mean']]

    df['cpu_power'] = df.loc[:,['p0_power.mean','p1_power.mean']].sum(axis=1)
    df.loc[df[['p0_power.mean','p1_power.mean']].isnull().all(1),'cpu_power']=np.nan
    df['cpu_nan'] = df.loc[:,['p0_power.mean','p1_power.mean']].isnull().sum(axis=1)

    df['gpu_power'] = df.loc[:,['p0_gpu0_power.mean', 'p0_gpu1_power.mean', 'p0_gpu2_power.mean', 
        'p1_gpu0_power.mean', 'p1_gpu1_power.mean', 'p1_gpu2_power.mean']].sum(axis=1)
    df.loc[df[['p0_gpu0_power.mean', 'p0_gpu1_power.mean', 'p0_gpu2_power.mean', 
        'p1_gpu0_power.mean', 'p1_gpu1_power.mean', 'p1_gpu2_power.mean']].isnull().all(1),'gpu_power']=np.nan
    df['gpu_nan'] = df.loc[:,['p0_gpu0_power.mean', 'p0_gpu1_power.mean', 'p0_gpu2_power.mean', 
        'p1_gpu0_power.mean', 'p1_gpu1_power.mean', 'p1_gpu2_power.mean']].isnull().sum(axis=1)
    
    res = df.groupby('timestamp').agg(
            mean_cpu_power=('cpu_power', 'mean'), std_cpu_power=('cpu_power','std'), min_cpu_power=('cpu_power','min'), 
            max_cpu_power=('cpu_power','max'), q25_cpu_power=('cpu_power', q25), q50_cpu_power=('cpu_power', q50), 
            q75_cpu_power=('cpu_power', q75), count_cpu_power=('cpu_power', 'count'), size_cpu_power=('cpu_power', 'size'),
            cpu_nans=('cpu_nan','sum'),
            mean_gpu_power=('gpu_power', 'mean'), std_gpu_power=('gpu_power','std'), min_gpu_power=('gpu_power','min'), 
            max_gpu_power=('gpu_power','max'), q25_gpu_power=('gpu_power', q25), q50_gpu_power=('gpu_power', q50), 
            q75_gpu_power=('gpu_power', q75), count_gpu_power=('gpu_power', 'count'), size_gpu_power=('gpu_power', 'size'),
            gpu_nans=('gpu_nan', 'sum')).reset_index()
    return res.filter(COLUMN_NAMES)
                                            
def compute_day(offset, date_key, ddf):
    """Computation of a day worth of data"""
    # If performing joins, this is probably the best place to get the client
    # and send the data to the workers, and then performing per partition joins
    # at pandas level
    return (
        offset,
        date_key,
        ddf.map_partitions(
            compute_partition, meta=pd.DataFrame(columns = COLUMN_NAMES)).compute()
    )


def handle_result(context, res):
    """Handle results (sink)"""
    # Context can be used to maintain cross date state
    _, date_key, df = res
    
    ##shape of df/head
    save_file=f'{DEST_DIR}/{date_key}.csv'
    df.to_csv(save_file, index=False) 
    logger.info(res)


@logger.catch
def compute(client):
    """Main computation loop

    Computation is done using futures instead of delays to reduce the impact of
    the scheduler delays
    """


    # A loop that uses the big fat cursor on DATASTREAM_BASE
    # The cursor is smart enough to load only the data relevant to a particular
    # window.  Also, it does not read data if already read (i.e., overlapping
    # segments).  This *FAT* window moves from day 1 to the end
    cursor = DayCursor(
        client, basedir=SOURCE_DIR, cursor_stop_offset=CURSOR_STOP_OFFSET,
        # Data we read (if column is not None and list, it will read that column
        index='timestamp',
        columns=None,
        # Days to attach prior or after the current date
        prev_days=0, next_days=0,
        # Skip condition
        skip_fn=skip_if,
        # Whether we load the per day into memory when iterating
        persist=True,
    )


    logger.info("Beginning iteration")
    context = {}
    futures = as_completed()
    for offset, date_key, ddf in cursor.iter():
        # Based on the information, do compute for a day
        # We submit the per day compute as a future
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
    logger.info(f'job_run_time: {datetime.now()-start_time}')
    logger.info("Iteration finished")
    return 0

#
# Submission block that takes care of the entrypoints
#

if __name__ == "__main__":
    andes_dask_batch(compute, script=COMPUTE_SCRIPT, watchdog=WATCHDOG_INTERVAL_SEC)

