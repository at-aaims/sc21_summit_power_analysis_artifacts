#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 16
#SBATCH -J power_ts_job_ignorant
#SBATCH -t 10:00:00
#SBATCH -A gen150
#SBATCH -o ../logs/power_ts_job_aware-%J.out
import os
import sys
import time
import glob
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
from refinery218.filestream import DayCursor
import pdb

COMPUTE_SCRIPT = "power_ts_job_aware.py"

start_time = datetime.now()

# Source dataset base location
SOURCE_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/10s_agg'
DEST_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_job_aware_10s'
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
COLUMN_NAMES = ['timestamp','allocation_id','count_hostname','count_node_name','job_sch_node_num','count_inp',
        'size_inp','sum_inp','count_top','size_top','sum_top','mean_inp','std_inp','max_inp','min_inp',
        'mean_top','std_top','max_top','min_top','q25_inp','q50_inp','q75_inp','q25_top','q50_top','q75_top']

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

def compute_partition(df, date_key):
    """Perform compute on a partition

    Partitions are perfectly aligned along 1 minute and the timestamp index is
    sorted.

    Below is a reasonably costly operation that aggregates data from
    each timestep.  That dataframe would aggregate what the timestep had (i.e.,
    measurements from all the hosts)  Also, this is done with *all* of the
    columns.
    """
    logger.info(f'compute partition df={df}')
    logger.info(f'Dataframe type df={type(df)}')
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns = COLUMN_NAMES)
    #Get the todays and yesterdays data from job scheduler dataset
    ystrday_date_str = datetime.strftime(datetime.strptime(date_key,"%Y%m%d")-timedelta(1),"%Y%m%d")
    next_date_str = datetime.strftime(datetime.strptime(date_key,"%Y%m%d")+timedelta(1),"%Y%m%d")

    DS1 = f'/gpfs/alpine/proj-shared/stf218/data/lake.dev/summit_jobs_full_perdate_parquet/{date_key}.parquet/part.0.parquet'
    DS2 = f'/gpfs/alpine/proj-shared/stf218/data/lake.dev/summit_jobs_full_perdate_parquet/{ystrday_date_str}.parquet/part.0.parquet'
    DS3 = f'/gpfs/alpine/proj-shared/stf218/data/lake.dev/summit_jobs_full_perdate_parquet/{next_date_str}.parquet/part.0.parquet'
    DATASET = [DS1,DS2,DS3]
    df_job_sch = pd.concat([pd.read_parquet(d, engine='pyarrow') for d in DATASET])
    df_job_sch = df_job_sch.loc[:,['allocation_id','primary_job_id','node_name','num_nodes','begin_time','end_time']]
    
    #Convert the begin and end timestamps into 10s using floor and ceil function 
    df_job_sch['end_time'] = df_job_sch['end_time'].dt.ceil('10s')
    df_job_sch['begin_time'] = pd.to_datetime(df_job_sch['begin_time'],  format='%Y-%m-%d %H:%M:%S')
    df_job_sch['begin_time'] = df_job_sch['begin_time'].dt.floor('10s')

    logger.info(f'mydateDate Key type {date_key}')
    logger.info(f'mydateDate Key columns {df.index}')
     
    df.reset_index(inplace=True)
    if 'timestamp' not in df.columns:
        logger.info("Time stamp column not available \n ss{df.head()}")

    one_min_df = pd.DataFrame()
    for timestep in pd.unique(df['timestamp']):
        temp_df_power = df[df['timestamp']==timestep]
        temp_df_power = temp_df_power.loc[:,['timestamp','hostname','input_power.mean','total_power.mean']]

        temp_df_job_schd = df_job_sch[(df_job_sch['begin_time']<= timestep) & (df_job_sch['end_time']>= timestep)]
        temp_df_job_schd = temp_df_job_schd.loc[:,['allocation_id','node_name', 'num_nodes']]

        if temp_df_job_schd.dropna().empty:
            return pd.DataFrame(columns = COLUMN_NAMES)

        temp_comb = temp_df_power.merge(temp_df_job_schd, how='inner',  left_on='hostname', right_on='node_name')
        t1 = temp_comb.groupby(['timestamp','allocation_id']).agg(count_hostname=('hostname','count'),
                count_node_name=('node_name', 'count'),job_sch_node_num=('num_nodes','first'),
                count_inp=('input_power.mean', 'count'),size_inp=('input_power.mean', 'size'), 
                sum_inp=('input_power.mean', 'sum'),count_top=('total_power.mean', 'count'),
                size_top=('total_power.mean', 'size'), sum_top=('total_power.mean', 'sum'),
                mean_inp=('input_power.mean', 'mean'),std_inp=('input_power.mean', 'std'), 
                max_inp=('input_power.mean', 'max'), min_inp=('input_power.mean', 'min'),
                mean_top=('total_power.mean', 'mean'),std_top=('total_power.mean', 'std'), 
                max_top=('total_power.mean', 'max'), min_top=('total_power.mean', 'min'),
                q25_inp=('input_power.mean', q25),q50_inp=('input_power.mean', q50), 
                q75_inp=('input_power.mean', q75),q25_top=('total_power.mean', q25),
                q50_top=('total_power.mean', q50), q75_top=('total_power.mean', q75)).reset_index()
        
        one_min_df = pd.concat([one_min_df,t1],axis=0) 
    return one_min_df.filter(COLUMN_NAMES)

                                            
def compute_day(offset, date_key, ddf):
    """Computation of a day worth of data"""
    # If performing joins, this is probably the best place to get the client
    # and send the data to the workers, and then performing per partition joins
    # at pandas level
    logger.info('')
    return (
        offset,
        date_key,
        ddf.map_partitions(
            compute_partition, date_key, meta=pd.DataFrame(columns = COLUMN_NAMES)).compute()
    )


def handle_result(context, res):
    """Handle results (sink)"""
    # Context can be used to maintain cross date state
    _, date_key, df = res

    ##shape of df/head
    save_file=f'{DEST_DIR}/{date_key}.csv'
    df.to_csv(save_file, index=False) 
    logger.info(f'result written, {res}')


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

