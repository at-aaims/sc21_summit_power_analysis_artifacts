#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 16
#SBATCH -J power_ts_job_ignorant
#SBATCH -t 4:00:00
#SBATCH -A gen150
#SBATCH -o ../logs/power_ts_job_ignorant-%J.out
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


COMPUTE_SCRIPT = "power_ts_job_ignorant.py"

start_time = datetime.now()

# Source dataset base location
SOURCE_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/10s_agg'
DEST_DIR='/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/power_ts_10s'
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
COLUMN_NAMES = ["timestamp", "count_inp", "size_inp", "sum_inp", "count_top", "size_top", "sum_top", 
                "count_ps0", "size_ps0", "sum_ps0", "count_ps1", "size_ps1", "sum_ps1", "count_ps_1_2", 
                "size_ps_1_2", "sum_ps_1_2","mean_inp", "std_inp", "max_inp", "min_inp", "mean_top", "std_top",
                "max_top", "min_top", "mean_ps0", "std_ps0", "max_ps0", "min_ps0", "mean_ps1", "std_ps1", 
                "max_ps1", "min_ps1", "mean_ps_1_2", "std_ps_1_2", "max_ps_1_2", "min_ps_1_2", "q25_inp", 
                "q50_inp", "q75_inp", "q25_top", "q50_top", "q75_top", "q25_ps0", "q50_ps0", "q75_ps0", 
                "q25_ps1", "q50_ps1", "q75_ps1", "q25_ps_1_2", "q50_ps_1_2", "q75_ps_1_2"]


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
    logger.info(f'compute partition df={df}')
    logger.info(f'Dataframe type df={type(df)}')
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns = COLUMN_NAMES)
    try:
        df['ps_1_2_input_power.mean'] = pd.to_numeric(df['ps0_input_power.mean'],errors='coerce')+
                                        pd.to_numeric(df['ps1_input_power.mean'],errors='coerce')
        return df.groupby('timestamp').agg(
                count_inp=('input_power.mean', 'count'),size_inp=('input_power.mean', 'size'), 
                sum_inp=('input_power.mean', 'sum'),count_top=('total_power.mean', 'count'),
                size_top=('total_power.mean', 'size'), sum_top=('total_power.mean', 'sum'),
                count_ps0=('ps0_input_power.mean', 'count'),size_ps0=('ps0_input_power.mean', 'size'), 
                sum_ps0=('ps0_input_power.mean', 'sum'),count_ps1=('ps1_input_power.mean', 'count'),
                size_ps1=('ps1_input_power.mean', 'size'), sum_ps1=('ps1_input_power.mean', 'sum'),
                count_ps_1_2=('ps_1_2_input_power.mean', 'count'),size_ps_1_2=('ps_1_2_input_power.mean', 'size'), 
                sum_ps_1_2=('ps_1_2_input_power.mean', 'sum'),
                
                mean_inp=('input_power.mean', 'mean'),std_inp=('input_power.mean', 'std'), 
                max_inp=('input_power.mean', 'max'), min_inp=('input_power.mean', 'min'),
                mean_top=('total_power.mean', 'mean'),std_top=('total_power.mean', 'std'), 
                max_top=('total_power.mean', 'max'), min_top=('total_power.mean', 'min'),
                mean_ps0=('ps0_input_power.mean', 'mean'),std_ps0=('ps0_input_power.mean', 'std'), 
                max_ps0=('ps0_input_power.mean', 'max'), min_ps0=('ps0_input_power.mean', 'min'),
                mean_ps1=('ps1_input_power.mean', 'mean'),std_ps1=('ps1_input_power.mean', 'std'), 
                max_ps1=('ps1_input_power.mean', 'max'), min_ps1=('ps1_input_power.mean', 'min'),
                mean_ps_1_2=('ps_1_2_input_power.mean', 'mean'),std_ps_1_2=('ps_1_2_input_power.mean', 'std'), 
                max_ps_1_2=('ps_1_2_input_power.mean', 'max'), min_ps_1_2=('ps_1_2_input_power.mean', 'min'),
                
                q25_inp=('input_power.mean', q25),q50_inp=('input_power.mean', q50), 
                q75_inp=('input_power.mean', q75),q25_top=('total_power.mean', q25),
                q50_top=('total_power.mean', q50), q75_top=('total_power.mean', q75),
                q25_ps0=('ps0_input_power.mean', q25),q50_ps0=('ps0_input_power.mean', q50), 
                q75_ps0=('ps0_input_power.mean', q75), q25_ps1=('ps1_input_power.mean', q25),
                q50_ps1=('ps1_input_power.mean', q50), q75_ps1=('ps1_input_power.mean', q75),
                q25_ps_1_2=('ps_1_2_input_power.mean', q25),q50_ps_1_2=('ps_1_2_input_power.mean', q50), 
                q75_ps_1_2=('ps_1_2_input_power.mean', q75)).reset_index()
    except :
        logger.info("EXCEPTION!!!!")

                                            
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

