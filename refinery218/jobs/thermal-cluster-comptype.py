#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 16
#SBATCH -J thermal-cluster-comptype
#SBATCH -t 24:00:00
#SBATCH -A stf218
#SBATCH -o ../logs/thermal-cluster-comptype-%J.out
import os
import sys
import time
import glob
from itertools import product
from functools import partial
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
from refinery218.filestream import DayCursor


COMPUTE_SCRIPT = 'thermal-cluster-comptype.py'

CEP = '/gpfs/alpine/stf218/proj-shared/data/lake.dev/summit_cooling_plant/interp_cep_data.parquet'
SRC_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc/10s_agg'
DST_DIR = '/gpfs/alpine/stf218/proj-shared/data/lake/summit_thermal_cluster_comptype_cep'

TIME = 'timestamp'
COMPTYPES = ['gpu_core', 'gpu_mem', 'dimm', 'cpu_core']
GPU_CORES, GPU_MEMS = [[f'gpu{gpu}_{sensor}_temp.mean' for gpu in range(6)] for sensor in ['core', 'mem']]
DIMMS = [f'dimm{dimm}_temp.mean' for dimm in range(16)]
CPU_CORES = [f'p{p}_core{core}_temp.mean' for p, core in product(range(2), set(range(24)) - {13})]
COMPTYPE_COLGROUPS = [GPU_CORES, GPU_MEMS, DIMMS, CPU_CORES]

RES_COLUMNS = [comptype + '.' + aggtype for comptype in COMPTYPES
               for aggtype in ['mean', 'std', 'min', 'max', 'q25', 'q50', 'q75', 'size', 'count']]
EMPTY_MAPPED_PARTITION = pd.DataFrame(columns=RES_COLUMNS)

# If not None, cursor stops at the offset from the beginning
# Useful for initial development & debugging
CURSOR_STOP_OFFSET = None

# andes_dask_batch has a watchdog that restarts the dask cluster
# when no heartbeat (calling watchdog_heartbeat()) is seen within
# WATCHDOG_INTERVAL_SEC
WATCHDOG_INTERVAL_SEC = 1450

# Pre-commit tasks to overlap scheduling delay & computation
OVERLAP_DAY_COUNT = 1


def skip_if(cursor, offset, date_key):
    """Return True if we want to skip a particular date"""
    # Useful to skip work that already has been done
    return False


def compute_partition(df):
    """Perform compute on a partition

    Partitions are perfectly aligned along 1 minute and the timestamp index is
    sorted.

    Below is a reasonably costly operation that aggregates data from
    each timestep.  That dataframe would aggregate what the timestep had (i.e.,
    measurements from all the hosts)  Also, this is done with *all* of the
    columns.
    """
    if not isinstance(df, pd.DataFrame) and not isinstance(df, dd.DataFrame):
        return EMPTY_MAPPED_PARTITION
    
    df = df.reset_index()
    agg = []
    for comptype, cols in zip(COMPTYPES, COMPTYPE_COLGROUPS):
        df_comptype = pd.melt(df[cols + [TIME]], id_vars=TIME, var_name='_').drop(columns='_')
        agg_comptype = df_comptype.groupby(TIME).agg(
            ['mean', 'std', 'min', 'max', *[lambda x: x.quantile(q) for q in [.25, .50, .75]], 'size', 'count'])
        agg.append(agg_comptype)
    agg = pd.concat(agg, axis=1)
    
    agg.columns = RES_COLUMNS
    
    return agg


def compute_day(offset, date_key, ddf):
    """Computation of a day worth of data"""
    
    return (
        offset,
        date_key,
        ddf.map_partitions(compute_partition).compute()
    )


def handle_result(context, res):
    """Handle results (sink)"""

    _, date_key, df = res
    
    if not df.empty:
        if context['cep'].index.tz != df.index.tz:
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

    # A loop that uses the big fat cursor on DATASTREAM_BASE
    # The cursor is smart enough to load only the data relevant to a particular
    # window.  Also, it does not read data if already read (i.e., overlapping
    # segments).  This *FAT* window moves from day 1 to the end
    cursor = DayCursor(
        client, basedir=SRC_DIR, cursor_stop_offset=CURSOR_STOP_OFFSET,
        # Data we read (if column is not None and list, it will read that column
        index='timestamp',
        columns=GPU_CORES + GPU_MEMS + DIMMS + CPU_CORES,
        # Days to attach prior or after the current date
        prev_days=0, next_days=0,
        # Skip condition
        skip_fn=skip_if,
        # Whether we load the per day into memory when iterating
        persist=True,
    )

    # Read interpolated CEP data.
    context = {'cep': pd.read_parquet(CEP, engine='pyarrow')}
        
    logger.info("Beginning iteration")
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
    logger.info("Iteration finished")
    return 0

#
# Submission block that takes care of the entrypoints
#

if __name__ == "__main__":
    andes_dask_batch(compute, script=COMPUTE_SCRIPT, watchdog=WATCHDOG_INTERVAL_SEC)
