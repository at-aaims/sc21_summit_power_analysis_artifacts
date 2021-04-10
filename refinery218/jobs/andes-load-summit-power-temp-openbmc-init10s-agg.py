#!../.venv.andes/bin/python3
#SBATCH -M andes
#SBATCH -N 16
#SBATCH -J andes-load-summit-power-temp-openbmc-init10s-agg
#SBATCH -t 24:00:00
#SBATCH -A stf218
#SBATCH -o ../logs/andes-load-summit-power-temp-openbmc-init10s-agg-%J.out
#SBATCH --mail-type=ALL
"""
andes-load-summit-power-temp-openbmc-hieragg.py - Load 10s init_agg and 10s final agg

Loads an 10 second aggregated lossless aggregated version of the dataset from
raw tar files.  Deals with unaligned rows that are in the wrong date.
Generates and merges 10s aggregations into a single large file after the
aggregations.
"""

import os
import glob
from datetime import datetime, timedelta
import hashlib
import shutil
import time
from loguru import logger
import click
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from multiprocessing import Process
from dask.distributed import wait, progress, as_completed
from refinery218.ore import merge_dask_multifile_parquet
from refinery218.ore import from_archival_stream
from refinery218.olcf import set_scratch_base, get_scratch_base
from refinery218.olcf import andes_dask_batch, watchdog_heartbeat
from refinery218.utils import scratch, scratch_free
from refinery218.hier_agg import init_agg, finalize_agg
from refinery218.schema import summit_openbmc_telemetry_fields, get_summit_openbmc_telemetry_schema
from refinery218.schema import hier_agg_lossless_schema, hier_agg_final_schema


# Project dependent information
PROJECT = ""
USER = os.environ['USER']
APP_NAME = "summit_power_temp_openbmc"
set_scratch_base(project=PROJECT, user=USER, app_name=APP_NAME)

# Script global
COMPUTE_SCRIPT = "andes-load-summit-power-temp-openbmc-init10s-agg.py"
BATCH_SIZE = 1
WATCHDOG_INTERVAL = 450

# Raw stream where the micro batch files land on
RAW_STREAM = f"/gpfs/alpine/stf008/proj-shared/stf008stc/data/lake/openbmc.summit.raw"
# The archive where the raw files are tarr'ed per day and stored
RAW_ARCHIVE = f"/hpss/prod/stf218/proj-shared/data/lake/openbmc.summit.raw"
# The location where the data is staged in from HPSS
RAW_STAGE = f"/gpfs/alpine/stf218/proj-shared/data/lake/openbmc.summit.raw.tar"

# Target
APP_DATASET_BASE = f"/gpfs/alpine/stf218/proj-shared/data/lake/summit_power_temp_openbmc"
APP_DATASET_1HZ = f"{APP_DATASET_BASE}/data"
APP_DATASET_10S_INIT = f"{APP_DATASET_BASE}/10s_agg_lossless"
APP_DATASET_10S_AGG = f"{APP_DATASET_BASE}/10s_agg"

COLS = [
    fld.name for fld in summit_openbmc_telemetry_fields
    if fld.name not in ['source',]
]
OUTPUT_SCHEMA_1HZ = pa.schema([
    fld for fld in summit_openbmc_telemetry_fields
    if fld.name not in ['source',]
] + [
    pa.field("gpu_total_power", pa.int16()),
    pa.field("cpu_total_power", pa.int16()),
    pa.field("cpu_io_power", pa.int16()),
    pa.field("mem_power", pa.int16()),
    pa.field("gpu_core_max_temp", pa.int8()),
    pa.field("gpu_mem_max_temp", pa.int8()),
    pa.field("dimm_max_temp", pa.int8()),
    pa.field("p0_max_temp", pa.int8()),
    pa.field("p1_max_temp", pa.int8()),
    pa.field("input_power", pa.int16()),
])
OUTPUT_SCHEMA_INIT_AGG = hier_agg_lossless_schema(OUTPUT_SCHEMA_1HZ)
OUTPUT_SCHEMA_FINAL_AGG = hier_agg_final_schema(OUTPUT_SCHEMA_1HZ)


#
# summit_power_temp_openbmc - tar stream helpers
#


def openbmc_tar(dt):
    """Genereate the tar source filename"""
    datekey = dt.strftime('%Y%m%d')
    fn = f"openbmc-{dt.strftime('%Y%m%d')}.tar"
    fn = os.path.join(RAW_STAGE, fn)
    return fn


def openbmc_seg(scratch_base, segment_base, dt):
    """Generate segment filenames"""
    return os.path.join(
        scratch_base, segment_base,
        f"openbmc-{dt.strftime('%Y%m%d')}-{dt.strftime('%H%M%S')}.parquet"
    )


def untar_task(date_key, scratch_key=None, dask_dataframe=False):
    # Create a scratch space for the target
    raw_tar_input = openbmc_tar(datetime.strptime(date_key, "%Y%m%d"))
    if scratch_key is None:
        scratch_key = hashlib.sha256(raw_tar_input.encode()).hexdigest()
    scratch_base = scratch(scratch_key)

    # Load a day
    ddf = from_archival_stream(
        datetime.strptime(date_key, "%Y%m%d"),
        tarpath_base=RAW_STAGE,
        segment_base="openbmc.summit.raw",
        prefix="openbmc-",
        repartition_freq="min",
        scratch_key=scratch_key,
        columns=COLS,
        dask_dataframe=dask_dataframe
    )
    if not dask_dataframe:
        return 0
    return ddf


def initial_summations(ddf):
    """Byproducts that can only be calculated here"""
    # GPU power
    ddf['gpu_total_power'] = ddf['p0_gpu0_power'] \
        + ddf['p0_gpu1_power'] \
        + ddf['p0_gpu2_power'] \
        + ddf['p1_gpu0_power'] \
        + ddf['p1_gpu1_power'] \
        + ddf['p1_gpu2_power']
    # CPU power
    ddf['cpu_total_power'] = ddf['p0_power'] + ddf['p1_power']
    # CPU I/O power
    ddf['cpu_io_power'] = ddf['p0_io_power'] + ddf['p1_io_power']
    # Memory power (DIMM)
    ddf['mem_power'] = ddf['p0_mem_power'] + ddf['p1_mem_power']

    # CPU thermals
    ddf['gpu_core_max_temp'] = ddf[
        ["gpu0_core_temp", "gpu1_core_temp",
         "gpu2_core_temp", "gpu3_core_temp",
         "gpu4_core_temp",
         "gpu5_core_temp"]
    ].max(axis=1)

    # GPU thermals
    ddf['gpu_mem_max_temp'] = ddf[
        ["gpu0_mem_temp", "gpu1_mem_temp", "gpu2_mem_temp", "gpu3_mem_temp", "gpu4_mem_temp", "gpu5_mem_temp"]
    ].max(axis=1)

    # DIMM thermals
    ddf['dimm_max_temp'] = ddf[
        ["dimm0_temp",
         "dimm1_temp",
         "dimm2_temp",
         "dimm3_temp",
         "dimm4_temp",
         "dimm5_temp",
         "dimm6_temp",
         "dimm7_temp",
         "dimm8_temp",
         "dimm9_temp",
         "dimm10_temp",
         "dimm11_temp",
         "dimm12_temp",
         "dimm13_temp",
         "dimm14_temp",
         "dimm15_temp"]
    ].max(axis=1)

    ddf['p0_max_temp'] = ddf[
        ["p0_core0_temp",
        "p0_core1_temp",
        "p0_core2_temp",
        "p0_core3_temp",
        "p0_core4_temp",
        "p0_core5_temp",
        "p0_core6_temp",
        "p0_core7_temp",
        "p0_core8_temp",
        "p0_core9_temp",
        "p0_core10_temp",
        "p0_core11_temp",
        "p0_core12_temp",
        #"p0_core13_temp",
        "p0_core14_temp",
        "p0_core15_temp",
        "p0_core16_temp",
        "p0_core17_temp",
        "p0_core18_temp",
        "p0_core19_temp",
        "p0_core20_temp",
        "p0_core21_temp",
        "p0_core22_temp",
        "p0_core23_temp"]
    ].max(axis=1)

    ddf['p1_max_temp'] = ddf[
        ["p1_core0_temp",
        "p1_core1_temp",
        "p1_core2_temp",
        "p1_core3_temp",
        "p1_core4_temp",
        "p1_core5_temp",
        "p1_core6_temp",
        "p1_core7_temp",
        "p1_core8_temp",
        "p1_core9_temp",
        "p1_core10_temp",
        "p1_core11_temp",
        "p1_core12_temp",
        #"p1_core13_temp",
        "p1_core14_temp",
        "p1_core15_temp",
        "p1_core16_temp",
        "p1_core17_temp",
        "p1_core18_temp",
        "p1_core19_temp",
        "p1_core20_temp",
        "p1_core21_temp",
        "p1_core22_temp",
        "p1_core23_temp"]
    ].max(axis=1)

    # Input power
    ddf['input_power'] = ddf['ps0_input_power'] + ddf['ps1_input_power']
    return ddf



def load_and_init_agg(
    client, date_key,
    onehz_dask_output, onehz_dask_output_tmp,
    init_dask_output, init_dask_output_tmp,
    fin_dask_output, fin_dask_output_tmp
):
    """Get a day from an archival stream"""
    begin_tstmp = time.time()
    logger.info(f"Aggregation on {date_key} - begin")

    # No need to proceed if we have all outputs out there
    if os.access(onehz_dask_output, os.F_OK) \
       and os.access(init_dask_output, os.F_OK) \
       and os.access(fin_dask_output, os.F_OK):
        logger.info(f"No work to do for {date_key}")
        return

    # If we decide to work, then we erase everything and start from scratch
    shutil.rmtree(onehz_dask_output, ignore_errors=True)
    shutil.rmtree(init_dask_output, ignore_errors=True)
    shutil.rmtree(fin_dask_output, ignore_errors=True)

    # Ensure the directories
    os.makedirs(os.path.dirname(onehz_dask_output), exist_ok=True)
    os.makedirs(os.path.dirname(init_dask_output), exist_ok=True)
    os.makedirs(os.path.dirname(fin_dask_output), exist_ok=True)

    # Create a scratch space for the target
    raw_tar_input = openbmc_tar(datetime.strptime(date_key, "%Y%m%d"))
    scratch_key = hashlib.sha256(raw_tar_input.encode()).hexdigest()

    # Untar the file and request a dask dataframe
    ddf = untar_task(date_key, scratch_key=scratch_key, dask_dataframe=True)

    # Initial aggregates (additional columns)
    # and persist
    ddf = initial_summations(ddf)
    ddf = client.persist(ddf)
    ddf.to_parquet(onehz_dask_output_tmp, engine='pyarrow', overwrite=True,
                   schema=OUTPUT_SCHEMA_1HZ)
    os.rename(onehz_dask_output_tmp, onehz_dask_output)
    ddf = client.persist(ddf.drop('node_state', axis=1))

    # Perform init_agg and output
    # - Before aggregating drop 'node_state'
    # - FIXME: what should we do for Nan values?
    init_ddf = init_agg(
        ddf, "10s", "timestamp", [], ['hostname'], aligned=True
    ).reset_index()
    init_ddf = client.persist(init_ddf)
    init_ddf = client.persist(init_ddf.set_index('timestamp', sorted=True))

    # At this point, we need to infer the proper schema
    init_ddf.to_parquet(
        init_dask_output_tmp, engine="pyarrow",
        overwrite=True, schema=OUTPUT_SCHEMA_INIT_AGG
    )
    os.rename(init_dask_output_tmp, init_dask_output)

    # Perform finalize_agg and output
    fini_ddf = finalize_agg(
        init_ddf, group_cols=['timestamp', 'hostname']
    ).reset_index()
    fini_ddf = client.persist(fini_ddf)
    fini_ddf = client.persist(fini_ddf.set_index('timestamp', sorted=True))
    fini_ddf.to_parquet(
        fin_dask_output_tmp, engine="pyarrow", overwrite=True,
        schema=OUTPUT_SCHEMA_FINAL_AGG
    )
    os.rename(fin_dask_output_tmp, fin_dask_output)

    del fini_ddf
    del init_ddf
    del ddf

    # Remove the scratch space
    scratch_free(scratch_key)
    logger.info(f"Aggregation on {date_key} done - runtime {(time.time() - begin_tstmp):.3f} sec")


def merge_dask_output(args):
    """Merge dask output for finalized aggregations"""
    date_key, basedir = args
    output = f"{basedir}/{date_key}.parquet"
    output_tmp = f"{output}.tmp"
    dask_input = f"{output}.dask"
    begin_tstmp = time.time()

    logger.info(f"Merging finalize output {basedir}/{date_key} for into a single parquetfile ")
    if os.access(output, os.F_OK):
        logger.info(f"{output} already exists")
        if os.access(dask_input, os.F_OK):
            logger.info(f"removing old {dask_input}")
            shutil.rmtree(dask_input, ignore_errors=True)
        return {'result': f'{output} already done'}

    # Merge files
    merge_dask_multifile_parquet(dask_input, output_tmp, batch_size=BATCH_SIZE)

    # Commit the results
    if os.access(output_tmp, os.F_OK):
        os.rename(output_tmp, output)

    logger.info(f"Merge dask output {basedir}/{date_key} removing original")
    shutil.rmtree(dask_input, ignore_errors=True)
    logger.info(f"Merge dask output {basedir}/{date_key} done - runtime {(time.time() - begin_tstmp):.3f} sec")
    return {'result': f'{output} success'}


def get_tar_date_keys():
    """Find work to do based on the existing tar files"""
    return list(map(
        lambda fullpath: os.path.basename(fullpath).split('.')[0].split('-')[1],
        glob.glob(f"{RAW_STAGE}/*.tar")
    ))


def find_work_to_do():
    ret = []
    work_to_do_first = [
        '20210114', '20210115', '20210116'
    ]
    for date_key in get_tar_date_keys():
        if not os.access(f"{APP_DATASET_1HZ}/{date_key}.parquet", os.F_OK) \
           or not os.access(f"{APP_DATASET_10S_INIT}/{date_key}.parquet", os.F_OK) \
           or not os.access(f"{APP_DATASET_10S_AGG}/{date_key}.parquet", os.F_OK):
            ret.append(date_key)
    return work_to_do_first + sorted(ret)


def find_pending_dask_outputs(basedir):
    """FInd dask outputs from a basedir"""
    work_to_do = [
        (date_key, basedir) for date_key in get_tar_date_keys()
        if os.access(f"{basedir}/{date_key}.parquet.dask", os.F_OK) and
            not os.access(f"{basedir}/{date_key}.parquet", os.F_OK)
    ]
    return sorted(work_to_do)


def handle_file_merge_jobs():
    """Submit pending file merge jobs all at once"""
    import dask.bag as db
    logger.info("Submitting pending file merge jobs - begin")
    work_to_do = \
            find_pending_dask_outputs(APP_DATASET_1HZ) \
            + find_pending_dask_outputs(APP_DATASET_10S_INIT) \
            + find_pending_dask_outputs(APP_DATASET_10S_AGG)
    logger.info(work_to_do)
    logger.info(db.from_sequence(work_to_do).map(merge_dask_output).compute())
    logger.info("Submitting pending file merge jobs - end")


@logger.catch
def compute(client):
    """Perform compute"""
    logger.info("Job begin")
    begin_tstmp = time.time()
    work_to_do = find_work_to_do()
    last_idx = len(work_to_do)

    merge_futures = as_completed()

    # Make a beat
    watchdog_heartbeat()

    for i, date_key in enumerate(work_to_do):

        # Filenames
        onehz_output = f"{APP_DATASET_1HZ}/{date_key}.parquet"
        onehz_dask_output = f"{onehz_output}.dask"
        onehz_dask_output_tmp = f"{onehz_dask_output}.tmp"

        init_output = f"{APP_DATASET_10S_INIT}/{date_key}.parquet"
        init_dask_output = f"{init_output}.dask"
        init_dask_output_tmp = f"{init_dask_output}.tmp"

        fin_output = f"{APP_DATASET_10S_AGG}/{date_key}.parquet"
        fin_dask_output = f"{fin_output}.dask"
        fin_dask_output_tmp = f"{fin_dask_output}.tmp"

        # No need to proceed if we have all outputs out there
        if (os.access(init_output, os.F_OK) or os.access(init_dask_output, os.F_OK)) \
           and (os.access(fin_output, os.F_OK) or os.access(fin_dask_output, os.F_OK))\
           and (os.access(onehz_output, os.F_OK) or os.access(onehz_dask_output, os.F_OK)):

            if not os.access(init_output, os.F_OK):
                merge_futures.add(
                    client.submit(merge_dask_output, (date_key, APP_DATASET_10S_INIT))
                )
            if not os.access(fin_output, os.F_OK):
                merge_futures.add(
                    client.submit(merge_dask_output, (date_key, APP_DATASET_10S_AGG))
                )
            if not os.access(onehz_output, os.F_OK):
                merge_futures.add(
                    client.submit(merge_dask_output, (date_key, APP_DATASET_1HZ))
                )

            logger.info(f"No work to do for {date_key}")
            continue

        # submit untar of the next date_key and get a future
        next_untar = None
        if i + 1 < last_idx:
            next_date_key = work_to_do[i + 1]
            logger.info(f"Prefetching next date key {next_date_key}")
            next_untar = Process(target=untar_task, args=(next_date_key,))
            next_untar.start()

        # The foreground work
        load_and_init_agg(
            client, date_key,
            onehz_dask_output, onehz_dask_output_tmp,
            init_dask_output, init_dask_output_tmp,
            fin_dask_output, fin_dask_output_tmp
        )

        # Submit the futures
        merge_futures.add(
            client.submit(merge_dask_output, (date_key, APP_DATASET_1HZ))
        )
        merge_futures.add(
            client.submit(merge_dask_output, (date_key, APP_DATASET_10S_INIT))
        )
        merge_futures.add(
            client.submit(merge_dask_output, (date_key, APP_DATASET_10S_AGG))
        )

        # Wait for the background date_key and then move
        if next_untar is not None and next_untar.is_alive():
            logger.info("Waiting for the background untar to finish")
            for i in range(10):
                if next_untar.exitcode is None:
                    logger.info(f"Waiting untar to finish")
                    next_untar.join(3)
            logger.info(f"Untar finished with {next_untar.exitcode}")
            logger.info("Good to co with the next untar'ed task")

        # Make a beat
        watchdog_heartbeat()

    # Spawn a background heartbeat generator
    def watchdog_heartbeat_handler():
        while True:
            watchdog_heartbeat()
            time.sleep(30)
    heartbeat_proc = Process(target=watchdog_heartbeat_handler)
    heartbeat_proc.start()

    # Wait for all the futures to finish
    logger.info(f"Job waiting for background jobs to finish")
    for future in merge_futures:
        logger.info(future.result())

    # End with another file merge task
    logger.info(f"Performing end of job file merging")
    handle_file_merge_jobs()

    heartbeat_proc.kill()
    logger.info(f"Job end - runtime {(time.time() - begin_tstmp):.3f} sec")
    return 0


#
# Submission block that takes care of the entrypoints
#


if __name__ == "__main__":
    andes_dask_batch(compute, script=COMPUTE_SCRIPT, watchdog=WATCHDOG_INTERVAL)
