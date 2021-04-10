"""
ore.py - Ore handling (raw data handling) functions and utilities
"""
import os
import glob
from loguru import logger
import click
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import hashlib
from datetime import datetime, timedelta
import dask.dataframe as dd
from .utils import scratch, scratch_free


def untar_from_file(source, destination):
    """Untar data which is from the tape archive"""
    marker = f"{destination}/.untar.done"
    # Check done marker
    if os.access(marker, os.F_OK):
        logger.info(f"Found destination done marker - {marker}")
        return

    # Create destination directory and untar
    os.makedirs(destination, exist_ok=True)
    logger.info(f'Performing untar from {source} to {destination}')
    # TODO use subprocess 'run'
    os.system(f'tar xf {source} -C {destination}')
    logger.info('Untar from {source} to {destination} finished')

    # Create marker to mark that we're done
    with open(marker, "w") as f:
        f.write(source)


def _handle_row_group(writer, idx, batch):
    """Emit a row group using the writer"""
    # Filter out invalid files when processing
    def is_valid(fn):
        try:
            pq.ParquetFile(fn)
        except Exception as e:
            logger.warning(f"invalid {fn} detected, skipping it")
            return False
        return True
    valid_batch = sorted(filter(is_valid, batch))
    if not valid_batch:
        return

    # Concatenate the valid batch into a single table
    huge_table = pa.concat_tables([
        pq.read_table(f, use_threads=False)
        for f in valid_batch
    ])
    writer.write_table(huge_table)

    # Only if we need to measure the partition size
    #df = huge_table.to_pandas()
    #lst = df.memory_usage()
    #print(lst.sum() / (1024 * 1024))


def merge_parquet_files(src, dst_fn, batch_size=5):
    """Merge multiple parquet files into one"""
    # Source is a set of files (glob), but the destination is a file
    files = []
    if type(src) is str:
        files = sorted(glob.glob(src))
    elif type(src) is list:
        files = src

    # The main batching loop for the batch
    batch_items = batch_size
    batch = list()
    batch_idx = 0

    # Get the schema from the first file
    table_schema = pq.read_table(files[0]).schema

    # Write
    with pq.ParquetWriter(dst_fn, table_schema) as writer:
        for i, fn in enumerate(files):
            # Accumulate "batch_size" files and create a row_group
            # We assume the source files have only one row group
            batch.append(fn)
            batch_items -= 1
            if batch_items == 0:
                if batch_size < 10 and i % 100 == 0:
                    logger.info(f"{fn} to {dst_fn} row_group {batch_idx}")
                _handle_row_group(writer, batch_idx, batch)
                batch_items = batch_size
                batch = list()
                batch_idx += 1


def merge_from_files(files, destination,
                     parquet_pattern="**.parquet",
                     batch_size=10,
                     overwrite_destination=False):
    """Merge many files into one single parquet file"""
    # Work only if we don't have the destination
    if not overwrite_destination and os.access(destination, os.F_OK):
        logger.info(f"Skipping {filename} as we have found an existing destination file")
        return

    # Attempt getting a list from the files
    if type(files) is str:
        files = glob.glob(files)

    # Prepare the destination file and location
    tmpdest = f"{destination}.tmp"
    if os.access(tmpdest, os.F_OK):
        os.unlink(tmpdest)
    os.makedirs(os.path.dirname(tmpdest), exist_ok=True)

    # Work on merging the tar files into one single large file
    merge_parquet_files(files, tmpdest, batch_size=batch_size)

    # Renaming the temporary file to the actual file is the end of the
    # transaction
    os.rename(tmpdest, destination)


def merge_from_tar(filename, destination,
                   parquet_pattern="**.parquet",
                   batch_size=10,
                   overwrite_destination=False):
    """Merge many parquet files in a raw tar file into a single parquet file"""
    # Work only if we don't have the destination
    if not overwrite_destination and os.access(destination, os.F_OK):
        logger.info(f"Skipping {filename} as we have found an existing destination file")
        return

    # Get scratch area and resume where we have left
    # The scratch nae
    filename_hash = hashlib.sha256(destination.encode()).hexdigest()
    logger.info(f"Working on {filename} producing {destination}: batch_size={batch_size}, scratch hash = {filename_hash}")
    scratch_dir = scratch(filename_hash)
    scratch_untar = f"{scratch_dir}/tar"
    os.makedirs(scratch_untar, exist_ok=True)

    # Untar the file and get the filenames
    untar_from_file(filename, scratch_untar)
    files = glob.glob(f"{scratch_untar}/**/{parquet_pattern}", recursive=True)

    # Handle the files
    merge_from_files(files, destination,
                     parquet_pattern=parquet_pattern,
                     batch_size=batch_size,
                     overwrite_destination=overwrite_destination)

    # Erase the scratch area as we're done
    scratch_free(filename_hash)
    logger.info(f"work on {filename} to {destination} complete")


def _tar_fn(tarpath_base, prefix, dt):
    return f"{tarpath_base}/{prefix}{dt.strftime('%Y%m%d')}.tar"


def _segment_fn(scratch_base, segment_base, prefix, dt):
    return f"{scratch_base}/{segment_base}/{prefix}{dt.strftime('%Y%m%d')}-{dt.strftime('%H%M%S')}.parquet"


def from_archival_stream(
    dt,
    prefix="",
    tarpath_base=os.getcwd(),
    tarpath_fn=_tar_fn,
    segment_fn=_segment_fn,
    segment_base=".",
    segment_resolution=timedelta(minutes=1),
    front_segments=1, back_segments=1,
    time_index="timestamp",
    repartition_freq=None,
    columns=None,
    scratch_key=None,
    dask_dataframe=True
):
    """Select data from a day resolution stream of archive files

    From a datastream accumulation chunked and archived in days, untar
    and return a dask dataframe that contains only a single day worth of data.
    The data stream is accumulated in small batches (i.e., 1 minute batch) which
    takes up one parquet file per batch.  And then tar'ed per day.

    This function deal with impurities due to the out-of-order nature of the
    individual rows within the data stream.  As some rows can be in other dates
    (i.e., t-1 or t+1 if you are selecting rows from date t), the function also
    untars adjacent parquet files (controled by front_segments & back_segments)
    to be able to recover them.
    """
    # Get date related setpoints
    datekey = dt.strftime("%Y%m%d")
    archdt_resolution = timedelta(days=1)
    archdt_floor = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    archdt_ceil = archdt_floor + archdt_resolution

    logger.info(f"Working on {datekey}")

    # Come up with the per day archival files
    prv_tar = tarpath_fn(tarpath_base, prefix, dt - archdt_resolution)
    tgt_tar = tarpath_fn(tarpath_base, prefix, dt)
    nxt_tar = tarpath_fn(tarpath_base, prefix, dt + archdt_resolution)

    # Come up with the segment filenames
    if scratch_key is None:
        scratch_key = hashlib.sha256(tgt_tar.encode()).hexdigest()
    scratch_base = scratch(scratch_key)

    # Frontside
    if front_segments < 0:
        front_segments = 0
    front_path = [
        segment_fn(scratch_base, segment_base, prefix, archdt_floor - k * segment_resolution)
        for k in range(1, front_segments + 1)
    ]
    front_fn = [os.path.join(".", segment_base, os.path.basename(fn)) for fn in front_path]
    #print(front_path, front_fn)

    # Backside
    if back_segments < 0:
        back_segments = 0
    back_path = [
        segment_fn(scratch_base, segment_base, prefix, archdt_ceil + k * segment_resolution)
        for k in range(0, front_segments)
    ]
    back_fn = [os.path.join(".", segment_base, os.path.basename(fn)) for fn in back_path]

    # Extract main tar file
    front_tar_cmd = ' '.join(['tar', 'xf', prv_tar, f"--directory={scratch_base}", ] + front_fn)
    back_tar_cmd = ' '.join(['tar', 'xf', nxt_tar, f"--directory={scratch_base}"] + back_fn)
    tgt_tar_cmd = ' '.join(['tar', 'xf', tgt_tar, f"--directory={scratch_base}"])
    untar_marker = os.path.join(scratch_base, '.untar.done.mrk')

    # Untar only if we don't see the marker or if the user requested
    if not os.access(untar_marker, os.F_OK):
        logger.info(f"> Untar {tgt_tar}: {tgt_tar_cmd}")
        res = os.system(tgt_tar_cmd)
        if res != 0:
            raise RuntimeError(f"Untar {tgt_tar} failure")
        if os.access(prv_tar, os.F_OK):
            logger.info(f"> Untar {prv_tar}: {front_tar_cmd}")
            os.system(front_tar_cmd)
        if os.access(nxt_tar, os.F_OK):
            logger.info(f"> Untar {nxt_tar}: {back_tar_cmd}")
            os.system(back_tar_cmd)
        logger.info("> Untar finished")
    else:
        logger.info(f"> Untar'ed files exists")

    # Mark the untar operation
    with open(os.path.join(scratch_base, '.untar.done.mrk'), "w") as f:
        f.write(' ')

    # Work on the files but only on the valid ones
    pattern = os.path.join(os.path.dirname(_segment_fn(scratch_base, segment_base, prefix, datetime.now())), f"{prefix}*.parquet")
    raw_filelist = glob.glob(pattern)
    def is_valid(fn):
        try:
            pq.ParquetFile(fn)
        except Exception as e:
            logger.warning(f"invalid {fn} detected, skipping it")
            return False
        return True
    valid_filelist = sorted(filter(is_valid, raw_filelist))

    if not dask_dataframe:
        return valid_filelist

    # Return the dask dataframe in a delayed fashion
    ddf = dd.read_parquet(
        valid_filelist,
        index=False,
        columns=columns,
        engine="pyarrow",
        split_row_groups=True,
        gather_statistics=True).set_index(time_index).loc[datekey]

    # Repartition if requested with a frequency
    if repartition_freq is not None:
        ddf = ddf.repartition(freq=repartition_freq)

    # Return the delayed dask dataframe
    return ddf


def merge_dask_multifile_parquet(src, dst_fn, batch_size=5):
    """Merges a multifile to_parquet output from Dask into one file"""
    cnt = len(glob.glob(f"{src}/part.*.parquet"))
    filelist = [f"{src}/part.{i}.parquet" for i in range(cnt)]
    merge_parquet_files(filelist, dst_fn, batch_size=batch_size)
