"""
filestream.py - data streams coming in as files
"""
import os
import sys
import glob
from datetime import datetime, timedelta
from loguru import logger
import dask.dataframe as dd


def _default_keys_fn(basedir, key_format):
    """
    default key generation based on looking at the basedir input
    """
    return list(map(
        lambda fullpath:
            datetime.strptime(os.path.basename(fullpath).split('.')[0], key_format),
        sorted(glob.glob(os.path.join(basedir, '*.parquet')))
    ))


def _default_get_file_fn(dt_key, index=None, columns=None, basedir=None):
    """Get a file from a date"""
    fn = f"{basedir}/{dt_key}.parquet"
    if not os.access(fn, os.F_OK):
        return None
    return dd.read_parquet(
        fn, index=index, columns=columns,
        split_row_groups=True, gather_statistics=True, engine="pyarrow")



class DayCursor(object):
    """
    Big Fat Cursor that can go through a datastream with lots of parallelism
    """
    def __init__(self, client,
                 index='timestamp', columns=None,
                 prev_days=1, next_days=1,
                 keys=None,
                 key_format='%Y%m%d',
                 keys_fn=_default_keys_fn,
                 get_file_fn=_default_get_file_fn,
                 skip_fn=None,
                 basedir=None,
                 persist=True,
                 cursor_stop_offset=None):
        self.client = client
        self.index = index
        self.columns = columns
        self.prev_days = prev_days
        self.next_days = next_days
        self.keys = keys
        self.keys_fn = keys_fn
        self.get_file_fn = get_file_fn
        self.skip_fn = skip_fn
        self.key_format = key_format
        self.basedir = basedir
        self.persist = persist
        self.cursor_stop_offset = cursor_stop_offset

        if self.keys is None:
            self.keys = self.keys_fn(self.basedir, self.key_format)


    def iter(self):
        """Iterator that loads the data into memory before returning

        Returns datekey and the corresponding dask data frmae
        """
        persisted = {}
        for offset, dt in enumerate(self.keys):
            new_persisted = {}
            new_frameset = []
            date_key = dt.strftime('%Y%m%d')

            if self.skip_fn and self.skip_fn(self, offset, date_key):
                logger.info(f"Skipping {date_key}")
                continue

            frameset_dts = sorted([dt] \
                + [dt - timedelta(days=i + 1) for i in range(self.prev_days)] \
                + [dt + timedelta(days=i + 1) for i in range(self.next_days)])
            for mdt in frameset_dts:
                dt_key = mdt.strftime('%Y%m%d')

                # Either fetch from memory or read into cluster memory
                ddf = persisted.get(dt_key, None)
                if ddf is None:
                    ddf = self.get_file_fn(dt_key, index=self.index,
                                           columns=self.columns,
                                           basedir=self.basedir)
                    if ddf is None:
                        continue
                    if self.persist:
                        ddf = self.client.persist(ddf)

                # Add to the new frameset
                new_frameset.append(ddf)
                new_persisted[dt_key] = ddf

            # Dereference last persisted set
            del persisted
            persisted = new_persisted
            concat_ddf = dd.concat(new_frameset)

            # Yield to the processing context
            yield offset, date_key, concat_ddf

            del concat_ddf

            if self.cursor_stop_offset is not None \
               and offset >= self.cursor_stop_offset:
                break

