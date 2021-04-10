import os
import glob
from datetime import datetime
import pytest
import refinery218.darshan
from refinery218.darshan import darshan_util_load, darshan_util_unload
from refinery218.darshan import read_meta, append_meta, to_unique_pandas
from refinery218.darshan import date_meta_to_pandas


@pytest.mark.skipif(
    os.environ.get('LMOD_SYSTEM_NAME', None) not in ['summit', 'andes', 'dtn'],
    reason="test only applicable to designated systems"
)
def test_darshan_module_load_unload():
    """Summit and Andes should have darshan"""
    lmod_system_name = os.environ.get('LMOD_SYSTEM_NAME', None)
    # We try darshan which we know that exists in the clusters we know
    assert(os.system('file `which darshan-parser`') != 0)
    darshan_util_load()
    assert(os.system('file `which darshan-parser`') == 0)
    darshan_util_unload()
    assert(os.system('file `which darshan-parser`') != 0)


@pytest.mark.skipif(
    os.environ.get('LMOD_SYSTEM_NAME', None) not in ['summit', 'andes', 'dtn'],
    reason="test only applicable to designated systems"
)
def test_parse_header():
    """Test darshan parse header by putting in a known file"""
    TEST_CNT = 10
    darshan_util_load()
    files = glob.glob(f"/gpfs/alpine/darshan/summit/2019/12/29/*.darshan")
    from collections import defaultdict
    columns = defaultdict(list)
    for i, fn in enumerate(sorted(files)):
        if i == TEST_CNT:
            break
        try:
            meta = read_meta(fn)
            columns = append_meta(columns, meta)
        except Exception as e:
            continue
    df = to_unique_pandas(columns)
    print(df[['jobid', 'uid', 'exe', 'start_time', 'end_time']])
    darshan_util_unload()


@pytest.mark.skipif(
    os.environ.get('LMOD_SYSTEM_NAME', None) not in ['summit', 'andes', 'dtn'],
    reason="test only applicable to designated systems"
)
def test_get_day_meta():
    darshan_util_load()
    df = date_meta_to_pandas("summit", datetime(2019, 12, 29))
    print(df[['jobid', 'uid', 'exe', 'start_time', 'end_time']])
    print(df[['jobid', 'runtime', 'start_time_utc', 'end_time_utc']])
    darshan_util_unload()

