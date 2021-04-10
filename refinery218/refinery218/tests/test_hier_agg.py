import pytest
import numpy as np
import pandas as pd
import dask.dataframe as dd

from refinery218.hier_agg import init_agg, coarsen_agg, finalize_agg


def test_init_agg():
    times = pd.date_range('2020-02-02', periods=6, freq='h')
    values = [-1, 1, np.nan, np.nan, 1, np.nan]
    ddf = dd.from_pandas(pd.DataFrame({'t': times, 'x': values}), npartitions=1)
    
    ddf_agg = init_agg(ddf, time_col='t', time_res='2h')
    df_agg = ddf_agg.compute()
    assert df_agg.iloc[1, 1:].isna().all()
    assert (df_agg.fillna(0).to_numpy() == np.array([[2, 0, 2, -1, 1], [0, 0, 0, 0, 0], [1, 1, 0, 1, 1]])).all()

    ddf_coarse = coarsen_agg(ddf_agg, time_res='4h', time_col='t')
    assert (ddf_coarse.compute().to_numpy() == np.array([[2, 0, 2, -1, 1], [1, 1, 0, 1, 1]])).all()

    ddf_final = finalize_agg(ddf_coarse)
    df_final = ddf_final.compute()
    assert np.isnan(df_final.iloc[1, 1])
    assert (df_final.fillna(0).to_numpy() == np.array([[0, 2**.5, -1, 1], [1, 0, 1, 1]])).all()
