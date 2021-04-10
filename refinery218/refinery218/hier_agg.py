from functools import partial
from itertools import chain
from copy import copy
import pandas as pd
import dask.dataframe as dd


def rename(df):
    '''Rename columns to allow element-wise operations between dataframes of the same size.'''
    return df.rename(columns={col: i for i, col in enumerate(df.columns)})


def get_vars_from_agg_cols(df_agg, group_cols=[]):
    '''Extract variable names from column names in aggregated data.'''
    return pd.unique(['.'.join(col.split('.')[:-1]) for col in df_agg.columns if col not in group_cols])


def make_agg_cols(variables, agg_type):
    '''Compile column names for given aggregate type.'''
    return [f'{x}.{agg_type}' for x in variables]


def coarsen_time_col(df, time_res, time_col):
    '''Create copy of data with datetime column coarsened to given time resolution.'''
    df = copy(df)
    df[time_col] = df[time_col].dt.floor(time_res)

    return df


def normalize_index(df, cols):
    '''Ensure that none of given columns are part of dataframe index.'''
    if set(cols) - set(df.columns):
        df = df.reset_index()

    return df


def init_agg(df_raw, time_res, time_col,
             before_group_cols=[], after_group_cols=[],
             aligned=False,
             split_out=10):
    '''
    Aggregate raw data to given time resolution to allow subsequent coarsening
    and/or producing mean, std, min, and max aggregates.

    Parameters
    ----------
    df_raw : dataframe
    time_res : str
        Time resolution, e.g. '15s' or '2min'.
    time_col : str or None
        Name of datetime column.
    before_group_cols : list of str, optional
        Names of columns to aggregate by before datetime column.
    after_group_cols : list of str, optional
        Names of columns to aggregate by after datetime column.
    aligned : True or False
        Whether we can rely on sorted and aligned partitions for optimization
    split_out: defaults to 10 (> 0)
        Applicable only when aligned == False.  Controls the # of partitions of
        the internal groupby method

    Returns
    -------
    dataframe
        Sum, count, total squared deviation, min, and max aggregates.
    '''
    # Coarse datetime column to given time resolution.
    group_cols = before_group_cols + [time_col] + after_group_cols
    df_raw = normalize_index(df_raw, group_cols)
    df_raw = coarsen_time_col(df_raw, time_res, time_col)

    # Produce intermediate aggregates at the given time resolution.
    if not aligned:
        df_agg = df_raw.groupby(group_cols).agg(
            ['count', 'sum', 'var', 'max', 'min'], split_out=split_out
        )
    else:
        df_agg = df_raw.map_partitions(
            lambda part: part.groupby(group_cols).agg(
                ['count', 'sum', 'var', 'max', 'min']
            )
        )

    # Flatten column names of the intermediate aggregates.
    variables = [col for col in df_raw.columns if col not in group_cols]
    cols = partial(make_agg_cols, variables)
    df_agg.columns = chain.from_iterable(
        zip(cols('count'), cols('sum'), cols('tot_sq_dev'), cols('min'), cols('max')))

    # Change variance to total squared deviation.
    tot_sq_devs = rename(df_agg[cols('tot_sq_dev')])
    counts = rename(df_agg[cols('count')])
    df_agg[cols('tot_sq_dev')] = tot_sq_devs.mask(counts == 1, other=0) * (counts - 1)
    
    # Change zero sums to NaNs as needed.
    sums = rename(df_agg[cols('sum')])
    df_agg[cols('sum')] = sums.mask(counts == 0)
    
    return df_agg


def coarsen_agg(df_agg, time_res, time_col, before_group_cols=[], after_group_cols=[]):
    '''
    Coarsen time resolution of losslessly aggregated data.

    Parameters
    ----------
    df_agg : dataframe
        Sum, count, total squared deviation, min, and max aggregates.
    time_res : str
        Coarser time resolution (e.g. '15s' or '2min'), ideally a multiple of current one.
    time_col : str
        Name of datetime column.
    before_group_cols : list of str, optional
        Names of columns to aggregate by before datetime column.
    after_group_cols : list of str, optional
        Names of columns to aggregate by after datetime column.

    Returns
    -------
    dataframe
        Sum, count, total squared deviation, min, and max aggregates at coarser time resolution.
    '''
    # Coarse datetime column to given time resolution.
    group_cols = before_group_cols + [time_col] + after_group_cols
    df_agg = normalize_index(df_agg, group_cols)
    df_agg = coarsen_time_col(df_agg, time_res, time_col)

    # Produce coarsened mean broadcasted to original shape.
    variables = get_vars_from_agg_cols(df_agg, group_cols=group_cols)
    cols = partial(make_agg_cols, variables)
    df_agg_grouped = df_agg.groupby(group_cols)
    coarse_counts = rename(df_agg_grouped[cols('count')].transform('sum', meta=df_agg[cols('count')]._meta))
    coarse_sums = rename(df_agg_grouped[cols('sum')].transform('sum', meta=df_agg[cols('sum')]._meta))
    coarse_means = coarse_sums / coarse_counts

    # Produce total squared deviation with respect to coarsened mean.
    counts = rename(df_agg[cols('count')])
    sums = rename(df_agg[cols('sum')])
    tot_sq_devs = rename(df_agg[cols('tot_sq_dev')])
    df_agg[cols('tot_sq_dev')] = tot_sq_devs + counts * (sums / counts - coarse_means)**2

    # Coarsen lossless aggregates.
    cols_to_aggregate = chain.from_iterable(
        zip(cols('count'), cols('sum'), cols('tot_sq_dev'), cols('min'), cols('max')))
    agg_functions = ['sum', 'sum', 'sum', 'min', 'max'] * len(cols(''))
    df_coarse = df_agg.groupby(group_cols).agg(dict(zip(cols_to_aggregate, agg_functions)))

    return df_coarse


def finalize_agg(df_agg, group_cols=[]):
    '''
    Compute final aggregates from lossless ones.

    Parameters
    ----------
    df_agg : dataframe
        Sum, count, total squared deviation, min, and max aggregates.
    group_cols : list of str, optional
        Names of datetime and other group-by columns that are not part of index.

    Returns
    -------
    dataframe
        Mean, standard deviation, min, and max aggregates.
    '''
    # Extract group-by columns that are not part of index.
    group_cols = [col for col in group_cols if col in df_agg.columns]
    groups = df_agg[group_cols]

    # Produce means and standard deviations.
    variables = get_vars_from_agg_cols(df_agg, group_cols=group_cols)
    cols = partial(make_agg_cols, variables)
    counts = rename(df_agg[cols('count')])
    sums = rename(df_agg[cols('sum')])
    tot_sq_devs = rename(df_agg[cols('tot_sq_dev')])
    means = (sums / counts).rename(
        columns={old: new for old, new in zip(counts.columns, cols('mean'))})
    stds = ((tot_sq_devs / (counts - 1))**.5).rename(
        columns={old: new for old, new in zip(counts.columns, cols('std'))})

    # Compose final aggregates and reoder them by variable.
    df_final = dd.concat(
        [groups, means, stds, df_agg[cols('min') + cols('max')]], axis=1, ignore_unknown_divisions=True)
    df_final = df_final[group_cols + list(
        chain.from_iterable(zip(cols('mean'), cols('std'), cols('min'), cols('max'))))]

    return df_final
