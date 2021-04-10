# Summit thermal cluster-level time series for component types

## About

Contains information about Summit GPU failures along with their thermal extremity. Spans 2019-12-27 through 2021-01-31. Obtained using `gpu-failures-thermal.py`.

## Structure

- `orig_idx`: failure id in the raw failure data
- `timestamp`: time of failure rounded to 10-second time resolution
- `hostname`: node
- `xid`: NVIDIA error code
- `GPU`: GPU placement within a node
- `exact`: time of failure
- `temp`: temperature of the offending GPU at the time of failure
- `z_score`: z-score of temperature of the offending GPU at the time of failure
- `mean`: average GPU temperature across all GPUs within the job at the time of failure
- `std`: standard deviation of GPU temperature across all GPUs within the job at the time of failure
- `processed`: whether the thermal information about failure has been already populated for this row