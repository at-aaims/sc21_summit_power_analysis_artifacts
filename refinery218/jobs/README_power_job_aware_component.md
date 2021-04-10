# CPU and GPU power component usage for jobs in Summit

## About
This is a document for power component (cpus and gpus) consumption allocation_id wise  dataset for Summit cluster.
Every row in the dataset corresponds to a unique allocation_id
The script used for this dataset is `power_job_aware_component.py`

Fields description:
-`allocation_id`: Corresponds to unique job that ran on summit (2019-12-28 to 2021-01-29)
-`begin_time`: Begin time of job
-`end_time`: End time of job
-`mean_mean_cpu_pwr`: Mean cpu power per node
-`max_mean_cpu_pwr`: Maximum of the mean cpu power across nodes
-`max_max_cpu_pwr`: Maximmum of the cpu power for a job
-`mean_mean_gpu_pwr`: Mean of GPU power
-`max_mean_gpu_pwr`: Maximum of the mean GPU power
-`max_max_gpu_pwr`: Maximum of the GPU power for a job
