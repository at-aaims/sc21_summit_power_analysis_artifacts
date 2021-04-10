# Jobwise Summit energy dataset

## About
This is a document for energy consumption dataset allocation_id/jobid wise for Summit cluster.
Every row in the dataset crosseponds to unique allocation id
The script useed for the dataset is `job_energy.py`

Field description:
-`allocation_id`: Corresponds to a unique job that ran on Summit
-`begin_time`: Begin time of a job
-`end_time`: End time of a job
-`primary_job_id`: Jobid of jobs
-`energy`: Total energy consumed by jobs
-`gpu_energy`: GPU energy consumed by jobs
-`num_nodes`: Number of nodes on whihc jobs run
-`num_gpus`: Number of GPUs used
-`job_domain`: Domain Science of jobs


