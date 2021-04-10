# Jobwise Summit power dataset

## About
This is a document for power consumption dataset allocation_id/jobid wise for Summit cluster.
Every row in the dataset crosseponds to unique allocation id
The script useed for the dataset is `power_job_aware.py`

Field description:
-`allocation_id`: Corresponds to a unique job that ran on Summit
-`begin_time`: Begin time of a job
-`end_time`: End time of a job
-`max_sum_top`: Maximum of sum of total power for a job across all nodes on which job has run
-`max_sum_inp`: Maximum of sum of input power for a job across all nodes on which job has run
-`mean_sum_inp`: Mean of sum of input power for a job across all nodes on which job has run
-`max_max_inp`: Maximum of max input power for a job
-`min_min_inp`: Minimum of min input power for a job
-`hosts_count`: Number of nodes for which we have data (should be similar to host size)
-`hosts_size`: Number of nodes on which job has run, include nodes with NaN values

