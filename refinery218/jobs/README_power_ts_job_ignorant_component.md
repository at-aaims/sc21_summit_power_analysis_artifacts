# Summit cluster level CPU and GPU component power timeseries

## About
This is a document for generating power component (cpus and gpus) consumption timeseries dataset for Summit cluster.
The script used for generating this dataset is `power_ts_job_ignorant_component.py`.
The datset dataset has a frequency of 10s and the fields with description are given below:

        
-`timestamp`: timestamp 10 seconds interval (2019-12-28 to 2021-01-30)
-`mean_cpu_power`: Mean value of the cpu powers of all nodes(Note: every node has two cpus)
-`std_cpu_power`: Std deviation of the cpu powers of all nodes(Note: every node has two cpus)
-`min_cpu_power`: Minimum value of the cpu powers of all nodes(Note: every node has two cpus)
-`max_cpu_power`: Maximum value of the cpu powers of all nodes(Note: every node has two cpus)
-`q25_cpu_power`: 25 percent quantile of the cpu powers of all nodes(Note: every node has two cpus)
-`q50_cpu_power`: 50 percent quantile of the cpu powers of all nodes(Note: every node has two cpus)
-`q75_cpu_power`: 75  percent quantile of the cpu powers of all nodes(Note: every node has two cpus)
-`count_cpu_power`: Number nodes for whuch cpu power is available for a particular allocation_id at particular time instance
-`size_cpu_power`: Including NaNs, number nodes for whuch cpu power is available for a particular allocation_id at particular time instance
-`cpu_nans`: Number of nans across cpus
-`mean_gpu_power`: Mean value of the gpu powers of all nodes(Note: every node has six gpus)
-`std_gpu_power`: Std deviation value of the gpu powers of all nodes(Note: every node has six gpus)
-`min_gpu_power`: Minimum value of the gpu powers of all nodes(Note: every node has six gpus)
-`max_gpu_power`: Maximum value of the gpu powers of all nodes(Note: every node has six gpus)
-`q25_gpu_power`: 25 percent quantile of the gpu powers of all nodes(Note: every node has six gpus)
-`q50_gpu_power`: 50 percent quantile of the gpu powers of all nodes(Note: every node has six gpus)
-`q75_gpu_power`: 75  percent quantile of the gpu powers of all nodes(Note: every node has six gpus)
-`count_gpu_power`: Number nodes for whuch gpu power is available for a particular allocation_id at particular time instance
-`size_gpu_power`:Including NaNs, Number nodes for whuch gpu power is available for a particular allocation_id at particular time instance
-`gpu_nans`: Number of nans across gpus
