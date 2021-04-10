# Summit job aware power timeseries

## About
This is a document for power  consumption timeseries dataset for Summit cluster allocation_id wise.
For every job that has run on Summit, the dataset provides its power consumptions at a frequency of 10 sec.
`power_ts_job_aware.py` script was used to generate the dataset.

Field description:
-`timestamp`: timestamp 10 seconds interval
-`allocation_id`: Corresponds to unique job that ran oin summit
-`count_hostname`: Number of hostname for which the data is avalialbe at particular time instance
-`count_node_name`: Number of hostname/nodename from scheduler data, this should be similar to count_hostname
-`job_sch_node_num`: Number of nodes calculated from job schedule data and should match count_hostname, and count_node_name
-`count_inp`: Input power count, number of nodes availble for sum operation excluding NaNs
-`size_inp`: Input power size, number of nodes availble for sum operation including NaNs 
-`sum_inp`: Sum of input power, sum of the input power values for all nodes 
-`count_top`: Total power count, number of nodes availble for sum operation excluding NaNs
-`size_top`: Total power size, number of nodes availble for sum operation including NaNs 
-`sum_top`: Sum of total power, sum of the total power values for all nodes 
-`mean_inp`: Mean of input power values of all nodes at a particular time instance
-`std_inp`: Std deviation of the input power values at a particular time instance
-`max_inp`: Maximum value of the input power across all nodes at a particular time instance
-`min_inp`: Minimum value of the input power across all nodes at a particular time instance
-`mean_top`: Mean of total power values of all nodes at a particular time instance
-`std_top`: Std deviation of the the total power values at a particular time instance
-`max_top`: Maximum value of the total power across all nodes at a particular time instance
-`min_top`: Minimum value of the total power across all nodes at a particular time instance
-`q25_inp`: 25% quantile  of the input power across all nodes at a particular time instance 
-`q50_inp`: 50% quantile  of the input power across all nodes at a particular time instance 
-`q75_inp`: 75% quantile  of the input power across all nodes at a particular time instance 
-`q25_top`: 25% quantile  of the total power across all nodes at a particular time instance 
-`q50_top`: 50% quantile  of the total power across all nodes at a particular time instance 
-`q75_top`: 75% quantile  of the total power across all nodes at a particular time instance 
