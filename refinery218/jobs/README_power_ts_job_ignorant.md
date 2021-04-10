# Summit cluster level job agnostic time series for power features

## About
This is a description document for power consumption of timeseries dataset for Summit cluster level.
The datset dataset is generated using `power_ts_job_ignorant.py,` and the dataset is agnostic of the jobs
The dataset like other timeseries data has a frequency of 10s and the fields with description is given below:


-`timestamp`: timestamp 10 seconds interval
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
-`mean_ps0`: Mean of ps0 power values of all nodes at a particular time instance
-`std_ps0`: Std deviation of ps0 power values of all nodes at a particular time instance
-`max_ps0`: Maximum value of ps0 power values of all nodes at a particular time instance
-`min_ps0`: Minimum value of ps0 power values of all nodes at a particular time instance
-`mean_ps1`: Mean value of ps0 power values of all nodes at a particular time instance
-`std_ps1`: Std deviation of ps0 power values of all nodes at a particular time instance
-`max_ps1`: Maximum value of ps1 power values of all nodes at a particular time instance
-`min_ps1`: Minimum value of ps1 power values of all nodes at a particular time instance
-`mean_ps_1_2`: Mean value of sum of ps0 and ps1 columns across all nodes at a particular time instance
-`std_ps_1_2`: std deviation of sum of ps0 and ps1 columns across all nodes at a particular time instance
-`max_ps_1_2`: Maximum value of sum of ps0 and ps1 columns across all nodes at a particular time instance
-`min_ps_1_2`: Minimum value of sum of ps0 and ps1 columns across all nodes at a particular time instance
-`q25_inp`: 25% quantile  of the input power across all nodes at a particular time instance
-`q50_inp`: 50% quantile  of the input power across all nodes at a particular time instance
-`q75_inp`: 75% quantile  of the input power across all nodes at a particular time instance
-`q25_top`: 25% quantile  of the total power across all nodes at a particular time instance
-`q50_top`: 50% quantile  of the total power across all nodes at a particular time instance
-`q75_top`: 75% quantile  of the total power across all nodes at a particular time instance
-`q25_ps0`: 25% quantile  of the ps0 power across all nodes at a particular time instance
-`q50_ps0`: 50% quantile  of the ps0 power across all nodes at a particular time instance
-`q75_ps0`: 75% quantile  of the ps0 power across all nodes at a particular time instance
-`q25_ps1`: 25% quantile  of the ps1 power across all nodes at a particular time instance
-`q50_ps1`: 50% quantile  of the ps1 power across all nodes at a particular time instance
-`q75_ps1`: 75% quantile  of the ps1 power across all nodes at a particular time instance
-`q25_ps_1_2`: 25% quantile  of (ps1 + ps2) columns power across all nodes at a particular time instance
-`q50_ps_1_2`: 50% quantile  of (ps1 + ps2) columns power across all nodes at a particular time instance
-`q75_ps_1_2`: 75% quantile  of (ps1 + ps2) columns power across all nodes at a particular time instance
