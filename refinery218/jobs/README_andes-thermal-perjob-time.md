# Summit thermal per-node job-level time series

## About

Contains per-node job-level aggregated Summit thermal time series starting from December 2019.
Obtained using `andes-thermal-perjob-time.py` 

## Structure

- `timestamp`: 10-second time resolution
- `allocation_id`: job id
- `hostname`: number of nodes
- `any_nan`: number of nodes with any NaN temperatures
- `hot_gpus`: list of node with GPU components > 83ºC (encoded as 0 if ≤83ºC and 1 if >83ºC)
- `n_cores_band0`: number of GPU cores in temperature band 0
- `n_cores_band1`: number of GPU cores in temperature band 1
- `n_cores_band2`: number of GPU cores in temperature band 2
- `n_cores_band3`: number of GPU cores in temperature band 3
- `n_cores_band4`: number of GPU cores in temperature band 4
- `n_mem_band0`: number of GPU memories in temperature band 0
- `n_mem_band1`: number of GPU memories in temperature band 1
- `n_mem_band2`: number of GPU memories in temperature band 2
- `n_mem_band3`: number of GPU memories in temperature band 3
- `n_mem_band4`: number of GPU memories in temperature band 4
- `mtwst`: MTW supply temperature in ˚F
- `cep_mtw_tons`: Cooling provided with the MTW (from chillers + cooling tower) in tons
- `almk100_leakdetection_alarm`: Leak detection alarm
- `k100_pue`: K100 PUE calculation
- `cep_cooling_tower_tons`: Cooling provided from the cooling tower in tons
- `cep_outside_air_dry_bulb_temp`: Outside air dry bulb temperature measured from the CEP
- `cep_outside_air_dew_point_temp`: Outside air dew point temperature measured from the CEP
- `cep_make_up_flow`: Make up flow
- `cep_outside_air_wet_bulb_temp`: Outside air wet bulb temperature measured from the CEP
- `k100_space_temp_4`: K100 Space temperature sensor 4
- `k100_space_temp_2`: K100 Space temperature sensor 2
- `mt_loop_diff_press`: MT loop differential pressure
- `cep_chilled_water_tons`: Cooling provided from the chilled water in tons
- `chw_flowrate`: Chilled water (5600) flowrate 
- `almepo_shutdown_alarm`: Emergency power off alarm
- `cep_outside_air_rh`: Outside air relative humidity measured from the CEP
- `almmtw_makeup_emergency_shutdown_alarm`: Makeup emergency shutdown alarm
- `k100_total_power`: K100 Total power measured from the MSBs
- `cep_kw_per_ton`: CEP Efficiency - kW used per cooling ton
- `mtwflw`: MTW supply flow in gallons per minute
- `k100_space_temp_1`: K100 Space temperature sensor 1
- `ct_water_flowrate`: Cooling tower water flowrate
- `k100_space_temp_3`: K100 Space temperature sensor 3
- `mtwrt`: MTW return temperature temperature in ˚F
