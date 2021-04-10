# Summit thermal cluster-level time series for component types

## About

Contains cluster-level Summit thermal measurements, aggregated across components of the same type at each 10s timestep. The resulting time series of the aggregates is combined with interpolated CEP data. Spans 2019-12-27 through 2021-01-31. Obtained using `thermal-cluster-comptype.py` based on `timestamp`.

## Structure

- `timestamp`: 10-second time resolution
- `gpu_core.mean`: average temperature across all GPU cores in a cluster
- `gpu_core.std`: standard deviation of temperature across all GPU cores in a cluster
- `gpu_core.min`: minimum temperature across all GPU cores in a cluster
- `gpu_core.max`: maximum temperature across all GPU cores in a cluster
- `gpu_core.q25`: 25th-percentile temperature across all GPU cores in a cluster
- `gpu_core.q50`: median temperature across all GPU cores in a cluster
- `gpu_core.q75`: 75th-percentile temperature across all GPU cores in a cluster
- `gpu_core.count`: number of non-empty temperature measurements from all GPU cores in a cluster
- `gpu_core.size`: number of all temperature measurements from all GPU cores in a cluster
- `gpu_mem.mean`: average temperature across all GPU memorys in a cluster
- `gpu_mem.std`: standard deviation of temperature across all GPU memorys in a cluster
- `gpu_mem.min`: minimum temperature across all GPU memorys in a cluster
- `gpu_mem.max`: maximum temperature across all GPU memorys in a cluster
- `gpu_mem.q25`: 25th-percentile temperature across all GPU memorys in a cluster
- `gpu_mem.q50`: median temperature across all GPU memorys in a cluster
- `gpu_mem.q75`: 75th-percentile temperature across all GPU memorys in a cluster
- `gpu_mem.count`: number of non-empty temperature measurements from all GPU memories in a cluster
- `gpu_mem.size`: number of all temperature measurements from all GPU memories in a cluster
- `dimm.mean`: average temperature across all DIMMs in a cluster
- `dimm.std`: standard deviation of temperature across all DIMMs in a cluster
- `dimm.min`: minimum temperature across all DIMMs in a cluster
- `dimm.max`: maximum temperature across all DIMMs in a cluster
- `dimm.q25`: 25th-percentile temperature across all DIMMs in a cluster
- `dimm.q50`: median temperature across all DIMMs in a cluster
- `dimm.q75`: 75th-percentile temperature across all DIMMs in a cluster
- `dimm.count`: number of non-empty temperature measurements from all DIMMs in a cluster
- `dimm.size`: number of all temperature measurements from all DIMMs in a cluster
- `cpu_core.mean`: average temperature across all CPU cores in a cluster
- `cpu_core.std`: standard deviation of temperature across all CPU cores in a cluster
- `cpu_core.min`: minimum temperature across all CPU cores in a cluster
- `cpu_core.max`: maximum temperature across all CPU cores in a cluster
- `cpu_core.q25`: 25th-percentile temperature across all CPU cores in a cluster
- `cpu_core.q50`: median temperature across all CPU cores in a cluster
- `cpu_core.q75`: 75th-percentile temperature across all CPU cores in a cluster
- `cpu_core.count`: number of non-empty temperature measurements from all CPU cores in a cluster
- `cpu_core.size`: number of all temperature measurements from all CPU cores in a cluster
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
