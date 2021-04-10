"""
schema.py: Schema repository for known datasets for the refinery
"""
import pyarrow as pa
import pyarrow.types as pt

#
# Utilities
#


def get_pa_type_factory(pa_type):
    """return type factory"""
    if pt.is_int8(pa_type):
        return pa.int8
    elif pt.is_int16(pa_type):
        return pa.int16
    elif pt.is_int32(pa_type):
        return pa.int32
    elif pt.is_int64(pa_type):
        return pa.int64

    elif pt.is_uint8(pa_type):
        return pa.uint8
    elif pt.is_uint16(pa_type):
        return pa.uint16
    elif pt.is_uint32(pa_type):
        return pa.uint32
    elif pt.is_uint64(pa_type):
        return pa.uint64

    elif pt.is_float32(pa_type):
        return pa.float64
    elif pt.is_float64(pa_type):
        return pa.float64

    return pa_type


def widen_pa_type(pa_type):
    """Given the type, return a type one step wider"""
    if pt.is_int8(pa_type):
        return pa.int16
    elif pt.is_int16(pa_type):
        return pa.int32
    elif pt.is_int32(pa_type):
        return pa.int64

    elif pt.is_uint8(pa_type):
        return pa.uint16
    elif pt.is_uint16(pa_type):
        return pa.uint32
    elif pt.is_uint32(pa_type):
        return pa.uint64

    elif pt.is_float32(pa_type):
        return pa.float64
    elif pt.is_float64(pa_type):
        return pa.float64

    return pa_type


#
# Schema used for Summit's OpenBMC Telemetry Stream data
#


summit_openbmc_telemetry_fields = [
    # Meta info
    pa.field('timestamp', pa.timestamp('s', tz="UTC")),
    pa.field('source', pa.string()),
    pa.field('node_state', pa.string()),

    # Hostname
    pa.field('hostname', pa.string()),

    # GPU power
    pa.field("p0_gpu0_power", pa.int16()),
    pa.field("p0_gpu1_power", pa.int16()),
    pa.field("p0_gpu2_power", pa.int16()),
    pa.field("p1_gpu0_power", pa.int16()),
    pa.field("p1_gpu1_power", pa.int16()),
    pa.field("p1_gpu2_power", pa.int16()),

    # CPU power
    pa.field("p0_power", pa.int16()),
    pa.field("p1_power", pa.int16()),

    # Node power
    pa.field("total_power", pa.int16()),
    pa.field("io_power", pa.int16()),
    pa.field("fan_disk_power", pa.int16()),
    pa.field("p0_io_power", pa.int16()),
    pa.field("p1_io_power", pa.int16()),
    pa.field("p0_mem_power", pa.int16()),
    pa.field("p1_mem_power", pa.int16()),

    # GPU thermals
    pa.field("gpu0_core_temp", pa.int8()),
    pa.field("gpu0_mem_temp", pa.int8()),
    pa.field("gpu1_core_temp", pa.int8()),
    pa.field("gpu1_mem_temp", pa.int8()),
    pa.field("gpu2_core_temp", pa.int8()),
    pa.field("gpu2_mem_temp", pa.int8()),
    pa.field("gpu3_core_temp", pa.int8()),
    pa.field("gpu3_mem_temp", pa.int8()),
    pa.field("gpu4_core_temp", pa.int8()),
    pa.field("gpu4_mem_temp", pa.int8()),
    pa.field("gpu5_core_temp", pa.int8()),
    pa.field("gpu5_mem_temp", pa.int8()),

    # DIMM temperature
    pa.field("dimm0_temp", pa.int8()),
    pa.field("dimm1_temp", pa.int8()),
    pa.field("dimm2_temp", pa.int8()),
    pa.field("dimm3_temp", pa.int8()),
    pa.field("dimm4_temp", pa.int8()),
    pa.field("dimm5_temp", pa.int8()),
    pa.field("dimm6_temp", pa.int8()),
    pa.field("dimm7_temp", pa.int8()),
    pa.field("dimm8_temp", pa.int8()),
    pa.field("dimm9_temp", pa.int8()),
    pa.field("dimm10_temp", pa.int8()),
    pa.field("dimm11_temp", pa.int8()),
    pa.field("dimm12_temp", pa.int8()),
    pa.field("dimm13_temp", pa.int8()),
    pa.field("dimm14_temp", pa.int8()),
    pa.field("dimm15_temp", pa.int8()),

    # p0 core temperature
    pa.field("p0_core0_temp", pa.int8()),
    pa.field("p0_core1_temp", pa.int8()),
    pa.field("p0_core2_temp", pa.int8()),
    pa.field("p0_core3_temp", pa.int8()),
    pa.field("p0_core4_temp", pa.int8()),
    pa.field("p0_core5_temp", pa.int8()),
    pa.field("p0_core6_temp", pa.int8()),
    pa.field("p0_core7_temp", pa.int8()),
    pa.field("p0_core8_temp", pa.int8()),
    pa.field("p0_core9_temp", pa.int8()),
    pa.field("p0_core10_temp", pa.int8()),
    pa.field("p0_core11_temp", pa.int8()),
    pa.field("p0_core12_temp", pa.int8()),
    pa.field("p0_core14_temp", pa.int8()),
    pa.field("p0_core15_temp", pa.int8()),
    pa.field("p0_core16_temp", pa.int8()),
    pa.field("p0_core17_temp", pa.int8()),
    pa.field("p0_core18_temp", pa.int8()),
    pa.field("p0_core19_temp", pa.int8()),
    pa.field("p0_core20_temp", pa.int8()),
    pa.field("p0_core21_temp", pa.int8()),
    pa.field("p0_core22_temp", pa.int8()),
    pa.field("p0_core23_temp", pa.int8()),

    # p1 core temperature
    pa.field("p1_core0_temp", pa.int8()),
    pa.field("p1_core1_temp", pa.int8()),
    pa.field("p1_core2_temp", pa.int8()),
    pa.field("p1_core3_temp", pa.int8()),
    pa.field("p1_core4_temp", pa.int8()),
    pa.field("p1_core5_temp", pa.int8()),
    pa.field("p1_core6_temp", pa.int8()),
    pa.field("p1_core7_temp", pa.int8()),
    pa.field("p1_core8_temp", pa.int8()),
    pa.field("p1_core9_temp", pa.int8()),
    pa.field("p1_core10_temp", pa.int8()),
    pa.field("p1_core11_temp", pa.int8()),
    pa.field("p1_core12_temp", pa.int8()),
    pa.field("p1_core14_temp", pa.int8()),
    pa.field("p1_core15_temp", pa.int8()),
    pa.field("p1_core16_temp", pa.int8()),
    pa.field("p1_core17_temp", pa.int8()),
    pa.field("p1_core18_temp", pa.int8()),
    pa.field("p1_core19_temp", pa.int8()),
    pa.field("p1_core20_temp", pa.int8()),
    pa.field("p1_core21_temp", pa.int8()),
    pa.field("p1_core22_temp", pa.int8()),
    pa.field("p1_core23_temp", pa.int8()),

    # power supply temperature
    pa.field("p0_vcs_temp", pa.int8()),
    pa.field("p0_vdd_temp", pa.int8()),
    pa.field("p0_vddr_temp", pa.int8()),
    pa.field("p0_vdn_temp", pa.int8()),
    pa.field("p1_vcs_temp", pa.int8()),
    pa.field("p1_vdd_temp", pa.int8()),
    pa.field("p1_vddr_temp", pa.int8()),
    pa.field("p1_vdn_temp", pa.int8()),

    # MISC thermal
    pa.field("ambient", pa.int8()),
    pa.field("pcie", pa.int8()),

    # power supplies
    pa.field("ps0_input_power", pa.int16()),
    pa.field("ps0_input_voltage", pa.float32()),
    pa.field("ps0_output_current", pa.float32()),
    pa.field("ps0_output_voltage", pa.float32()),
    pa.field("ps1_input_power", pa.int16()),
    pa.field("ps1_input_voltage", pa.float32()),
    pa.field("ps1_output_current", pa.float32()),
    pa.field("ps1_output_voltage", pa.float32()),
]


def get_summit_openbmc_telemetry_schema():
    """Schema of IBM AC922L Witherspoon OpenBMC Telemetry Stream"""
    return pa.schema(summit_openbmc_telemetry_fields)



def hier_agg_lossless_schema(original_schema):
    """Infer the correct output schema as a result of init_agg in hier_agg

    sum: cast upwards width
    tot_sq_dev: cast upwards width
    max: same type
    min: same type
    """
    # Iterate original schema and infer new ones
    new_schema_lst = [
        pa.field('timestamp', pa.timestamp('s', tz="UTC")),
        pa.field('hostname', pa.string()),
    ]
    for field in original_schema:
        if  field.name in ['timestamp', 'hostname', 'node_state', 'source']:
            continue
        new_schema_lst.append(pa.field(f"{field.name}.count", widen_pa_type(field.type)()))
        new_schema_lst.append(pa.field(f"{field.name}.sum", widen_pa_type(field.type)()))
        if field.type.bit_width < 32:
            new_schema_lst.append(pa.field(f"{field.name}.tot_sq_dev", pa.float32()))
        else:
            new_schema_lst.append(pa.field(f"{field.name}.tot_sq_dev", pa.float64()))
        new_schema_lst.append(pa.field(f"{field.name}.max", get_pa_type_factory(field.type)()))
        new_schema_lst.append(pa.field(f"{field.name}.min", get_pa_type_factory(field.type)()))
    return pa.schema(new_schema_lst)


def hier_agg_final_schema(original_schema):
    """Infer the correct output schema

    # if original is float
    sum: cast upwards width
    tot_sq_dev: cast upwards width
    max: same type
    min: same type

    # If original is integer
    sum: cast upwards width
    """
    # Iterate original schema and infer new ones
    new_schema_lst = [
        pa.field('timestamp', pa.timestamp('s', tz="UTC")),
        pa.field('hostname', pa.string()),
    ]
    for field in original_schema:
        if  field.name in ['timestamp', 'hostname', 'node_state', 'source']:
            continue
        if field.type.bit_width < 32:
            new_schema_lst.append(pa.field(f"{field.name}.mean", pa.float32()))
            new_schema_lst.append(pa.field(f"{field.name}.std", pa.float32()))
        else:
            new_schema_lst.append(pa.field(f"{field.name}.mean", pa.float64()))
            new_schema_lst.append(pa.field(f"{field.name}.std", pa.float64()))
        new_schema_lst.append(pa.field(f"{field.name}.max", get_pa_type_factory(field.type)()))
        new_schema_lst.append(pa.field(f"{field.name}.min", get_pa_type_factory(field.type)()))
    return pa.schema(new_schema_lst)

