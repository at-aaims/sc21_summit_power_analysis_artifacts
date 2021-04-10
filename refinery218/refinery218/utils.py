"""
Common utilities
"""
import os
import sys
import socket
from datetime import datetime, timedelta
from loguru import logger


SCRATCH_BASE = None


LMOD_LOCATIONS = {
    "summit": {
        "lmod": "/sw/summit/lmod/lmod/init",
        "modulepath": "/sw/summit/modulefiles"
    },
    "andes": {
        "lmod": "/sw/andes/lmod/lmod/init",
        "modulepath": "/sw/andes/modulefiles"
    }
}


def lmod_module_init(cluster_name):
    try:
        sys.path.insert(0, LMOD_LOCATIONS[cluster_name]["lmod"])
        from env_modules_python import module
        module('use', LMOD_LOCATIONS[cluster_name]["modulepath"])
    except KeyError as e:
        raise RuntimeError(f'unknown cluster name {cluster_name}')


def lmod_module_load(cluster_name, module_name):
    """Wrapper for lmod 'module' to load a module from the cluster sw repo"""
    try:
        sys.path.insert(0, LMOD_LOCATIONS[cluster_name]["lmod"])
        from env_modules_python import module
        module('use', LMOD_LOCATIONS[cluster_name]["modulepath"])
    except KeyError as e:
        raise RuntimeError(f'unknown cluster name {cluster_name}')
    module('load', module_name)
    logger.info(f"loaded {module_name} from {cluster_name}")


def lmod_module_unload(cluster_name, module_name):
    """Wrapper for lmod 'module' to unload a module"""
    try:
        sys.path.insert(0, LMOD_LOCATIONS[cluster_name]["lmod"])
        from env_modules_python import module
        module('use', LMOD_LOCATIONS[cluster_name]["modulepath"])
    except KeyError as e:
        raise RuntimeError(f'unknown cluster name {cluster_name}')
    module('unload', module_name)


def date_range(start_date, end_date):
    """Get dates from start to end"""
    return [(start_date + timedelta(n))
            for n in range(int ((end_date - start_date).days) + 1)]


def set_scratch_base(scratch_base):
    """Set the scratch base"""
    global SCRATCH_BASE
    SCRATCH_BASE = scratch_base


def get_scratch_base():
    """Get the scratch base"""
    global SCRATCH_BASE
    return SCRATCH_BASE


def scratch(key, clean=False):
    """Get the scratch directory ready"""
    if SCRATCH_BASE is None:
        raise RuntimeError("No scratch base set")
    tmpdir = os.path.join(SCRATCH_BASE, key)
    if clean:
        os.rmdir(tmpdir)
    if not os.access(tmpdir, os.F_OK):
        os.makedirs(tmpdir, exist_ok=False)
    return tmpdir


def scratch_free(key):
    """Release and deallocate the scratch"""
    if SCRATCH_BASE is None:
        raise RuntimeError("No scratch base set")
    tmpdir = os.path.join(SCRATCH_BASE, key)
    os.system(f"rm -rf {tmpdir}")
    return tmpdir


def scratch_meta(key):
    """Get the meta directory prepared and ready for use"""
    if SCRATCH_BASE is None:
        raise RuntimeError("No scratch base set")
    metadir = os.path.join(scratch_base, key, ".meta")
    os.makedirs(tmpdir, exist_ok=False)
    return metadir

