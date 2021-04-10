import os
import pytest
from refinery218.utils import lmod_module_load, lmod_module_unload


def test_lmod_if_available():
    lmod_system_name = os.environ.get('LMOD_SYSTEM_NAME', None)
    if not lmod_system_name:
        # We just return here assuming we're running the test on a 
        # place where we do have LMOD
        return
    # We try darshan which we know that exists in the clusters we know
    assert(os.system('file `which darshan-parser`') != 0)
    lmod_module_load(lmod_system_name, "darshan-util")
    assert(os.system('file `which darshan-parser`') == 0)
    lmod_module_unload(lmod_system_name, "darshan-util")
    assert(os.system('file `which darshan-parser`') != 0)
