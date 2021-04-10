"""
setup.py
"""
import os
import sys
import re
import subprocess
from collections import defaultdict
from functools import cmp_to_key
from pkg_resources import parse_version
from setuptools import setup, find_packages
from codecs import open
from os import path


here = path.abspath(".")


#
# Helpers
#


VERSION = "0.0.1"


def version():
    """
    Retrieve the version
    """
    return VERSION


#
# Learn much from the configuration file
#


config = {}
with open(os.path.join(here, '.config.env'), encoding='utf-8') as f:
    for raw_line in f.readlines():
        line = raw_line.strip()
        if not line or line[0] == '#':
            continue
        tokens = line.split('=')
        key = tokens[0].strip()
        value = tokens[1].strip()
        config[key] = value


def slugify(raw_string):
    def filter_nonalpha(ch):
        if ch.isalnum():
            return ch
        return ''
    return ''.join(list(map(filter_nonalpha, raw_string)))


# Globals from the config
repo_org = config['REPO_ORG']
repo_name = config['REPO_NAME']
account = config['ACCOUNT']
group = config['GROUP']
module_name = slugify(repo_name)
author_name = config['OWNER_NAME']
author_email = config['OWNER_EMAIL']


# Get the long description from the README file
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


# Get the short description from the README file (it is the title of the text)
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    short_description = f.readline()


# Read the requirements, filtered with things setuptools could understand
requirements = list()
REQUIREMENTS_FN = os.path.join(here, "requirements.txt")
if os.access(REQUIREMENTS_FN, os.F_OK):
    with open(REQUIREMENTS_FN) as f:
        for line in f:
            line_split = line.split()
            if not line_split or line_split[0].startswith('#'):
                continue
            requirements.append(line.split()[0])

# Setup
setup(
    name=module_name,
    description=short_description,
    long_description=long_description,
    version=version(),
    url=f'https://github.com/{repo_org}/{repo_name}',
    author=author_name,
    author_email=author_email,
    license='MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: System :: Systems Administration',
        'Topic :: Terminals',
        'Topic :: Utilities',
    ],
    keywords='Data management, Data Analytics',
    install_requires=requirements,
    packages=[module_name],
    package_dir={
        module_name: f'./{module_name}'
    },
)
