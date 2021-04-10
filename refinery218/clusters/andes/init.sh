#!/bin/bash
. ./.config.env
. ./clusters/env-common.sh
. ./clusters/andes/env.sh

# Install virtualenv
echo "@ initializing python virtual environment at ${VENV}"
if [ ! -e ${VENV} ]; then
	. ./clusters/${SYSNAME}/env.sh
	python3 -m venv --prompt ${REPO_NAME}.${SYSNAME} ${VENV}
	${VENV}/bin/pip install --upgrade pip
fi
# Install requirements
${VENV}/bin/pip install -r ./clusters/${SYSNAME}/requirements.txt

# Perform common stuff
./clusters/init-common.sh
