#!/bin/bash
. ./.config.env
. ./clusters/env-common.sh
. ./clusters/summit/env.sh
mkdir -p ${PYTHONUSERBASE}

# Install requirements
pip install -r ./clusters/${SYSNAME}/requirements.txt

# Create a shebang stub that picks up the right environment
# when a native python script is submitted to LSF
PROXY=${VENV}/bin/python3-proxy
rm -rf ${PROXY}
echo "#!/bin/bash" > ${PROXY}
echo '. '`pwd`'/clusters/'${SYSNAME}'/env.sh' >> ${PROXY}
echo 'export PYTHONUSERBASE='`pwd`/${VENV} >> ${PROXY}
echo `which python3`' $@' >> ${PROXY}
chmod 755 ${PROXY}

# Perform common stuff
./clusters/init-common.sh
