# Andes environment for data analysis
export SYSNAME=andes
export VENV=.venv.andes
module load python/3.7-anaconda3
if [ -e ${VENV}/bin/activate ]; then
	. ${VENV}/bin/activate
fi
