# DTN Environment for data analysis
export SYSNAME=dtn
export VENV=.venv.dtn
module load python/3.7.0-anaconda3-2018.12
if [ -e ${VENV}/bin/activate ]; then
	. ${VENV}/bin/activate
fi
