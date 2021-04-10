# Summit environment for data analytics
export SYSNAME=summit
export VENV=.venv.summit
module load ums
module load ums-gen119
module load nvidia-rapids/0.16
export PYTHONUSERBASE=`pwd`/${VENV}
export PATH=${PYTHONUSERBASE}/bin:${PATH}
export OMP_PROC_BIND=FALSE
