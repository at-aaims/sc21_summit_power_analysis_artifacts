"""
olcf.py - OLCF site specific native python batch script drivers """
import os
import sys
import time
import subprocess
import selectors
from loguru import logger
from .utils import set_scratch_base as original_set_scratch_base
from .utils import get_scratch_base as original_get_scratch_base


#
# Globals
#


WATCHDOG_HEARTBEAT_STRING = "WATCHDOG_HEARTBEAT_STRING_WATCHDOG_HEARTBEAT_STRING"


def watchdog_heartbeat():
    """Watchdog heartbeat"""
    print(WATCHDOG_HEARTBEAT_STRING)


#
# OLCF environment setup
#


def set_project(project):
    """Set the project"""
    global PROJECT
    PROJECT = project


def get_project():
    """get project from what the user have set"""
    global PROJECT
    if PROJECT is None:
        raise RuntimeError("No projects set")
    return PROJECT


def get_user():
    """get username"""
    user = os.environ.get('USER', None)
    if user is None:
        raise RuntimeError("Cannot determine user name")
    return user


def set_scratch_base(app_name='default', project=None, user=None):
    """Setting scratch base"""
    if project is None:
        project = get_project()
    if user is None:
        user = get_user()
    original_set_scratch_base(f"/gpfs/alpine/{project}/scratch/{user}/.tmp/{app_name}")


def get_scratch_base():
    """Forward the scratch base"""
    return original_get_scratch_base()


#
# OLCF context Helpers
#


def andes_is_batch():
    if not os.environ.get("SLURM_JOB_UID", None):
        return False
    return True


def summit_is_batch():
    if not os.environ.get("LSB_JOBID", None):
        return False
    return True


#
# Summit batch job
#


def summit_dask_gpu_batch(compute_fn, script=None):
    """Run compute_fn on a Batch Dask GPU cluster from Summit

    Meant to be invoked from the "__main__" context
    ```
    from refinery218.olcf import summit_dask_gpu_batch

    def compute(client):
        pass

    if __name__ == "__main__":
        summit_dask_gpu_batch(compute, script_name="hello_world.py")
    ```
    """
    # Variables default or derived from the environment
    USER = os.environ['USER']
    ACCOUNT = os.environ.get("LSB_PROJECT_NAME", "").lower()
    JOB_ID = os.environ.get("LSB_JOBID", None)

    # Default is to create one worker in development mode
    NWORKERS_ON_DEBUG = 1
    SCHEDULER_FILE = f"./.dask-scheduler-{JOB_ID}.json"
    # Using Burst Buffer as the spill area
    LOCAL_DIRECTORY = f"/mnt/bb/{USER}"

    # The scheduler invoking command
    SCHEDULER_LAUNCH_CMD = ' '.join([
        "dask-scheduler",
        "--port", "0",
        "--dashboard-address", ":0",
        "--interface", "ib0",
        "--scheduler-file", SCHEDULER_FILE,
    ])

    WORKER_LAUNCH_CMD = ' '.join([
        "jsrun",
        "--smpiargs='none'",
        "--tasks_per_rs", "1",
        "--cpu_per_rs", "42",
        "--gpu_per_rs", "6",
        "--rs_per_host", "1",
        "--bind", "rs",
        "--exit_on_error", "0",
        "dask-cuda-worker",
            "--nthreads", "7",
            "--memory-limit", "85GB",
            "--device-memory-limit", "16GB",
            # FIXME: RMM size needs to be determined
            #"--rmm-pool-size", "32GB",
            "--death-timeout", "60",
            "--interface", "ib0",
            "--enable-infiniband",
            # FIXME: Due to RMM overflow, we disable nvlink for now
            #"--enable-nvlink",
            "--no-dashboard", "--reconnect", "--scheduler-file", SCHEDULER_FILE,
            "--local-directory", LOCAL_DIRECTORY,
    ])

    CLIENT_LAUNCH_CMD = [
        "python3", f"./{script}", "compute"
    ]

    # A local process outside of a batch job context for development
    if not JOB_ID:
        import dask
        from distributed import LocalCluster, Client
        # A debugging session with only a few workers & threads
        cluster = LocalCluster(processes=True,
                               n_workers=2, threads_per_worker=1,
                               dashboard_address=f":{os.environ.get('DASK_BATCH_DASHBOARD_ADDRESS', '0')}",
                               memory_limit="8GB")
        # Set memory spill policy to a Null one
        dask.config.set({
            'worker.memory': {
                'target': False, 'spill': False, 'pause': 0.8, 'terminate': 0.95
            }
        })
        client = Client(cluster)
        logger.warning(f"Dashbaord address is set to {client.cluster.dashboard_link}")
        compute_fn(client)
        client.shutdown()
        client.close()
        sys.exit(0)


    # Scheduler
    logger.info("Starting scheduler")
    sched_proc = subprocess.Popen(
        SCHEDULER_LAUNCH_CMD,
        stdout=subprocess.PIPE,
        shell=True,
        universal_newlines=True)
    if sched_proc.returncode:
        logger.error("Failed spawning scheduler")
        sys.exit(sched_proc.returncode)
    logger.info("Spawned scheduler, waiting for it to stabilize")
    time.sleep(5)

    # Workers
    logger.info("Starting workers")
    worker_pool_proc = subprocess.Popen(
        WORKER_LAUNCH_CMD,
        stdout=subprocess.PIPE,
        shell=True,
        universal_newlines=True)
    if worker_pool_proc.returncode:
        logger.error("Failed spawning worker pool")
        os.system("jskill all")
        sys.exit(worker_pool_proc.returncode)
    time.sleep(10)

    # The actual task context with many workers
    logger.info("Starting the client")
    from dask.distributed import Client
    from dask.distributed import performance_report
    client = Client(scheduler_file=SCHEDULER_FILE)
    with performance_report(filename=f"../logs/{script.split('.')[0]}-{JOB_ID}.html"):
        ret = compute_fn(client)

    # Shutdown the cluster and disconnect
    logger.info("Sending out shutdown signal")
    client.shutdown()
    client.close(timeout=30)
    logger.info("Client finished")

    # Kill dask worker launcher if still alive
    logger.info("Killing dask workers")
    worker_pool_proc.kill()
    worker_pool_proc.wait()
    time.sleep(5)

    # Kill scheduler it is still alive
    logger.info("Killing dask scheduler")
    sched_proc.kill()
    sched_proc.wait()
    if os.access(SCHEDULER_FILE, os.F_OK):
        os.unlink(SCHEDULER_FILE)

    # Cleanup remaining processes (i.e., nannies)
    logger.info("Cleaning up")
    os.system("jskill -i all")
    time.sleep(5)

    logger.info(f"Shutdown success - return code {ret}")
    sys.exit(ret)


def andes_dask_batch(compute_fn, script=None,
                     watchdog=0, watchdog_heartbeat=[]):
    """Run compute_fn on a Batch Dask CPU cluster from Andes
    """
    # Variables default or derived from the environment
    USER = os.environ['USER']
    ACCOUNT = os.environ.get("SLURM_JOB_ACCOUNT", "").lower()
    JOB_ID = os.environ.get("SLURM_JOB_UID", None)

    # Default is to create two workers in development mode
    NWORKERS_ON_DEBUG = 2
    # 8 workers x 4 thread per Andes node (32 core machine)
    NWORKERS_PER_NODE = 8
    NTHREADS_PER_WORKER = 4
    # Also, 32GB memory per worker
    WORKER_MEMORY = 32000000000
    # Using the infiniband interface
    PROTOCOL = "tcp"
    INTERFACE = "ib0"
    # And using the MEMBERWORK area GPFS as a localhost spill area
    LOCAL_DIRECTORY = f"{os.environ['MEMBERWORK']}/{ACCOUNT}/.tmp/dask-mpi"

    # A local process outside of a batch job context for development
    if not JOB_ID:
        import dask
        from distributed import LocalCluster, Client
        # A debugging session with only a few workers & threads
        cluster = LocalCluster(processes=True,
                               n_workers=2,
                               threads_per_worker=1,
                               dashboard_address=f":{os.environ.get('DASK_BATCH_DASHBOARD_ADDRESS', '0')}",
                               memory_limit="8GB")

        # Set memory spill policy to a Null one
        dask.config.set({
            'worker.memory': {
                'target': False, 'spill': False, 'pause': 0.8, 'terminate': 0.95
            }
        })
        client = Client(cluster)
        logger.warning(f"Dashbaord address is set to {client.cluster.dashboard_link}")
        compute_fn(client)
        client.shutdown()
        client.close()
        sys.exit(0)

    # A launch context
    if len(sys.argv) != 2:
        # The primary watchdog loop for the Dask cluster
        # Monitors the stdout & stderr looking for a particular string output as an
        # indication of a "heartbeat'.  If watchdog > 0 then it will wait > 1
        # seconds until it sees the next heartbeat.  If it does not see a
        # heartbeat within watchdog seconds, it will kill the mpijob and
        # loop.
        # When the MPI job exited with a proper code or anything, then the loop
        # will break and the job will finish.
        # If the watchdog argument is 0 or under, then there will be no
        # watchdog activities waiting for the heartbeats.

        # preamble
        os.system("ulimit -n 3000 2>&1 >/dev/null")
        while True:
            # Launch the cluster
            logger.info("Starting dask-mpi cluster")
            cluster_proc = subprocess.Popen(
                ["mpirun", "-N", f"{NWORKERS_PER_NODE}", f"{sys.executable}", f"{script}", "worker"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                #shell=True,
                universal_newlines=True)
            if cluster_proc.returncode:
                logger.error("Failed starting the cluster")
                sys.exit(cluster_proc.returncode)

            # The monitoring loop
            sel = selectors.DefaultSelector()
            sel.register(cluster_proc.stdout, selectors.EVENT_READ)
            sel.register(cluster_proc.stderr, selectors.EVENT_READ)
            watchdog_timer = time.time()
            while True:
                events = sel.select(timeout=1.0)
                watchdog_epoch = time.time() - watchdog_timer
                if cluster_proc.poll() is not None and cluster_proc.returncode:
                    logger.error(f'cluster died with {cluster_proc}.returncode')
                    break

                # If there were no events (timed out)
                if not events:
                    # Check if watchdog is expired and break if it did
                    if watchdog > 0 and watchdog_epoch >= watchdog:
                        break
                    continue

                # We have something to read
                key = events[0][0]
                line = key.fileobj.readline()
                if not line:
                    logger.info("End of communication")
                    break

                # We check once in a while whether the watchdog expired
                if watchdog > 0 and watchdog_epoch >= watchdog:
                    break

                # Check if the line has the heartbeat string
                got_heartbeat = False
                if watchdog > 0:
                    for heartbeat in watchdog_heartbeat + [WATCHDOG_HEARTBEAT_STRING]:
                        if heartbeat not in line:
                            continue
                        logger.info(
                            f"Got watchdog heartbeat in {watchdog_epoch} seconds with {watchdog - watchdog_epoch} seconds left"
                        )
                        watchdog_timer = time.time()
                        got_heartbeat = True
                        break
                if got_heartbeat:
                    continue

                # Forward the line
                if key.fileobj is cluster_proc.stdout:
                    print(line, end="")
                else:
                    print(line, end="", file=sys.stderr)

            # If watchdog is expired, kill the cluster and continue
            if watchdog > 0 and time.time() - watchdog_timer >= watchdog:
                # Watchdog has expired
                logger.warning(f"Watchdog expired after {watchdog_epoch} seconds")
                logger.info(f"Signalling the process")

                # Kill the cluster
                cluster_proc.terminate()
                logger.info(f"Waiting the process")
                cluster_proc.wait(timeout=10)
                if cluster_proc.poll() is None:
                    logger.warning(f"Sending SIGKILL")
                    cluster_proc.kill()

                # After the process finished, we simply break with the
                # counter as-is
                logger.info("Respawning the cluster")
                continue

            # Otherwise, we broke out due to EOF
            # We end...
            cluster_proc.wait(timeout=10)
            if cluster_proc.poll() is None:
                cluster_proc.kill()
            logger.info(f"Shutdown success - return code {cluster_proc.returncode}")
            sys.exit(cluster_proc.returncode)

    # The actual task context with many workers
    assert(len(sys.argv) == 2)
    from dask_mpi import initialize
    from dask.distributed import Client
    from dask.distributed import performance_report
    initialize(
        nthreads=NTHREADS_PER_WORKER,
        interface=INTERFACE,
        protocol=PROTOCOL,
        memory_limit=WORKER_MEMORY,
        local_directory=LOCAL_DIRECTORY,
    )
    logger.info("Starting the client")
    client = Client()
    with performance_report(filename=f"../logs/{script.split('.')[0]}-{JOB_ID}.html"):
        ret = compute_fn(client)
    logger.info("Sending out shutdown signal")
    client.close(timeout=30)
    logger.info("Client finished")
    sys.exit(ret)
