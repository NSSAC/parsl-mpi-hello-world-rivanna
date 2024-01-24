#!/usr/bin/env python
"""Parsl config for Rivanna."""

import os
import sys
import sqlite3
from pathlib import Path
from subprocess import run

from addict import Dict

import parsl
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher, SingleNodeLauncher
from parsl.monitoring import MonitoringHub
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface

from parsl_helpers import make_fresh_dir

# Maximum number of compute nodes to use for this workflow
MAX_NODES = 5
WALLTIME = "1:00:00"

# Rivanna credentials
PARITION = "bii"
ACCOUNT = "bii_nssac"
QOS = "normal"

# This is a property of your MPI jobs
NODES_PER_MPI_TASK = 2

# This is a property of Rivanna BII nodes
CPUS_PER_NODE = 37

NUM_TASKS = NODES_PER_MPI_TASK * CPUS_PER_NODE
MPIRUN_COMMAND = f"mpirun --oversubscribe --map-by ppr:{CPUS_PER_NODE}:node --rank-by node --bind-to core -n {NUM_TASKS} --mca mpi_preconnect_all true"

# Give your parsl jobs access to you your python files and conda environemnt
WORKER_CONDA_ENV = "parsl-test"
CURDIR = Path(__file__).parent
USER = os.environ["USER"]
WORKER_INIT = f"""
module load gcc/11.4.0 openmpi/4.1.4

. /home/{USER}/miniconda3/etc/profile.d/conda.sh
conda init
conda activate {WORKER_CONDA_ENV}

export PYTHONPATH='{CURDIR!s}'
"""


def make_executor(conf: Dict) -> HighThroughputExecutor:
    script_dir = str(conf.parsl_work_dir / "script_dir")
    htex_work_dir = str(conf.parsl_work_dir / "htex_work_dir")
    worker_logdir_root = str(conf.parsl_work_dir / "worker_logdir_root")

    htex = HighThroughputExecutor(
        provider=SlurmProvider(
            partition=PARITION,
            account=ACCOUNT,
            qos=QOS,
            channel=LocalChannel(script_dir=script_dir),
            nodes_per_block=conf.nodes_per_block,
            cores_per_node=CPUS_PER_NODE,
            min_blocks=0,
            init_blocks=0,
            max_blocks=conf.max_blocks,
            parallelism=conf.parallelism,
            walltime=WALLTIME,
            worker_init=WORKER_INIT,
            exclusive=False,
            launcher=conf.launcher,
        ),
        label=conf.label,
        address=address_by_interface("ib0"),
        working_dir=htex_work_dir,
        worker_logdir_root=worker_logdir_root,
        max_workers=conf.max_workers,
        cores_per_worker=conf.cores_per_worker,
    )

    return htex


# To be used for running serial tasks
def make_serial_executor(parsl_work_dir: Path) -> HighThroughputExecutor:
    conf = Dict(
        label="serial",
        nodes_per_block=1,
        max_blocks=MAX_NODES,
        launcher=SingleNodeLauncher(),
        max_workers=CPUS_PER_NODE,
        cores_per_worker=1,
        parallelism=1.0,
        parsl_work_dir=parsl_work_dir,
    )
    return make_executor(conf)


# To be used for running mpi tasks
def make_parallel_executor(parsl_work_dir: Path) -> HighThroughputExecutor:
    conf = Dict(
        label="parallel",
        nodes_per_block=NODES_PER_MPI_TASK,
        max_blocks=MAX_NODES // NODES_PER_MPI_TASK,
        launcher=SimpleLauncher(),
        max_workers=1,
        cores_per_worker=1e-6,
        parallelism=NODES_PER_MPI_TASK,
        parsl_work_dir=parsl_work_dir,
    )

    return make_executor(conf)


def setup_parsl(output_root: Path):
    parsl_work_dir = output_root / "parsl_root"
    make_fresh_dir(parsl_work_dir)

    serial_htex = make_serial_executor(parsl_work_dir)
    parallel_htex = make_parallel_executor(parsl_work_dir)

    log_endpoint = "sqlite:///" + str(parsl_work_dir / "monitoring.db")

    monitoring = MonitoringHub(
        hub_address=address_by_interface("ib0"),
        hub_port=20355,
        monitoring_debug=False,
        resource_monitoring_interval=10,
        logging_endpoint=log_endpoint,
        logdir=str(parsl_work_dir / "monitoring_logs"),
    )

    config = Config(
        executors=[serial_htex, parallel_htex],
        monitoring=monitoring,
        run_dir=str(parsl_work_dir / "runinfo"),
        max_idletime=30,
        strategy="htex_auto_scale",
        initialize_logging=True,
    )

    return parsl.load(config)


def less_outfile(output_root: Path, tid: int, stderr: bool):
    """Find stderr file for tasks."""
    db = output_root / "parsl_root/monitoring.db"
    with sqlite3.connect(db) as con:
        sql = """
        select task_executor, block_id
        from try
        where task_id = ? and try_id = 0
        """
        cur = con.execute(sql, (tid,))
        rows = cur.fetchall()
        if len(rows) != 1:
            sys.exit(1)

        task_executor, block_id = rows[0]

    if stderr:
        fname_pattern = f"parsl.{task_executor}.block-{block_id}.*.*.submit.stderr"
    else:
        fname_pattern = f"parsl.{task_executor}.block-{block_id}.*.*.submit.stdout"

    log_root = output_root / "parsl_root"
    outfiles = list(log_root.rglob(fname_pattern))

    if not outfiles:
        print("No matching files found.")
        return

    print("Matching files:")
    for file in outfiles:
        print(str(file))

    cmd = ["less", "-K", str(outfiles[0])]
    try:
        run(cmd, stdin=sys.stdin, stdout=sys.stdout)
    except KeyboardInterrupt:
        pass
