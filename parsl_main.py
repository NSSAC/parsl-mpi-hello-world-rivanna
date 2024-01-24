#!/usr/bin/env python
"""Run hello world on Rivanna."""

import os
from pathlib import Path

import click

from config_rivanna import setup_parsl, less_outfile, MPIRUN_COMMAND
from parsl_helpers import mpi_hello_world, hostname, wait_for_tasks

USER = os.environ["USER"]
OUTPUT_ROOT = Path(f"/scratch/{USER}/mpi-hello-world-workflow")


@click.group()
def cli():
    pass


@cli.command()
def run():
    print("Setting up Parsl")
    setup_parsl(OUTPUT_ROOT)

    print("Creating MPI tasks ...")
    hello1_task = mpi_hello_world(MPIRUN_COMMAND, output_file=(OUTPUT_ROOT / "hello1"))
    hello2_task = mpi_hello_world(MPIRUN_COMMAND, output_file=(OUTPUT_ROOT / "hello2"))

    print("Waiting for MPI tasks to complete ...")
    wait_for_tasks(hello1_task, hello2_task)

    print("Creating serial tasks ...")
    hostname_tasks = []
    for i in range(100):
        hostname_tasks.append(hostname(OUTPUT_ROOT / f"hostname-{i}"))

    print("Waiting for serial tasks to complete")
    wait_for_tasks(*hostname_tasks)


@cli.command()
@click.argument(
    "tid",
    type=int,
    required=True,
)
def outless(tid: int):
    less_outfile(OUTPUT_ROOT, tid, False)


@cli.command()
@click.argument(
    "tid",
    type=int,
    required=True,
)
def errless(tid: int):
    less_outfile(OUTPUT_ROOT, tid, True)


if __name__ == "__main__":
    cli()
