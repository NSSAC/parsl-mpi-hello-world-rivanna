"""Parsl apps."""

import shlex
import shutil
from pathlib import Path
from textwrap import dedent
from typing import Callable, ParamSpec
from concurrent.futures import as_completed


import parsl
from parsl.dataflow.futures import AppFuture
from parsl.app.errors import BashExitFailure


def make_fresh_dir(dir: Path) -> None:
    if dir.exists():
        shutil.rmtree(dir)
    dir.mkdir(mode=0o770, parents=True, exist_ok=False)


def cmd_str(input: str) -> str:
    x = dedent(input)
    x = x.strip()
    x = shlex.split(x)
    x = " ".join(x)
    return x


Param = ParamSpec("Param")


def serial_bash_app(func: Callable[Param, str]) -> Callable[Param, AppFuture]:
    bash_app = parsl.bash_app(executors=["serial"])
    return bash_app(func)


def parallel_bash_app(func: Callable[Param, str]) -> Callable[Param, AppFuture]:
    bash_app = parsl.bash_app(executors=["parallel"])
    return bash_app(func)


def wait_for_tasks(*tasks: AppFuture) -> list:
    results = []
    for task in as_completed(tasks):
        task_tid = task.tid  # type: ignore
        try:
            results.append(task.result())
        except BashExitFailure as e:
            print(
                f"Task failed: tid={task_tid}, app_name={e.app_name}, exitcode={e.exitcode}"
            )
            raise SystemExit(1)
    return results


@parallel_bash_app
def mpi_hello_world(
    mpirun_command: str,
    output_file: Path,
) -> str:
    cmd = f"""
    {mpirun_command} ./mpi_hello_world > '{output_file!s}'
    """
    return cmd_str(cmd)


@serial_bash_app
def hostname(
    output_file: Path,
) -> str:
    cmd = f"""
    hostname > '{output_file!s}'
    """
    return cmd_str(cmd)
