#!/usr/bin/env python3
from __future__ import annotations
import logging
from pathlib import Path
import shutil
import signal
import timeit
from typing import *
import os, sys, re
from asyncio.subprocess import DEVNULL, PIPE, STDOUT, Process
from dataclasses import dataclass, field
import asyncio

type Outcome = Literal["found_issue", "found_no_issue"]

@dataclass
class Config:
    out_root: Path
    test_command: list[str]
    re_exit_found_issue: re.Pattern[bytes]
    re_exit_found_no_issue: re.Pattern[bytes]
    parallel_runner_count: int
    """
    Whether timeout is an issue or not.
    If set to 'found_no_issue' it will be ignored, not necessarily because of it
    not being a bug, but because of it not being the issue you're looking for.
    If you want to investigate if a timeout occurs, set to 'found_issue'.
    """
    timeout_outcome: Outcome = field(default="found_issue")
    test_timeout_seconds: float = field(default=30)
    """
    Whether to kill the process in which the issue was found.
    """
    kill_process_with_issue: bool = field(default=False)

    def path_worker(self, worker_id: int):
        return self.out_root / f"worker-{worker_id:03}"
    def path_test(self, worker_id: int, test_run_id: int):
        return self.path_worker(worker_id) / f"test-{test_run_id:06}"
    def path_stdout(self, worker_id: int, test_run_id: int):
        return self.path_test(worker_id, test_run_id) / "stdout.txt"

type IssueReason = Literal["match_issue", "timeout", "eof"]

@dataclass
class TestWithIssue:
    worker_id: int
    test_run_id: int
    reason: IssueReason
    process: Process

TEST_WITH_ISSUE: Optional[TestWithIssue] = None

COUNT_RUNS_DONE = 0
START_TIME = timeit.default_timer()

def gen_status_text():
    time_elapsed = timeit.default_timer() - START_TIME
    if time_elapsed < 0.1:  # avoid div/0
        return "Just started..."
    runs_per_min = 60 * COUNT_RUNS_DONE / time_elapsed
    return f"{COUNT_RUNS_DONE} runs completed in {time_elapsed:.0f} seconds ({runs_per_min:.1f} runs/min)"

async def gentle_terminate_and_wait(proc: Process):
    logging.debug(f"Sending SIGTERM to {proc.pid}")
    os.killpg(proc.pid, signal.SIGINT)
    try:
        async with asyncio.timeout(2):
            await proc.wait()
    except TimeoutError:
        logging.debug(f"Sending SIGKILL to {proc.pid}")
        os.killpg(proc.pid, signal.SIGKILL)
        await proc.wait()
    logging.debug(f"Process {proc.pid} reaped")

async def follow_file_lines(file_path: Path) -> AsyncGenerator[bytes, None]:
    proc = await asyncio.create_subprocess_exec("tail", "-n", "+0", "-f", str(file_path),
            stdout=PIPE, stderr=DEVNULL, stdin=DEVNULL)
    try:
        assert proc.stdout is not None
        async for line in proc.stdout:
            yield line
    finally:
        proc.terminate()
        await proc.wait()

async def do_single_run(config: Config, worker_id: int, test_run_id: int) -> Optional[TestWithIssue]:
    global TEST_WITH_ISSUE
    stdout_path = config.path_stdout(worker_id, test_run_id)
    logging.debug(f"Worker {worker_id}, run {test_run_id}. Monitoring {stdout_path}")
    with open(stdout_path, "wb") as f:
        proc = await asyncio.create_subprocess_exec(*config.test_command,
            config.path_test(worker_id, test_run_id),
            preexec_fn=os.setpgrp, # make subprocess become group leader
            stdin=DEVNULL, stdout=f.fileno(), stderr=STDOUT)

    try:
        finish_reason: Optional[str] = None
        try:
            async with asyncio.timeout(config.test_timeout_seconds):
                async for line in follow_file_lines(stdout_path):
                    if config.re_exit_found_issue.search(line):
                        finish_reason = "match_issue"
                        break
                    if config.re_exit_found_no_issue.search(line):
                        finish_reason = "match_no_issue"
                        break
                else:
                    raise RuntimeError("unreachable: follow_file_lines() must never finish")
        except TimeoutError:
            finish_reason = "timeout"
        found_issue = finish_reason == "match_issue" or \
            (finish_reason == "timeout" and config.timeout_outcome == "found_issue")
        logging.debug(f"Finish reason: {finish_reason}, found_issue={found_issue}")
        if (is_first_repro := found_issue and TEST_WITH_ISSUE is None):
            assert finish_reason != "match_no_issue"
            TEST_WITH_ISSUE = TestWithIssue(worker_id, test_run_id, finish_reason, proc)
        if config.kill_process_with_issue or not found_issue or not is_first_repro:
            await gentle_terminate_and_wait(proc)
        return TEST_WITH_ISSUE if is_first_repro else None
    except asyncio.CancelledError as err:
        await gentle_terminate_and_wait(proc)
        raise err

async def worker_main(config: Config, worker_id: int):
    global COUNT_RUNS_DONE
    try:
        run_id = 0
        while True:
            run_id += 1
            config.path_test(worker_id, run_id).mkdir(mode=0o755)
            result = await do_single_run(config, worker_id, run_id)
            COUNT_RUNS_DONE += 1
            print(gen_status_text())
            if result is not None:
                return  # found the issue!
            shutil.rmtree(config.path_test(worker_id, run_id))
    except asyncio.CancelledError:
        pass

async def main(config: Config):
    config.out_root.mkdir(mode=0o755, parents=True, exist_ok=True)
    # Clear the directory if not empty.
    for child in config.out_root.iterdir():
        if child.name.startswith("worker-"):  # avoid accidental rm -rf /
            shutil.rmtree(child)
    tasks_by_worker_id: dict[int, asyncio.Task] = {}
    for worker_id in range(1, config.parallel_runner_count + 1):
        config.path_worker(worker_id).mkdir(mode=0o755)
        task = asyncio.create_task(worker_main(config, worker_id))
        tasks_by_worker_id[worker_id] = task
    done, pending = await asyncio.wait(tasks_by_worker_id.values(), return_when=asyncio.FIRST_COMPLETED)
    for task in done:
        task.result()  # consume result (None) or propagate exception
    # If an exception hasn't been raised already, we must have a run that reproduced the issue.
    assert TEST_WITH_ISSUE is not None
    for task in pending:
        task.cancel()
    print(f"Worker {TEST_WITH_ISSUE.worker_id} reproduced an issue on run "
          f"{TEST_WITH_ISSUE.test_run_id}: {TEST_WITH_ISSUE.reason}\n"
          + str(config.path_test(TEST_WITH_ISSUE.worker_id, TEST_WITH_ISSUE.test_run_id)))
    # Wait for tasks to finish
    for task in tasks_by_worker_id.values():
        await task
    if not config.kill_process_with_issue:
        print(f"Affected test process has PID {TEST_WITH_ISSUE.process.pid}.")
        print(f"You can attach a debugger to it or analyze its logs at this time.")
        try:
            await TEST_WITH_ISSUE.process.wait()
            print(f"Test subprocess exited.")
        except asyncio.CancelledError:
            print(f"Terminating test subprocess due to ^C.")
            await gentle_terminate_and_wait(TEST_WITH_ISSUE.process)


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main(Config(
        parallel_runner_count=4,
        out_root=Path("/tmp/flake-chaser-logs/"),
        test_command=["./fake_flake.py"],
        re_exit_found_issue=re.compile(rb"\bfailed\b"),
        re_exit_found_no_issue=re.compile(rb"\bsucceeded\b"),
        kill_process_with_issue=False,
    )))