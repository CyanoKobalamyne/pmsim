#!/bin/env python3
"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
import random
import statistics
from typing import Dict, List

from executors import FullExecutor, RandomExecutor
from factories import RandomFactory
from schedulers import ConstantTimeScheduler, TournamentScheduler
from simulator import Simulator


def _main() -> None:
    random.seed(0)

    parser = ArgumentParser(
        description="Run puppetmaster on a range of input distributions."
    )
    parser.add_argument(
        "template", help="transaction template file", type=FileType("rt")
    )
    parser.add_argument("n", help="total number of transactions", type=int)
    parser.add_argument(
        "-m", "--memsize", help="memory size (# of objects)", default=1024, type=int
    )
    parser.add_argument(
        "-p",
        "--poolsize",
        help="size of scheduling pool (lookahead)",
        type=int,
        default=1,
    )
    parser.add_argument(
        "-z",
        "--zipf-param",
        help="parameter of the Zipf's law distribution",
        default=0,
        type=float,
    )
    parser.add_argument(
        "-r",
        "--repeats",
        help="number of times the experiment with a given set of parameters is re-run",
        default=10,
        type=int,
    )
    parser.add_argument(
        "--log-max-stime",
        help="Log-2 of the maximum scheduling time",
        default=10,
        type=int,
    )
    parser.add_argument(
        "--log-max-cores",
        help="Log-2 of the maximum number of cores",
        default=10,
        type=int,
    )
    args = parser.parse_args()

    sched_times = [0, *(2 ** logstime for logstime in range(args.log_max_stime + 1))]
    core_counts = [2 ** logcores for logcores in range(args.log_max_cores + 1)]

    title = "Average total throughput"
    col1_header = "t_sched"
    col1_width = max(len(col1_header), len(str(sched_times[-1]))) + 2
    col1_template = f"{{0:<{col1_width}}}"
    precision = 5
    col_width = max(len(f"{100:.{precision}f}"), len(str(core_counts[-1])))
    cols_header_template = "".join(
        f"{{{i + 1}:{col_width}d}}  " for i in range(len(core_counts))
    )
    cols_body_template = "".join(
        f"{{{i + 1}:{col_width}.{precision}f}}  " for i in range(len(core_counts))
    )
    header_template = col1_template + cols_header_template
    body_template = col1_template + cols_body_template

    def run_sim(sched_cls, sched_args, exec_cls, exec_args, use_pool=True):
        print(" " * col1_width + title)
        print(header_template.format(col1_header, *core_counts))
        for sched_time in sched_times:
            throughputs: List[float] = []
            for core_count in core_counts:
                results: List[int] = []
                for _ in range(args.repeats):
                    transactions = iter(tr_factory)
                    scheduler = sched_cls(sched_time, **sched_args)
                    executor = exec_cls(**exec_args)
                    sim = Simulator(
                        transactions,
                        scheduler,
                        executor,
                        core_count,
                        args.poolsize if use_pool else None,
                    )
                    results.append(sim.run())
                throughputs.append(tr_factory.total_time / statistics.mean(results))
            print(body_template.format(f"{sched_time}", *throughputs))
        print("")

    print(
        f"Template: {os.path.basename(args.template.name)}\n"
        f"No. of transactions: {args.n}\n"
        f"Memory size (-m): {args.memsize}\n"
        f"Scheduling pool size or lookahead (-p): {args.poolsize or 'infinite'}\n"
        f"Object address distribution's Zipf parameter (-z): {args.zipf_param:.2f}\n"
        f"Runs per configuration (-r): {args.repeats}\n"
    )

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = RandomFactory(
        args.memsize, tr_types.values(), args.n, args.repeats, args.zipf_param
    )

    print("Tournament scheduler (pipelined)")
    run_sim(
        TournamentScheduler, {"is_pipelined": True}, RandomExecutor, {},
    )

    print("Tournament scheduler (non-pipelined)")
    run_sim(
        TournamentScheduler, {"is_pipelined": False}, RandomExecutor, {},
    )

    print("Constant-time scheduler")
    run_sim(
        ConstantTimeScheduler, {}, RandomExecutor, {},
    )

    print("Constant-time scheduler with infinite lookahead")
    run_sim(
        ConstantTimeScheduler, {}, RandomExecutor, {}, use_pool=False,
    )

    print("Constant-time scheduler with optimal execution policy")
    run_sim(
        ConstantTimeScheduler, {}, FullExecutor, {},
    )

    print(
        "Constant-time scheduler with infinite lookahead and optimal execution policy"
    )
    run_sim(
        ConstantTimeScheduler, {}, FullExecutor, {}, use_pool=False,
    )


if __name__ == "__main__":
    _main()
