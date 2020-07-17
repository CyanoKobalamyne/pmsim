#!/bin/env python3
"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
from pathlib import PurePath
import random
import statistics
from typing import Dict, List

import numpy as np
import matplotlib.pyplot as plt

from executors import OptimalExecutor, RandomExecutor
from factories import RandomFactory
from schedulers import GreedyScheduler, MaximalScheduler, TournamentScheduler
from simulator import Simulator


def _main() -> None:
    random.seed(0)

    parser = ArgumentParser(
        description="Puppetmaster, a hardware accelerator for transactional memory"
    )
    subparsers = parser.add_subparsers(title="commands")

    # Options for tabulation script.
    tput_parser = subparsers.add_parser("tput", help="tabulate throughputs")
    tput_parser.add_argument(
        "--log-max-stime",
        help="Log-2 of the maximum scheduling time",
        default=10,
        type=int,
    )
    tput_parser.add_argument(
        "--log-max-cores",
        help="Log-2 of the maximum number of cores",
        default=10,
        type=int,
    )
    tput_parser.set_defaults(func=_make_throughput_table)

    # Options for statistics plotting script.
    stat_parser = subparsers.add_parser("stat", help="plot system statistics")
    stat_parser.add_argument(
        "-t", "--op-time", help="length of one hardware operation", default=0, type=int,
    )
    stat_parser.add_argument(
        "-c", "--num-cores", help="number of execution cores", default=1, type=int,
    )
    stat_parser.set_defaults(func=_make_stats_plot)

    # General options.
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
        "-q",
        "--queuesize",
        help="size of queue for transactions waiting to be executed",
        type=int,
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
        "-v", "--verbose", help="print debugging information", action="store_true",
    )

    args = parser.parse_args()

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = RandomFactory(
        args.memsize, tr_types.values(), args.n, args.repeats, args.zipf_param
    )

    args.func(args, tr_factory)


def _make_throughput_table(args, tr_factory) -> None:
    sched_times = [0, *(2 ** logstime for logstime in range(args.log_max_stime + 1))]
    core_counts = [2 ** logcores for logcores in range(args.log_max_cores + 1)]

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

    print(
        f"Template: {os.path.basename(args.template.name)}\n"
        f"No. of transactions: {args.n}\n"
        f"Memory size (-m): {args.memsize}\n"
        f"Scheduling pool size or lookahead (-p): {args.poolsize or 'infinite'}\n"
        f"Execution queue size (-q): {args.queuesize or 'infinite'}\n"
        f"Object address distribution's Zipf parameter (-z): {args.zipf_param:.2f}\n"
        f"Runs per configuration (-r): {args.repeats}\n"
    )

    def run_sims(sched_cls, sched_args={}, exec_cls=RandomExecutor, exec_args={}):
        print(sched_cls(**sched_args).name)
        print(" " * col1_width + "Average total throughput")
        print(header_template.format(col1_header, *core_counts))
        for sched_time in sched_times:
            throughputs: List[float] = []
            for core_count in core_counts:
                results = [
                    path[-1].clock
                    for _, path in _run_sim(
                        args,
                        sched_time,
                        core_count,
                        tr_factory,
                        sched_cls,
                        sched_args,
                        exec_cls,
                        exec_args,
                    )
                ]
                throughputs.append(tr_factory.total_time / statistics.mean(results))
            print(body_template.format(f"{sched_time}", *throughputs))
        print()

    run_sims(TournamentScheduler)

    run_sims(TournamentScheduler, {"is_pipelined": True})

    run_sims(GreedyScheduler)

    run_sims(MaximalScheduler)

    run_sims(GreedyScheduler, {}, OptimalExecutor)

    run_sims(MaximalScheduler, {}, OptimalExecutor)


def _make_stats_plot(args, tr_factory) -> None:
    filename = (
        f"{PurePath(args.template.name).stem}_{args.n}_m{args.memsize}_p{args.poolsize}"
        f"_q{args.queuesize if args.queuesize is not None else 'inf'}"
        f"_z{args.zipf_param}_t{args.op_time}_c{args.num_cores}.pdf"
    )

    fig, axes = plt.subplots(args.repeats, 4)

    j = 0

    def run_sims(sched_cls, sched_args={}):
        nonlocal j
        title = sched_cls(**sched_args).name
        print(title)
        lines = []
        for i, path in _run_sim(
            args, args.op_time, args.num_cores, tr_factory, sched_cls, sched_args
        ):
            merged_path = {
                state.clock: [
                    len(state.pending),
                    len(state.scheduled),
                    len(state.cores),
                ]
                for state in path
            }
            times = np.array(list(merged_path.keys()))
            stats = np.array(list(merged_path.values()))
            axis = axes[i][j]
            lines.append(axis.plot(times, stats))
            if i == 0:
                axis.set_title(title)
        j += 1
        return lines

    run_sims(TournamentScheduler)

    midlines = run_sims(TournamentScheduler, {"is_pipelined": True})

    run_sims(GreedyScheduler)

    if args.poolsize <= 20:
        run_sims(MaximalScheduler)

    axes[-1][1].legend(
        handles=midlines[-1],
        labels=["pending", "scheduled", "executing"],
        loc="upper center",
        bbox_to_anchor=(0.5, -0.2),
        fancybox=False,
        shadow=False,
        ncol=3,
    )
    plt.show()
    fig.savefig(filename)


def _run_sim(
    args,
    op_time,
    num_cores,
    tr_factory,
    sched_cls,
    sched_args,
    exec_cls=RandomExecutor,
    exec_args={},
):
    for i in range(args.repeats):
        transactions = iter(tr_factory)
        scheduler = sched_cls(op_time, args.poolsize, args.queuesize, **sched_args)
        executor = exec_cls(**exec_args)
        sim = Simulator(transactions, scheduler, executor, num_cores)
        yield i, sim.run(args.verbose)


if __name__ == "__main__":
    _main()
