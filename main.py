#!/bin/env python3
"""Main executable for running puppetmaster."""
import bisect
import json
import os
import random
import statistics
from argparse import ArgumentParser, FileType, Namespace
from pathlib import PurePath
from typing import Dict, List, Mapping, Sequence, Type

import matplotlib.pyplot as plt
import numpy as np

from executors import OptimalExecutor, RandomExecutor
from factories import RandomFactory
from model import TransactionExecutor, TransactionFactory, TransactionScheduler
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

    # Options for pool size finder.
    psfind_parser = subparsers.add_parser("psfind", help="tabulate optimal pool sizes")
    psfind_parser.add_argument(
        "--log-max-stime",
        help="Log-2 of the maximum scheduling time",
        default=10,
        type=int,
    )
    psfind_parser.add_argument(
        "--log-max-cores",
        help="Log-2 of the maximum number of cores",
        default=10,
        type=int,
    )
    psfind_parser.set_defaults(func=_make_ps_table)

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
        "-v", "--verbose", help="print debugging information", action="count", default=0
    )

    args = parser.parse_args()

    print(
        f"Template: {os.path.basename(args.template.name)}\n"
        f"No. of transactions: {args.n}\n"
        f"Memory size (-m): {args.memsize}\n"
        f"Scheduling pool size or lookahead (-p): {args.poolsize or 'infinite'}\n"
        f"Execution queue size (-q): {args.queuesize or 'infinite'}\n"
        f"Object address distribution's Zipf parameter (-z): {args.zipf_param:.2f}\n"
        f"Runs per configuration (-r): {args.repeats}\n"
    )

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = RandomFactory(
        args.memsize, tr_types.values(), args.n, args.repeats, args.zipf_param
    )

    args.func(args, tr_factory)


def _make_throughput_table(args, tr_factory) -> None:
    sched_times = [0, *(2 ** logstime for logstime in range(args.log_max_stime + 1))]
    core_counts = [2 ** logcores for logcores in range(args.log_max_cores + 1)]

    title, thead, tbody = _get_table_templates(
        sched_times,
        core_counts,
        max_value=100,
        precision=5,
        label="Average total throughput",
    )

    def run_sims(sched_cls, sched_args={}, exec_cls=RandomExecutor, exec_args={}):
        print(f"{sched_cls(**sched_args).name} with {exec_cls(**exec_args).name}")
        print(title)
        print(thead.format(*core_counts))
        for sched_time in sched_times:
            args.op_time = sched_time
            throughputs: List[float] = []
            for core_count in core_counts:
                args.num_cores = core_count
                results = [
                    path[-1].clock
                    for _, path in _run_sim(
                        args, tr_factory, sched_cls, sched_args, exec_cls, exec_args
                    )
                ]
                throughputs.append(tr_factory.total_time / statistics.mean(results))
            print(tbody.format(sched_time, *throughputs))
        print()

    run_sims(TournamentScheduler)

    run_sims(TournamentScheduler, {"is_pipelined": True})

    run_sims(GreedyScheduler)

    if args.poolsize <= 20:
        run_sims(MaximalScheduler)

    if args.queuesize is None and args.n < 10 or args.queuesize < 10:
        run_sims(GreedyScheduler, {}, OptimalExecutor)

    if args.poolsize <= 20 and (
        args.queuesize is None and args.n < 10 or args.queuesize < 10
    ):
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
        for i, path in _run_sim(args, tr_factory, sched_cls, sched_args):
            scheduled_counts = {}
            for state in path:
                if state.clock not in scheduled_counts:
                    scheduled_counts[state.clock] = len(state.scheduled)
            times = np.array(list(scheduled_counts.keys()))
            stats = np.array(list(scheduled_counts.values()))
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
        labels=["scheduled"],
        loc="upper center",
        bbox_to_anchor=(0.5, -0.2),
        fancybox=False,
        shadow=False,
        ncol=3,
    )
    plt.show()
    fig.savefig(filename)


def _make_ps_table(args, tr_factory) -> None:
    class ScheduledCountSequence(Sequence[int]):
        def __getitem__(self, key):
            args.poolsize = key
            if args.verbose == 1:
                print("Trying pool size of", args.poolsize, end="...")
            if args.verbose >= 2:
                print("Trying pool size of", args.poolsize)
            min_sched_counts = []
            for _, path in _run_sim(args, tr_factory, TournamentScheduler):
                scheduled_counts = {}
                for state in path:
                    if state.clock == 0:
                        # Skip over starting state.
                        continue
                    if not state.incoming:
                        # Skip over tail.
                        break
                    if state.clock not in scheduled_counts:
                        scheduled_counts[state.clock] = len(state.scheduled)
                min_sched_count = min(scheduled_counts.values())
                min_sched_counts.append(min_sched_count)
                if args.verbose == 1:
                    print(min_sched_count, end=", ")
                if min_sched_count == 0:
                    break
            if args.verbose == 1:
                print()
            if args.verbose >= 2:
                print("Results:", ", ".join(map(str, min_sched_counts)))
            return min_sched_count

        def __len__(self):
            return args.n

    sched_times = [2 ** logstime for logstime in range(args.log_max_stime + 1)]
    core_counts = [2 ** logcores for logcores in range(args.log_max_cores + 1)]

    title, thead, tbody = _get_table_templates(
        sched_times,
        core_counts,
        max_value=args.n,
        precision=0,
        label="Minimum pool size",
    )

    print(title)
    print(thead.format(*core_counts))
    for sched_time in sched_times:
        args.op_time = sched_time
        min_poolsizes = []
        for core_count in core_counts:
            args.num_cores = core_count
            min_poolsize = bisect.bisect(ScheduledCountSequence(), 0, lo=1)
            min_poolsizes.append(min_poolsize)
        print(tbody.format(sched_time, *min_poolsizes))
    print()


def _run_sim(
    args: Namespace,
    tr_factory: TransactionFactory,
    sched_cls: Type[TransactionScheduler],
    sched_args: Mapping = {},
    exec_cls: Type[TransactionExecutor] = RandomExecutor,
    exec_args: Mapping = {},
):
    for i in range(args.repeats):
        transactions = tr_factory.__iter__()
        scheduler = sched_cls(args.op_time, args.poolsize, args.queuesize, **sched_args)
        executor = exec_cls(**exec_args)
        sim = Simulator(transactions, scheduler, executor, args.num_cores)
        yield i, sim.run(args.verbose)


def _get_table_templates(sched_times, core_counts, max_value, precision, label):
    col1_header = "t_sched"
    col1_width = max(len(col1_header), len(str(sched_times[-1]))) + 2
    col1_template = f"{{0:<{col1_width}}}"
    col_width = max(len(f"{max_value:.{precision}f}"), len(str(core_counts[-1])))
    cols_header_template = "".join(
        f"{{{i}:{col_width}d}}  " for i in range(len(core_counts))
    )
    cols_body_template = "".join(
        f"{{{i + 1}:{col_width}.{precision}f}}  " for i in range(len(core_counts))
    )
    title = " " * col1_width + label
    header_template = col1_template.format(col1_header) + cols_header_template
    body_template = col1_template + cols_body_template
    return title, header_template, body_template


if __name__ == "__main__":
    _main()
