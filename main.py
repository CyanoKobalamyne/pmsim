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


def get_args() -> Namespace:
    """Parse and print command-line arguments."""
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
    tput_parser.set_defaults(func=make_throughput_table)

    # Options for statistics plotting script.
    stat_parser = subparsers.add_parser("stat", help="plot system statistics")
    stat_parser.add_argument(
        "-t", "--op-time", help="length of one hardware operation", default=1, type=int,
    )
    stat_parser.add_argument(
        "-c", "--num-cores", help="number of execution cores", default=4, type=int,
    )
    stat_parser.set_defaults(func=make_stats_plot)

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
    psfind_parser.set_defaults(func=make_ps_table)

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
        default=16,
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

    return args


def make_throughput_table(args: Namespace, tr_factory: TransactionFactory) -> None:
    """Print normalized throughput as a function of scheduling time and core count."""
    sched_times = [0, *(2 ** logstime for logstime in range(args.log_max_stime + 1))]
    core_counts = [2 ** logcores for logcores in range(args.log_max_cores + 1)]

    thead, tbody = get_table_templates(
        varname="Average normalized throughput",
        xname="Number of cores",
        yname="HW operation time",
        xvals=core_counts,
        yvals=sched_times,
        max_value=100,
        precision=5,
    )

    def run_sims(sched_cls, sched_args={}, exec_cls=RandomExecutor, exec_args={}):
        print(f"{sched_cls(**sched_args).name} with {exec_cls(**exec_args).name}\n")
        print(thead)
        for sched_time in sched_times:
            args.op_time = sched_time
            throughputs: List[float] = []
            for core_count in core_counts:
                args.num_cores = core_count
                results = [
                    path[-1].clock
                    for _, path in run_sim(
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

    if (args.queuesize is None and args.n < 10) or (
        args.queuesize is not None and args.queuesize < 10
    ):
        run_sims(GreedyScheduler, {}, OptimalExecutor)

    if args.poolsize <= 20 and (
        (args.queuesize is None and args.n < 10)
        or (args.queuesize is not None and args.queuesize < 10)
    ):
        run_sims(MaximalScheduler, {}, OptimalExecutor)


def make_stats_plot(args: Namespace, tr_factory: TransactionFactory) -> None:
    """Plot number of scheduled transactions as a function of time."""
    filename = (
        f"{PurePath(args.template.name).stem}_{args.n}_m{args.memsize}_p{args.poolsize}"
        f"_q{args.queuesize if args.queuesize is not None else 'inf'}"
        f"_z{args.zipf_param}_t{args.op_time}_c{args.num_cores}.pdf"
    )

    fig, axes = plt.subplots(args.repeats, 4)

    j = 0

    def run_sims(sched_cls, sched_args={}):
        nonlocal j
        title = f"{sched_cls(**sched_args).name}"
        print(f"{title}\n")
        lines = []
        for i, path in run_sim(args, tr_factory, sched_cls, sched_args):
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


def make_ps_table(args: Namespace, tr_factory: TransactionFactory) -> None:
    """Print minimum pool size as a function of scheduling time and core count."""

    class ScheduledCountSequence(Sequence[int]):
        def __getitem__(self, key):
            args.poolsize = key
            if args.verbose == 1:
                print("Trying pool size of", args.poolsize, end="...")
            if args.verbose >= 2:
                print("Trying pool size of", args.poolsize)
            min_sched_counts = []
            for _, path in run_sim(args, tr_factory, TournamentScheduler):
                scheduled_counts = {}
                for state in path:
                    if state.clock == 0:
                        # Skip over starting state.
                        continue
                    if not state.incoming:
                        # Skip over tail.
                        break
                    if state.clock not in scheduled_counts:
                        scheduled_counts[state.clock] = (
                            len(state.scheduled) - state.core_count + len(state.cores)
                        )
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

    thead, tbody = get_table_templates(
        varname="Minimum pool size for keeping the cores busy",
        xname="Number of cores",
        yname="HW operation time",
        xvals=core_counts,
        yvals=sched_times,
        max_value=args.n,
        precision=0,
    )

    print(f"{TournamentScheduler().name} with {RandomExecutor().name}\n")
    print(thead)
    for sched_time in sched_times:
        args.op_time = sched_time
        min_poolsizes = []
        for core_count in core_counts:
            args.num_cores = core_count
            min_poolsize = bisect.bisect_left(ScheduledCountSequence(), 0, lo=1)
            min_poolsizes.append(min_poolsize)
        print(tbody.format(sched_time, *min_poolsizes))
    print()


def run_sim(
    args: Namespace,
    tr_factory: TransactionFactory,
    sched_cls: Type[TransactionScheduler],
    sched_args: Mapping = {},
    exec_cls: Type[TransactionExecutor] = RandomExecutor,
    exec_args: Mapping = {},
):
    """Yield index and path through the state space found by the simulator."""
    for i in range(args.repeats):
        transactions = tr_factory.__iter__()
        scheduler = sched_cls(args.op_time, args.poolsize, args.queuesize, **sched_args)
        executor = exec_cls(**exec_args)
        sim = Simulator(transactions, scheduler, executor, args.num_cores)
        yield i, sim.run(args.verbose)


def get_table_templates(
    varname: str,
    xname: str,
    yname: str,
    xvals: Sequence[int],
    yvals: Sequence[int],
    max_value: float,
    precision: int,
):
    """Return table header and table body row template."""
    col1_width = max(len(xname), len(yname), len(str(max(yvals))))
    col1_template = f"{{:<{col1_width}}} | "
    max_val_width = len(f"{max_value:.{precision}f}")
    cols_header = "  ".join(f"{x:{max(max_val_width, len(str(x)))}d}" for x in xvals)
    cols_body_template = "  ".join(
        f"{{:{max(max_val_width, len(str(x)))}.{precision}f}}" for x in xvals
    )
    header_line_1 = col1_template.format(yname) + varname
    header_line_2 = col1_template.format(xname) + cols_header
    hline_width = max(len(header_line_1), len(header_line_2))
    divider = "-" * hline_width
    header = f"{header_line_1}\n{divider}\n{header_line_2}\n{divider}"
    body_template = col1_template + cols_body_template
    return header, body_template


if __name__ == "__main__":
    random.seed(0)

    args = get_args()

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = RandomFactory(
        args.memsize, tr_types.values(), args.n, args.repeats, args.zipf_param
    )

    args.func(args, tr_factory)
