#!/bin/env python3
"""Main executable for running puppetmaster."""
import bisect
import json
import os
import random
import statistics
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool
from pathlib import PurePath
from typing import Dict, List, Sequence, Set

try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    plt = np = None  # type: ignore

from api import ObjSetMakerFactory, TransactionExecutor, TransactionSchedulerFactory
from executors import RandomExecutor
from generator import TransactionGeneratorFactory
from pmtypes import SimulationParams, Transaction
from schedulers import (
    GreedySchedulerFactory,
    MaximalSchedulerFactory,
    TournamentSchedulerFactory,
)
from sets import FiniteObjSetMakerFactory, IdealObjSetMakerFactory
from simulator import Simulator

DEFAULT_EXECUTOR = RandomExecutor()
DEFAULT_OBJ_SET_MAKER_FACTORY = IdealObjSetMakerFactory()


def get_args() -> Namespace:
    """Parse and print command-line arguments."""
    parser = ArgumentParser(
        description="Puppetmaster, a hardware accelerator for transactional memory"
    )
    subparsers = parser.add_subparsers(title="commands")

    # Options for tabulation script.
    tpar_parser = subparsers.add_parser("tpar", help="tabulate parallelism")
    tpar_parser.add_argument(
        "--log-max-stime",
        help="Log-2 of the maximum scheduling time",
        default=10,
        type=int,
    )
    tpar_parser.add_argument(
        "--log-max-cores",
        help="Log-2 of the maximum number of cores",
        default=10,
        type=int,
    )
    tpar_parser.set_defaults(func=make_parallelism_table)

    if plt is not None:
        # Options for statistics plotting script.
        stat_parser = subparsers.add_parser("stat", help="plot system statistics")
        stat_parser.add_argument(
            "-t",
            "--op-time",
            help="length of one hardware operation",
            default=1,
            type=int,
        )
        stat_parser.add_argument(
            "-c", "--num-cores", help="number of execution cores", default=4, type=int
        )
        stat_parser.set_defaults(func=make_stats_plot)

        # Options for latency plotting script.
        latency_parser = subparsers.add_parser(
            "latency", help="plot transaction latencies"
        )
        latency_parser.add_argument(
            "-t",
            "--op-time",
            help="length of one hardware operation",
            default=1,
            type=int,
        )
        latency_parser.add_argument(
            "-c", "--num-cores", help="number of execution cores", default=4, type=int
        )
        latency_parser.set_defaults(func=make_latency_plot)

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
    parser.add_argument("template", help="transaction template file", type=str)
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

    args.n = 2 ** (getattr(args, "log_max_cores", 10) + 1)

    print(
        f"Template: {os.path.basename(args.template)}\n"
        f"No. of transactions: {args.n}\n"
        f"Memory size (-m): {args.memsize}\n"
        f"Scheduling pool size or lookahead (-p): {args.poolsize or 'infinite'}\n"
        f"Execution queue size (-q): {args.queuesize or 'infinite'}\n"
        f"Object address distribution's Zipf parameter (-z): {args.zipf_param:.2f}\n"
        f"Runs per configuration (-r): {args.repeats}\n"
    )

    return args


def run_prl_sims(core_counts, op_time, sched_factory, executor, obj_set_maker_factory):
    """Return average parallelism for all core counts and the given operation time."""
    prls: List[float] = []
    for core_count in core_counts:
        results = []
        params = SimulationParams(op_time, core_count, ARGS.poolsize, ARGS.queuesize)
        for _, path in run_sim(
            params,
            tr_factory,
            sched_factory,
            executor,
            obj_set_maker_factory,
        ):
            start = end = t_prev = t_cur = None
            total = 0
            for state in path:
                # Skip over warm-up phase (until first transaction completes).
                if (
                    len(state.incoming)
                    + len(state.pending)
                    + len(state.scheduled)
                    + len(state.cores)
                    == ARGS.n
                ):
                    continue
                if start is None:
                    start = state.clock
                    t_prev = start
                else:
                    t_cur = state.clock
                    total += len(state.cores) * (t_cur - t_prev)
                    t_prev = t_cur
                # Skip over tail (when pool is empty).
                if not state.incoming and len(state.pending) < ARGS.poolsize:
                    end = state.clock
                    break
            assert start is not None and end is not None
            if start == end:
                results.append(len(state.cores) + 1)
            else:
                results.append(total / (end - start))
            if ARGS.verbose >= 1:
                rename_steps = path[-1].obj_set_maker.history
                print(
                    f"Rename steps: {statistics.mean(rename_steps):.2f} (avg), "
                    f"{statistics.median(rename_steps)} (median), "
                    f"{max(rename_steps)} (max)"
                )
        prls.append(statistics.mean(results))
    return prls


def make_parallelism_table(tr_factory: TransactionGeneratorFactory) -> None:
    """Print parallelism as a function of scheduling time and core count."""
    sched_times = [0, *(2 ** logstime for logstime in range(ARGS.log_max_stime + 1))]
    core_counts = [2 ** logcores for logcores in range(ARGS.log_max_cores + 1)]

    thead, tbody = get_table_templates(
        varname="Steady-state parallelism",
        xname="Number of cores",
        yname="HW operation time",
        xvals=core_counts,
        yvals=sched_times,
        max_value=max(core_counts),
        precision=1,
    )

    def run_sims(
        sched_factory: TransactionSchedulerFactory,
        executor: TransactionExecutor = DEFAULT_EXECUTOR,
        obj_set_maker_factory: ObjSetMakerFactory = DEFAULT_OBJ_SET_MAKER_FACTORY,
    ):
        print(get_title(sched_factory, executor, obj_set_maker_factory))
        print()
        print(thead)
        sim_params = [
            (
                core_counts,
                sched_time,
                sched_factory,
                executor,
                obj_set_maker_factory,
            )
            for sched_time in sched_times
        ]
        for sched_time, prls in zip(
            sched_times, PROCESS_POOL.starmap(run_prl_sims, sim_params)
        ):
            print(tbody.format(sched_time, *prls))
        print()

    for n in (2, 3, 4, 5, None):
        try:
            run_sims(
                sched_factory=TournamentSchedulerFactory(),
                obj_set_maker_factory=FiniteObjSetMakerFactory(1024, n_hash_funcs=n),
            )
        except RuntimeError:
            print("--- Failed ---")
            print()
            continue


def make_stats_plot(tr_factory: TransactionGeneratorFactory) -> None:
    """Plot number of scheduled transactions as a function of time."""
    filename = (
        f"{PurePath(ARGS.template).stem}_{ARGS.n}_m{ARGS.memsize}_p{ARGS.poolsize}"
        f"_q{ARGS.queuesize if ARGS.queuesize is not None else 'inf'}"
        f"_z{ARGS.zipf_param}_t{ARGS.op_time}_c{ARGS.num_cores}.pdf"
    )
    params = SimulationParams(
        ARGS.op_time, ARGS.num_cores, ARGS.poolsize, ARGS.queuesize
    )

    fig, axes = plt.subplots(ARGS.repeats, 4)

    j = 0

    def run_sims(sched_factory: TransactionSchedulerFactory):
        nonlocal j
        print(get_title(sched_factory))
        print()
        lines = []
        for i, path in run_sim(params, tr_factory, sched_factory):
            scheduled_counts = {}
            for state in path:
                if state.clock not in scheduled_counts:
                    scheduled_counts[state.clock] = len(state.scheduled)
            times = np.array(list(scheduled_counts.keys()))
            stats = np.array(list(scheduled_counts.values()))
            axis = axes[i][j]
            lines.append(axis.plot(times, stats))
            if i == 0:
                axis.set_title(get_title(get_title(sched_factory)))
        j += 1
        return lines

    run_sims(TournamentSchedulerFactory())

    midlines = run_sims(TournamentSchedulerFactory(is_pipelined=True))

    run_sims(GreedySchedulerFactory())

    if ARGS.poolsize <= 20:
        run_sims(MaximalSchedulerFactory())

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


def make_latency_plot(tr_factory: TransactionGeneratorFactory) -> None:
    """Plot number of scheduled transactions as a function of time."""
    filename = (
        f"{PurePath(ARGS.template).stem}_latency_{ARGS.n}_m{ARGS.memsize}"
        f"_p{ARGS.poolsize}_q{ARGS.queuesize if ARGS.queuesize is not None else 'inf'}"
        f"_z{ARGS.zipf_param}_t{ARGS.op_time}_c{ARGS.num_cores}.pdf"
    )
    params = SimulationParams(
        ARGS.op_time, ARGS.num_cores, ARGS.poolsize, ARGS.queuesize
    )

    fig, axes = plt.subplots(ARGS.repeats, 4)

    j = 0

    def run_sims(sched_factory: TransactionSchedulerFactory):
        nonlocal j
        print(get_title(sched_factory))
        print()
        lines = []
        for i, path in run_sim(params, tr_factory, sched_factory):
            start_times = {}
            end_times = {}
            prev_pending: Set[Transaction] = set()
            prev_executing: Set[Transaction] = set()
            for state in path:
                for tr in prev_pending - state.pending:
                    start_times[tr] = state.clock
                prev_pending = state.pending
                for tr in prev_executing - {c.transaction for c in state.cores}:
                    end_times[tr] = state.clock + tr.time
                prev_executing = {c.transaction for c in state.cores}
            latencies = {end_times[t] - start_times[t] for t in start_times}
            hist, bin_edges = np.histogram(list(latencies), bins=32)
            print(hist)
            axis = axes[i][j]
            lines.append(axis.plot(bin_edges[1:], hist))
            if i == 0:
                axis.set_title(get_title(sched_factory))
        j += 1
        return lines

    run_sims(TournamentSchedulerFactory())

    midlines = run_sims(TournamentSchedulerFactory(is_pipelined=True))

    run_sims(GreedySchedulerFactory())

    if ARGS.poolsize <= 20:
        run_sims(MaximalSchedulerFactory())

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


def make_ps_table(tr_factory: TransactionGeneratorFactory) -> None:
    """Print minimum pool size as a function of scheduling time and core count."""
    sched_factory = TournamentSchedulerFactory()
    executor = RandomExecutor()

    class ScheduledCountSequence(Sequence[int]):
        def __init__(self, op_time, core_num):
            self.op_time = op_time
            self.core_num = core_num

        def __getitem__(self, key):
            pool_size = key
            if ARGS.verbose == 1:
                print("Trying pool size of", pool_size, end="...")
            if ARGS.verbose >= 2:
                print("Trying pool size of", pool_size)
            min_sched_counts = []
            params = SimulationParams(
                self.op_time, self.core_num, pool_size, ARGS.queuesize
            )
            for _, path in run_sim(ARGS, params, tr_factory, sched_factory, executor):
                scheduled_counts = {}
                for state in path:
                    # Skip over warm-up phase (until first transaction completes).
                    if (
                        len(state.incoming)
                        + len(state.pending)
                        + len(state.scheduled)
                        + len(state.cores)
                        == ARGS.n
                    ):
                        continue
                    if state.clock not in scheduled_counts:
                        scheduled_counts[state.clock] = (
                            len(state.scheduled) - state.core_count + len(state.cores)
                        )
                    # Skip over tail (when pool is empty).
                    if not state.incoming and len(state.pending) < ARGS.poolsize:
                        break
                min_sched_count = min(scheduled_counts.values())
                min_sched_counts.append(min_sched_count)
                if ARGS.verbose == 1:
                    print(min_sched_count, end=", ")
                if min_sched_count == 0:
                    break
            if ARGS.verbose == 1:
                print()
            if ARGS.verbose >= 2:
                print("Results:", ", ".join(map(str, min_sched_counts)))
            return min_sched_count

        def __len__(self):
            return ARGS.n

    sched_times = [2 ** logstime for logstime in range(ARGS.log_max_stime + 1)]
    core_counts = [2 ** logcores for logcores in range(ARGS.log_max_cores + 1)]

    thead, tbody = get_table_templates(
        varname="Minimum pool size for keeping the cores busy",
        xname="Number of cores",
        yname="HW operation time",
        xvals=core_counts,
        yvals=sched_times,
        max_value=ARGS.n,
        precision=0,
    )

    print(get_title(sched_factory, executor))
    print()
    print(thead)
    for sched_time in sched_times:
        min_poolsizes = []
        for core_count in core_counts:
            scheduled_counts = ScheduledCountSequence(sched_time, core_count)
            min_poolsize = bisect.bisect_left(scheduled_counts, 0, lo=1)
            min_poolsizes.append(min_poolsize)
        print(tbody.format(sched_time, *min_poolsizes))
    print()


def run_sim(
    params: SimulationParams,
    gen_factory: TransactionGeneratorFactory,
    sched_factory: TransactionSchedulerFactory,
    executor: TransactionExecutor = DEFAULT_EXECUTOR,
    obj_set_maker_factory: ObjSetMakerFactory = DEFAULT_OBJ_SET_MAKER_FACTORY,
):
    """Yield index and path through the state space found by the simulator."""
    for i in range(ARGS.repeats):
        gen = tr_factory()
        obj_set_maker = obj_set_maker_factory()
        scheduler = sched_factory(params.op_time, params.pool_size, params.queue_size)
        sim = Simulator(gen, obj_set_maker, scheduler, executor, params.core_num)
        yield i, sim.run(ARGS.verbose)


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


def get_title(*args: object):
    """Return title for printing."""
    return ", ".join([str(arg).lower() for arg in args]).capitalize()


if __name__ == "__main__":
    random.seed(0)

    ARGS = get_args()

    with open(ARGS.template, "rt") as template_file:
        tr_types: Dict[str, Dict[str, int]] = json.load(template_file)

    tr_factory = TransactionGeneratorFactory(
        ARGS.memsize, tr_types, ARGS.n, ARGS.repeats, ARGS.zipf_param
    )

    PROCESS_POOL = Pool()
    ARGS.func(tr_factory)
