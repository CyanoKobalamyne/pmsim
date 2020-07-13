#!/bin/env python3
"""Plot statistics of resource usage during simulations."""

from argparse import ArgumentParser, FileType
import json
from pathlib import PurePath
import random
from typing import Dict

import numpy as np
import matplotlib.pyplot as plt

from executors import RandomExecutor
from factories import RandomFactory
from schedulers import GreedyScheduler, MaximalScheduler, TournamentScheduler
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
        default=5,
        type=int,
    )
    parser.add_argument(
        "-t", "--op-time", help="length of one hardware operation", default=0, type=int,
    )
    parser.add_argument(
        "-c", "--num-cores", help="number of execution cores", default=1, type=int,
    )
    args = parser.parse_args()

    filename = (
        f"{PurePath(args.template.name).stem}_{args.n}_m{args.memsize}_p{args.poolsize}"
        f"_q{args.queuesize if args.queuesize is not None else 'inf'}"
        f"_z{args.zipf_param}_t{args.op_time}_c{args.num_cores}.pdf"
    )

    fig, axes = plt.subplots(args.repeats, 4)

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = RandomFactory(
        args.memsize, tr_types.values(), args.n, args.repeats, args.zipf_param
    )

    j = 0

    def run_sim(title, sched_cls, sched_args):
        nonlocal j
        print(title)
        lines = []
        for i in range(args.repeats):
            transactions = iter(tr_factory)
            scheduler = sched_cls(args.op_time, args.queuesize, **sched_args)
            executor = RandomExecutor()
            sim = Simulator(
                transactions, scheduler, executor, args.num_cores, args.poolsize
            )
            path = sim.run()
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

    run_sim("Tournament scheduler", TournamentScheduler, {})

    midlines = run_sim(
        "Tournament scheduler (fully pipelined)",
        TournamentScheduler,
        {"is_pipelined": True},
    )

    run_sim("Greedy scheduler", GreedyScheduler, {})

    if args.poolsize <= 20:
        run_sim("Maximal scheduler", MaximalScheduler, {})

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


if __name__ == "__main__":
    _main()
