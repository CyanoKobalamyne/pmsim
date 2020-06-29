#!/bin/env python3
"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
import random
import statistics

from executors import RandomExecutor
from generators import RandomGenerator
from schedulers import ConstantTimeScheduler
from simulator import Simulator


def _main():
    parser = ArgumentParser(
        description="Run puppetmaster on a range of input " "distributions."
    )
    parser.add_argument(
        "template", help="transaction template file", type=FileType("rt")
    )
    parser.add_argument("n", help="total number of transactions", type=int)
    parser.add_argument(
        "-m", "--memsize", help="memory size (# of objects)", default=1000, type=int
    )
    parser.add_argument("-p", "--poolsize", help="size of scheduling pool", type=int)
    parser.add_argument(
        "-e",
        "--schedule",
        help="number of transactions to " "schedule in one round",
        type=int,
        default=1,
    )
    parser.add_argument(
        "-s", help="parameter of the Zipf's law distribution", default=1, type=float
    )
    parser.add_argument(
        "-r",
        "--repeats",
        help="number of times the "
        "experiment with a given set of parameters is re-run",
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
    col_width = max(len(f"{10:.{precision}f}"), len(str(core_counts[-1])))
    cols_header_template = "".join(
        f"{{{i + 1}:{col_width}d}}  " for i in range(len(core_counts))
    )
    cols_body_template = "".join(
        f"{{{i + 1}:{col_width}.{precision}f}}  " for i in range(len(core_counts))
    )
    header_template = col1_template + cols_header_template
    body_template = col1_template + cols_body_template

    print(
        f"Parameters:\n"
        f"- template: {os.path.basename(args.template.name)}\n"
        f"- transactions: {args.n}\n"
        f"- [m]emory size: {args.memsize}\n"
        f"- scheduling [p]ool size: {args.poolsize or 'infinite'}\n"
        f"- concurr[e]ntly scheduled transactions: {args.schedule}\n"
        f"- object di[s]tribution parameter: {args.s:.2f}\n"
    )
    print(" " * col1_width + title)
    print(header_template.format(col1_header, *core_counts))

    tr_types = json.load(args.template)
    total_weight = sum(tr["weight"] for tr in tr_types.values())
    n_total_objects = 0
    total_tr_time = 0
    for tr in tr_types.values():
        tr["N"] = int(round(args.n * tr["weight"] / total_weight))
        n_total_objects += tr["N"] * (tr["reads"] + tr["writes"])
        total_tr_time += tr["N"] * tr["time"]
    n_total_objects *= len(sched_times) * len(core_counts) * args.repeats
    tr_gen = RandomGenerator(args.memsize, n_total_objects, args.s)

    for sched_time in sched_times:
        throughputs = []
        for core_count in core_counts:
            results = []
            for _ in range(args.repeats):
                tr_data = []
                for type_ in tr_types.values():
                    tr_data.extend(type_ for i in range(type_["N"]))
                random.shuffle(tr_data)
                transactions = []
                for d in tr_data:
                    tr = next(tr_gen(d["reads"], d["writes"], d["time"], d["N"]))
                    transactions.append(tr)
                    if "rotate_most_popular" in d and d["rotate_most_popular"]:
                        obj = next(iter(tr.write_set))
                        tr_gen.swap_most_popular(obj)
                scheduler = ConstantTimeScheduler(
                    sched_time, n_transactions=args.schedule
                )
                executor = RandomExecutor(core_count)
                sim = Simulator(scheduler, executor, args.poolsize)
                results.append(sim.run(transactions))
            throughputs.append(total_tr_time / statistics.mean(results))
        print(body_template.format(f"{sched_time}", *throughputs))


if __name__ == "__main__":
    _main()
