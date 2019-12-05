"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
import random
import statistics

from machine import ConstantTimeScheduler, Machine
from transaction import TransactionGenerator


SCHEDULING_TIMES = list(range(11))
CORES = [1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128]


def _main():
    parser = ArgumentParser(description="Run puppetmaster on a range of input "
                            "distributions.")
    parser.add_argument('template', help="transaction template file",
                        type=FileType('rt'))
    parser.add_argument('n', help="total number of transactions", type=int)
    parser.add_argument('-m', '--memsize', help="memory size (# of objects)",
                        default=1000, type=int)
    parser.add_argument('-p', '--poolsize', help="size of scheduling pool",
                        type=int)
    parser.add_argument('-e', '--schedule', help="number of transactions to "
                        "schedule in one round", type=int, default=1)
    parser.add_argument('-s', help="parameter of the Zipf's law distribution",
                        default=1, type=float)
    parser.add_argument('-r', '--repeats', help="number of times the "
                        "experiment with a given set of parameters is re-run",
                        default=10, type=int)
    args = parser.parse_args()

    label = "Cores"
    col_label = "Sched. time"
    hcol_width = max(len(label),
                     len(col_label) + len(str(max(SCHEDULING_TIMES))) + 1)
    hcol = f"{{0:<{hcol_width}}}  "
    col_width = max(len(f"{10:.3f}"), len(str(max(CORES))))
    cols = "".join(f"{{{i + 1}:{col_width}.3f}}  "
                   for i in range(len(CORES)))
    hline = hcol + "".join(f"{{{i + 1}:{col_width}d}}  "
                           for i in range(len(CORES)))
    line = hcol + cols

    print(
        f"Average throughput for:\n"
        f"- template: {os.path.basename(args.template.name)}\n"
        f"- transactions: {args.n}\n"
        f"- [m]emory size: {args.memsize}\n"
        f"- scheduling [p]ool size: {args.poolsize or 'infinite'}\n"
        f"- concurr[e]ntly scheduled transactions: {args.schedule}\n"
        f"- object di[s]tribution parameter: {args.s:.2f}\n")
    print(hline.format(label, *CORES))

    tr_types = json.load(args.template)
    total_weight = sum(tr["weight"] for tr in tr_types.values())
    n_total_objects = 0
    for tr in tr_types.values():
        tr["N"] = int(round(args.n * tr["weight"] / total_weight))
        n_total_objects += tr["N"] * (tr["reads"] + tr["writes"])
    n_total_objects *= len(SCHEDULING_TIMES) * len(CORES) * args.repeats
    tr_gen = TransactionGenerator(args.memsize, n_total_objects, args.s)

    for sched_time in SCHEDULING_TIMES:
        avg_throughputs = []
        for cores in CORES:
            results = []
            for i in range(args.repeats):
                tr_data = []
                for type_ in tr_types.values():
                    tr_data.extend(type_ for i in range(type_["N"]))
                random.shuffle(tr_data)
                transactions = []
                for d in tr_data:
                    tr = next(tr_gen(d["reads"], d["writes"], d["time"], d["N"]))
                    transactions.append(tr)
                    if "rotate_most_popular" in d:
                        obj = next(iter(tr.write_set))
                        tr_gen.swap_most_popular(obj)
                scheduler = ConstantTimeScheduler(
                    sched_time, n_transactions=args.schedule)
                machine = Machine(cores, args.poolsize, scheduler)
                results.append(machine.run(transactions))
            avg_throughputs.append(args.n / statistics.mean(results))
        print(line.format(f"{col_label} {sched_time}", *avg_throughputs))


if __name__ == "__main__":
    _main()
