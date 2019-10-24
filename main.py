"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
import random
import statistics

from generate import gen_transactions
from puppetmaster import ConstantTimeScheduler, Machine


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
    col_width = max(len(f"{1:.3f}"), len(str(max(CORES))))
    cols = "".join(f"{{{i + 1}:{col_width}.3f}}  "
                   for i in range(len(CORES)))
    hline = hcol + "".join(f"{{{i + 1}:{col_width}d}}  "
                           for i in range(len(CORES)))
    line = hcol + cols

    print(
        f"Average throughput w.r.t. number of cores for:\n"
        f"- template: {os.path.basename(args.template.name)}\n"
        f"- transactions: {args.n}\n"
        f"- memory size: {args.memsize}\n"
        f"- scheduling pool size: {args.poolsize or 'infinite'}\n"
        f"- concurrently scheduled transactions: {args.schedule}\n"
        f"- object distribution parameter: {args.s:.2f}\n")
    print(hline.format(label, *CORES))

    tr_types = json.load(args.template)
    total_weight = sum(tr["weight"] for tr in tr_types.values())
    for tr in tr_types.values():
        tr["N"] = args.n * tr["weight"] / total_weight
    tr_gen = gen_transactions(args.memsize, args.s)

    for sched_time in SCHEDULING_TIMES:
        avg_throughputs = []
        for cores in CORES:
            results = []
            for i in range(args.repeats):
                transactions = []
                weight_sum = sum(tr["weight"] for tr in tr_types.values())
                for name, prop in tr_types.items():
                    N = int(round(args.n * prop["weight"] / weight_sum))
                    transactions.extend(
                        tr_gen(prop["reads"], prop["writes"], prop["time"], N))
                random.shuffle(transactions)
                scheduler = ConstantTimeScheduler(
                    sched_time, n_transactions=args.schedule)
                machine = Machine(cores, args.poolsize, scheduler)
                results.append(machine.run(transactions))
            avg_throughputs.append(args.n / statistics.mean(results))
        print(line.format(f"{col_label} {sched_time}", *avg_throughputs))


if __name__ == "__main__":
    _main()
