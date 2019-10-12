"""Main executable for running puppetmaster."""
from argparse import ArgumentParser, FileType
import json
import os
import statistics

from generate import gen_transactions
from puppetmaster import Machine, Scheduler


SCHEDULING_TIMES = list(range(11))
MEMORY_SIZES = [0.01, 0.1, 1, 10, 100]


def _main():
    parser = ArgumentParser(description="Run puppetmaster on a range of input "
                            "distributions.")
    parser.add_argument('template', help="transaction template file",
                        type=FileType('rt'))
    parser.add_argument('n', help="total number of transactions", type=int)
    parser.add_argument('-c', '--cores', help="number of cores in the machine",
                        default=4, type=int)
    parser.add_argument('-s', help="parameter of the Zipf's law distribution",
                        default=1, type=float)
    parser.add_argument('-r', '--repeats', help="number of times the "
                        "experiment with a given set of parameters is re-run",
                        default=10, type=int)
    args = parser.parse_args()

    label = "MemSize \\ SchedTime"
    hcol = f"{{0:<{max(len(label), len(str(max(MEMORY_SIZES) * args.n)))}}}  "
    col_width = max(len(f"{1:.3f}"), len(str(max(SCHEDULING_TIMES))))
    cols = "".join(f"{{{i + 1}:{col_width}.3f}}  "
                   for i in range(len(SCHEDULING_TIMES)))
    hline = hcol + "".join(f"{{{i + 1}:{col_width}d}}  "
                           for i in range(len(SCHEDULING_TIMES)))
    line = hcol + cols

    print(f"Average throughput for run with {args.n} transactions and "
          f"{os.path.basename(args.template.name)} template "
          f"on {args.cores} cores where s={args.s:.2f}")
    print("----------")
    print(hline.format(label, *SCHEDULING_TIMES))

    tr_types = json.load(args.template)
    total_weight = sum(tr["weight"] for tr in tr_types.values())
    for tr in tr_types.values():
        tr["N"] = args.n * tr["weight"] / total_weight

    for multiplier in MEMORY_SIZES:
        mem_size = int(round(multiplier * args.n))
        tr_gen = gen_transactions(mem_size, args.s)
        avg_throughputs = []
        for sched_time in SCHEDULING_TIMES:
            results = []
            for i in range(args.repeats):
                transactions = []
                weight_sum = sum(tr["weight"] for tr in tr_types.values())
                for name, prop in tr_types.items():
                    N = int(round(args.n * prop["weight"] / weight_sum))
                    transactions.extend(
                        tr_gen(prop["reads"], prop["writes"], prop["time"], N))
                machine = Machine(n_cores=args.cores,
                                  scheduler=Scheduler(),
                                  scheduling_time=sched_time)
                results.append(machine.run(transactions))
            avg_throughputs.append(args.n / statistics.mean(results))
        print(line.format(mem_size, *avg_throughputs))


if __name__ == "__main__":
    _main()
