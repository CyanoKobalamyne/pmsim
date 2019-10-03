"""Main executable for running puppetmaster."""
import statistics
from argparse import ArgumentParser

from generate import gen_transactions
from puppetmaster import Machine, Scheduler


def _main():
    parser = ArgumentParser(description="Run puppetmaster on a range of input distributions.")
    parser.add_argument('n', help="total number of transactions", type=int)
    parser.add_argument('max_time', help="maximum transaction time", type=int)
    parser.add_argument('-s', '--step', help="difference in between transaction times of consecutive runs", default=1, type=int)
    parser.add_argument('-o', '--objects', help="maximum number of objects in a transaction", default=20, type=int)
    parser.add_argument('-x', '--multiplier', help="multiplier factor between the difference in the maximum size of the read and write sets for a transaction", default=10, type=int)
    parser.add_argument('-m', '--memory-size', help="total size of memory", default=1000, type=int)
    parser.add_argument('-c', '--cores', help="number of cores in the machine", default=4, type=int)
    parser.add_argument('-r', '--repeats', help="number of times the experiment with a given set of parameters is re-run", default=100, type=int)
    args = parser.parse_args()
    print(f"Arguments: {vars(args)}\n")

    max_write_objects = int(round(args.objects / (args.multiplier + 1)))
    max_read_objects = int(round(args.objects * args.multiplier / (args.multiplier + 1)))

    print("Tr. time  Min     Avg  Max")
    for time in range(1, args.max_time + 1, args.step):
        results = []
        for i in range(args.repeats):
            tr_gen = gen_transactions(max_read_objects, max_write_objects, time, args.memory_size)
            transactions = [next(tr_gen) for _ in range(args.n)]
            machine = Machine(n_cores=args.cores, scheduler=Scheduler())
            results.append(machine.run(transactions))
        print(f"{time:<8d}  {min(results):3d}  {statistics.mean(results):6.2f}  {max(results):3d}")


if __name__ == "__main__":
    _main()
