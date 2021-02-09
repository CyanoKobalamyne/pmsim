#!/bin/env python3
"""Script to export generated transactions."""
import json
import random
from argparse import ArgumentParser, FileType, Namespace
from itertools import zip_longest as lzip
from typing import Dict, Iterable

from generator import TransactionGenerator, TransactionGeneratorFactory


def get_args() -> Namespace:
    """Parse and print command-line arguments."""
    parser = ArgumentParser(
        description="Export transactions generated for Puppetmaster"
    )
    parser.add_argument("template", help="template file", type=FileType("rt"))
    parser.add_argument("count", help="number of transactions", type=int)
    parser.add_argument(
        "-f", "--format", help="output format", choices=FORMATTERS, default="csv"
    )
    parser.add_argument(
        "-m", "--memory-size", help="memory size (# of objects)", type=int, default=1024
    )
    parser.add_argument(
        "-z", "--zipf-param", help="Zipf distribution parameter", type=float, default=0
    )
    return parser.parse_args()


def to_csv(trs: TransactionGenerator, max_reads: int, max_writes: int) -> Iterable[str]:
    """Yield transactions as comma-separated values (CSV)."""
    # Header
    read_obj_labels = ",".join(map("Read object {}".format, range(max_reads)))
    written_obj_labels = ",".join(map("Written object {}".format, range(max_writes)))
    yield f"Type,{read_obj_labels},{written_obj_labels}"

    def csv_join(values: Iterable, n_fields: int):
        return ",".join(str(v) for v, _ in lzip(values, range(n_fields), fillvalue=""))

    for transaction in trs:
        if transaction is None:
            continue
        read_obj_fields = csv_join(transaction.read_set, max_reads)
        written_obj_fields = csv_join(transaction.write_set, max_writes)
        yield f"{transaction.label},{read_obj_fields},{written_obj_fields}"


FORMATTERS = {"csv": to_csv}

if __name__ == "__main__":
    random.seed(0)

    args = get_args()

    tr_types: Dict[str, Dict[str, int]] = json.load(args.template)
    tr_factory = TransactionGeneratorFactory(
        mem_size=args.memory_size,
        tr_types=tr_types,
        tr_count=args.count,
        zipf_param=args.zipf_param,
    )

    formatter = FORMATTERS[args.format]
    transactions = tr_factory()
    max_reads = max(t["reads"] for t in tr_types.values())
    max_writes = max(t["writes"] for t in tr_types.values())
    for line in formatter(transactions, max_reads, max_writes):
        print(line)
