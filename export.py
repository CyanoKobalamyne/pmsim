#!/bin/env python3
"""Script to export generated transactions."""
import itertools
import json
import random
from argparse import ArgumentParser, FileType, Namespace
from collections.abc import Collection, Iterable, Mapping

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


def to_csv(
    tr_types: Collection[Mapping[str, int]], tr_generator: TransactionGenerator
) -> Iterable[str]:
    """Yield transactions as comma-separated values (CSV)."""
    # Header
    max_read_set_size = max(t["reads"] for t in tr_types)
    max_write_set_size = max(t["writes"] for t in tr_types)
    read_obj_labels = map("Read object {}".format, range(max_read_set_size))
    written_obj_labels = map("Written object {}".format, range(max_write_set_size))
    yield f"Type,{','.join(read_obj_labels)},{','.join(written_obj_labels)}"

    def csvify(values: Collection[int], length: int):
        return ",".join(
            str(value)
            for value, _ in itertools.zip_longest(values, range(length), fillvalue="")
        )

    for transaction in tr_generator:
        if transaction is None:
            continue
        yield (
            f"{transaction.label},{csvify(transaction.read_set, max_read_set_size)},"
            f"{csvify(transaction.write_set, max_write_set_size)}"
        )


FORMATTERS = {"csv": to_csv}

if __name__ == "__main__":
    random.seed(0)

    args = get_args()

    tr_types: dict[str, dict[str, int]] = json.load(args.template)
    tr_factory = TransactionGeneratorFactory(
        mem_size=args.memory_size,
        tr_types=tr_types,
        tr_count=args.count,
        zipf_param=args.zipf_param,
    )

    formatter = FORMATTERS[args.format]
    for line in formatter(tr_types.values(), tr_factory()):
        print(line)
