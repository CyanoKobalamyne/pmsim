"""Classes that create transaction generators for Puppetmaster."""

import itertools
import random
from typing import Iterable, Iterator, Mapping

from more_itertools import SequenceView

from generators import TransactionGenerator
from model import TransactionFactory
from pmtypes import Transaction


class RandomFactory(TransactionFactory):
    """Makes new transaction generators based on a parametrized distribution."""

    def __init__(
        self,
        mem_size: int,
        tr_types: Iterable[Mapping[str, int]],
        tr_count: int,
        run_count: int = 1,
        zipf_param: int = 0,
    ) -> None:
        """Create a new factory for transactions.

        A pool of objects with size `memory_size` is created, and the read and
        write sets are chosen from there in accordance with Zipf's law.

        Arguments:
            mem_size: size of the pool from which objects in the read and
                      write sets are selected
            tr_types: transaction configurations, mappings with the following entries:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            tr_count: total number of transactions needed per run
            run_count: number of simulation runs (that should have different input)
            zipf_param: parameter of the Zipf's law distribution
        """
        self.tr_count = tr_count
        self.run_count = run_count
        self.run_index = 0

        # Compute exact transaction and object counts and total time.
        tr_counts = []
        self.obj_count = 0
        self.total_tr_time = 0
        total_weight = sum(tr["weight"] for tr in tr_types)
        for tr in tr_types:
            cur_tr_count = int(round(tr_count * tr["weight"] / total_weight))
            tr_counts.append(cur_tr_count)
            self.obj_count += cur_tr_count * (tr["reads"] + tr["writes"])
            self.total_tr_time += cur_tr_count * tr["time"]

        # Generate transaction metadata in randomized order.
        one_tr_data = list(
            itertools.chain(
                *(
                    (tr_type for _ in range(tr_counts[i]))
                    for i, tr_type in enumerate(tr_types)
                )
            )
        )
        self.tr_data = []
        for _ in range(run_count):
            random.shuffle(one_tr_data)
            self.tr_data.extend(one_tr_data)

        # Generate memory addresses according to distribution.
        addr_count = self.obj_count * run_count
        weights = [1 / (i + 1) ** zipf_param for i in range(mem_size)]
        self.addresses = random.choices(range(mem_size), k=addr_count, weights=weights)

    def __iter__(self) -> Iterator[Transaction]:
        """Create a new iterator of transactions."""
        if self.run_index == self.run_count:
            self.run_index = 0
        tr_start = self.tr_count * self.run_index
        addr_start = self.obj_count * self.run_index
        self.run_index += 1
        tr_end = self.tr_count * self.run_index
        addr_end = self.obj_count * self.run_index
        tr_data = SequenceView(self.tr_data)[tr_start:tr_end]
        addresses = SequenceView(self.addresses)[addr_start:addr_end]
        return TransactionGenerator(tr_data, addresses)

    def __len__(self):
        """Return the number of transactions per iterator."""
        return len(self.tr_data)

    @property
    def total_time(self) -> int:
        """See TransactionFactory.total_time."""
        return self.total_tr_time
