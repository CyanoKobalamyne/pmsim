"""Classes that create transaction generators for Puppetmaster."""

import random
from typing import Iterable, Iterator, List, Mapping

from more_itertools import SequenceView

from generators import TransactionGenerator
from model import TransactionFactory
from pmtypes import Transaction


class RandomFactory(TransactionFactory):
    """Makes new transaction generators based on a parametrized distribution."""

    def __init__(
        self,
        memory_size: int,
        tr_types: Iterable[Mapping[str, int]],
        tr_count: int,
        gen_count: int = 1,
        s_param: int = 0,
    ) -> None:
        """Create a new factory for transactions.

        A pool of objects with size `memory_size` is created, and the read and
        write sets are chosen from there in accordance with Zipf's law.

        Arguments:
            memory_size: size of the pool from which objects in the read and
                         write sets are selected
            tr_types: transaction configurations, mappings with the following entries:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            tr_count: total number of transactions needed per run
            gen_count: number of runs
            s_param: parameter of the Zipf's law distribution
        """
        zipf_weights = [1 / (i + 1) ** s_param for i in range(memory_size)]
        total_weight = sum(tr["weight"] for tr in tr_types)
        self.gen_count = gen_count
        self.tr_counts = []
        self.obj_count = 0
        self.total_tr_time = 0
        for tr in tr_types:
            cur_tr_count = int(round(tr_count * tr["weight"] / total_weight))
            self.obj_count += cur_tr_count * (tr["reads"] + tr["writes"])
            self.total_tr_time += cur_tr_count * tr["time"]
            self.tr_counts.append(cur_tr_count)
        self.tr_data: List[Mapping[str, int]] = []
        for i, tr_type in enumerate(tr_types):
            self.tr_data.extend(tr_type for _ in range(self.tr_counts[i]))
        n_total_objects = self.obj_count * gen_count
        self.addresses = random.choices(
            range(memory_size), weights=zipf_weights, k=n_total_objects
        )
        self.gens = 0

    def __call__(self) -> Iterator[Transaction]:
        """See TransactionFactory.__call__."""
        random.shuffle(self.tr_data)
        addr_start = self.obj_count * self.gens
        self.gens += 1
        addr_end = self.obj_count * self.gens
        addresses = SequenceView(self.addresses)[addr_start:addr_end]
        return TransactionGenerator(self.tr_data, addresses)

    @property
    def total_time(self) -> int:
        """See TransactionFactory.total_time."""
        return self.total_tr_time
