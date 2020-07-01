"""Classes that generate transactions for Puppetmaster."""

import random
from typing import Iterable, Iterator, List, Mapping, Sequence

from more_itertools import SequenceView

from model import TransactionFactory
from pmtypes import Transaction


class RandomFactory(TransactionFactory):
    """Generates new transactions based on a parametrized distribution."""

    def __init__(
        self,
        memory_size: int,
        tr_types: Iterable[Mapping[str, int]],
        tr_count: int,
        gen_count: int = 1,
        s_param: int = 0,
    ) -> None:
        """Create a new generator for `Transaction`s.

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
        self.tr_types = tuple(dict(cfg) for cfg in tr_types)
        total_weight = sum(tr["weight"] for tr in self.tr_types)
        self.gen_count = gen_count
        self.tr_count = 0
        self.total_tr_time = 0
        for tr in self.tr_types:
            tr["N"] = int(round(tr_count * tr["weight"] / total_weight))
            self.tr_count += tr["N"] * (tr["reads"] + tr["writes"])
            self.total_tr_time += tr["N"] * tr["time"]
        n_total_objects = self.tr_count * gen_count
        self.addresses = random.choices(
            range(memory_size), weights=zipf_weights, k=n_total_objects
        )
        self.gens = 0

    def __call__(self) -> Iterator[Transaction]:
        """See TransactionGenerator.__call__."""
        addr_start = self.tr_count * self.gens
        self.gens += 1
        addr_end = self.tr_count * self.gens
        addresses = SequenceView(self.addresses)[addr_start:addr_end]
        return self.TrGenerator(self, addresses)

    class TrGenerator(Iterator[Transaction]):
        """Yields new transactions."""

        def __init__(self, factory: "RandomFactory", addresses: Sequence[int]) -> None:
            """Create new TrGenerator."""
            self.factory = factory
            self.addresses = addresses
            self.tr_data: List[Mapping[str, int]] = []
            for type_ in self.factory.tr_types:
                self.tr_data.extend(type_ for i in range(type_["N"]))
            random.shuffle(self.tr_data)
            self.index = 0
            self.address_index = 0

        def __next__(self) -> Transaction:
            """Return next transaction with the given configuration."""
            if self.index == len(self.tr_data):
                raise StopIteration
            tr_conf = self.tr_data[self.index]
            self.index += 1
            read_start = self.address_index
            self.address_index += tr_conf["reads"]
            read_end = write_start = self.address_index
            self.address_index += tr_conf["writes"]
            write_end = self.address_index
            if self.address_index > len(self.addresses):
                raise RuntimeError("not enough addresses available")
            read_set = set(self.addresses[read_start:read_end])
            write_set = set(self.addresses[write_start:write_end])
            return Transaction(read_set, write_set, tr_conf["time"])

    @property
    def total_time(self) -> int:
        """See TransactionGenerator.total_time."""
        return self.total_tr_time
