"""Classes that generate transactions for Puppetmaster."""

import random
from typing import Generator, Iterable, Iterator, List, Mapping

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
        self.objects = [object() for _ in range(memory_size)]
        zipf_weights = [1 / (i + 1) ** s_param for i in range(memory_size)]
        self.tr_types = tuple(dict(cfg) for cfg in tr_types)
        total_weight = sum(tr["weight"] for tr in self.tr_types)
        n_total_objects = 0
        self.total_tr_time = 0
        for tr in self.tr_types:
            tr["N"] = int(round(tr_count * tr["weight"] / total_weight))
            n_total_objects += tr["N"] * (tr["reads"] + tr["writes"])
            self.total_tr_time += tr["N"] * tr["time"]
        n_total_objects *= gen_count
        self.indices = random.choices(
            range(memory_size), weights=zipf_weights, k=n_total_objects
        )
        self.last_used = 0

    def __call__(self) -> Generator[Transaction, None, None]:
        """See TransactionGenerator.__call__."""
        return self.TrGenerator(self)

    class TrGenerator(Iterator[Transaction]):
        """Yields new transactions."""

        def __init__(self, factory: "RandomFactory") -> None:
            """Create new TrGenerator."""
            self.factory = factory
            self.tr_data: List[Mapping[str, int]] = []
            for type_ in self.factory.tr_types:
                self.tr_data.extend(type_ for i in range(type_["N"]))
            random.shuffle(self.tr_data)
            self.index = 0

        def __next__(self) -> Transaction:
            """Return next transaction with the given configuration."""
            if len(self.factory.indices) < self.factory.last_used:
                raise RuntimeError(
                    f"not enough objects generated, "
                    f"{len(self.factory.indices)} < {self.factory.last_used}."
                )
            if self.index == len(self.tr_data):
                raise StopIteration
            tr_conf = self.tr_data[self.index]
            self.index += 1
            start = self.factory.last_used
            mid = start + tr_conf["reads"]
            end = mid + tr_conf["writes"]
            self.factory.last_used = end
            read_set = {
                self.factory.objects[i] for i in self.factory.indices[start:mid]
            }
            write_set = {self.factory.objects[i] for i in self.factory.indices[mid:end]}
            return Transaction(read_set, write_set, tr_conf["time"])

    @property
    def total_time(self) -> int:
        """See TransactionGenerator.total_time."""
        return self.total_tr_time
