"""Classes that generate transactions for Puppetmaster."""

import random
from typing import Iterable, List, Mapping

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
        s: int = 1,
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
            s: parameter of the Zipf's law distribution
        """
        self.objects = [object() for _ in range(memory_size)]
        zipf_weights = [1 / (i + 1) ** s for i in range(memory_size)]
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

    def __call__(self) -> Iterable[Transaction]:
        """See TransactionGenerator.__call__."""
        tr_data: List[Mapping[str, int]] = []
        for type_ in self.tr_types:
            tr_data.extend(type_ for i in range(type_["N"]))
        random.shuffle(tr_data)
        for d in tr_data:
            tr = self.get_next(d)
            yield tr
            if "rotate_most_popular" in d and d["rotate_most_popular"]:
                obj = next(iter(tr.write_set))
                self.swap_most_popular(obj)

    def get_next(self, tr_conf: Mapping[str, int]) -> Transaction:
        """Return next transaction with the given configuration."""
        if len(self.indices) <= self.last_used:
            raise RuntimeError(
                f"not enough objects generated, "
                f"{len(self.indices)} > {self.last_used}."
            )
        start = self.last_used
        mid = start + tr_conf["reads"]
        end = mid + tr_conf["writes"]
        self.last_used = end
        read_set = {self.objects[i] for i in self.indices[start:mid]}
        write_set = {self.objects[i] for i in self.indices[mid:end]}
        return Transaction(read_set, write_set, tr_conf["time"])

    @property
    def total_time(self) -> int:
        """See TransactionGenerator.total_time."""
        return self.total_tr_time

    def swap_most_popular(self, obj: object) -> None:
        """Swap `obj` with the most popular object in the distribution."""
        if self.objects[0] is not obj:
            i = self.objects.index(obj)
            self.objects[1 : i + 1] = self.objects[:i]
            self.objects[0] = obj
