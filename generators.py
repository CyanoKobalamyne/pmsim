"""Classes that generate transactions for Puppetmaster."""

import random

from model import Transaction, TransactionGenerator


class RandomGenerator(TransactionGenerator):
    """Generates new transactions based on a parametrized distribution."""

    def __init__(self, memory_size, n_total_objects, s=1):
        """Create a new generator for `Transaction`s.

        A pool of objects with size `memory_size` is created, and the read and
        write sets are chosen from there in accordance with Zipf's law.

        Arguments:
            memory_size: size of the pool from which objects in the read and
                         write sets are selected
            n_total_objects: number of objects that will be needed by the
                             generated transactions
            s: parameter of the Zipf's law distribution
        """
        self.objects = [object() for _ in range(memory_size)]
        zipf_weights = [1 / (i + 1) ** s for i in range(memory_size)]
        self.indices = random.choices(
            range(memory_size), weights=zipf_weights, k=n_total_objects
        )
        self.last_used = 0

    def __call__(self, read_set_size, write_set_size, time):
        """See TransactionGenerator.__call__."""
        if len(self.indices) <= self.last_used:
            raise RuntimeError(
                f"not enough objects generated, "
                f"{len(self.indices)} > {self.last_used}."
            )
        start = self.last_used
        mid = start + read_set_size
        end = mid + write_set_size
        self.last_used = end
        read_set = {self.objects[i] for i in self.indices[start:mid]}
        write_set = {self.objects[i] for i in self.indices[mid:end]}
        return Transaction(read_set, write_set, time)

    def swap_most_popular(self, obj):
        """Swap `obj` with the most popular object in the distribution."""
        if self.objects[0] is not obj:
            i = self.objects.index(obj)
            self.objects[1 : i + 1] = self.objects[:i]
            self.objects[0] = obj
