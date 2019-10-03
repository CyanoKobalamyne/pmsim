"""Generators supplying data for simulations."""
import random

from puppetmaster import Transaction


def gen_transactions(max_read_objects, max_write_objects, max_time, memory_size):
    """Yield `Transaction`s with the specified parameters.

    A pool of objects with size `memory_size` is created first,
    and the read and write sets are chosen uniformly from this set.

    Arguments:
        size_range: minimum and maximum size of read and write sets
        time_range: minimum and maximum transaction time
        memory_size: total number of objects from which the read and write
                     sets are selected

    Yields:
        Transaction: a Transaction with a randomly chosen size and time

    """
    object_pool = [object() for _ in range(memory_size)]

    def objects(max_size):
        return random.choices(object_pool, k=random.randint(1, max_size))

    while True:
        yield Transaction(
            set(objects(max_read_objects)),
            set(objects(max_write_objects)),
            random.randint(1, max_time))
