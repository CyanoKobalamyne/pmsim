"""Generators supplying data for simulations."""
import random

from puppetmaster import Transaction


def gen_transactions(max_read_objects, max_write_objects, max_time,
                     memory_size):
    """Yield `Transaction`s with the specified parameters.

    A pool of objects with size `memory_size` is created first,
    and the read and write sets are chosen from this set in accordance
    with Zipf's Law.

    Arguments:
        max_read_objects: maximum size of the read sets
        max_write_objects: maximum size of the write sets
        max_time: maximum transaction time
        memory_size: total number of objects from which the read and write
                     sets are selected

    Yields:
        Transaction: a Transaction with a randomly chosen size and time

    """
    object_pool = [object() for _ in range(memory_size)]

    def objects(*size_range):
        min_, max_ = size_range
        zipf_weights = [1 / (i + 1) for i in range(max_ - min_ + 1)]
        size = random.choices(list(range(min_, max_ + 1)),
                              weights=zipf_weights)[0]
        return random.choices(object_pool, k=size)

    while True:
        yield Transaction(
            set(objects(1, max_read_objects)),
            set(objects(0, max_write_objects)),
            random.randint(1, max_time))
