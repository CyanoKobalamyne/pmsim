"""Generators supplying data for simulations."""
import random

from puppetmaster import Transaction


def gen_transactions(memory_size, read_set_size, write_set_size, time, s=1):
    """Yield `Transaction`s with the specified parameters.

    A pool of objects with size `memory_size` is created first,
    and the read and write sets are chosen from this set in accordance
    with Zipf's law.

    Arguments:
        memory_size: size of the pool from which objects in the read and
                     write sets are selected
        read_set_size: size of the read sets
        write_set_size: size of the write sets
        time: transaction time
        s: parameter of the Zipf's law distribution

    Yields:
        a `Transaction` with a randomly chosen size and time

    """
    object_pool = [object() for _ in range(memory_size)]
    zipf_weights = [1 / (i + 1)**s for i in range(memory_size)]

    def mem_objects(N):
        return random.choices(object_pool, k=N, weights=zipf_weights)

    while True:
        yield Transaction(
            set(mem_objects(read_set_size)),
            set(mem_objects(write_set_size)),
            time)
