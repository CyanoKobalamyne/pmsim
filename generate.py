"""Generators supplying data for simulations."""
import random

from puppetmaster import Transaction


def gen_transactions(memory_size, s=1):
    """Return a generator that yields `Transaction`s.

    A pool of objects with size `memory_size` is created first,
    and the read and write sets are chosen from this set in accordance
    with Zipf's law.

    Arguments:
        memory_size: size of the pool from which objects in the read and
                     write sets are selected
        s: parameter of the Zipf's law distribution

    Returns:
        a generator with three arguments (see below) that yields `Transaction`s
            read_set_size: size of the read sets
            write_set_size: size of the write sets
            time: transaction time

    """
    object_pool = [object() for _ in range(memory_size)]
    zipf_weights = [1 / (i + 1)**s for i in range(memory_size)]

    def mem_objects(N):
        return set(random.choices(object_pool, k=N, weights=zipf_weights))

    def generator(read_set_size, write_set_size, time, count):
        for _ in range(count):
            yield Transaction(
                mem_objects(read_set_size),
                mem_objects(write_set_size),
                time)

    return generator
