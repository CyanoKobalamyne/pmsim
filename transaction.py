"""Classes for representing, creating, and handling transactions."""
from collections.abc import MutableSet
import random


class Transaction:
    """An atomic operation in the model."""

    def __init__(self, read_set, write_set, time):
        """Create a transaction.

        Attributes:
            read_set: the set of objects that this transaction needs to read
            write_set: the set of objects that this transaction needs to write
                       and possibly also read
            time: the amount of time units it takes to execute this transaction

        """
        self.read_set = read_set
        self.write_set = write_set
        self.time = time

    def __hash__(self):
        """Return a hash value for this transaction."""
        return id(self)

    def __str__(self):
        """Return a human-readable representation of this transaction."""
        return f"<Transaction: id {id(self)}, length {self.time}"


class TransactionSet(MutableSet):
    """A set of transactions."""

    def __init__(self, transactions=()):
        """Create a new set."""
        self.transactions = set()
        self.read_set = set()
        self.write_set = set()
        for t in transactions:
            self.add(t)

    def __contains__(self, transaction):
        """Return if the given transaction is in the set."""
        return transaction in self.transactions

    def __iter__(self):
        """Yield each transaction in the set."""
        yield from self.transactions

    def __len__(self):
        """Return the number of transactions in the set."""
        return len(self.transactions)

    def add(self, transaction):
        """Add a new transaction to the set."""
        self.transactions.add(transaction)
        self.read_set |= transaction.read_set
        self.write_set |= transaction.write_set

    def discard(self, transaction):
        """Remove a transaction from the set.

        Warning: does not update the combined read and write sets.
        """
        self.transactions.discard(transaction)

    def compatible(self, transaction):
        """Return whether the given transaction is compatible with this set."""
        for read_obj in transaction.read_set:
            if read_obj in self.write_set:
                return False
        for write_obj in transaction.write_set:
            if write_obj in self.read_set or write_obj in self.write_set:
                return False
        return True


class TransactionGenerator:
    """Generates new transactions based on a parametrized distribution."""

    def __init__(self, memory_size, s=1):
        """Create a new generator for `Transaction`s.

        A pool of objects with size `memory_size` is created, and the read and
        write sets are chosen from there in accordance with Zipf's law.

        Arguments:
            memory_size: size of the pool from which objects in the read and
                         write sets are selected
            s: parameter of the Zipf's law distribution
        """
        self.object_pool = [object() for _ in range(memory_size)]
        self.weights = [1 / (i + 1)**s for i in range(memory_size)]

    def __call__(self, read_set_size, write_set_size, time, count):
        """Yield new `Transaction`s.

        Arguments:
            read_set_size: size of the read sets
            write_set_size: size of the write sets
            time: transaction time

        Yields:
            transactions with the specified properties.

        """
        size = read_set_size + write_set_size
        k = count * size
        objects = random.choices(self.object_pool, k=k, weights=self.weights)
        for i in range(count):
            start = i * count
            mid = start + read_set_size
            end = mid + write_set_size
            read_set = set(objects[start:mid])
            write_set = set(objects[mid:end])
            yield Transaction(read_set, write_set, time)
