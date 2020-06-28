"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from collections.abc import MutableSet


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


class TransactionGenerator(ABC):
    """Generates new transactions."""

    @abstractmethod
    def __call__(self, read_set_size, write_set_size, time, count):
        """Yield new `Transaction`s.

        Arguments:
            read_set_size: size of the read sets
            write_set_size: size of the write sets
            time: transaction time

        Yields:
            transactions with the specified properties.

        """


class TransactionScheduler(ABC):
    """Represents that scheduling unit within Puppetmaster."""

    @abstractmethod
    def run(self, pending, ongoing):
        """Try scheduling a batch of transactions.

        Arguments:
            pending: set of transactions waiting to be executed
            ongoing: set of transactions currently being executed

        Returns:
            a set of transactions ready to be executed concurrently with the
            currently running ones without conflicts

        """


class Core:
    """Component of `Machine` executing a single transaction.

    Attributes:
        clock (int): time elapsed since the start of the machine in "ticks"
        transaction (Transaction): the transaction being executed or None if
                                   the core is idle

    """

    def __init__(self, clock_start=0, transaction=None):
        """Create a new core.

        Arguments:
            clock_start (int): initial value for the clock
            transaction: initial transaction being executed

        """
        self.clock = clock_start
        self.transaction = transaction


class Machine:
    """Device capable of executing transactions in parallel."""

    def __init__(self, n_cores, pool_size):
        """Create a new machine.

        Arguments:
            n_cores (int): number of execution units (cores) available
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)

        """
        self.cores = [Core() for _ in range(n_cores)]
        self.pool_size = pool_size
