"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from collections.abc import MutableSet
from dataclasses import dataclass
from typing import Optional


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


class TimedComponent(ABC):
    """Common superclass for all components with an internal clock."""

    @property
    @abstractmethod
    def clock(self):
        """Return the value of the global clock of the component."""

    @clock.setter
    def clock(self, value):
        """Set the value of the global clock of the component.

        The clock is monotonic, i.e. it will never be decreased.
        """


class TransactionGenerator(ABC):
    """Generates new transactions."""

    @abstractmethod
    def __call__(self, read_set_size, write_set_size, time):
        """Return a new transaction.

        Arguments:
            read_set_size: size of the read sets
            write_set_size: size of the write sets
            time: transaction time

        Returns:
            transaction with the specified properties.

        """


class TransactionScheduler(TimedComponent, ABC):
    """Represents the scheduling unit within Puppetmaster."""

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


class TransactionExecutor(TimedComponent, ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    @abstractmethod
    def push(self, scheduled):
        """Choose transaction(s) to execute from scheduled set.

        Removes the transactions executed, if any.
        """

    @abstractmethod
    def pop(self):
        """Remove the first finished transaction.

        Returns:
            int: current clock of core that was just flushed.
        """

    @abstractmethod
    def has_free_cores(self):
        """Return true if there are idle cores."""

    @property
    @abstractmethod
    def running(self):
        """Return list of currently executing transactions."""


@dataclass
class Core:
    """Component executing a single transaction.

    Attributes:
        clock (int): time elapsed since the start of the machine in "ticks"
        transaction (Transaction): the transaction being executed or None if
                                   the core is idle

    """

    clock: int = 0
    transaction: Optional[Transaction] = None
