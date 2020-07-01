"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod


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


class TransactionFactory(ABC):
    """Factory for generators of transactions."""

    @abstractmethod
    def __call__(self):
        """Return a new generator object that yields transactions.

        The number and properties of transactions depend on the specific class and the
        arguments given to its constructor.
        """

    @property
    @abstractmethod
    def total_time(self):
        """Return the time it takes to execute all transactions serially."""


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
