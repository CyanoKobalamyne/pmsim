"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from typing import Iterable, MutableSet, Sized

from pmtypes import MachineState, Transaction


class TimedComponent(ABC):
    """Common superclass for all components with an internal clock."""

    @property
    @abstractmethod
    def clock(self) -> int:
        """Return the value of the global clock of the component."""

    @clock.setter
    def clock(self, value: int) -> None:
        """Set the value of the global clock of the component.

        The clock is monotonic, i.e. it will never be decreased.
        """


class TransactionFactory(Iterable[Transaction], Sized, ABC):
    """Factory for generators of transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(TimedComponent, ABC):
    """Represents the scheduling unit within Puppetmaster."""

    def __init__(self):
        """Perform common initialization."""
        self._clock = 0

    @abstractmethod
    def run(
        self, pending: Iterable[Transaction], ongoing: Iterable[Transaction]
    ) -> MutableSet[Transaction]:
        """Try scheduling a batch of transactions.

        Arguments:
            pending: transactions waiting to be executed
            ongoing: transactions currently being executed

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts

        """

    @property
    def clock(self) -> int:
        """See TimedComponent.clock."""
        return self._clock

    @clock.setter
    def clock(self, value: int) -> None:
        """See TimedComponent.set_clock."""
        self._clock = max(self._clock, value)


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    is_busy: bool

    @abstractmethod
    def push(self, state: MachineState) -> None:
        """Choose transaction(s) to execute from scheduled set.

        Removes the transactions executed, if any.
        """
