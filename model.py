"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from typing import Iterable, MutableSet, Sized

from pmtypes import MachineState, Transaction


class TransactionFactory(Iterable[Transaction], Sized, ABC):
    """Factory for generators of transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> MutableSet[Transaction]:
        """Try scheduling a batch of transactions.

        Arguments:
            pending: transactions waiting to be executed
            ongoing: transactions currently being executed

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts

        """


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> None:
        """Choose transaction(s) to execute from scheduled set.

        Removes the transactions executed, if any.
        """
