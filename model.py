"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from typing import Iterable, MutableSet, Sized, Tuple

from pmtypes import MachineState, Transaction, TransactionSet


class TransactionFactory(Iterable[Transaction], Sized, ABC):
    """Factory for generators of transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    def run(self, state: MachineState) -> None:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """
        ongoing = TransactionSet(
            core.transaction for core in state.cores if core.transaction is not None
        )
        scheduled, time = self.schedule(ongoing, state.pending)
        state.scheduler_clock += time
        state.scheduled = scheduled
        state.pending -= scheduled

    @abstractmethod
    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Tuple[MutableSet[Transaction], int]:
        """Schedule one or more transactions."""


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> None:
        """Choose transaction(s) to execute from scheduled set.

        Removes the transactions executed, if any.
        """
