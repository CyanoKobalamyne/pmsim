"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from collections.abc import Sized
import itertools
from typing import Iterable, MutableSet, Tuple

from pmtypes import MachineState, Transaction, TransactionSet


class TransactionFactory(Iterable[Transaction], Sized, ABC):
    """Factory for generators of transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    def __init__(self, queue_size: int = None):
        """Create a new scheduler.

        Arguments:
            queue_size: maximum number of transactions that can be waiting for execution
                        (unlimited if None)
        """
        self.queue_size = queue_size

    def run(self, state: MachineState) -> None:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """
        if self.queue_size == len(state.scheduled):
            return
        ongoing = TransactionSet(core.transaction for core in state.cores)
        for tr in state.scheduled:
            ongoing.add(tr)
        scheduled, time = self.schedule(ongoing, state.pending)
        state.clock += time
        if scheduled:
            count = (
                self.queue_size - len(state.scheduled)
                if self.queue_size is not None
                else None
            )
            scheduled = set(itertools.islice(scheduled, count))
            state.scheduled |= scheduled
            state.pending -= scheduled

    @abstractmethod
    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Tuple[MutableSet[Transaction], int]:
        """Schedule one or more transactions."""


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Choose transaction(s) to execute from scheduled set.

        The input state should not be used by the caller after this method returns,
        because it might be the same object as one of the returned states.
        """
