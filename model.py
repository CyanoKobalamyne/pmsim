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

    def __init__(self, op_time: int = 0, pool_size: int = None, queue_size: int = None):
        """Create a new scheduler.

        Arguments:
            op_time: number of cycles the scheduler takes to execute a single operation
            pool_size: number of tranactions seen by the scheduler simultaneously
                       (all of them if None)
            queue_size: maximum number of transactions that can be waiting for execution
                        (unlimited if None)
        """
        self.op_time = op_time
        self.pool_size = pool_size
        self.queue_size = queue_size

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """
        # Don't do anything if queue is full.
        if self.queue_size == len(state.scheduled):
            return [state]
        state = state.copy()
        # Fill up pending pool.
        while self.pool_size is None or len(state.pending) < self.pool_size:
            try:
                state.pending.add(next(state.incoming))
            except StopIteration:
                break
        # Try scheduling a batch of new transactions.
        ongoing = TransactionSet(core.transaction for core in state.cores)
        for tr in state.scheduled:
            ongoing.add(tr)
        out_states = []
        for scheduled, time in self.schedule(ongoing, state.pending):
            new_state = state.copy()
            new_state.clock += time
            if scheduled:
                count = (
                    self.queue_size - len(new_state.scheduled)
                    if self.queue_size is not None
                    else None
                )
                scheduled = set(itertools.islice(scheduled, count))
                new_state.scheduled |= scheduled
                new_state.pending -= scheduled
            out_states.append(new_state)
        return out_states

    @abstractmethod
    def schedule(
        self, ongoing: TransactionSet, pending: Iterable[Transaction]
    ) -> Iterable[Tuple[MutableSet[Transaction], int]]:
        """Schedule one or more transactions."""

    @property
    @abstractmethod
    def name(self):
        """Return a human-readable name for this scheduler."""


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Choose transaction(s) to execute from scheduled set.

        The input state should not be used by the caller after this method returns,
        because it might be the same object as one of the returned states.
        """
