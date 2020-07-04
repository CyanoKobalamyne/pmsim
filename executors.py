"""Implementations of the execution policy of Puppetmaster."""

import operator
from typing import Iterable

from model import TransactionExecutor
from pmtypes import MachineState, Transaction


class RandomExecutor(TransactionExecutor):
    """Chooses a random queued transaction to be scheduled on each step."""

    def push(self, state: MachineState) -> None:
        """See TransactionExecutor.push."""
        free_cores = [core for core in state.cores if core.transaction is None]
        # Execute scheduled transaction on first idle core.
        core = min(free_cores, key=operator.attrgetter("clock"))
        # Execute one transaction.
        tr = state.scheduled.pop()
        core.transaction = tr
        core.clock += tr.time
        state.is_busy = True

    def pop(self, state: MachineState) -> int:
        """See TransactionExecutor.pop."""
        free_cores = [core for core in state.cores if core.transaction is None]
        busy_cores = [core for core in state.cores if core.transaction is not None]
        core = min(busy_cores, key=operator.attrgetter("clock"))
        finish = core.clock
        core.transaction = None
        for core in free_cores:
            core.clock = finish
        if len(busy_cores) == 1:
            state.is_busy = False
        return finish

    @staticmethod
    def has_free_cores(state: MachineState) -> bool:
        """See TransactionExecutor.has_free_cores."""
        return any(core.transaction is None for core in state.cores)

    @staticmethod
    def running(state: MachineState) -> Iterable[Transaction]:
        """See TransactionExecutor.running."""
        transactions = [core.transaction for core in state.cores]
        return [tr for tr in transactions if tr is not None]

    @staticmethod
    def get_clock(state: MachineState) -> int:
        """See TimedComponent.clock."""
        return min(core.clock for core in state.cores)

    @staticmethod
    def set_clock(state: MachineState, value: int) -> None:
        """See TimedComponent.set_clock."""
        for core in state.cores:
            if core.transaction is None and core.clock < value:
                core.clock = value
