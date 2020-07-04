"""Implementations of the execution policy of Puppetmaster."""

import operator

from model import TransactionExecutor
from pmtypes import MachineState


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
