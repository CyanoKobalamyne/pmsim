"""Implementations of the execution policy of Puppetmaster."""

import operator
from typing import Iterable

from model import TransactionExecutor
from pmtypes import MachineState


class RandomExecutor(TransactionExecutor):
    """Chooses a random queued transaction to be scheduled on each step."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        free_cores = [core for core in state.cores if core.transaction is None]
        # Execute scheduled transaction on first idle core.
        core = min(free_cores, key=operator.attrgetter("clock"))
        # Execute one transaction.
        tr = state.scheduled.pop()
        core.transaction = tr
        core.clock += tr.time
        state.is_busy = True
        return [state]


class FullExecutor(TransactionExecutor):
    """Chooses every possible execution path."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        # Find an idle core.
        i_min = min(range(len(state.cores)), key=lambda i: state.cores[i].clock)
        # Generate output state for each scheduled transaction.
        out_states = []
        for tr in state.scheduled:
            new_state = state.copy()
            new_state.scheduled.remove(tr)
            new_state.cores[i_min].transaction = tr
            new_state.cores[i_min].clock += tr.time
            new_state.is_busy = True
            out_states.append(new_state)
        return out_states
