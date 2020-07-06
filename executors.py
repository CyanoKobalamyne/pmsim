"""Implementations of the execution policy of Puppetmaster."""

import heapq
from typing import Iterable

from model import TransactionExecutor
from pmtypes import Core, MachineState


class RandomExecutor(TransactionExecutor):
    """Chooses a random queued transaction to be scheduled on each step."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        # Execute scheduled transaction on first idle core.
        core = heapq.heappop(state.free_cores)
        # Execute one transaction.
        tr = state.scheduled.pop()
        core.transaction = tr
        core.clock += tr.time
        heapq.heappush(state.busy_cores, core)
        return [state]


class FullExecutor(TransactionExecutor):
    """Chooses every possible execution path."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        # Find an idle core.
        core = heapq.heappop(state.free_cores)
        # Generate output state for each scheduled transaction.
        out_states = []
        for tr in state.scheduled:
            new_core = Core(core.clock + tr.time, tr)
            new_state = state.copy()
            new_state.scheduled.remove(tr)
            heapq.heappush(new_state.busy_cores, new_core)
            out_states.append(new_state)
        return out_states
