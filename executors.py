"""Implementations of the execution policy of Puppetmaster."""

import copy
import operator
from typing import Iterable

from model import TransactionExecutor
from pmtypes import Core, MachineState


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
        # Find all idle cores.
        core_ixs = []
        min_clock = state.cores[0].clock
        for i, core in enumerate(state.cores):
            if core.transaction is not None:
                continue
            if core.clock == min_clock:
                core_ixs.append(i)
            elif core.clock < min_clock:
                min_clock = core.clock
                core_ixs = [i]
        # Generate output states for all core-transaction pairs.
        out_states = []
        for i in core_ixs:
            core = state.cores[i]
            for tr in state.scheduled:
                new_state = copy.copy(state)
                new_state.scheduled = {t for t in state.scheduled if t != tr}
                new_state.cores[i] = Core(core.clock + tr.time, tr)
                new_state.is_busy = True
                out_states.append(new_state)
        return out_states
