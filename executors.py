"""Implementations of the execution policy of Puppetmaster."""

import heapq
import itertools
import typing
from typing import Iterable

from model import TransactionExecutor
from pmtypes import Core, MachineState, Transaction


class RandomExecutor(TransactionExecutor):
    """Chooses random scheduled transactions to be executed on each free core."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        # Execute one transaction.
        while len(state.cores) < state.core_count:
            try:
                tr = state.scheduled.pop()
            except KeyError:
                break
            core = Core(state.clock + tr.time, tr)
            heapq.heappush(state.cores, core)
        return [state]


class OptimalExecutor(TransactionExecutor):
    """Explores every possible execution path."""

    def run(self, state: MachineState) -> Iterable[MachineState]:
        """See TransactionExecutor.push."""
        # Generate output state for each scheduled transaction combination.
        n_free_cores = state.core_count - len(state.cores)
        tr_combos = (
            itertools.combinations(state.scheduled, n_free_cores)
            if n_free_cores < len(state.scheduled)
            else typing.cast(Iterable[Iterable[Transaction]], [state.scheduled])
        )
        out_states = []
        for tr_combo in tr_combos:
            new_state = state.copy()
            for tr in tr_combo:
                new_core = Core(state.clock + tr.time, tr)
                new_state.scheduled.remove(tr)
                heapq.heappush(new_state.cores, new_core)
            out_states.append(new_state)
        return out_states
