"""Main Puppetmaster simulator class."""

import operator
from typing import Iterator

from model import TransactionExecutor, TransactionScheduler
from pmtypes import MachineState, Transaction


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: Iterator[Transaction],
        scheduler: TransactionScheduler,
        executor: TransactionExecutor,
        core_count: int = 1,
        pool_size: int = None,
    ) -> None:
        """Create a new simulator.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute
            scheduler: component for scheduling transactions
            executor: component for executing transactions on the processing cores
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)

        """
        self.scheduler = scheduler
        self.executor = executor
        self.poolsize = pool_size
        self.start_state = MachineState(transactions, core_count=core_count)

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        state = self.start_state
        while state:
            free_cores = [core for core in state.cores if core.transaction is None]
            if not state.scheduled and state.scheduler_clock <= min(
                core.clock for core in state.cores
            ):
                # Fill up pending pool.
                self._fill_pool(state)
                # Try scheduling a batch of new transactions.
                self.scheduler.run(state)
            if free_cores and state.scheduled:
                # If some core were idle while the scheduler was working,
                # move their clocks forward.
                for core in free_cores:
                    core.clock = max(core.clock, state.scheduler_clock)
                # Execute a scheduled transaction.
                self.executor.run(state)
            else:
                # Remove first finished transaction.
                busy_cores = [
                    core for core in state.cores if core.transaction is not None
                ]
                finished_core = min(busy_cores, key=operator.attrgetter("clock"))
                finish = finished_core.clock
                finished_core.transaction = None
                if len(busy_cores) == 1:
                    state.is_busy = False
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                state.scheduler_clock = max(state.scheduler_clock, finish)
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                for core in free_cores:
                    core.clock = state.scheduler_clock
                finished_core.clock = state.scheduler_clock

        return min(core.clock for core in state.cores)

    def _fill_pool(self, state: MachineState) -> None:
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(state.pending) < self.poolsize:
            try:
                state.pending.add(next(state.incoming))
            except StopIteration:
                break
