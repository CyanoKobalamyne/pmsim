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
        self.transactions = transactions
        self.scheduler = scheduler
        self.executor = executor
        self.core_count = core_count
        self.poolsize = pool_size

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        self.state = MachineState(core_count=self.core_count)
        while (
            self.transactions
            or self.state.pending
            or self.state.scheduled
            or self.state.is_busy
        ):
            transactions = [core.transaction for core in self.state.cores]
            running = [tr for tr in transactions if tr is not None]
            free_cores = [core for core in self.state.cores if core.transaction is None]
            if not self.state.scheduled and self.scheduler.clock <= min(
                core.clock for core in self.state.cores
            ):
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                self.state.scheduled = self.scheduler.run(self.state.pending, running)
                self.state.pending -= self.state.scheduled
            if free_cores and self.state.scheduled:
                # If some core were idle while the scheduler was working,
                # move their clocks forward.
                for core in free_cores:
                    core.clock = max(core.clock, self.scheduler.clock)
                # Execute a scheduled transaction.
                self.executor.run(self.state)
            else:
                # Remove first finished transaction.
                busy_cores = [
                    core for core in self.state.cores if core.transaction is not None
                ]
                finished_core = min(busy_cores, key=operator.attrgetter("clock"))
                finish = finished_core.clock
                finished_core.transaction = None
                if len(busy_cores) == 1:
                    self.state.is_busy = False
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                self.scheduler.clock = finish
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                for core in free_cores:
                    core.clock = self.scheduler.clock
                finished_core.clock = self.scheduler.clock

        return min(core.clock for core in self.state.cores)

    def _fill_pool(self) -> None:
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(self.state.pending) < self.poolsize:
            try:
                self.state.pending.add(next(self.transactions))
            except StopIteration:
                break
