"""Main Puppetmaster simulator class."""

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
            running = self.executor.running(self.state)
            if (
                not self.state.scheduled
                and self.scheduler.clock <= self.executor.get_clock(self.state)
            ):
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                self.state.scheduled = self.scheduler.run(self.state.pending, running)
                self.state.pending -= self.state.scheduled
            if self.executor.has_free_cores(self.state) and self.state.scheduled:
                # If the executor was idle while the scheduler was working,
                # move its clock forward.
                self.executor.set_clock(self.state, self.scheduler.clock)
                # Execute a scheduled transaction.
                self.executor.push(self.state)
            else:
                # Remove first finished transaction.
                finish = self.executor.pop(self.state)
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                self.scheduler.clock = finish
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                self.executor.set_clock(self.state, self.scheduler.clock)

        return self.executor.get_clock(self.state)

    def _fill_pool(self) -> None:
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(self.state.pending) < self.poolsize:
            try:
                self.state.pending.add(next(self.transactions))
            except StopIteration:
                break
