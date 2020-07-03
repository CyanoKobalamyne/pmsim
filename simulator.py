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
        self.poolsize = pool_size

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        self.state = MachineState()
        while (
            self.transactions
            or self.state.pending
            or self.state.scheduled
            or self.executor.is_busy
        ):
            running = self.executor.running
            if not self.state.scheduled and self.scheduler.clock <= self.executor.clock:
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                self.state.scheduled = self.scheduler.run(self.state.pending, running)
                self.state.pending -= self.state.scheduled
            if self.executor.has_free_cores() and self.state.scheduled:
                # If the executor was idle while the scheduler was working,
                # move its clock forward.
                self.executor.clock = self.scheduler.clock
                # Execute a scheduled transaction.
                self.executor.push(self.state.scheduled)
            else:
                # Remove first finished transaction.
                finish = self.executor.pop()
                # If the scheduler was idle until the first core freed up, move its
                # clock forward.
                self.scheduler.clock = finish
                # If the free cores are running behind the scheduler, move their clocks
                # forward.
                self.executor.clock = self.scheduler.clock

        return self.executor.clock

    def _fill_pool(self) -> None:
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(self.state.pending) < self.poolsize:
            try:
                self.state.pending.add(next(self.transactions))
            except StopIteration:
                break
