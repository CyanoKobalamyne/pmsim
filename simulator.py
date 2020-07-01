"""Main Puppetmaster simulator class."""

from typing import Iterable, MutableSet, Set

from model import TransactionExecutor, TransactionScheduler
from pmtypes import Transaction


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(
        self,
        transactions: Iterable[Transaction],
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
        self.transactions = iter(transactions)
        self.scheduler = scheduler
        self.executor = executor
        self.poolsize = pool_size

    def run(self) -> int:
        """Simulate execution of a set of transactions on this machine.

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        self.pending: Set[Transaction] = set()
        scheduled: MutableSet[Transaction] = set()
        self.is_tr_left = True
        while self.is_tr_left or self.pending or scheduled or self.executor.is_busy:
            running = self.executor.running
            if not scheduled and self.scheduler.clock <= self.executor.clock:
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                scheduled = self.scheduler.run(self.pending, running)
                self.pending -= scheduled
            if self.executor.has_free_cores() and scheduled:
                # If the executor was idle while the scheduler was working,
                # move its clock forward.
                self.executor.clock = self.scheduler.clock
                # Execute a scheduled transaction.
                self.executor.push(scheduled)
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
        while self.poolsize is None or len(self.pending) < self.poolsize:
            try:
                self.pending.add(next(self.transactions))
            except StopIteration:
                self.is_tr_left = False
                break
