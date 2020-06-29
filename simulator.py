"""Main Puppetmaster simulator class."""

from executors import RandomExecutor


class Simulator:
    """Simulates executing a set of transactions."""

    def __init__(self, n_cores, pool_size, scheduler):
        """Create a new simulator.

        Arguments:
            n_cores (int): number of execution units (cores) available
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)
            scheduler (Scheduler): object used to schedule transactions on
                                   the cores

        """
        self.executor = RandomExecutor(n_cores)
        self.scheduler = scheduler
        self.poolsize = pool_size

    def run(self, transactions):
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        self.transactions = iter(transactions)
        self.pending = set()
        scheduled = set()
        self.is_tr_left = True
        while self.is_tr_left or self.pending or scheduled or self.executor.is_busy:
            running = self.executor.running
            if not scheduled:
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                scheduled = self.scheduler.run(self.pending, running)
                self.pending -= scheduled
            if self.executor.has_free_cores() and scheduled:
                # If the executor was idle while the scheduler was working,
                # move its clock forward.
                self.executor.clock = max(self.scheduler.clock, self.executor.clock)
                # Execute a scheduled transaction.
                self.executor.push(scheduled)
            else:
                # Remove first finished transaction.
                finish = self.executor.pop()
                # If the scheduler was idle until the first core freed up,
                # move its clock forward.
                self.scheduler.clock = max(finish, self.scheduler.clock)

        return max(map(self.executor.clock_fn, self.executor.cores))

    def _fill_pool(self):
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(self.pending) < self.poolsize:
            try:
                self.pending.add(next(self.transactions))
            except StopIteration:
                self.is_tr_left = False
                break