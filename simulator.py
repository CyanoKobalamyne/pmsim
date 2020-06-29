"""Main Puppetmaster simulator class."""

import operator

from model import Machine


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
        self.machine = Machine(n_cores)
        self.scheduler = scheduler
        self.poolsize = pool_size

    def run(self, transactions):
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        clock_fn = operator.attrgetter("clock")
        self.transactions = iter(transactions)
        self.pending = set()
        scheduled = set()
        self.is_tr_left = True
        is_busy = False
        while self.is_tr_left or self.pending or scheduled or is_busy:
            free_cores = [
                core for core in self.machine.cores if core.transaction is None
            ]
            transactions = [core.transaction for core in self.machine.cores]
            running = [tr for tr in transactions if tr is not None]
            if not scheduled:
                # Fill up pending pool.
                self._fill_pool()
                # Try scheduling a batch of new transactions.
                scheduled = self.scheduler.run(self.pending, running)
                self.pending -= scheduled
            if free_cores and scheduled:
                # Execute scheduled transaction on first idle core.
                core = min(free_cores, key=clock_fn)
                # If the core was idle while the scheduler was working,
                # move its clock forward.
                core.clock = max(self.scheduler.clock, core.clock)
                # Execute one transaction.
                tr = scheduled.pop()
                core.transaction = tr
                core.clock += tr.time
                is_busy = True
            else:
                # Remove first finished transaction.
                busy_cores = [
                    core for core in self.machine.cores if core.transaction is not None
                ]
                core = min(busy_cores, key=clock_fn)
                finish = core.clock
                core.transaction = None
                for core in free_cores:
                    core.clock = finish
                # If the scheduler was idle until the first core freed up,
                # move its clock forward.
                self.scheduler.clock = max(finish, self.scheduler.clock)
                if len(busy_cores) == 1:
                    is_busy = False

        return max(map(clock_fn, self.machine.cores))

    def _fill_pool(self):
        """Fill up the scheduling pool."""
        while self.poolsize is None or len(self.pending) < self.poolsize:
            try:
                self.pending.add(next(self.transactions))
            except StopIteration:
                self.is_tr_left = False
                break
