"""Implementations of the execution policy of Puppetmaster."""

import operator

from model import Core, TransactionExecutor


class RandomExecutor(TransactionExecutor):
    """Chooses a random queued transaction to be scheduled on each step."""

    def __init__(self, n_cores):
        """Create new executor.

        Arguments:
            n_cores (int): number of execution units (cores) available

        """
        self.cores = [Core() for _ in range(n_cores)]
        self.is_busy = False
        self.clock_fn = operator.attrgetter("clock")

    def push(self, scheduled):
        """See TransactionExecutor.push."""
        free_cores = [core for core in self.cores if core.transaction is None]
        # Execute scheduled transaction on first idle core.
        core = min(free_cores, key=self.clock_fn)
        # Execute one transaction.
        tr = scheduled.pop()
        core.transaction = tr
        core.clock += tr.time
        self.is_busy = True

    def pop(self):
        """See TransactionExecutor.pop."""
        free_cores = [core for core in self.cores if core.transaction is None]
        busy_cores = [core for core in self.cores if core.transaction is not None]
        core = min(busy_cores, key=self.clock_fn)
        finish = core.clock
        core.transaction = None
        for core in free_cores:
            core.clock = finish
        if len(busy_cores) == 1:
            self.is_busy = False
        return finish

    def has_free_cores(self):
        """See TransactionExecutor.has_free_cores."""
        return any(core.transaction is None for core in self.cores)

    @property
    def running(self):
        """See TransactionExecutor.running."""
        transactions = [core.transaction for core in self.cores]
        return [tr for tr in transactions if tr is not None]

    @property
    def clock(self):
        """See TimedComponent.clock."""
        return min(core.clock for core in self.cores)

    @clock.setter
    def clock(self, value):
        """See TimedComponent.set_clock."""
        for core in self.cores:
            if core.transaction is None and core.clock < value:
                core.clock = value