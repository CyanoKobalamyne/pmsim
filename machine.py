"""Puppet Master: simulation of a hardware-based distributed scheduler."""
import operator

from transaction import TransactionSet


class Core:
    """Component of `Machine` executing a single transaction.

    Attributes:
        clock (int): time elapsed since the start of the machine in "ticks"
        transaction (Transaction): the transaction being executed or None if
                                   the core is idle

    """

    def __init__(self, clock_start=0, transaction=None):
        """Create a new core.

        Arguments:
            clock_start (int): initial value for the clock
            transaction: initial transaction being executed

        """
        self.clock = clock_start
        self.transaction = transaction


class Machine:
    """Device capable of executing transactions in parallel."""

    def __init__(self, n_cores, pool_size, scheduler):
        """Create a new machine.

        Arguments:
            n_cores (int): number of execution units (cores) available
            pool_size (int): number of tranactions seen by the scheduler
                             simultaneously (all of them if None)
            scheduler (Scheduler): object used to schedule transactions on
                                   the cores

        """
        self.cores = [Core() for _ in range(n_cores)]
        self.pool_size = pool_size
        self.scheduler = scheduler

    def run(self, transactions):
        """Simulate execution of a set of transactions on this machine.

        Arguments:
            transactions (Iterable[Transaction]): transactions to execute

        Returns:
            int: amount of time (ticks) it took to execute all transactions

        """
        clock_fn = operator.attrgetter('clock')
        scheduler_clock = 0
        tr_iter = iter(transactions)
        pending = set()
        scheduled = set()
        is_tr_left = True
        while pending or is_tr_left:
            free_cores = [core for core in self.cores
                          if core.transaction is None]
            transactions = [core.transaction for core in self.cores]
            running = [tr for tr in transactions if tr is not None]
            if not scheduled:
                # Fill up pending pool.
                while self.pool_size is None or len(pending) < self.pool_size:
                    try:
                        pending.add(next(tr_iter))
                    except StopIteration:
                        is_tr_left = False
                        break
                # Try scheduling a batch of new transactions.
                scheduled, sched_time = self.scheduler.run(pending, running)
                pending -= scheduled
                scheduler_clock += sched_time
            if free_cores and scheduled:
                # Execute scheduled transaction on first idle core.
                core = min(free_cores, key=clock_fn)
                # If the core was idle while the scheduler was working,
                # move its clock forward.
                core.clock = max(scheduler_clock, core.clock)
                # Execute one transaction.
                tr = scheduled.pop()
                core.transaction = tr
                core.clock += tr.time
            else:
                # Remove first finished transaction.
                busy_cores = [core for core in self.cores
                              if core.transaction is not None]
                core = min(busy_cores, key=clock_fn)
                finish = core.clock
                core.transaction = None
                for core in free_cores:
                    core.clock = finish
                # If the scheduler was idle until the first core freed up,
                # move its clock forward.
                scheduler_clock = max(finish, scheduler_clock)

        return max(map(clock_fn, self.cores))


class ConstantTimeScheduler:
    """Implementation of a simple scheduler."""

    def __init__(self, scheduling_time=0, n_transactions=1):
        """Initialize a new scheduler.

        Arguments:
            scheduling_time (int): constant amount of time the scheduler takes
                                   to choose the next transaction to execute

        """
        self.scheduling_time = scheduling_time
        self.n = n_transactions

    def run(self, pending, ongoing):
        """Try scheduling a batch of transactions.

        Arguments:
            pending: set of transactions waiting to be executed
            ongoing: set of transactions currently being executed

        Returns:
            a transaction that can be executed concurrently with the
            currently running ones without conflicts, or None

        """
        # Filter out candidates compatible with ongoing.
        ongoing = TransactionSet(ongoing)
        candidates = TransactionSet()
        for tr in pending:
            if len(candidates) == self.n:
                break
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
        return candidates, self.scheduling_time
