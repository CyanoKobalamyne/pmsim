"""Puppet master: simulation of a hardware-based distributed scheduler."""
from collections.abc import MutableSet
import operator


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


class Transaction:
    """An atomic operation in the model."""

    def __init__(self, read_set, write_set, time):
        """Create a transaction.

        Attributes:
            read_set: the set of objects that this transaction needs to read
            write_set: the set of objects that this transaction needs to write
                       and possibly also read
            time: the amount of time units it takes to execute this transaction

        """
        self.read_set = read_set
        self.write_set = write_set
        self.time = time

    def __hash__(self):
        """Return a hash value for this transaction."""
        return id(self)

    def __str__(self):
        """Return a human-readable representation of this transaction."""
        return f"<Transaction: id {id(self)}, length {self.time}"


class TransactionSet(MutableSet):
    """A set of disjoint transactions."""

    def __init__(self, transactions=()):
        """Create a new set.

        Throws ValueError if any two of the transactions conflict.
        """
        self.transactions = set()
        self.read_set = set()
        self.write_set = set()
        for t in transactions:
            self.add(t)

    def __contains__(self, transaction):
        """Return if the given transaction is in the set."""
        return transaction in self.transactions

    def __iter__(self):
        """Yield each transaction in the set."""
        yield from self.transactions

    def __len__(self):
        """Return the number of transactions in the set."""
        return len(self.transactions)

    def add(self, transaction):
        """Add a new transaction to the set.

        Throws ValueError if the transactions conflicts with the ones already
        in the set.
        """
        if self.compatible(transaction):
            self.transactions.add(transaction)
            self.read_set |= transaction.read_set
            self.write_set |= transaction.write_set
        else:
            raise ValueError("incompatible transaction.")

    def discard(self, transaction):
        """Remove a transaction from the set.

        Warning: does not update the combined read and write sets.
        """
        self.transactions.discard(transaction)

    def compatible(self, transaction):
        """Return whether the given transaction is compatible with this set."""
        for read_obj in transaction.read_set:
            if read_obj in self.write_set:
                return False
        for write_obj in transaction.write_set:
            if write_obj in self.read_set:
                return False
            if write_obj in self.write_set:
                return False
        return True
