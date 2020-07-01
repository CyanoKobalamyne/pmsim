"""Scheduler implementations."""

from model import TransactionScheduler
from pmtypes import TransactionSet


class ConstantTimeScheduler(TransactionScheduler):
    """Implementation of a simple scheduler."""

    def __init__(self, scheduling_time=0, n_transactions=1):
        """Initialize a new scheduler.

        Arguments:
            scheduling_time (int): constant amount of time the scheduler takes
                                   to choose the next transaction to execute

        """
        self.scheduling_time = scheduling_time
        self.n = n_transactions
        self._clock = 0

    def run(self, pending, ongoing):
        """See TransacionScheduler.run."""
        # Filter out candidates compatible with ongoing.
        ongoing = TransactionSet(ongoing)
        candidates = TransactionSet()
        for tr in pending:
            if len(candidates) == self.n:
                break
            if ongoing.compatible(tr) and candidates.compatible(tr):
                candidates.add(tr)
        self._clock += self.scheduling_time
        return candidates

    @property
    def clock(self):
        """See TimedComponent.clock."""
        return self._clock

    @clock.setter
    def clock(self, value):
        """See TimedComponent.set_clock."""
        self._clock = max(self._clock, value)
