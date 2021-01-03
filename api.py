"""Abstractions used in the simulator."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pmtypes import MachineState, Transaction


class ObjSet(Set[int]):
    """Set data structure for memory objects (addresses)."""


class ObjSetMaker(ABC):
    """Makes object sets for a given simulation."""

    @abstractmethod
    def __call__(self, objects: Iterable[int] = ()) -> ObjSet:
        """Return a set containing objects."""

    def free(self, transaction: Transaction) -> None:
        """Free resources associated with the transaction."""
        pass  # Does nothing by default.


class ObjSetMakerFactory(ABC):
    """Creates object set makers with fixed parameters."""

    @abstractmethod
    def __call__(self) -> ObjSetMaker:
        """Return new object set generator."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    op_time: int
    pool_size: Optional[int]
    queue_size: Optional[int]

    @abstractmethod
    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """


class TransactionSchedulerFactory(ABC):
    """Parametrized factory for schedulers."""

    @abstractmethod
    def __call__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None
    ) -> TransactionScheduler:
        """Create a new scheduler.

        Arguments:
            op_time: number of cycles the scheduler takes to execute a single operation
            pool_size: number of tranactions seen by the scheduler simultaneously
                       (all of them if None)
            queue_size: maximum number of transactions that can be waiting for execution
                        (unlimited if None)
        """


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    def __init__(self, **kwargs):
        """Create new executor."""

    @abstractmethod
    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Choose transaction(s) to execute from scheduled set.

        The input state should not be used by the caller after this method returns,
        because it might be the same object as one of the returned states.
        """
