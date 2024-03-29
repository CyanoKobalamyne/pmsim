"""Abstractions used in the simulator."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, AbstractSet, Iterable, Optional, Sequence

if TYPE_CHECKING:
    from pmtypes import MachineState, Transaction


class ObjSet(AbstractSet[int]):
    """Set data structure for memory objects (addresses)."""

    @abstractmethod
    def __or__(self, other: AbstractSet) -> ObjSet:
        """Return the union of this set and the other set."""

    @abstractmethod
    def __and__(self, other: AbstractSet) -> ObjSet:
        """Return the intersection of this set and the other set."""

    @abstractmethod
    def copy(self) -> ObjSet:
        """Return a copy of this set."""


class ObjSetMaker(ABC):
    """Makes object sets for a given simulation."""

    history: Optional[Sequence[int]] = None

    @abstractmethod
    def __call__(self, objects: Iterable[int] = ()) -> ObjSet:
        """Return a set containing objects."""

    @abstractmethod
    def free_objects(self, objects: Iterable[int]) -> None:
        """Free resources associated with objects in the set."""

    def free(self, transaction: Transaction) -> None:
        """Free resources associated with the transaction."""
        self.free_objects(transaction.read_set)
        self.free_objects(transaction.write_set)


class ObjSetMakerFactory(ABC):
    """Creates object set makers with fixed parameters."""

    @abstractmethod
    def __call__(self) -> ObjSetMaker:
        """Return new object set generator."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    clock_period: int
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
        self, clock_period: int = 0, pool_size: int = None, queue_size: int = None
    ) -> TransactionScheduler:
        """Create a new scheduler.

        Arguments:
            clock_period: clock period of the scheduler
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
