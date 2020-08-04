"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from typing import AbstractSet, Iterable, Protocol, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from pmtypes import MachineState, Transaction, TransactionGenerator


T = TypeVar("T")


class AbstractSetType(Protocol[T]):
    """Protocol for callables that return sets."""

    def __call__(self, objects: Iterable[T] = ()) -> AbstractSet[T]:
        """Return a set containing objects."""
        raise NotImplementedError


class AddressSetMaker(AbstractSetType[int]):
    """Makes address set instances for a given simulation."""

    def free(self, transaction: "Transaction"):
        """Free resources associated with the transaction."""
        pass  # Does nothing by default.


class AddressSetMakerFactory(ABC):
    """Makes generators of address set with a fixed set of parameters."""

    @abstractmethod
    def __call__(self) -> AddressSetMaker:
        """Return new address set generator."""


class TransactionGeneratorFactory(ABC):
    """Factory for generators of transactions."""

    @abstractmethod
    def __call__(self) -> "TransactionGenerator":
        """Return a generator of transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    @abstractmethod
    def __init__(
        self, op_time: int = 0, pool_size: int = None, queue_size: int = None, **kwargs
    ):
        """Create a new scheduler.

        Arguments:
            op_time: number of cycles the scheduler takes to execute a single operation
            pool_size: number of tranactions seen by the scheduler simultaneously
                       (all of them if None)
            queue_size: maximum number of transactions that can be waiting for execution
                        (unlimited if None)
        """

    @abstractmethod
    def run(self, state: "MachineState") -> Iterable["MachineState"]:
        """Try scheduling a batch of transactions.

        Arguments:
            state: current state of the machine

        Returns:
            transactions ready to be executed concurrently with the currently running
            ones without conflicts
        """

    @property
    @abstractmethod
    def name(self):
        """Return a human-readable name for this scheduler."""


class TransactionExecutor(ABC):
    """Represents the execution policy for the processing units in Puppetmaster."""

    def __init__(self, **kwargs):
        """Create new executor."""

    @abstractmethod
    def run(self, state: "MachineState") -> Iterable["MachineState"]:
        """Choose transaction(s) to execute from scheduled set.

        The input state should not be used by the caller after this method returns,
        because it might be the same object as one of the returned states.
        """

    @property
    @abstractmethod
    def name(self):
        """Return a human-readable name for this executor."""
