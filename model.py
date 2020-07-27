"""Abstractions used in the simulator."""

from abc import ABC, abstractmethod
from collections.abc import Sized
from typing import AbstractSet, Iterable, Type

from pmtypes import MachineState, Transaction, TransactionGenerator


class TransactionFactory(Iterable[Transaction], Sized, ABC):
    """Factory for generators of transactions."""

    def __iter__(self, set_type: Type[AbstractSet[int]] = set) -> TransactionGenerator:
        """Return a special iterator over transactions."""

    @property
    @abstractmethod
    def total_time(self) -> int:
        """Return the time it takes to execute all transactions serially."""


class TransactionScheduler(ABC):
    """Represents the scheduling unit within Puppetmaster."""

    @abstractmethod
    def run(self, state: MachineState) -> Iterable[MachineState]:
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
    def run(self, state: MachineState) -> Iterable[MachineState]:
        """Choose transaction(s) to execute from scheduled set.

        The input state should not be used by the caller after this method returns,
        because it might be the same object as one of the returned states.
        """

    @property
    @abstractmethod
    def name(self):
        """Return a human-readable name for this executor."""
