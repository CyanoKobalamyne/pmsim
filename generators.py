"""Helper classes for factories that generate transactions."""

from __future__ import annotations

import copy
from typing import Iterable, Iterator, Mapping, Sequence

from pmtypes import Transaction


class TransactionGenerator(Iterator[Transaction]):
    """Yields new transactions based on configuration and available addresses."""

    def __init__(
        self, tr_data: Iterable[Mapping[str, int]], addresses: Sequence[int]
    ) -> None:
        """Create new TransactionGenerator.

        Arguments:
            tr_data: configuration for each transaction, which need the following keys:
                "read": size of the read set
                "write": size of the write set
                "time": transaction time
            addresses: addresses available for transactions (assigned sequentially)
        """
        self.tr_data = list(tr_data)
        self.addresses = addresses
        self.tr_index = 0
        self.address_index = 0

    def __next__(self) -> Transaction:
        """Return next transaction."""
        if not self:
            raise StopIteration
        tr_conf = self.tr_data[self.tr_index]
        self.tr_index += 1
        read_start = self.address_index
        self.address_index += tr_conf["reads"]
        read_end = write_start = self.address_index
        self.address_index += tr_conf["writes"]
        write_end = self.address_index
        read_set = self.addresses[read_start:read_end]
        write_set = self.addresses[write_start:write_end]
        return Transaction(read_set, write_set, tr_conf["time"])

    def __bool__(self) -> bool:
        """Return true if there are transactions left."""
        return self.tr_index != len(self.tr_data)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"{self.__class__.__name__}(tr_data={self.tr_data!r}, addresses="
            f"{self.addresses!r}, tr_index={self.tr_index!r}, address_index="
            f"{self.address_index!r})"
        )

    def __str__(self) -> str:
        """Return a user-friendly string representation of this object."""
        return (
            f"{self.__class__.__name__}(Transaction {self.tr_index}/"
            f"{len(self.tr_data)})"
        )

    def __deepcopy__(self, memo: dict) -> TransactionGenerator:
        """Make a deep copy of the object.

        Since these generators are immutable, we actually make a shallow copy.
        """
        return copy.copy(self)
