"""Helper classes for factories that generate transactions."""

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
