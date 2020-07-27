"""Set implementations for Puppetmaster."""

from typing import AbstractSet, Iterable, Iterator


class ApproximateAddressSet(AbstractSet[int]):
    """Bloom filter-like implementation of an integer set."""

    def __init__(self, objects: Iterable[int] = (), /, *, size=1024):
        """Create a new set containing objects."""
        self.bits = 0
        self.size = size
        for obj in objects:
            self.bits |= 1 << (obj % size)

    def __contains__(self, obj: object) -> bool:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[int]:
        """Not implemented."""
        raise NotImplementedError

    def __len__(self) -> int:
        """Not implemented."""
        raise NotImplementedError

    def __bool__(self) -> bool:
        """Return False if set is empty."""
        return self.bits != 0

    def __or__(self, other: AbstractSet) -> "ApproximateAddressSet":
        """Return the union of this set and the other set."""
        if isinstance(other, ApproximateAddressSet):
            out = ApproximateAddressSet()
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> "ApproximateAddressSet":
        """Return the intersection of this set and the other set."""
        if isinstance(other, ApproximateAddressSet):
            out = ApproximateAddressSet()
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )
