"""Set implementations for Puppetmaster."""

from __future__ import annotations

from typing import (
    AbstractSet,
    Callable,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    Optional,
)

from api import ObjSet, ObjSetMaker, ObjSetMakerFactory


class IdealObjSet(set, ObjSet):
    """Wrapper around the built-in set."""


class IdealObjSetMaker(ObjSetMaker):
    """Wrapper around the built-in set class."""

    def __call__(self, objects: Iterable[int] = ()) -> IdealObjSet:
        """Return new built-in set."""
        return IdealObjSet(objects)

    def free_objects(self, objects: Iterable[int]) -> None:
        """See ObjSetMaker.free_objects."""
        pass


class IdealObjSetMakerFactory(ObjSetMakerFactory):
    """Factory for (wrapped) built-in sets."""

    def __call__(self):
        """See ObjSetMakerFactory.__call__."""
        return IdealObjSetMaker()

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        return "Idealized set"


class ApproximateObjSet(ObjSet):
    """Bloom filter-like implementation of an integer set."""

    def __init__(self, objects: Iterable[int] = (), /, *, size: int):
        """Initialize set to contain objects."""
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

    def __or__(self, other: AbstractSet) -> ApproximateObjSet:
        """Return the union of this set and the other set."""
        if isinstance(other, ApproximateObjSet):
            out = ApproximateObjSet(size=self.size)
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> ApproximateObjSet:
        """Return the intersection of this set and the other set."""
        if isinstance(other, ApproximateObjSet):
            out = ApproximateObjSet(size=self.size)
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )


class ApproximateObjSetMaker(ObjSetMaker):
    """Makes approximate object set instances."""

    def __init__(self, factory: ApproximateObjSetMakerFactory):
        """Initialize approximate object set maker."""
        self.size = factory.size

    def __call__(self, objects: Iterable[int] = ()) -> ApproximateObjSet:
        """Return new approximate set."""
        return ApproximateObjSet(objects, size=self.size)

    def free_objects(self, objects: Iterable[int]) -> None:
        """See ObjSetMaker.free_objects."""
        pass


class ApproximateObjSetMakerFactory(ObjSetMakerFactory):
    """Factory for approximate set maker instances with preset arguments."""

    def __init__(self, size: int):
        """Initialize factory for approximate object set makers.

        Arguments:
            size: width of bit vector used to represent the set
        """
        self.size = size
        self.generator = ApproximateObjSetMaker(self)

    def __call__(self) -> ApproximateObjSetMaker:
        """Return new approximate object set maker."""
        return self.generator

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        return f"Approximate set ({self.size} bits)"


class FiniteObjSet(ObjSet):
    """Fixed-size set with a global renaming table."""

    def __init__(
        self,
        objects: Iterable[int] = (),
        /,
        *,
        size: int,
        renaming_table: MutableMapping[int, int],
    ):
        """Initialize set to contain objects."""
        self.bits = 0
        self.objs = [-1] * size
        self.size = size
        self.table = renaming_table
        inserted = set()
        for obj in objects:
            if obj in inserted:
                # Ignore duplicates.
                continue
            try:
                index = renaming_table[obj]
                inserted.add(obj)
            except KeyError:
                for iobj in inserted:
                    del renaming_table[iobj]
                raise ValueError("renaming table can't accept this object")
            self.bits |= 1 << index
            self.objs[index] = obj

    def __contains__(self, obj: object) -> bool:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[int]:
        """Yield each object in the set."""
        for obj in self.objs:
            if obj != -1:
                yield obj

    def __len__(self) -> int:
        """Not implemented."""
        raise NotImplementedError

    def __bool__(self) -> bool:
        """Return False if set is empty."""
        return self.bits != 0

    def __or__(self, other: AbstractSet) -> FiniteObjSet:
        """Return the union of this set and the other set."""
        if isinstance(other, FiniteObjSet):
            out = FiniteObjSet(size=self.size, renaming_table=self.table)
            out.bits = self.bits | other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )

    def __and__(self, other: AbstractSet) -> FiniteObjSet:
        """Return the intersection of this set and the other set."""
        if isinstance(other, FiniteObjSet):
            out = FiniteObjSet(size=self.size, renaming_table=self.table)
            out.bits = self.bits & other.bits
            return out
        else:
            raise TypeError(
                f"other set must have type {self.__class__.__name__}, not {type(other)}"
            )


class FiniteObjSetMaker(ObjSetMaker, MutableMapping[int, int]):
    """Makes fixed-size object sets that use a global renaming table."""

    def __init__(self, factory: FiniteObjSetMakerFactory):
        """Initialize finite set maker."""
        self.size = factory.size
        self.hash_fn = factory.hash_fn
        self.n_hash_funcs = factory.n_hash_funcs
        self.table = [(-1, 0)] * factory.size
        self.history: List[int] = []

    def __call__(self, objects: Iterable[int] = ()) -> FiniteObjSet:
        """Return new fixed-size set."""
        return FiniteObjSet(objects, size=self.size, renaming_table=self)

    def free_objects(self, objects: Iterable[int]) -> None:
        """Free resources associated with objects in the set."""
        for obj in objects:
            del self[obj]

    def __getitem__(self, obj: int) -> int:
        """Return name for object smaller than the table size."""
        assert obj != -1
        for i in range(self.n_hash_funcs):
            h = self.hash_fn(i, obj)
            prev_obj, count = self.table[h]
            if prev_obj == -1:
                self.table[h] = (obj, 1)
            elif prev_obj == obj:
                self.table[h] = (obj, count + 1)
            else:
                continue
            self.history.append(i + 1)
            return h
        raise KeyError("renaming table is full")

    def __delitem__(self, obj: int) -> None:
        """Remove an object from the renaming table."""
        assert obj != -1
        for i in range(self.n_hash_funcs):
            h = self.hash_fn(i, obj)
            prev_obj, count = self.table[h]
            assert prev_obj != -1 and count > 0 or prev_obj == -1 and count == 0
            if prev_obj == obj and count == 1:
                self.table[h] = (-1, 0)
            elif prev_obj == obj and count > 1:
                self.table[h] = (obj, count - 1)
            else:
                continue
            break
        else:
            raise KeyError("object not found")

    def __setitem__(self, obj: int, name: int) -> None:
        """Not implemented."""
        raise NotImplementedError

    def __iter__(self):
        """Not implemented."""
        raise NotImplementedError

    def __len__(self):
        """Not implemented."""
        raise NotImplementedError


class FiniteObjSetMakerFactory(ObjSetMakerFactory):
    """Factory for fixed-size object set makers with preset arguments."""

    def __init__(
        self,
        size: int,
        hash_fn: Optional[Callable[[int, int], int]] = None,
        n_hash_funcs: Optional[int] = None,
    ):
        """Initialize factory with set size and hash functions."""
        self.size = size
        self.hash_fn = (lambda i, x: (x + i) % size) if hash_fn is None else hash_fn
        self.n_hash_funcs = size if n_hash_funcs is None else n_hash_funcs

    def __call__(self, objects: Iterable[int] = ()) -> FiniteObjSetMaker:
        """Return new fixed-size set maker."""
        return FiniteObjSetMaker(self)

    def __str__(self) -> str:
        """Return human-readable name for the sets."""
        return f"Fixed-size set ({self.size} bits, {self.n_hash_funcs} hash functions)"
