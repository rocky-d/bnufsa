import collections
import copy
from typing import Generic, Self, TypeVar

__all__ = [
    "TimeWindowEmptyError",
    "TimeWindow",
    "SparseTimeWindow",
]


class TimeWindowEmptyError(Exception):
    pass


T = TypeVar("T")


class TimeWindow(Generic[T]):
    __slots__ = (
        "_interval",
        "_items",
        "_timestamps",
    )

    def __init__(
        self,
        interval: int | float,
    ) -> None:
        self._interval = interval
        self._items = collections.deque()
        self._timestamps = collections.deque()

    def __len__(
        self,
    ) -> int:
        return len(self._items)

    @property
    def interval(
        self,
    ) -> int:
        return self._interval

    def _add(
        self,
        item: T,
        timestamp: int | float,
    ) -> None:
        self._items.append(item)
        self._timestamps.append(timestamp)

    def _del(
        self,
        timestamp: int | float,
    ) -> None:
        items = self._items
        timestamps = self._timestamps
        timestamp -= self._interval
        while 0 < len(timestamps) and timestamps[0] < timestamp:
            items.popleft()
            timestamps.popleft()

    def push(
        self,
        item: T,
        timestamp: int | float,
    ) -> None:
        self._del(timestamp)
        self._add(item, timestamp)

    def empty(
        self,
    ) -> bool:
        return 0 == len(self._items)

    def head(
        self,
    ) -> tuple[T, int | float]:
        if self.empty():
            raise TimeWindowEmptyError
        return self._items[0], self._timestamps[0]

    def tail(
        self,
    ) -> tuple[T, int | float]:
        if self.empty():
            raise TimeWindowEmptyError
        return self._items[-1], self._timestamps[-1]

    def clear(
        self,
    ) -> None:
        self._items.clear()
        self._timestamps.clear()

    def copy(
        self,
        *,
        deep: bool = True,
    ) -> Self:
        return copy.deepcopy(self) if deep else copy.copy(self)


class SparseTimeWindow(TimeWindow[T]):
    __slots__ = ("_unit",)

    def __init__(
        self,
        interval,
        *,
        unit: int | float = 0,
    ) -> None:
        super().__init__(interval)
        self._unit = unit

    @property
    def unit(
        self,
    ) -> int:
        return self._unit

    def push(
        self,
        item: T,
        timestamp: int | float,
    ) -> None:
        if not self.empty() and timestamp - self.tail()[1] < self._unit:
            return
        super().push(item, timestamp)
