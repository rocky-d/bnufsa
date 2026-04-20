import copy
import sortedcontainers as sc
from loguru import logger
from typing import Self
from binance_sdk_derivatives_trading_usds_futures.websocket_streams.models import (
    OrderTradeUpdateO,
)

__all__ = [
    "OrderTypeUnacceptedError",
    "OrderStatusUnacceptedError",
    "OrderBook",
    "LimitOrderBook",
]


class OrderTypeUnacceptedError(Exception):
    pass


class OrderStatusUnacceptedError(Exception):
    pass


class OrderBook:
    __slots__ = (
        "_otuos",
        "_bids",
        "_asks",
    )

    ACCEPTED_ORDER_TYPES = (
        "LIMIT",
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
    )
    ACCEPTED_ORDER_STATUSES = (
        "NEW",
        "PARTIALLY_FILLED",
    )

    @staticmethod
    def _is_bid(
        otuo: OrderTradeUpdateO,
    ) -> bool:
        return (
            "LIMIT" == otuo.o
            and "BUY" == otuo.S
            or otuo.o.startswith("STOP")
            and "SELL" == otuo.S
            or otuo.o.startswith("TAKE_PROFIT")
            and "BUY" == otuo.S
        )

    @classmethod
    def _key(
        cls,
        otuo: OrderTradeUpdateO,
    ) -> tuple[float, int]:
        p = float(otuo.p if "LIMIT" == otuo.o else otuo.sp)
        return (-p if cls._is_bid(otuo) else p), -otuo.T

    def __init__(
        self,
    ) -> None:
        self._otuos = {}
        self._bids = sc.SortedDict()
        self._asks = sc.SortedDict()

    def _side(
        self,
        otuo: OrderTradeUpdateO,
    ) -> sc.SortedDict:
        return self._bids if self._is_bid(otuo) else self._asks

    def add(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        if otuo.o not in self.ACCEPTED_ORDER_TYPES:
            raise OrderTypeUnacceptedError
        if otuo.X not in self.ACCEPTED_ORDER_STATUSES:
            raise OrderStatusUnacceptedError
        logger.debug(f"{otuo.c = }")
        self._side(otuo)[self._key(otuo)] = otuo
        self._otuos[otuo.c] = otuo

    def remove(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        logger.debug(f"{otuo.c = }")
        del self._side(otuo)[self._key(otuo)]
        del self._otuos[otuo.c]

    def set(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        if otuo.c in self._otuos:
            self.remove(self._otuos[otuo.c])
        if (
            otuo.o in self.ACCEPTED_ORDER_TYPES
            and otuo.X in self.ACCEPTED_ORDER_STATUSES
        ):
            self.add(otuo)

    def peek_bids(
        self,
        index: int,
    ) -> OrderTradeUpdateO:
        return self._bids.peekitem(index)[1]

    def peek_asks(
        self,
        index: int,
    ) -> OrderTradeUpdateO:
        return self._asks.peekitem(index)[1]

    def pop_bids(
        self,
        index: int,
    ) -> OrderTradeUpdateO:
        # return self._otuos.pop(self._bids.popitem(index)[1].c)
        otuo = self.peek_bids(index)
        self.remove(otuo)
        return otuo

    def pop_asks(
        self,
        index: int,
    ) -> OrderTradeUpdateO:
        # return self._otuos.pop(self._asks.popitem(index)[1].c)
        otuo = self.peek_asks(index)
        self.remove(otuo)
        return otuo

    def len_bids(
        self,
    ) -> int:
        return len(self._bids)

    def len_asks(
        self,
    ) -> int:
        return len(self._asks)

    def len(
        self,
    ) -> int:
        return self.len_bids() + self.len_asks()

    def contains_bids(
        self,
        otuo: OrderTradeUpdateO,
    ) -> bool:
        return self._key(otuo) in self._bids

    def contains_asks(
        self,
        otuo: OrderTradeUpdateO,
    ) -> bool:
        return self._key(otuo) in self._asks

    def contains(
        self,
        otuo: OrderTradeUpdateO,
    ) -> bool:
        return self.contains_bids(otuo) or self.contains_asks(otuo)

    def clear(
        self,
    ) -> None:
        self._otuos.clear()
        self._bids.clear()
        self._asks.clear()

    def copy(
        self,
        *,
        deep: bool = True,
    ) -> Self:
        return copy.deepcopy(self) if deep else copy.copy(self)


class LimitOrderBook(OrderBook):
    __slots__ = ()

    ACCEPTED_ORDER_TYPES = ("LIMIT",)

    @staticmethod
    def _is_bid(
        otuo: OrderTradeUpdateO,
    ) -> bool:
        return "BUY" == otuo.S

    @classmethod
    def _key(
        cls,
        otuo: OrderTradeUpdateO,
    ) -> tuple[float, int]:
        p = float(otuo.p)
        return (-p if cls._is_bid(otuo) else p), -otuo.T
