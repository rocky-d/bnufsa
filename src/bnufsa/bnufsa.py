import aiocsv
import aiofiles
import asyncio as aio
import atask
import collections
import functools
from aiocsv.protocols import CsvDialectArg, CsvDialectKwargs
from concurrent.futures import Executor
from contextvars import Context
from loguru import logger
from os import PathLike
from pydantic import BaseModel
from typing import Any, Callable, Coroutine, Literal, Self, TypeAlias
from binance_common.utils import get_uuid
from binance_common.models import WebsocketApiResponse
from binance_common.configuration import (
    ConfigurationRestAPI as ConfigRestAPI,
    ConfigurationWebSocketAPI as ConfigWebSocketAPI,
    ConfigurationWebSocketStreams as ConfigWebSocketStreams,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api import (
    DerivativesTradingUsdsFuturesRestAPI as RestAPIClient,
)
from binance_sdk_derivatives_trading_usds_futures.websocket_api import (
    DerivativesTradingUsdsFuturesWebSocketAPI as WebSocketAPIClient,
)
from binance_sdk_derivatives_trading_usds_futures.websocket_streams import (
    DerivativesTradingUsdsFuturesWebSocketStreams as WebSocketStreamsClient,
)
from binance_sdk_derivatives_trading_usds_futures.websocket_api.models import (
    CancelOrderResponse,
    NewOrderPositionSideEnum,
    NewOrderResponse,
    NewOrderSelfTradePreventionModeEnum,
    NewOrderSideEnum,
    NewOrderTimeInForceEnum,
)
from binance_sdk_derivatives_trading_usds_futures.websocket_streams.models import (
    IndividualSymbolBookTickerStreamsResponse,
    UserDataStreamEventsResponse,
    AccountConfigUpdate,
    AccountUpdate,
    ConditionalOrderTriggerReject,
    GridUpdate,
    Listenkeyexpired,
    MarginCall,
    OrderTradeUpdate,
    StrategyUpdate,
    TradeLite,
)

from .ls_pair import LSPair, LSPairManager, Role
from .order_book import LimitOrderBook
from .time_window import SparseTimeWindow

__all__ = [
    "PositionUnclosedError",
    "BNUFSARecorder",
    "BNUFSATrader",
    "BNUFSAController",
    "BNUFSAMonitor",
    "BNUFSA",
]


class PositionUnclosedError(Exception):
    pass


StrOrBytesPath: TypeAlias = str | bytes | PathLike[str] | PathLike[bytes]
FileDescriptorOrPath: TypeAlias = int | StrOrBytesPath
Opener: TypeAlias = Callable[[FileDescriptorOrPath, int], int]


class BNUFSARecorder(atask.AsyncTask[None]):
    def __init__(
        self,
        *,
        file: FileDescriptorOrPath,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: Opener | None = None,
        loop: aio.AbstractEventLoop | None = None,
        executor: Executor | None = None,
        restval: Any | None = "",
        extrasaction: Literal["raise", "ignore"] = "raise",
        dialect: CsvDialectArg = "excel",
        csv_dialect_kwargs: CsvDialectKwargs | None = None,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(name=name, context=context)
        self._file = file
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._closefd = closefd
        self._opener = opener
        self._loop = loop
        self._executor = executor
        self._restval = restval
        self._extrasaction = extrasaction
        self._dialect = dialect
        self._csv_dialect_kwargs = (
            CsvDialectKwargs() if csv_dialect_kwargs is None else csv_dialect_kwargs
        )
        self._que = collections.deque()

    def record(
        self,
        *pairs: LSPair,
    ) -> None:
        self._que.extend(pairs)

    async def _recorder(
        self,
    ) -> None:
        async with aiofiles.open(
            file=self._file,
            mode=self._mode,
            buffering=self._buffering,
            encoding=self._encoding,
            errors=self._errors,
            newline=self._newline,
            closefd=self._closefd,
            opener=self._opener,
            loop=self._loop,
            executor=self._executor,
        ) as asyncfile:
            writer = aiocsv.AsyncDictWriter(
                asyncfile=asyncfile,
                fieldnames=list(LSPair()),
                restval=self._restval,
                extrasaction=self._extrasaction,
                dialect=self._dialect,
                **self._csv_dialect_kwargs,
            )
            await writer.writeheader()
            while True:
                await aio.sleep(0.0)
                if 0 == len(self._que):
                    continue
                await writer.writerow(self._que.popleft())

    async def _atask(
        self,
    ) -> None:
        async with aio.TaskGroup() as tg:
            tg.create_task(
                self._recorder(),
                name=f"{self.name}.{self._recorder.__name__}",
                context=self.context,
            )


class BNUFSATrader(atask.AsyncTask[None]):
    def __init__(
        self,
        *,
        rest_api: RestAPIClient,
        ws_api: WebSocketAPIClient,
        ws_streams: WebSocketStreamsClient,
        symbol: str,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(name=name, context=context)
        self._rest_api = rest_api
        self._ws_api = ws_api
        self._ws_streams = ws_streams
        self._symbol = symbol
        self._que = aio.Queue()
        for ei_symbol in self._rest_api.exchange_information().data().symbols:
            if self._symbol != ei_symbol.symbol:
                continue
            for ft in ei_symbol.filters:
                if "PRICE_FILTER" == ft.filter_type:
                    price_filter = ft
                elif "LOT_SIZE" == ft.filter_type:
                    lot_size = ft
                elif "MARKET_LOT_SIZE" == ft.filter_type:
                    market_lot_size = ft
            break
        self._tick_size = float(price_filter.tick_size)
        if str(self._tick_size).endswith(".0"):
            self._tick_size = int(self._tick_size)
        self._step_size = float(lot_size.step_size)
        if str(self._step_size).endswith(".0"):
            self._step_size = int(self._step_size)
        self._step_size_market = float(market_lot_size.step_size)
        if str(self._step_size_market).endswith(".0"):
            self._step_size_market = int(self._step_size_market)

    def _round(
        self,
        num: float,
        step: int | float,
    ) -> int | float:
        div, mod = divmod(num, step)
        if step < mod + mod:
            div += 1
        s = str(step)
        num = div * step
        if isinstance(num, int):
            return num
        if "e-" not in s:
            ndigits = len(s) - 2
        elif "." not in s:
            ndigits = int(s.split("e-")[1])
        else:
            ndigits = int(s.split("e-")[1]) + len(s.split(".")[1].split("e-")[0])
        return round(num, ndigits=ndigits)

    @staticmethod
    def _order_producer(
        func: Callable[
            ..., tuple[str, Coroutine[Any, Any, WebsocketApiResponse[BaseModel]]]
        ],
    ) -> tuple[str, aio.Task[WebsocketApiResponse[BaseModel]]]:
        @functools.wraps(func)
        def wrapper(
            self: Self,
            *args,
            **kwargs,
        ) -> tuple[str, aio.Task[WebsocketApiResponse[BaseModel]]]:
            new_client_order_id, coro = func(self, *args, **kwargs)
            task = aio.create_task(
                coro,
                name=f"{self.name}.{func.__name__}:{new_client_order_id}",
                context=self.context,
            )
            self._que.put_nowait(task)
            log_msg = f"{self}.{func.__name__}({', '.join(map(repr, args))}{'' if 0 == len(args) or 0 == len(kwargs) else ', '}{', '.join(f'{k}={repr(v)}' for k, v in kwargs.items())})\n{new_client_order_id = }"
            logger.info(log_msg)
            return new_client_order_id, task

        return wrapper

    @_order_producer
    def limit(
        self,
        *,
        side: NewOrderSideEnum,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        price: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size),
                price=self._round(price, self._tick_size),
                type="LIMIT",
                time_in_force=NewOrderTimeInForceEnum.GTC,
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def market(
        self,
        *,
        side: NewOrderSideEnum,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size_market),
                type="MARKET",
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def order_opa(
        self,
        *,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        price: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        side = {
            NewOrderPositionSideEnum.LONG: NewOrderSideEnum.BUY,
            NewOrderPositionSideEnum.SHORT: NewOrderSideEnum.SELL,
        }[position_side]
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size),
                price=self._round(price, self._tick_size),
                type="LIMIT",
                time_in_force=NewOrderTimeInForceEnum.GTC,
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def order_opb(
        self,
        *,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        side = {
            NewOrderPositionSideEnum.LONG: NewOrderSideEnum.BUY,
            NewOrderPositionSideEnum.SHORT: NewOrderSideEnum.SELL,
        }[position_side]
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size_market),
                type="MARKET",
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def order_sl(
        self,
        *,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        stop_price: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        side = {
            NewOrderPositionSideEnum.LONG: NewOrderSideEnum.SELL,
            NewOrderPositionSideEnum.SHORT: NewOrderSideEnum.BUY,
        }[position_side]
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size_market),
                stop_price=self._round(stop_price, self._tick_size),
                type="STOP_MARKET",
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def order_tp(
        self,
        *,
        position_side: NewOrderPositionSideEnum,
        quantity: float,
        price: float,
        new_client_order_id: str | None = None,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[NewOrderResponse]]]:
        if new_client_order_id is None:
            new_client_order_id = get_uuid()
        side = {
            NewOrderPositionSideEnum.LONG: NewOrderSideEnum.SELL,
            NewOrderPositionSideEnum.SHORT: NewOrderSideEnum.BUY,
        }[position_side]
        return (
            new_client_order_id,
            self._ws_api.new_order(
                new_client_order_id=new_client_order_id,
                symbol=self._symbol,
                position_side=position_side,
                side=side,
                quantity=self._round(quantity, self._step_size),
                price=self._round(price, self._tick_size),
                type="LIMIT",
                time_in_force=NewOrderTimeInForceEnum.GTC,
                self_trade_prevention_mode=NewOrderSelfTradePreventionModeEnum.EXPIRE_TAKER,
            ),
        )

    @_order_producer
    def cancel_order(
        self,
        *,
        orig_client_order_id: str,
    ) -> tuple[str, Coroutine[Any, Any, WebsocketApiResponse[CancelOrderResponse]]]:
        return (
            orig_client_order_id,
            self._ws_api.cancel_order(
                orig_client_order_id=orig_client_order_id,
                symbol=self._symbol,
            ),
        )

    async def _order_consumer(
        self,
    ) -> None:
        task: aio.Task[WebsocketApiResponse[BaseModel]]
        while True:
            await aio.sleep(0.0)
            if self._que.empty():
                continue
            task = self._que.get_nowait()
            await task
            self._que.task_done()
            logger.trace(f"{task.get_name() = }")

    async def astop(
        self,
    ) -> None:
        if not self.started:
            return
        await super().stop()
        await self._que.join()

    async def _atask(
        self,
    ) -> None:
        async with aio.TaskGroup() as tg:
            tg.create_task(
                self._order_consumer(),
                name=f"{self.name}.{self._order_consumer.__name__}",
                context=self.context,
            )


class BNUFSAController(atask.AsyncTask[None]):
    def __init__(
        self,
        recorder: BNUFSARecorder,
        trader: BNUFSATrader,
        *,
        quantity: float,
        risk: float,
        volatility: float,
        sl_volatility: float,
        close_volatility: float,
        open_volatility: float,
        open_slippage: float,
        open_sleep_interval: int,
        tw_interval: int,
        tw_capacity: int,
        capacity: int | None = None,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(name=name, context=context)
        self._recorder = recorder
        self._trader = trader
        self._quantity = quantity
        self._risk = risk
        self._volatility = volatility
        self._sl_volatility = sl_volatility
        self._close_volatility = close_volatility
        self._open_volatility = open_volatility
        self._open_slippage = open_slippage
        self._open_sleep_interval = open_sleep_interval
        self._bt = IndividualSymbolBookTickerStreamsResponse()
        self._tw = SparseTimeWindow(tw_interval, unit=tw_interval / tw_capacity)
        self._pair_mgr = LSPairManager(capacity=capacity)
        self._limit_ob = LimitOrderBook()
        self._canceling = set()
        self._restoring = {}

    def update_bt(
        self,
        bt: IndividualSymbolBookTickerStreamsResponse,
    ) -> None:
        self._bt = bt
        self._tw.push(bt, bt.T)

    def update_otu(
        self,
        otu: OrderTradeUpdate,
    ) -> None:
        otuo = otu.o
        cid = otuo.c
        if not self._pair_mgr.roles_contains(cid):
            return
        role = self._pair_mgr.roles_get(cid)
        head = self._pair_mgr.heads_get(cid)
        pair = self._pair_mgr.pairs_get(head)
        self._pair_mgr.set(otuo)
        self._limit_ob.set(otuo)
        log_msg = f"{otuo.S}:{otuo.ps}:{otuo.o}:{otuo.x}:{otuo.X}\n{role = }\n{cid = }\n{head = }"
        if "EXPIRED" == otuo.x and "EXPIRED_IN_MATCH" == otuo.X:
            logger.warning(log_msg)
            if "MARKET" == otuo.o:
                cid, task = self._trader.market(
                    side=NewOrderSideEnum(otuo.S),
                    position_side=NewOrderPositionSideEnum(otuo.ps),
                    quantity=self._quantity - float(otuo.z),
                    new_client_order_id=cid,
                )
            elif "LIMIT" == otuo.o:
                cid, task = self._trader.limit(
                    side=NewOrderSideEnum(otuo.S),
                    position_side=NewOrderPositionSideEnum(otuo.ps),
                    quantity=self._quantity - float(otuo.z),
                    price=float(otuo.p),
                    new_client_order_id=cid,
                )
            else:
                pass
            return
        else:
            logger.info(log_msg)
        if Role.OPA == role:
            if "TRADE" == otuo.x and "FILLED" == otuo.X:
                position_side = {
                    "LONG": "SHORT",
                    "SHORT": "LONG",
                }[otuo.ps]
                cid, task = self._trader.order_opb(
                    position_side=NewOrderPositionSideEnum(position_side),
                    quantity=self._quantity,
                )
                self._pair_mgr.pin_opb(cid, head)
            elif "CANCELED" == otuo.x == otuo.X:
                pair = self._pair_mgr.pop(cid)
                self._canceling.remove(cid)
            else:
                pass
        elif Role.OPB == role:
            if "TRADE" == otuo.x and "FILLED" == otuo.X:
                ap = pair.ap
                price_diff_tp = ap * self._volatility
                price_diff_sl = price_diff_tp * (1 - self._risk)
                position_side = "LONG"
                cid, task = self._trader.order_sl(
                    position_side=NewOrderPositionSideEnum(position_side),
                    quantity=self._quantity,
                    stop_price=ap - price_diff_sl,
                )
                self._pair_mgr.pin_sl(cid, head)
                position_side = "SHORT"
                cid, task = self._trader.order_sl(
                    position_side=NewOrderPositionSideEnum(position_side),
                    quantity=self._quantity,
                    stop_price=ap + price_diff_sl,
                )
                self._pair_mgr.pin_sl(cid, head)
                position_side = "LONG"
                cid, task = self._trader.order_tp(
                    position_side=NewOrderPositionSideEnum(position_side),
                    quantity=self._quantity,
                    price=ap + price_diff_tp,
                )
                self._pair_mgr.pin_tp(cid, head)
                position_side = "SHORT"
                cid, task = self._trader.order_tp(
                    position_side=NewOrderPositionSideEnum(position_side),
                    quantity=self._quantity,
                    price=ap - price_diff_tp,
                )
                self._pair_mgr.pin_tp(cid, head)
            else:
                pass
        elif Role.SL == role:
            if "NEW" == otuo.x == otuo.X and "MARKET" == otuo.o:
                if "LONG" == otuo.ps:
                    canceled = pair.tp_long
                    expired = self._limit_ob.peek_asks(-1)
                else:  # elif "SHORT" == otuo.ps:
                    canceled = pair.tp_shrt
                    expired = self._limit_ob.peek_bids(-1)
                canceled_cid = canceled.c
                expired_cid = expired.c
                if canceled_cid != expired_cid:
                    self._restoring[canceled_cid] = expired
                    self._restoring[expired_cid] = canceled
                    cid, task = self._trader.cancel_order(
                        orig_client_order_id=canceled_cid,
                    )
                if "FILLED" == pair.sl_long.X or "FILLED" == pair.sl_shrt.X:
                    self._pair_mgr.popping(cid)
                elif "NEW" == pair.sl_long.X and "NEW" == pair.sl_shrt.X:
                    sl = pair.sl_shrt if "LONG" == otuo.ps else pair.sl_long
                    cid, task = self._trader.cancel_order(
                        orig_client_order_id=sl.c,
                    )
                else:
                    pass
            elif "CANCELED" == otuo.x == otuo.X:
                statuses = "NEW", "EXPIRED", "CANCELED"
                if pair.tp_long.X in statuses and pair.tp_shrt.X in statuses:
                    position_side = otuo.ps
                    sl_volatility = {
                        "LONG": self._sl_volatility,
                        "SHORT": -self._sl_volatility,
                    }[position_side]
                    cid, task = self._trader.order_sl(
                        position_side=NewOrderPositionSideEnum(position_side),
                        quantity=self._quantity,
                        stop_price=pair.ap * (1 + sl_volatility),
                        new_client_order_id=cid,
                    )
                elif "FILLED" == pair.tp_long.X or "FILLED" == pair.tp_shrt.X:
                    pair = self._pair_mgr.pop(cid)
                    self._recorder.record(pair)
                    logger.success(f"{pair.pnl = }")
                else:
                    pass
            else:
                pass
        elif Role.TP == role:
            if "TRADE" == otuo.x and "FILLED" == otuo.X:
                sl = pair.sl_long if "LONG" == otuo.ps else pair.sl_shrt

                async def cancel_sl() -> None:  # TODO: Ugly handling
                    await aio.sleep(3.0)
                    cid, task = self._trader.cancel_order(
                        orig_client_order_id=sl.c,
                    )
                    self._pair_mgr.popping(cid)

                aio.create_task(cancel_sl(), context=self.context)
            elif "EXPIRED" == otuo.x == otuo.X:
                if self._pair_mgr.poppings_contains(cid):
                    pair = self._pair_mgr.pop(cid)
                    self._recorder.record(pair)
                    logger.warning(f"{pair.pnl = }")
                if cid in self._restoring:
                    canceled = self._restoring[cid]
                    expired = self._restoring[canceled.c] = otuo
                    if "CANCELED" == canceled.X:
                        cid, task = self._trader.order_tp(
                            position_side=NewOrderPositionSideEnum(expired.ps),
                            quantity=self._quantity,
                            price=float(expired.p),
                            new_client_order_id=expired.c,
                        )
                        del self._restoring[expired.c]
                        del self._restoring[canceled.c]
            elif "CANCELED" == otuo.x == otuo.X:
                if self._pair_mgr.poppings_contains(cid):
                    pair = self._pair_mgr.pop(cid)
                    self._recorder.record(pair)
                    logger.warning(f"{pair.pnl = }")
                if cid in self._restoring:
                    expired = self._restoring[cid]
                    canceled = self._restoring[expired.c] = otuo
                    if "EXPIRED" == expired.X:
                        cid, task = self._trader.order_tp(
                            position_side=NewOrderPositionSideEnum(expired.ps),
                            quantity=self._quantity,
                            price=float(expired.p),
                            new_client_order_id=expired.c,
                        )
                        del self._restoring[expired.c]
                        del self._restoring[canceled.c]
            else:
                pass

    async def _close_opa_orders(
        self,
    ) -> None:
        while True:
            await aio.sleep(0.0)
            for head, group in self._pair_mgr.groups_items():
                if head in self._canceling:
                    continue
                if not 1 == len(group):
                    continue
                pair = self._pair_mgr.pairs_get(head)
                if pair is None:
                    continue
                opa = pair.opa
                if opa is None:
                    continue
                bt_price = {
                    "LONG": self._bt.b,
                    "SHORT": self._bt.a,
                }[opa.ps]
                bt_price = float(bt_price)
                opa_price = float(opa.p)
                if not self._close_volatility <= abs(bt_price - opa_price) / opa_price:
                    continue
                cid, task = self._trader.cancel_order(
                    orig_client_order_id=head,
                )
                self._pair_mgr.popping(cid)
                self._canceling.add(cid)

    async def _open_opa_orders(
        self,
    ) -> None:
        while self._tw.empty():
            await aio.sleep(0.0)
        while True:
            await aio.sleep(0.0)
            if self._pair_mgr.full:
                continue
            head_bt, head_t = self._tw.head()
            tail_bt, tail_t = self._tw.tail()
            # fillrate = (tail_t - head_t) / self._tw.interval
            # if not ... <= fillrate:
            #     continue
            head_price = (float(head_bt.b) + float(head_bt.a)) / 2
            tail_price = (float(tail_bt.b) + float(tail_bt.a)) / 2
            volatility = (tail_price - head_price) / head_price
            if not self._open_volatility <= abs(volatility):
                continue
            position_side = "SHORT" if 0 < volatility else "LONG"
            slippage = {
                "LONG": -self._open_slippage,
                "SHORT": self._open_slippage,
            }[position_side]
            bt_price = {
                "LONG": self._bt.b,
                "SHORT": self._bt.a,
            }[position_side]
            bt_price = float(bt_price)
            cid, task = self._trader.order_opa(
                position_side=NewOrderPositionSideEnum(position_side),
                quantity=self._quantity,
                price=bt_price * (1 + slippage),
            )
            self._pair_mgr.pin_opa(cid)
            while self._pair_mgr.roles_contains(cid):
                await aio.sleep(0.0)
                pair = self._pair_mgr.pairs_get(cid)
                if pair is None:
                    continue
                opa = pair.opa
                if opa is None:
                    continue
                if "FILLED" != opa.X:
                    continue
                break
            else:
                continue
            await aio.sleep(self._open_sleep_interval / 1000)

    async def _atask(
        self,
    ) -> None:
        async with aio.TaskGroup() as tg:
            tg.create_task(
                self._open_opa_orders(),
                name=f"{self.name}.{self._open_opa_orders.__name__}",
                context=self.context,
            )
            tg.create_task(
                self._close_opa_orders(),
                name=f"{self.name}.{self._close_opa_orders.__name__}",
                context=self.context,
            )


class BNUFSAMonitor(atask.AsyncTask[None]):
    def __init__(
        self,
        controller: BNUFSAController,
        *,
        rest_api: RestAPIClient,
        ws_api: WebSocketAPIClient,
        ws_streams: WebSocketStreamsClient,
        symbol: str,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(name=name, context=context)
        self._controller = controller
        self._rest_api = rest_api
        self._ws_api = ws_api
        self._ws_streams = ws_streams
        self._symbol = symbol

    def _user_data_stream_on_message(
        self,
        data: Any,
    ) -> None:
        data = UserDataStreamEventsResponse.model_validate(data)
        actual_instance = data.actual_instance
        logger.debug(repr(actual_instance))
        if isinstance(actual_instance, AccountConfigUpdate):
            actual_instance: AccountConfigUpdate
        elif isinstance(actual_instance, AccountUpdate):
            actual_instance: AccountUpdate
        elif isinstance(actual_instance, ConditionalOrderTriggerReject):
            actual_instance: ConditionalOrderTriggerReject
        elif isinstance(actual_instance, GridUpdate):
            actual_instance: GridUpdate
        elif isinstance(actual_instance, Listenkeyexpired):
            actual_instance: Listenkeyexpired
        elif isinstance(actual_instance, MarginCall):
            actual_instance: MarginCall
        elif isinstance(actual_instance, OrderTradeUpdate):
            actual_instance: OrderTradeUpdate
            self._controller.update_otu(actual_instance)
        elif isinstance(actual_instance, StrategyUpdate):
            actual_instance: StrategyUpdate
        elif isinstance(actual_instance, TradeLite):
            actual_instance: TradeLite
        else:  # elif actual_instance is None:
            actual_instance: None

    def _book_ticker_stream_on_message(
        self,
        data: IndividualSymbolBookTickerStreamsResponse,
    ) -> None:
        logger.trace(repr(data))
        self._controller.update_bt(data)

    async def astart(
        self,
    ) -> None:
        if self.started:
            return
        resp = await aio.to_thread(self._rest_api.start_user_data_stream)
        self._listen_key = resp.data().listen_key
        self._user_data_stream = await self._ws_streams.user_data(self._listen_key)
        self._user_data_stream.on("message", self._user_data_stream_on_message)
        self._book_ticker_stream = (
            await self._ws_streams.individual_symbol_book_ticker_streams(self._symbol)
        )
        self._book_ticker_stream.on("message", self._book_ticker_stream_on_message)
        await super().start()

    async def astop(
        self,
    ) -> None:
        if not self.started:
            return
        await super().stop()
        await self._book_ticker_stream.unsubscribe()
        await self._user_data_stream.unsubscribe()
        # await aio.to_thread(self._rest_api.close_user_data_stream)

    async def _keep_user_data_stream(
        self,
    ) -> None:
        delay = 10 * 60 * 1.0
        sleep_task = aio.create_task(aio.sleep(delay), context=self.context)
        while True:
            await sleep_task
            sleep_task = aio.create_task(aio.sleep(delay), context=self.context)
            resp = await aio.to_thread(self._rest_api.start_user_data_stream)
            new_listen_key = resp.data().listen_key
            if self._listen_key == new_listen_key:
                continue
            await self._user_data_stream.unsubscribe()
            self._listen_key = new_listen_key
            self._user_data_stream = await self._ws_streams.user_data(self._listen_key)
            self._user_data_stream.on("message", self._user_data_stream_on_message)

    async def _atask(
        self,
    ) -> None:
        async with aio.TaskGroup() as tg:
            tg.create_task(
                self._keep_user_data_stream(),
                name=f"{self.name}.{self._keep_user_data_stream.__name__}",
                context=self.context,
            )


class BNUFSA(atask.AsyncTask[None]):
    def __init__(
        self,
        *,
        symbol: str,
        config_rest_api: ConfigRestAPI | None = None,
        config_ws_api: ConfigWebSocketAPI | None = None,
        config_ws_streams: ConfigWebSocketStreams | None = None,
        kwargs_recorder: dict[str, Any] | None = None,
        kwargs_trader: dict[str, Any] | None = None,
        kwargs_controller: dict[str, Any] | None = None,
        kwargs_monitor: dict[str, Any] | None = None,
        name: str | None = None,
        context: Context | None = None,
    ) -> None:
        super().__init__(name=name, context=context)
        self._symbol = symbol
        self._rest_api = RestAPIClient(
            ConfigRestAPI() if config_rest_api is None else config_rest_api
        )
        self._ws_api = WebSocketAPIClient(
            ConfigWebSocketAPI() if config_ws_api is None else config_ws_api
        )
        self._ws_streams = WebSocketStreamsClient(
            ConfigWebSocketStreams() if config_ws_streams is None else config_ws_streams
        )
        self._recorder = BNUFSARecorder(
            **({} if kwargs_recorder is None else kwargs_recorder),
        )
        self._trader = BNUFSATrader(
            rest_api=self._rest_api,
            ws_api=self._ws_api,
            ws_streams=self._ws_streams,
            symbol=self._symbol,
            **({} if kwargs_trader is None else kwargs_trader),
        )
        self._controller = BNUFSAController(
            self._recorder,
            self._trader,
            **({} if kwargs_controller is None else kwargs_controller),
        )
        self._monitor = BNUFSAMonitor(
            self._controller,
            rest_api=self._rest_api,
            ws_api=self._ws_api,
            ws_streams=self._ws_streams,
            symbol=self._symbol,
            **({} if kwargs_monitor is None else kwargs_monitor),
        )

    async def astart(
        self,
    ) -> None:
        if self.started:
            return
        logger.critical(f"Starting {self}")
        await self._ws_api.create_connection()
        await self._ws_streams.create_connection()
        await self._recorder.start()
        await self._trader.start()
        await self._controller.start()
        await self._monitor.start()
        await super().start()
        logger.critical(f"Started {self}")

    async def astop(
        self,
    ) -> None:
        if not self.started:
            return
        logger.critical(f"Stopping {self}")
        await super().stop()
        await self._monitor.stop()
        await self._controller.stop()
        await self._trader.stop()
        await self._recorder.stop()
        await self._ws_streams.close_connection()
        await self._ws_api.close_connection()
        logger.critical(f"Stopped {self}")

    async def _atask(
        self,
    ) -> None:
        resp = await self._ws_api.position_information_v2(symbol=self._symbol)
        data = resp.data()
        if 0 < len(data.result):
            raise PositionUnclosedError
        async with aio.TaskGroup() as tg:
            tg.create_task(
                self._recorder.join(),
                name=f"{self._recorder.name}.{self._recorder.join.__name__}",
                context=self.context,
            )
            tg.create_task(
                self._trader.join(),
                name=f"{self._trader.name}.{self._trader.join.__name__}",
                context=self.context,
            )
            tg.create_task(
                self._controller.join(),
                name=f"{self._controller.name}.{self._controller.join.__name__}",
                context=self.context,
            )
            tg.create_task(
                self._monitor.join(),
                name=f"{self._monitor.name}.{self._monitor.join.__name__}",
                context=self.context,
            )
