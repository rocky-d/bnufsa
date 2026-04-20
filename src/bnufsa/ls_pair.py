import copy
from enum import StrEnum
from loguru import logger
from typing import (
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    NewType,
    Self,
    TypeAlias,
    TypeVar,
    ValuesView,
)
from binance_sdk_derivatives_trading_usds_futures.websocket_streams.models import (
    OrderTradeUpdateO,
)

__all__ = [
    "LSPairManagerFullError",
    "NonPoppingError",
    "ClientOrderId",
    "Role",
    "RoleLS",
    "ForwardList",
    "OTUOForwardList",
    "LSPairKeysView",
    "LSPairValuesView",
    "LSPairItemsView",
    "LSPair",
    "LSPairManager",
]


class LSPairManagerFullError(Exception):
    pass


class NonPoppingError(Exception):
    pass


ClientOrderId = NewType("ClientOrderId", str)


class Role(StrEnum):
    OPA = "OPA"
    OPB = "OPB"
    SL = "SL"
    TP = "TP"

    def __repr__(
        self,
    ) -> str:
        return f"{self.__class__.__name__}.{self.name}"


class RoleLS(StrEnum):
    OPA = "OPA"
    OPB = "OPB"
    SL_LONG = "SL_LONG"
    SL_SHRT = "SL_SHRT"
    TP_LONG = "TP_LONG"
    TP_SHRT = "TP_SHRT"

    def __repr__(
        self,
    ) -> str:
        return f"{self.__class__.__name__}.{self.name}"


T = TypeVar("T")


class ForwardList(Generic[T]):
    __slots__ = ("_list",)

    def __init__(
        self,
        iterable: Iterable[T] = (),
    ) -> None:
        self._list: list[T] = list(iterable)

    def __repr__(
        self,
    ) -> str:
        return f"{self.__class__.__name__}({repr(self._list)})"

    def __len__(
        self,
    ) -> int:
        return len(self._list)

    def __iter__(
        self,
    ) -> Iterator[T]:
        return iter(self._list)

    def __reversed__(
        self,
    ) -> Iterator[T]:
        return reversed(self._list)

    def __getitem__(
        self,
        key: int,
    ) -> T:
        return self._list[key]

    def __contains__(
        self,
        value: T,
    ) -> bool:
        return value in self._list

    def append(
        self,
        value: T,
    ) -> None:
        self._list.append(value)

    def extend(
        self,
        iterable: Iterable[T],
    ) -> None:
        self._list.extend(iterable)

    def copy(
        self,
        *,
        deep: bool = True,
    ) -> Self:
        return copy.deepcopy(self) if deep else copy.copy(self)


OTUOForwardList: TypeAlias = ForwardList[OrderTradeUpdateO]


class LSPairKeysView(KeysView[RoleLS]):
    pass


class LSPairValuesView(ValuesView[OTUOForwardList]):
    pass


class LSPairItemsView(ItemsView[RoleLS, OTUOForwardList]):
    pass


class LSPair(Mapping[RoleLS, OTUOForwardList]):
    __slots__ = (
        "_roles",
        "_otuos",
        "_opa",
        "_opb",
        "_sl_long",
        "_sl_shrt",
        "_tp_long",
        "_tp_shrt",
    )

    def __init__(
        self,
        mapping: Mapping[RoleLS, OTUOForwardList]
        | ItemsView[RoleLS, OTUOForwardList]
        | Iterable[tuple[RoleLS, OTUOForwardList]] = (),
    ) -> None:
        self._roles: dict[ClientOrderId, Role] = {}
        self._otuos: dict[ClientOrderId, OTUOForwardList] = {}
        self._opa: OTUOForwardList = ForwardList()
        self._opb: OTUOForwardList = ForwardList()
        self._sl_long: OTUOForwardList = ForwardList()
        self._sl_shrt: OTUOForwardList = ForwardList()
        self._tp_long: OTUOForwardList = ForwardList()
        self._tp_shrt: OTUOForwardList = ForwardList()
        if isinstance(mapping, Mapping):
            mapping = mapping.items()
        for role, otuo_list in mapping:
            if 0 == len(otuo_list):
                continue
            self._roles[otuo_list[-1].c] = role
            self._otuos[otuo_list[-1].c] = self[role]
            self[role].extend(otuo_list)

    def __repr__(
        self,
    ) -> str:
        return f"{self.__class__.__name__}({{{', '.join(f'{repr(k)}: {repr(v)}' for k, v in self.items())}}})"

    def __len__(
        self,
    ) -> int:
        return len(RoleLS)

    def __iter__(
        self,
    ) -> Iterator[RoleLS]:
        return iter(RoleLS)

    def __contains__(
        self,
        key: str | RoleLS,
    ) -> bool:
        return key in RoleLS

    def __getitem__(
        self,
        key: str | RoleLS,
    ) -> OTUOForwardList:
        if key not in self:
            raise KeyError(key)
        match RoleLS(key):
            case RoleLS.OPA:
                result = self._opa
            case RoleLS.OPB:
                result = self._opb
            case RoleLS.SL_LONG:
                result = self._sl_long
            case RoleLS.SL_SHRT:
                result = self._sl_shrt
            case RoleLS.TP_LONG:
                result = self._tp_long
            case RoleLS.TP_SHRT:
                result = self._tp_shrt
        return result

    def get[T](
        self,
        key: str | RoleLS,
        default: T = None,
    ) -> OTUOForwardList | T:
        if key not in self:
            return default
        return self[key]

    def keys(
        self,
    ) -> LSPairKeysView:
        return LSPairKeysView(self)

    def values(
        self,
    ) -> LSPairValuesView:
        return LSPairValuesView(self)

    def items(
        self,
    ) -> LSPairItemsView:
        return LSPairItemsView(self)

    def otuos_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> OTUOForwardList | T:
        return self._otuos.get(key, default)

    def otuos_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._otuos.keys()

    def otuos_values(
        self,
    ) -> ValuesView[OTUOForwardList]:
        return self._otuos.values()

    def otuos_items(
        self,
    ) -> ItemsView[ClientOrderId, OTUOForwardList]:
        return self._otuos.items()

    def roles_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> Role | T:
        return self._roles.get(key, default)

    def roles_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._roles.keys()

    def roles_values(
        self,
    ) -> ValuesView[Role]:
        return self._roles.values()

    def roles_items(
        self,
    ) -> ItemsView[ClientOrderId, Role]:
        return self._roles.items()

    def is_opa_long(
        self,
    ) -> bool:
        return (
            0 < len(self._opa)
            and "LONG" == self._opa[-1].ps
            or 0 < len(self._opb)
            and "SHORT" == self._opb[-1].ps
        )

    def is_opb_shrt(
        self,
    ) -> bool:
        return self.is_opa_long()

    def is_opa_shrt(
        self,
    ) -> bool:
        return (
            0 < len(self._opa)
            and "SHORT" == self._opa[-1].ps
            or 0 < len(self._opb)
            and "LONG" == self._opb[-1].ps
        )

    def is_opb_long(
        self,
    ) -> bool:
        return self.is_opa_shrt()

    @property
    def all_opa(
        self,
    ) -> OTUOForwardList:
        return self._opa

    @property
    def all_opb(
        self,
    ) -> OTUOForwardList:
        return self._opb

    @property
    def all_op_long(
        self,
    ) -> OTUOForwardList:
        return self.opa_list if self.is_opa_long() else self.opb_list

    @property
    def all_op_shrt(
        self,
    ) -> OTUOForwardList:
        return self.opa_list if self.is_opa_shrt() else self.opb_list

    @property
    def all_sl_long(
        self,
    ) -> OTUOForwardList:
        return self._sl_long

    @property
    def all_sl_shrt(
        self,
    ) -> OTUOForwardList:
        return self._sl_shrt

    @property
    def all_tp_long(
        self,
    ) -> OTUOForwardList:
        return self._tp_long

    @property
    def all_tp_shrt(
        self,
    ) -> OTUOForwardList:
        return self._tp_shrt

    @property
    def opa(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._opa) else self._opa[-1]

    @opa.setter
    def opa(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.OPA
        self._otuos[otuo.c] = self._opa
        self._opa.append(otuo)

    @property
    def opb(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._opb) else self._opb[-1]

    @opb.setter
    def opb(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.OPB
        self._otuos[otuo.c] = self._opb
        self._opb.append(otuo)

    @property
    def op_long(
        self,
    ) -> OrderTradeUpdateO | None:
        return self.opa if self.is_opa_long() else self.opb

    @property
    def op_shrt(
        self,
    ) -> OrderTradeUpdateO | None:
        return self.opa if self.is_opa_shrt() else self.opb

    @property
    def sl_long(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._sl_long) else self._sl_long[-1]

    @sl_long.setter
    def sl_long(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.SL
        self._otuos[otuo.c] = self._sl_long
        self._sl_long.append(otuo)

    @property
    def sl_shrt(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._sl_shrt) else self._sl_shrt[-1]

    @sl_shrt.setter
    def sl_shrt(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.SL
        self._otuos[otuo.c] = self._sl_shrt
        self._sl_shrt.append(otuo)

    @property
    def tp_long(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._tp_long) else self._tp_long[-1]

    @tp_long.setter
    def tp_long(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.TP
        self._otuos[otuo.c] = self._tp_long
        self._tp_long.append(otuo)

    @property
    def tp_shrt(
        self,
    ) -> OrderTradeUpdateO | None:
        return None if 0 == len(self._tp_shrt) else self._tp_shrt[-1]

    @tp_shrt.setter
    def tp_shrt(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        self._roles[otuo.c] = Role.TP
        self._otuos[otuo.c] = self._tp_shrt
        self._tp_shrt.append(otuo)

    @property
    def ap(
        self,
    ) -> float:
        return (float(self.opa.ap) + float(self.opb.ap)) / 2

    @property
    def pnl(
        self,
    ) -> float:
        return (
            -float(self.opa.ap) * float(self.opa.z)
            + float(self.opb.ap) * float(self.opb.z)
            + float(self.sl_long.ap) * float(self.sl_long.z)
            - float(self.sl_shrt.ap) * float(self.sl_shrt.z)
            + float(self.tp_long.ap) * float(self.tp_long.z)
            - float(self.tp_shrt.ap) * float(self.tp_shrt.z)
        ) - (
            +float(self.opa.n)
            + float(self.opb.n)
            + float(self.sl_long.n)
            + float(self.sl_shrt.n)
            + float(self.tp_long.n)
            + float(self.tp_shrt.n)
        )


class LSPairManager:
    __slots__ = (
        "_capacity",
        "_roles",
        "_heads",
        "_groups",
        "_pairs",
        "_poppings",
    )

    def __init__(
        self,
        *,
        capacity: int | None = None,
    ) -> None:
        self._capacity = capacity
        self._roles: dict[ClientOrderId, Role] = {}
        self._heads: dict[ClientOrderId, ClientOrderId] = {}
        self._groups: dict[ClientOrderId, set[ClientOrderId]] = {}
        self._pairs: dict[ClientOrderId, LSPair] = {}
        self._poppings: set[ClientOrderId] = set()

    def __len__(
        self,
    ) -> int:
        return len(self._groups)

    @property
    def size(
        self,
    ) -> int:
        return len(self._roles)

    @property
    def capacity(
        self,
    ) -> int | None:
        return self._capacity

    @property
    def empty(
        self,
    ) -> bool:
        return 0 == len(self._groups)

    @property
    def full(
        self,
    ) -> bool:
        if self._capacity is None:
            return False
        return not len(self._groups) < self._capacity

    def roles_len(
        self,
    ) -> int:
        return len(self._roles)

    def roles_contains(
        self,
        key: ClientOrderId,
    ) -> bool:
        return key in self._roles

    def roles_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> Role | T:
        return self._roles.get(key, default)

    def roles_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._roles.keys()

    def roles_values(
        self,
    ) -> ValuesView[Role]:
        return self._roles.values()

    def roles_items(
        self,
    ) -> ItemsView[ClientOrderId, Role]:
        return self._roles.items()

    def heads_len(
        self,
    ) -> int:
        return len(self._heads)

    def heads_contains(
        self,
        key: ClientOrderId,
    ) -> bool:
        return key in self._heads

    def heads_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> ClientOrderId | T:
        return self._heads.get(key, default)

    def heads_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._heads.keys()

    def heads_values(
        self,
    ) -> ValuesView[ClientOrderId]:
        return self._heads.values()

    def heads_items(
        self,
    ) -> ItemsView[ClientOrderId, ClientOrderId]:
        return self._heads.items()

    def groups_len(
        self,
    ) -> int:
        return len(self._groups)

    def groups_contains(
        self,
        key: ClientOrderId,
    ) -> bool:
        head = self._heads.get(key)
        return head in self._groups

    def groups_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> set[ClientOrderId] | T:
        head = self._heads.get(key)
        return self._groups.get(head, default)

    def groups_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._groups.keys()

    def groups_values(
        self,
    ) -> ValuesView[set[ClientOrderId]]:
        return self._groups.values()

    def groups_items(
        self,
    ) -> ItemsView[ClientOrderId, set[ClientOrderId]]:
        return self._groups.items()

    def pairs_len(
        self,
    ) -> int:
        return len(self._pairs)

    def pairs_contains(
        self,
        key: ClientOrderId,
    ) -> bool:
        head = self._heads.get(key)
        return head in self._pairs

    def pairs_get[T](
        self,
        key: ClientOrderId,
        default: T = None,
    ) -> LSPair | T:
        head = self._heads.get(key)
        return self._pairs.get(head, default)

    def pairs_keys(
        self,
    ) -> KeysView[ClientOrderId]:
        return self._pairs.keys()

    def pairs_values(
        self,
    ) -> ValuesView[LSPair]:
        return self._pairs.values()

    def pairs_items(
        self,
    ) -> ItemsView[ClientOrderId, LSPair]:
        return self._pairs.items()

    def poppings_len(
        self,
    ) -> int:
        return len(self._poppings)

    def poppings_contains(
        self,
        key: ClientOrderId,
    ) -> bool:
        head = self._heads.get(key)
        return head in self._poppings

    def poppings_frozenset(
        self,
    ) -> frozenset[ClientOrderId]:
        return frozenset(self._poppings)

    def pin_opa(
        self,
        cid: ClientOrderId,
    ) -> None:
        if self.full:
            raise LSPairManagerFullError
        self._roles[cid] = Role.OPA
        self._heads[cid] = cid
        self._groups[cid] = {cid}
        logger.debug(f"{cid = }")

    def pin_opb(
        self,
        cid: ClientOrderId,
        head: ClientOrderId,
    ) -> None:
        self._roles[cid] = Role.OPB
        self._heads[cid] = head
        self._groups[head].add(cid)
        logger.debug(f"{cid = }\n{head = }")

    def pin_sl(
        self,
        cid: ClientOrderId,
        head: ClientOrderId,
    ) -> None:
        self._roles[cid] = Role.SL
        self._heads[cid] = head
        self._groups[head].add(cid)
        logger.debug(f"{cid = }\n{head = }")

    def pin_tp(
        self,
        cid: ClientOrderId,
        head: ClientOrderId,
    ) -> None:
        self._roles[cid] = Role.TP
        self._heads[cid] = head
        self._groups[head].add(cid)
        logger.debug(f"{cid = }\n{head = }")

    def set(
        self,
        otuo: OrderTradeUpdateO,
    ) -> None:
        cid = otuo.c
        head = self._heads[cid]
        if head not in self._pairs:
            self._pairs[head] = LSPair()
        pair = self._pairs[head]
        role = self._roles[cid]
        if Role.OPA == role:
            pair.opa = otuo
        elif Role.OPB == role:
            pair.opb = otuo
        elif Role.SL == role:
            if "LONG" == otuo.ps:
                pair.sl_long = otuo
            else:  # elif "SHORT" == otuo.ps:
                pair.sl_shrt = otuo
        elif Role.TP == role:
            if "LONG" == otuo.ps:
                pair.tp_long = otuo
            else:  # elif "SHORT" == otuo.ps:
                pair.tp_shrt = otuo
        logger.debug(f"{role = }\n{cid = }\n{head = }")

    def popping(
        self,
        cid: ClientOrderId,
    ) -> LSPair:
        head = self._heads[cid]
        self._poppings.add(head)
        group = self._groups[head]
        logger.debug(f"{head = }\n{group = }")
        return self._pairs[head]

    def pop(
        self,
        cid: ClientOrderId,
    ) -> LSPair:
        head = self._heads[cid]
        if head not in self._poppings:
            raise NonPoppingError
        self._poppings.remove(head)
        group = self._groups.pop(head)
        for cid in group:
            self._roles.pop(cid)
            self._heads.pop(cid)
        logger.debug(f"{head = }\n{group = }")
        return self._pairs.pop(head)

    def clear(
        self,
    ) -> None:
        self._roles.clear()
        self._heads.clear()
        self._groups.clear()
        self._pairs.clear()

    def copy(
        self,
        *,
        deep: bool = True,
    ) -> Self:
        return copy.deepcopy(self) if deep else copy.copy(self)
