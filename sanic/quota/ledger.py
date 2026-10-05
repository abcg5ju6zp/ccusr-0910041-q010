"""配额账本：租户/全局上限、预留凭证与余额流水。

账本只负责在一把异步锁内完成"检查—扣减—记账"的原子状态转移，所有
并发预留因此不可能同时越过租户或全局上限。凭证的终结（结算 / 取消 /
超时回收 / 冻结回收）以状态机保证：首个终结操作生效，重复调用一律
幂等返回，绝不重复计费或重复归还。
"""

from __future__ import annotations

import time

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from sanic.quota.exceptions import QuotaExceeded, TenantFrozen


class ReservationState(str, Enum):
    """预留凭证生命周期状态。"""

    HELD = "held"
    SETTLED = "settled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    FROZEN = "frozen"


class EntryKind(str, Enum):
    """流水条目类型，delta 均表示对"有效占用量"的有符号变化。"""

    RESERVE = "reserve"
    SETTLE = "settle"
    CANCEL = "cancel"
    EXPIRE = "expire"
    FREEZE_RECLAIM = "freeze_reclaim"
    FREEZE = "freeze"
    UNFREEZE = "unfreeze"
    LIMIT = "limit"


# 终结状态 -> 归还预留时使用的流水类型
_TERMINAL_REFUND = {
    ReservationState.CANCELLED: EntryKind.CANCEL,
    ReservationState.EXPIRED: EntryKind.EXPIRE,
    ReservationState.FROZEN: EntryKind.FREEZE_RECLAIM,
}


@dataclass(frozen=True)
class LedgerEntry:
    """一条租户余额流水，只包含该租户自身的数据。"""

    seq: int
    at: str
    tenant: str
    kind: EntryKind
    estimate: int = 0
    actual: int | None = None
    delta: int = 0
    balance_after: int = 0
    global_balance_after: int = 0
    reservation_id: str | None = None
    request_id: str | None = None
    reason: str | None = None

    def explain(self) -> str:
        """生成面向租户的、自解释的余额变化说明。"""
        sign = "+" if self.delta >= 0 else ""
        what = {
            EntryKind.RESERVE: "请求预留估算额度",
            EntryKind.SETTLE: "按实际用量结算并释放预留差额",
            EntryKind.CANCEL: "请求异常，归还全部预留",
            EntryKind.EXPIRE: "预留超时未完成，自动回收",
            EntryKind.FREEZE_RECLAIM: "租户被冻结，未完成预留被回收",
            EntryKind.FREEZE: "租户已被管理员冻结",
            EntryKind.UNFREEZE: "租户已被管理员解冻",
            EntryKind.LIMIT: "租户预算上限调整",
        }[self.kind]
        tail = f"，当前有效占用 {self.balance_after}"
        if self.kind == EntryKind.SETTLE and self.actual is not None:
            tail = f"，实际用量 {self.actual}{tail}"
        return f"{self.at} {what}：{sign}{self.delta}{tail}"


@dataclass
class Reservation:
    """预留凭证，在中间件、处理器与响应生命周期之间传递。"""

    id: str
    tenant: str
    estimate: int
    created_at: float
    expires_at: float | None
    request_id: str | None = None
    state: ReservationState = ReservationState.HELD
    actual: int | None = None
    closed_at: str | None = None
    close_reason: str | None = None
    close_kind: EntryKind | None = None

    @property
    def held(self) -> bool:
        """凭证是否仍持有额度。"""
        return self.state is ReservationState.HELD

    def to_dict(self) -> dict:
        """导出凭证状态（不含其他租户信息）。"""
        return {
            "id": self.id,
            "tenant": self.tenant,
            "estimate": self.estimate,
            "actual": self.actual,
            "state": self.state.value,
            "request_id": self.request_id,
            "closed_at": self.closed_at,
            "close_reason": self.close_reason,
        }


@dataclass
class _TenantAccount:
    limit: int | None = None
    frozen: bool = False
    used: int = 0  # 已结算的实际用量
    held: int = 0  # 未完成预留的估算总量
    events: list[LedgerEntry] = field(default_factory=list)


class Ledger:
    """进程内配额账本。

    Args:
        global_limit: 全局预算上限，None 表示不限。
        default_tenant_limit: 未显式配置租户的默认上限，None 表示不限。
        clock: 单调时钟（便于测试注入），默认 time.monotonic。
    """

    def __init__(
        self,
        *,
        global_limit: int | None = None,
        default_tenant_limit: int | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._global_limit = global_limit
        self._default_limit = default_tenant_limit
        self._clock = clock
        self._accounts: dict[str, _TenantAccount] = {}
        self._reservations: dict[str, Reservation] = {}
        self._global_used = 0
        self._global_held = 0
        self._seq = 0
        # 延迟创建：避免在无运行循环时实例化 Lock 产生绑定循环的问题
        self._lock = None

    @property
    def _ledger_lock(self):
        import asyncio

        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ------------------------------------------------------------------ #
    # 配置与查询
    # ------------------------------------------------------------------ #

    def _account(self, tenant: str) -> _TenantAccount:
        account = self._accounts.get(tenant)
        if account is None:
            account = _TenantAccount(limit=self._default_limit)
            self._accounts[tenant] = account
        return account

    async def set_limit(self, tenant: str, limit: int | None) -> None:
        """设置租户预算上限并记账（None 表示不限）。"""
        async with self._ledger_lock:
            account = self._account(tenant)
            account.limit = limit
            self._append(
                account,
                tenant,
                EntryKind.LIMIT,
                reason=f"预算上限设置为 {limit}",
            )

    async def set_global_limit(self, limit: int | None) -> None:
        """设置全局预算上限。"""
        async with self._ledger_lock:
            self._global_limit = limit

    async def balance(self, tenant: str) -> dict:
        """返回单个租户可见的余额视图。"""
        async with self._ledger_lock:
            account = self._account(tenant)
            return self._balance_payload(tenant, account)

    def _balance_payload(self, tenant: str, account: _TenantAccount) -> dict:
        consumed = account.used + account.held
        return {
            "tenant": tenant,
            "limit": account.limit,
            "used": account.used,
            "held": account.held,
            "consumed": consumed,
            "available": (
                None if account.limit is None else account.limit - consumed
            ),
            "frozen": account.frozen,
        }

    async def global_balance(self) -> dict:
        """返回全局余额视图（仅供管理员接口）。"""
        async with self._ledger_lock:
            consumed = self._global_used + self._global_held
            return {
                "limit": self._global_limit,
                "used": self._global_used,
                "held": self._global_held,
                "consumed": consumed,
                "available": (
                    None
                    if self._global_limit is None
                    else self._global_limit - consumed
                ),
                "tenants": sorted(self._accounts),
            }

    async def history(self, tenant: str) -> list[LedgerEntry]:
        """返回租户自己的余额变化流水，不包含任何其他租户数据。"""
        async with self._ledger_lock:
            account = self._account(tenant)
            return list(account.events)

    async def get_reservation(
        self, reservation_id: str, *, tenant: str | None = None
    ) -> Reservation:
        """按凭证 ID 取凭证；tenant 不匹配时按不存在处理，防止跨租户探测。"""
        from sanic.quota.exceptions import ReservationNotFound

        reservation = self._reservations.get(reservation_id)
        if reservation is None or (
            tenant is not None and reservation.tenant != tenant
        ):
            raise ReservationNotFound(f"预留凭证不存在: {reservation_id}")
        return reservation

    async def held_reservations(
        self, *, tenant: str | None = None
    ) -> list[Reservation]:
        """返回仍持有的预留（可按租户过滤），供管理与回收诊断使用。"""
        async with self._ledger_lock:
            return [
                reservation
                for reservation in self._reservations.values()
                if reservation.held
                and (tenant is None or reservation.tenant == tenant)
            ]

    # ------------------------------------------------------------------ #
    # 预留
    # ------------------------------------------------------------------ #

    async def reserve(
        self,
        tenant: str,
        estimate: int,
        *,
        ttl: float | None = None,
        request_id: str | None = None,
        reservation_id: str | None = None,
    ) -> Reservation:
        """原子地申请预留，同时守住租户与全局上限。"""
        if estimate < 0:
            raise ValueError("estimate 不能为负数")

        import uuid

        async with self._ledger_lock:
            account = self._account(tenant)
            if account.frozen:
                raise TenantFrozen(f"租户 {tenant} 已被冻结")

            tenant_after = account.used + account.held + estimate
            if account.limit is not None and tenant_after > account.limit:
                raise QuotaExceeded(
                    self._exceeded_message(
                        "租户", tenant, account.limit, tenant_after
                    ),
                    scope="tenant",
                    tenant=tenant,
                )

            global_after = self._global_used + self._global_held + estimate
            if (
                self._global_limit is not None
                and global_after > self._global_limit
            ):
                raise QuotaExceeded(
                    self._exceeded_message(
                        "全局", tenant, self._global_limit, global_after
                    ),
                    scope="global",
                    tenant=tenant,
                )

            now = self._clock()
            reservation = Reservation(
                id=reservation_id or uuid.uuid4().hex,
                tenant=tenant,
                estimate=estimate,
                created_at=now,
                expires_at=None if ttl is None else now + ttl,
                request_id=request_id,
            )
            self._reservations[reservation.id] = reservation

            account.held += estimate
            self._global_held += estimate
            self._append(
                account,
                tenant,
                EntryKind.RESERVE,
                estimate=estimate,
                delta=estimate,
                reservation=reservation,
            )
            return reservation

    @staticmethod
    def _exceeded_message(
        scope: str, tenant: str, limit: int, after: int
    ) -> str:
        return (
            f"{scope}配额不足（租户 {tenant}）："
            f"上限 {limit}，本次申请后将达 {after}"
        )

    # ------------------------------------------------------------------ #
    # 终结：结算 / 归还，全部幂等
    # ------------------------------------------------------------------ #

    async def settle(
        self,
        reservation_id: str,
        actual: int,
        *,
        reason: str | None = None,
    ) -> tuple[Reservation, bool]:
        """按实际用量结算。

        实际用量替换预留估算：held 减估算、used 加实际。重复结算或在
        已终结凭证上结算均为幂等空操作，返回 ``(凭证, 是否本次生效)``。
        """
        if actual < 0:
            raise ValueError("actual 不能为负数")

        async with self._ledger_lock:
            reservation = self._reservations[reservation_id]
            if not reservation.held:
                return reservation, False

            account = self._account(reservation.tenant)
            account.held -= reservation.estimate
            account.used += actual
            self._global_held -= reservation.estimate
            self._global_used += actual

            self._close(reservation, ReservationState.SETTLED)
            reservation.actual = actual
            reservation.close_reason = reason
            self._append(
                account,
                reservation.tenant,
                EntryKind.SETTLE,
                estimate=reservation.estimate,
                actual=actual,
                delta=actual - reservation.estimate,
                reservation=reservation,
                reason=reason,
            )
            return reservation, True

    async def release(
        self,
        reservation_id: str,
        *,
        state: ReservationState = ReservationState.CANCELLED,
        reason: str | None = None,
    ) -> tuple[Reservation, bool]:
        """归还全部预留（异常取消、超时回收、冻结回收共用）。

        已终结凭证上的重复归还为幂等空操作。
        """
        if state not in _TERMINAL_REFUND:
            raise ValueError(f"release 不接受终结状态 {state}")

        async with self._ledger_lock:
            return self._release_locked(
                self._reservations[reservation_id], state, reason
            )

    def _release_locked(
        self,
        reservation: Reservation,
        state: ReservationState,
        reason: str | None,
    ) -> tuple[Reservation, bool]:
        """锁内归还原语：供 release/freeze/sweep 复用，避免锁重入。"""
        if not reservation.held:
            return reservation, False

        account = self._account(reservation.tenant)
        account.held -= reservation.estimate
        self._global_held -= reservation.estimate

        kind = _TERMINAL_REFUND[state]
        self._close(reservation, state)
        reservation.close_reason = reason
        self._append(
            account,
            reservation.tenant,
            kind,
            estimate=reservation.estimate,
            delta=-reservation.estimate,
            reservation=reservation,
            reason=reason,
        )
        return reservation, True

    def _close(
        self, reservation: Reservation, state: ReservationState
    ) -> None:
        reservation.state = state
        reservation.closed_at = datetime.now(timezone.utc).isoformat()
        reservation.close_kind = _TERMINAL_REFUND.get(state, EntryKind.SETTLE)

    # ------------------------------------------------------------------ #
    # 冻结 / 解冻 / 超时回收
    # ------------------------------------------------------------------ #

    async def freeze(self, tenant: str) -> int:
        """冻结租户并强制回收其全部在途预留，返回回收数量。"""
        async with self._ledger_lock:
            account = self._account(tenant)
            account.frozen = True
            reclaimed = 0
            for reservation in self._reservations.values():
                if reservation.tenant == tenant and reservation.held:
                    _, applied = self._release_locked(
                        reservation,
                        ReservationState.FROZEN,
                        "管理员冻结租户",
                    )
                    reclaimed += int(applied)
            self._append(
                account,
                tenant,
                EntryKind.FREEZE,
                reason=f"冻结时回收 {reclaimed} 笔在途预留",
            )
            return reclaimed

    async def unfreeze(self, tenant: str) -> None:
        """解冻租户（已结算/回收的历史凭证不受影响）。"""
        async with self._ledger_lock:
            account = self._account(tenant)
            account.frozen = False
            self._append(
                account,
                tenant,
                EntryKind.UNFREEZE,
                reason="管理员解冻租户",
            )

    async def sweep_expired(self) -> list[Reservation]:
        """回收所有已过期但仍持有的预留，幂等且可反复调用。"""
        now = self._clock()
        expired: list[Reservation] = []
        async with self._ledger_lock:
            for reservation in list(self._reservations.values()):
                if (
                    reservation.held
                    and reservation.expires_at is not None
                    and reservation.expires_at <= now
                ):
                    closed, applied = self._release_locked(
                        reservation,
                        ReservationState.EXPIRED,
                        "超过预留 TTL 仍未完成",
                    )
                    if applied:
                        expired.append(closed)
        return expired

    # ------------------------------------------------------------------ #
    # 流水
    # ------------------------------------------------------------------ #

    def _append(
        self,
        account: _TenantAccount,
        tenant: str,
        kind: EntryKind,
        *,
        estimate: int = 0,
        actual: int | None = None,
        delta: int = 0,
        reservation: Reservation | None = None,
        reason: str | None = None,
    ) -> LedgerEntry:
        self._seq += 1
        entry = LedgerEntry(
            seq=self._seq,
            at=datetime.now(timezone.utc).isoformat(),
            tenant=tenant,
            kind=kind,
            estimate=estimate,
            actual=actual,
            delta=delta,
            balance_after=account.used + account.held,
            global_balance_after=self._global_used + self._global_held,
            reservation_id=reservation.id if reservation else None,
            request_id=reservation.request_id if reservation else None,
            reason=reason,
        )
        account.events.append(entry)
        return entry


__all__ = (
    "EntryKind",
    "Ledger",
    "LedgerEntry",
    "Reservation",
    "ReservationState",
)
