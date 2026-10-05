"""配额服务：账本之上的应用服务与超时回收循环。

``QuotaService`` 是中间件、处理器与管理接口共同使用的入口：申请预留、
按实际用量结算、异常归还、冻结租户、查询余额流水。内置的回收循环
（reaper）周期性扫描过期预留，保证客户端断开或处理器遗漏结算时额度
不会被永久占用。

``QuotaManager`` 维护按名注册的服务实例，便于多应用或多套预算隔离。
"""

from __future__ import annotations

import asyncio

from contextlib import suppress

from sanic.log import logger
from sanic.quota.exceptions import ReservationNotFound
from sanic.quota.ledger import (
    Ledger,
    LedgerEntry,
    Reservation,
    ReservationState,
)


_DEFAULT_TTL = object()


class QuotaService:
    """配额预留应用服务。

    Args:
        name: 服务名称，用于多实例注册与日志。
        ttl: 预留默认存活秒数，超时未结算将被回收循环归还。
        reaper_interval: 回收循环扫描间隔秒数，None 表示不自动扫描。
        **ledger_kwargs: 透传给 :class:`Ledger`（global_limit 等）。
    """

    def __init__(
        self,
        *,
        name: str = "default",
        ttl: float | None = 60.0,
        reaper_interval: float | None = 5.0,
        **ledger_kwargs,
    ) -> None:
        self.name = name
        self.ttl = ttl
        self.reaper_interval = reaper_interval
        self.ledger = Ledger(**ledger_kwargs)
        self._reaper_task: asyncio.Task | None = None
        self._reaper_stop: asyncio.Event | None = None

    # ------------------------------------------------------------------ #
    # 预留与终结
    # ------------------------------------------------------------------ #

    async def reserve(
        self,
        tenant: str,
        estimate: int,
        *,
        ttl: float | None | object = _DEFAULT_TTL,
        request_id: str | None = None,
    ) -> Reservation:
        """申请预留。ttl 缺省时使用服务级默认 TTL。"""
        if ttl is _DEFAULT_TTL:
            ttl = self.ttl
        return await self.ledger.reserve(
            tenant,
            estimate,
            ttl=ttl,  # type: ignore[arg-type]
            request_id=request_id,
        )

    async def settle(
        self,
        reservation: Reservation | str,
        actual: int,
        *,
        reason: str | None = None,
    ) -> bool:
        """按实际用量结算，返回是否本次生效（重复结算返回 False）。

        可直接传凭证对象（处理器常用）或凭证 ID（管理接口常用）。
        """
        reservation_id = (
            reservation if isinstance(reservation, str) else reservation.id
        )
        try:
            _, applied = await self.ledger.settle(
                reservation_id, actual, reason=reason
            )
        except KeyError:
            raise ReservationNotFound(
                f"预留凭证不存在: {reservation_id}"
            ) from None
        if applied:
            logger.debug(
                "quota[%s] settle %s actual=%s",
                self.name,
                reservation_id,
                actual,
            )
        return applied

    async def cancel(
        self,
        reservation: Reservation | str,
        *,
        reason: str | None = None,
    ) -> bool:
        """异常归还全部预留，幂等，返回是否本次生效。"""
        reservation_id = (
            reservation if isinstance(reservation, str) else reservation.id
        )
        try:
            _, applied = await self.ledger.release(
                reservation_id,
                state=ReservationState.CANCELLED,
                reason=reason,
            )
        except KeyError:
            raise ReservationNotFound(
                f"预留凭证不存在: {reservation_id}"
            ) from None
        if applied:
            logger.debug(
                "quota[%s] cancel %s reason=%s",
                self.name,
                reservation_id,
                reason,
            )
        return applied

    # ------------------------------------------------------------------ #
    # 管理操作
    # ------------------------------------------------------------------ #

    async def freeze(self, tenant: str) -> int:
        """冻结租户并回收其全部在途预留，返回回收数量。"""
        reclaimed = await self.ledger.freeze(tenant)
        logger.warning(
            "quota[%s] tenant %s frozen, %d reservation(s) reclaimed",
            self.name,
            tenant,
            reclaimed,
        )
        return reclaimed

    async def unfreeze(self, tenant: str) -> None:
        """解冻租户。"""
        await self.ledger.unfreeze(tenant)
        logger.warning("quota[%s] tenant %s unfrozen", self.name, tenant)

    async def set_limit(self, tenant: str, limit: int | None) -> None:
        """设置租户预算上限。"""
        await self.ledger.set_limit(tenant, limit)

    # ------------------------------------------------------------------ #
    # 查询：余额 + 可解释的变化流水
    # ------------------------------------------------------------------ #

    async def balance(self, tenant: str) -> dict:
        """返回租户自己的余额视图（limit/used/held/available 等）。"""
        return await self.ledger.balance(tenant)

    async def statement(
        self, tenant: str, *, limit: int | None = None
    ) -> dict:
        """返回余额及其变化解释。

        只包含该租户自身的条目；``changes`` 中每项带人类可读说明，
        可直接回传给租户回答"我的余额为什么变了"。
        """
        payload = await self.ledger.balance(tenant)
        entries = await self.ledger.history(tenant)
        if limit is not None:
            entries = entries[-limit:]
        payload["changes"] = [
            {
                "seq": entry.seq,
                "at": entry.at,
                "kind": entry.kind.value,
                "estimate": entry.estimate,
                "actual": entry.actual,
                "delta": entry.delta,
                "balance_after": entry.balance_after,
                "reservation_id": entry.reservation_id,
                "reason": entry.reason,
                "explanation": entry.explain(),
            }
            for entry in entries
        ]
        return payload

    async def global_balance(self) -> dict:
        """全局余额视图（仅管理员）。"""
        return await self.ledger.global_balance()

    async def reservation(
        self, reservation_id: str, *, tenant: str | None = None
    ) -> Reservation:
        """查询凭证；指定 tenant 时跨租户访问按不存在处理。"""
        return await self.ledger.get_reservation(reservation_id, tenant=tenant)

    async def sweep_expired(self) -> list[Reservation]:
        """立即执行一次过期回收，返回被回收的凭证。"""
        expired = await self.ledger.sweep_expired()
        for reservation in expired:
            logger.info(
                "quota[%s] reaped expired reservation %s (tenant=%s, "
                "estimate=%s)",
                self.name,
                reservation.id,
                reservation.tenant,
                reservation.estimate,
            )
        return expired

    # ------------------------------------------------------------------ #
    # 超时回收循环
    # ------------------------------------------------------------------ #

    def start_reaper(self) -> None:
        """在当前事件循环启动回收循环（重复调用安全）。"""
        if self.reaper_interval is None or (
            self._reaper_task is not None and not self._reaper_task.done()
        ):
            return
        self._reaper_stop = asyncio.Event()
        self._reaper_task = asyncio.ensure_future(self._reap())

    async def stop_reaper(self) -> None:
        """停止回收循环并等待退出（重复调用安全）。"""
        task = self._reaper_task
        stop = self._reaper_stop
        self._reaper_task = None
        self._reaper_stop = None
        if task is None:
            return
        if stop is not None:
            stop.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _reap(self) -> None:
        assert self._reaper_stop is not None
        stop = self._reaper_stop
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.reaper_interval
                )
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                break
            try:
                await self.sweep_expired()
            except Exception:  # noqa: BLE001 - 回收循环不能因单次错误退出
                logger.exception("quota[%s] reaper sweep failed", self.name)


class QuotaManager:
    """按名注册的 QuotaService 注册表。"""

    def __init__(self) -> None:
        self._services: dict[str, QuotaService] = {}

    def create(self, name: str = "default", **kwargs) -> QuotaService:
        """创建并注册一个服务，重名会报错以避免配置被静默覆盖。"""
        if name in self._services:
            raise RuntimeError(f"配额服务已存在: {name}")
        service = QuotaService(name=name, **kwargs)
        self._services[name] = service
        return service

    def get(self, name: str = "default") -> QuotaService:
        """获取已注册服务。"""
        try:
            return self._services[name]
        except KeyError:
            raise KeyError(f"配额服务未注册: {name}") from None

    def require(self, name: str = "default", **kwargs) -> QuotaService:
        """获取或惰性创建服务。"""
        if name not in self._services:
            return self.create(name, **kwargs)
        return self._services[name]

    def __contains__(self, name: str) -> bool:
        return name in self._services


__all__ = ("LedgerEntry", "QuotaManager", "QuotaService")
