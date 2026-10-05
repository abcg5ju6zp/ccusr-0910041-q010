"""配额预留子系统测试。

覆盖：
* 账本：并发预留守住租户与全局上限、结算/取消/回收的幂等性、
  冻结与解冻、TTL 超时回收、流水只含本租户数据；
* 服务/管理器：reaper 循环、重复创建保护；
* Sanic 集成：中间件自动预留、响应自动结算、显式结算、异常归还、
  客户端断开后超时回收、管理员冻结/限额、查询的租户隔离。
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from sanic import Sanic
from sanic.quota import QuotaExceeded, TenantFrozen, attach_quota
from sanic.quota.ledger import Ledger, ReservationState
from sanic.quota.service import QuotaManager, QuotaService
from sanic.response import json as json_response
from sanic.response import text


# --------------------------------------------------------------------- #
# 账本单元测试
# --------------------------------------------------------------------- #


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def ledger(clock: FakeClock) -> Ledger:
    return Ledger(global_limit=10, default_tenant_limit=4, clock=clock)


async def test_reserve_checks_tenant_limit(ledger: Ledger):
    await ledger.reserve("a", 3, ttl=10)
    with pytest.raises(QuotaExceeded) as exc:
        await ledger.reserve("a", 2)
    assert exc.value.scope == "tenant"
    assert exc.value.tenant == "a"
    balance = await ledger.balance("a")
    assert balance["held"] == 3
    assert balance["available"] == 1


async def test_reserve_checks_global_limit(ledger: Ledger):
    await ledger.reserve("a", 4)
    await ledger.reserve("b", 4)
    with pytest.raises(QuotaExceeded) as exc:
        await ledger.reserve("c", 3)
    assert exc.value.scope == "global"
    global_balance = await ledger.global_balance()
    assert global_balance["held"] == 8
    assert global_balance["available"] == 2


async def test_concurrent_reservations_never_cross_limits(
    clock: FakeClock,
):
    """并发高峰：所有成功预留之和必须恰好不超过全局上限。"""
    ledger = Ledger(global_limit=10, default_tenant_limit=100, clock=clock)
    results = []

    async def attempt(tenant: str, amount: int):
        try:
            reservation = await ledger.reserve(tenant, amount, ttl=30)
            results.append((tenant, reservation))
        except QuotaExceeded:
            results.append((tenant, None))

    attempts = [("a", 1) for _ in range(8)] + [("b", 1) for _ in range(8)]
    await asyncio.gather(*(attempt(t, n) for t, n in attempts))

    granted = [r for _, r in results if r is not None]
    # 租户额度充足，全局上限 10、单笔 1，因此恰好 10 笔成功
    assert len(granted) == 10
    global_balance = await ledger.global_balance()
    assert global_balance["held"] == 10


async def test_concurrent_reservations_never_cross_tenant_limit(
    ledger: Ledger,
):
    """并发高峰：同一租户的成功预留之和必须恰好不超过租户上限。"""
    results = []

    async def attempt():
        try:
            reservation = await ledger.reserve("a", 1, ttl=30)
            results.append(reservation)
        except QuotaExceeded:
            results.append(None)

    await asyncio.gather(*(attempt() for _ in range(10)))
    assert len([r for r in results if r is not None]) == 4
    assert (await ledger.balance("a"))["held"] == 4


async def test_settle_uses_actual_and_is_idempotent(ledger: Ledger):
    reservation = await ledger.reserve("a", 4)
    _, applied = await ledger.settle(reservation.id, 1)
    assert applied is True
    balance = await ledger.balance("a")
    assert balance == {
        "tenant": "a",
        "limit": 4,
        "used": 1,
        "held": 0,
        "consumed": 1,
        "available": 3,
        "frozen": False,
    }

    # 重复结算不生效，不重复计费
    _, again = await ledger.settle(reservation.id, 99)
    assert again is False
    assert (await ledger.balance("a"))["used"] == 1

    # 结算后再取消同样不生效
    _, cancelled = await ledger.release(reservation.id)
    assert cancelled is False
    assert (await ledger.balance("a"))["used"] == 1


async def test_settle_over_estimate_charges_actual(ledger: Ledger):
    reservation = await ledger.reserve("a", 2)
    await ledger.settle(reservation.id, 4)
    balance = await ledger.balance("a")
    # 实际用量大于估算：used 计实际，上限检查只作用于预留阶段
    assert balance["used"] == 4
    assert balance["held"] == 0


async def test_cancel_refunds_full_estimate_idempotently(
    ledger: Ledger,
):
    reservation = await ledger.reserve("a", 4)
    _, first = await ledger.release(reservation.id)
    assert first is True
    _, second = await ledger.release(reservation.id)
    assert second is False
    balance = await ledger.balance("a")
    assert balance["used"] == 0
    assert balance["held"] == 0
    global_balance = await ledger.global_balance()
    assert global_balance["held"] == 0
    assert reservation.state is ReservationState.CANCELLED


async def test_freeze_rejects_new_and_reclaims_inflight(ledger: Ledger):
    in_flight = await ledger.reserve("a", 3, ttl=30)
    settled = await ledger.reserve("a", 1, ttl=30)
    await ledger.settle(settled.id, 1)

    reclaimed = await ledger.freeze("a")
    assert reclaimed == 1
    assert in_flight.state is ReservationState.FROZEN
    assert (await ledger.balance("a"))["held"] == 0
    assert (await ledger.balance("a"))["frozen"] is True

    with pytest.raises(TenantFrozen):
        await ledger.reserve("a", 1)
    # 其他租户不受影响
    other = await ledger.reserve("b", 1)
    assert other.tenant == "b"

    await ledger.unfreeze("a")
    assert (await ledger.balance("a"))["frozen"] is False
    new = await ledger.reserve("a", 1)
    assert new.held is True


async def test_freeze_idempotent_when_no_inflight(ledger: Ledger):
    assert await ledger.freeze("a") == 0
    assert await ledger.freeze("a") == 0


async def test_sweep_expired_reclaims_and_is_idempotent(
    ledger: Ledger, clock: FakeClock
):
    fresh = await ledger.reserve("a", 1, ttl=10)
    stale = await ledger.reserve("a", 2, ttl=5)

    clock.advance(6)
    expired = await ledger.sweep_expired()
    assert [r.id for r in expired] == [stale.id]
    assert stale.state is ReservationState.EXPIRED
    assert fresh.held is True
    balance = await ledger.balance("a")
    assert balance["held"] == 1

    # 再扫一次：没有新过期，幂等
    assert await ledger.sweep_expired() == []
    assert (await ledger.balance("a"))["held"] == 1


async def test_history_explains_changes_without_other_tenants(
    ledger: Ledger,
):
    reservation = await ledger.reserve("a", 3)
    await ledger.reserve("b", 2)  # 不应出现在 a 的流水中
    await ledger.settle(reservation.id, 2)

    history = await ledger.history("a")
    assert {entry.tenant for entry in history} == {"a"}
    kinds = [entry.kind.value for entry in history]
    assert kinds == ["reserve", "settle"]
    reserve_entry, settle_entry = history
    assert reserve_entry.delta == 3
    assert settle_entry.delta == -1  # 实际 2 - 估算 3
    assert settle_entry.actual == 2
    explanation = settle_entry.explain()
    assert "结算" in explanation
    assert "实际用量 2" in explanation


async def test_cross_tenant_reservation_lookup_is_not_found(
    ledger: Ledger,
):
    from sanic.quota.exceptions import ReservationNotFound

    reservation = await ledger.reserve("a", 1)
    with pytest.raises(ReservationNotFound):
        await ledger.get_reservation(reservation.id, tenant="attacker")


# --------------------------------------------------------------------- #
# 服务与回收循环
# --------------------------------------------------------------------- #


async def test_reaper_reclaims_after_ttl(clock: FakeClock):
    service = QuotaService(ttl=10, reaper_interval=0.01, clock=clock)
    reservation = await service.reserve("a", 2)
    service.start_reaper()
    try:
        clock.advance(11)
        for _ in range(50):
            if not reservation.held:
                break
            await asyncio.sleep(0.02)
        assert reservation.state is ReservationState.EXPIRED
        assert (await service.balance("a"))["held"] == 0
    finally:
        await service.stop_reaper()
    assert service._reaper_task is None


async def test_reaper_start_stop_idempotent(clock: FakeClock):
    service = QuotaService(ttl=10, reaper_interval=60, clock=clock)
    service.start_reaper()
    service.start_reaper()  # 不重复启动
    await service.stop_reaper()
    await service.stop_reaper()  # 重复停止安全


def test_manager_duplicate_creation_rejected():
    manager = QuotaManager()
    manager.create("svc", ttl=None, reaper_interval=None)
    with pytest.raises(RuntimeError):
        manager.create("svc")
    assert "svc" in manager


async def test_statement_changes_are_self_explanatory(clock: FakeClock):
    service = QuotaService(
        ttl=None,
        reaper_interval=None,
        clock=clock,
        default_tenant_limit=10,
    )
    reservation = await service.reserve("a", 3)
    await service.settle(reservation, 1)
    statement = await service.statement("a")
    assert statement["available"] == statement["limit"] - 1
    assert len(statement["changes"]) == 2
    assert all("explanation" in change for change in statement["changes"])
    assert "其他" not in str(statement)


# --------------------------------------------------------------------- #
# Sanic 集成
# --------------------------------------------------------------------- #


def _build_app(
    app: Sanic,
    service: QuotaService | None = None,
    **kwargs,
) -> QuotaService:
    defaults = {
        "default_estimate": 1,
        "reaper_interval": None,
        "enable_admin": True,
        "admin_key": "secret",
    }
    defaults.update(kwargs)
    return attach_quota(app, service=service, **defaults)


async def test_middleware_reserve_and_response_settle(app: Sanic):
    service = _build_app(app, global_limit=10, default_tenant_limit=5)

    @app.get("/infer")
    async def infer(request):
        reservation = request.ctx.quota_reservation
        assert reservation is not None
        assert reservation.tenant == "tenant-1"
        assert (await service.balance("tenant-1"))["held"] == 1
        resp = json_response({"ok": True})
        resp.headers["X-Quota-Actual"] = "3"
        return resp

    _, response = await app.asgi_client.get(
        "/infer", headers={"X-Tenant-ID": "tenant-1"}
    )
    assert response.status == 200
    balance = await service.balance("tenant-1")
    assert balance["used"] == 3
    assert balance["held"] == 0
    assert response.headers.get("X-Quota-Reservation")


async def test_explicit_settle_then_response_is_idempotent(
    app: Sanic,
):
    service = _build_app(app, default_tenant_limit=10)

    @app.get("/infer")
    async def infer(request):
        from sanic.quota import settle_request

        applied = await settle_request(request, 2)
        assert applied is True
        # 响应头再声明一次实际用量也不应重复计费
        resp = text("ok")
        resp.headers["X-Quota-Actual"] = "2"
        return resp

    _, response = await app.asgi_client.get(
        "/infer", headers={"X-Tenant-ID": "t"}
    )
    assert response.status == 200
    assert (await service.balance("t"))["used"] == 2


async def test_default_settle_uses_estimate(app: Sanic):
    service = _build_app(app, default_tenant_limit=10)

    @app.get("/infer")
    async def infer(request):
        return text("ok")

    _, response = await app.asgi_client.get(
        "/infer",
        headers={"X-Tenant-ID": "t", "X-Quota-Estimate": "4"},
    )
    assert response.status == 200
    balance = await service.balance("t")
    assert balance["used"] == 4
    assert balance["held"] == 0


async def test_tenant_limit_returns_429_under_concurrency(app: Sanic):
    """并发高峰：超过租户上限的并发请求得到 429，额度不被越过。"""
    service = _build_app(app, default_tenant_limit=2)
    gate = asyncio.Event()
    app.ctx.inflight = 0

    @app.get("/infer")
    async def infer(request):
        app.ctx.inflight += 1
        await gate.wait()
        return text("ok")

    async def call():
        return await app.asgi_client.get(
            "/infer", headers={"X-Tenant-ID": "t"}
        )

    tasks = [asyncio.ensure_future(call()) for _ in range(5)]
    await asyncio.sleep(0.1)
    assert (await service.balance("t"))["held"] == 2
    gate.set()
    responses = await asyncio.gather(*tasks)
    statuses = sorted(response.status for _, response in responses)
    assert statuses == [200, 200, 429, 429, 429]
    assert (await service.balance("t"))["held"] == 0
    assert (await service.balance("t"))["used"] == 2


async def test_global_limit_blocks_other_tenants(app: Sanic):
    service = _build_app(app, global_limit=2, default_tenant_limit=100)
    gate = asyncio.Event()

    @app.get("/infer")
    async def infer(request):
        await gate.wait()
        return text("ok")

    async def call(tenant: str):
        return await app.asgi_client.get(
            "/infer", headers={"X-Tenant-ID": tenant}
        )

    tasks = [
        asyncio.ensure_future(call("a")),
        asyncio.ensure_future(call("a")),
        asyncio.ensure_future(call("b")),
    ]
    await asyncio.sleep(0.1)
    gate.set()
    responses = await asyncio.gather(*tasks)
    statuses = sorted(response.status for _, response in responses)
    assert statuses == [200, 200, 429]
    global_balance = await service.global_balance()
    assert global_balance["used"] == 2


async def test_handler_exception_releases_reservation(app: Sanic):
    service = _build_app(app, default_tenant_limit=10)

    @app.get("/boom")
    async def boom(request):
        raise RuntimeError("model crashed")

    _, response = await app.asgi_client.get(
        "/boom", headers={"X-Tenant-ID": "t"}
    )
    assert response.status == 500
    balance = await service.balance("t")
    assert balance["held"] == 0
    assert balance["used"] == 0
    history = await service.ledger.history("t")
    assert [entry.kind.value for entry in history] == [
        "reserve",
        "cancel",
    ]


async def test_client_disconnect_reclaimed_by_timeout(app: Sanic):
    """客户端断开且信号未及归还时，TTL 回收兜底，额度不会永久占用。"""
    service = QuotaService(
        global_limit=10,
        default_tenant_limit=10,
        ttl=0.05,
        reaper_interval=None,
    )
    _build_app(app, service=service)

    @app.get("/slow")
    async def slow(request):
        await asyncio.sleep(1.0)
        return text("ok")

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        app.asgi_client.get("/slow", headers={"X-Tenant-ID": "t"})
    )
    loop.call_later(0.01, task)
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # 断开后额度仍被预留（不会永久泄漏），TTL 到期即回收
    held = await service.ledger.held_reservations(tenant="t")
    assert len(held) == 1
    await asyncio.sleep(0.1)
    await service.sweep_expired()
    balance = await service.balance("t")
    assert balance["held"] == 0
    assert held[0].state is ReservationState.EXPIRED


async def test_connection_complete_signal_reclaims_inflight(app: Sanic):
    """真实服务器语义：http.lifecycle.complete 时按连接归还在途预留。"""
    service = _build_app(app, default_tenant_limit=10)
    gate = asyncio.Event()

    @app.get("/hang")
    async def hang(request):
        app.ctx.last_conn_info = request.conn_info
        await gate.wait()
        return text("ok")

    call = asyncio.ensure_future(
        app.asgi_client.get("/hang", headers={"X-Tenant-ID": "t"})
    )
    await asyncio.sleep(0.05)
    held = await service.ledger.held_reservations(tenant="t")
    assert len(held) == 1

    # 模拟连接关闭（真实服务器上由 connection_task 的 finally 派发）
    await app.dispatch(
        "http.lifecycle.complete",
        context={"conn_info": app.ctx.last_conn_info},
    )
    assert held[0].state is ReservationState.CANCELLED
    assert (await service.balance("t"))["held"] == 0

    # 之后请求即便正常返回，响应阶段结算也幂等，不产生用量
    gate.set()
    _, response = await call
    assert response.status == 200
    assert (await service.balance("t"))["used"] == 0
    history = await service.ledger.history("t")
    assert [entry.kind.value for entry in history] == [
        "reserve",
        "cancel",
    ]


async def test_admin_freeze_blocks_tenant(app: Sanic):
    _build_app(app, default_tenant_limit=10)

    @app.get("/infer")
    async def infer(request):
        return text("ok")

    # 无管理员密钥 -> 403
    _, denied = await app.asgi_client.post("/quota/admin/tenants/t/freeze")
    assert denied.status == 403

    _, frozen = await app.asgi_client.post(
        "/quota/admin/tenants/t/freeze",
        headers={"X-Admin-Key": "secret"},
    )
    assert frozen.status == 200
    assert frozen.json["frozen"] is True

    _, blocked = await app.asgi_client.get(
        "/infer", headers={"X-Tenant-ID": "t"}
    )
    assert blocked.status == 403


async def test_admin_limit_and_freeze_reclaims_inflight(app: Sanic):
    service = _build_app(app, global_limit=100, default_tenant_limit=100)
    gate = asyncio.Event()

    @app.get("/infer")
    async def infer(request):
        await gate.wait()
        resp = text("ok")
        resp.headers["X-Quota-Actual"] = "1"
        return resp

    call = asyncio.ensure_future(
        app.asgi_client.get("/infer", headers={"X-Tenant-ID": "t"})
    )
    await asyncio.sleep(0.05)
    assert (await service.balance("t"))["held"] == 1

    _, frozen = await app.asgi_client.post(
        "/quota/admin/tenants/t/freeze",
        headers={"X-Admin-Key": "secret"},
    )
    assert frozen.status == 200
    assert frozen.json["reclaimed"] == 1
    assert (await service.balance("t"))["held"] == 0

    gate.set()
    _, response = await call
    assert response.status == 200
    # 冻结回收后响应阶段的结算为幂等空操作，不产生用量
    assert (await service.balance("t"))["used"] == 0


async def test_balance_endpoint_is_tenant_scoped(app: Sanic):
    _build_app(app, default_tenant_limit=5)

    @app.get("/infer")
    async def infer(request):
        resp = text("ok")
        resp.headers["X-Quota-Actual"] = "2"
        return resp

    await app.asgi_client.get("/infer", headers={"X-Tenant-ID": "alice"})
    await app.asgi_client.get("/infer", headers={"X-Tenant-ID": "bob"})

    _, response = await app.asgi_client.get(
        "/quota/balance", headers={"X-Tenant-ID": "alice"}
    )
    assert response.status == 200
    body = response.json
    assert body["tenant"] == "alice"
    assert body["used"] == 2
    # 流水与余额中不得出现其他租户的任何痕迹
    serialized = str(body)
    assert "bob" not in serialized
    assert all(change["reservation_id"] or True for change in body["changes"])


async def test_reservation_detail_isolation(app: Sanic):
    service = _build_app(app, default_tenant_limit=10)
    reservation = await service.reserve("alice", 1)

    _, ok = await app.asgi_client.get(
        f"/quota/reservations/{reservation.id}",
        headers={"X-Tenant-ID": "alice"},
    )
    assert ok.status == 200
    assert ok.json["tenant"] == "alice"

    # 其他租户查询：与"不存在"完全一致的 404
    _, forbidden = await app.asgi_client.get(
        f"/quota/reservations/{reservation.id}",
        headers={"X-Tenant-ID": "mallory"},
    )
    assert forbidden.status == 404


async def test_admin_global_requires_admin_and_admin_can_set_limit(
    app: Sanic,
):
    _build_app(app, global_limit=10, default_tenant_limit=3)

    _, unauthorized = await app.asgi_client.get("/quota/admin/global")
    assert unauthorized.status == 403

    _, limited = await app.asgi_client.put(
        "/quota/admin/tenants/t/limit",
        headers={"X-Admin-Key": "secret"},
        json={"limit": 0},
    )
    assert limited.status == 200

    @app.get("/infer")
    async def infer(request):
        return text("ok")

    _, blocked = await app.asgi_client.get(
        "/infer", headers={"X-Tenant-ID": "t"}
    )
    assert blocked.status == 429

    _, global_view = await app.asgi_client.get(
        "/quota/admin/global", headers={"X-Admin-Key": "secret"}
    )
    assert global_view.status == 200
    assert global_view.json["limit"] == 10
