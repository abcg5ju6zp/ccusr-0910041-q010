"""Sanic 集成层：中间件、处理器与响应生命周期之间的凭证传递。

用法::

    from sanic import Sanic
    from sanic.quota import QuotaBlueprint, attach_quota

    app = Sanic("inference")
    quota = attach_quota(app, global_limit=1000)

    @app.post("/infer")
    async def infer(request):
        reservation = request.ctx.quota_reservation  # 中间件已预留
        result = await run_model(...)
        # 方式一：处理器显式按实际用量结算
        await quota.settle(reservation, actual=result.tokens)
        return json(result.as_dict())

    @app.post("/infer2")
    async def infer2(request):
        result = await run_model(...)
        # 方式二：用响应头声明实际用量，响应生命周期自动结算
        resp = json(result.as_dict())
        resp.headers["X-Quota-Actual"] = str(result.tokens)
        return resp

生命周期保证：

* 请求中间件原子预留，越过租户或全局上限直接 429，冻结租户 403；
* ``http.lifecycle.response`` 信号按实际用量结算（未声明则按估算结算）；
* ``http.lifecycle.exception`` 信号归还全部预留（含客户端断开导致的
  ``RequestCancelled``）；
* 连接直接消失、处理器遗漏结算时，reaper 按 TTL 超时回收；
* 所有结算/取消/回收均经由凭证状态机，重复调用保持幂等。
"""

from __future__ import annotations

from typing import Awaitable, Callable

from sanic import Blueprint
from sanic.exceptions import Forbidden
from sanic.log import logger
from sanic.quota.ledger import Reservation
from sanic.quota.service import QuotaManager, QuotaService
from sanic.request import Request
from sanic.response import json as json_response


#: 请求上下文中预留凭证的属性名
RESERVATION_CTX_ATTR = "quota_reservation"

#: 连接上下文中在途预留凭证列表的属性名（连接关闭时统一归还）
RESERVATION_CONN_ATTR = "quota_reservations"

#: 响应头：处理器声明本次实际用量
QUOTA_ACTUAL_HEADER = "X-Quota-Actual"

#: 响应头：回传预留凭证 ID 便于对账
QUOTA_RESERVATION_HEADER = "X-Quota-Reservation"

#: 请求头：租户标识
TENANT_HEADER = "X-Tenant-ID"

#: 请求头：本次请求的估算用量
ESTIMATE_HEADER = "X-Quota-Estimate"

#: 管理员鉴权头
ADMIN_KEY_HEADER = "X-Admin-Key"


TenantResolver = Callable[[Request], str | Awaitable[str]]
EstimateResolver = Callable[[Request], int | Awaitable[int]]


def _maybe_await(value):
    """同步值直接返回；可等待对象原样返回以便 await。"""
    import inspect

    if inspect.isawaitable(value):
        return value

    async def _identity():
        return value

    return _identity()


def default_tenant_resolver(request: Request) -> str:
    """默认租户解析：``X-Tenant-ID`` 请求头，缺失时为 ``anonymous``。"""
    return request.headers.getone(TENANT_HEADER, "anonymous")


def make_estimate_resolver(default_estimate: int = 1) -> EstimateResolver:
    """构造估算解析器：优先 ``X-Quota-Estimate`` 头，否则用默认值。"""

    def resolver(request: Request) -> int:
        raw = request.headers.getone(ESTIMATE_HEADER, None)
        if raw is None:
            return default_estimate
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default_estimate
        return max(value, 0)

    return resolver


def get_reservation(request: Request) -> Reservation | None:
    """取出请求中间件放入的预留凭证（未启用配额时为 None）。"""
    return getattr(request.ctx, RESERVATION_CTX_ATTR, None)


async def settle_request(request: Request, actual: int) -> bool:
    """处理器辅助函数：按实际用量结算当前请求的预留。

    与响应生命周期中的自动结算共用同一状态机，先调用也不会重复计费。
    """
    reservation = get_reservation(request)
    if reservation is None:
        return False
    service: QuotaService = request.app.ctx.quota
    return await service.settle(reservation, actual)


async def cancel_request(
    request: Request, *, reason: str | None = None
) -> bool:
    """处理器辅助函数：显式归还当前请求的预留，幂等。"""
    reservation = get_reservation(request)
    if reservation is None:
        return False
    service: QuotaService = request.app.ctx.quota
    return await service.cancel(reservation, reason=reason)


class QuotaBlueprint(Blueprint):
    """配额蓝图：管理/查询接口。

    路由（前缀可在 :func:`attach_quota` 配置）：

    * ``GET  <prefix>/balance`` —— 租户查自己的余额与变化解释；
    * ``GET  <prefix>/reservations/<reservation_id>`` —— 查自己的凭证；
    * ``POST <prefix>/admin/tenants/<tenant>/freeze`` —— 管理员冻结；
    * ``POST <prefix>/admin/tenants/<tenant>/unfreeze`` —— 管理员解冻；
    * ``PUT  <prefix>/admin/tenants/<tenant>/limit`` —— 设置租户上限；
    * ``GET  <prefix>/admin/global`` —— 全局余额。
    """

    def __init__(
        self,
        name: str = "quota",
        url_prefix: str = "/quota",
        *,
        admin_key: str | None = None,
        tenant_resolver: TenantResolver | None = None,
    ) -> None:
        super().__init__(name=name, url_prefix=url_prefix)
        self.ctx.admin_key = admin_key
        self.ctx.tenant_resolver = tenant_resolver
        self._register_routes()

    def service(self, request: Request) -> QuotaService:
        return request.app.ctx.quota

    async def _tenant(self, request: Request) -> str:
        value = self.ctx.tenant_resolver(request)
        return await _maybe_await(value)

    def _require_admin(self, request: Request) -> None:
        admin_key = self.ctx.admin_key
        if not admin_key:
            raise Forbidden("未配置管理员密钥，管理接口不可用")
        provided = request.headers.getone(ADMIN_KEY_HEADER, None)
        # 恒定比较，避免时序泄漏
        import hmac

        if provided is None or not hmac.compare_digest(provided, admin_key):
            raise Forbidden("管理员鉴权失败")

    def _register_routes(self) -> None:
        @self.get("/balance")
        async def balance(request: Request):
            tenant = await self._tenant(request)
            statement = await self.service(request).statement(tenant)
            return json_response(statement)

        @self.get("/reservations/<reservation_id:str>")
        async def reservation_detail(request: Request, reservation_id: str):
            tenant = await self._tenant(request)
            # tenant= 限制：跨租户访问与"不存在"返回相同的 404，
            # 不泄漏其他租户的凭证是否存在
            reservation = await self.service(request).reservation(
                reservation_id, tenant=tenant
            )
            return json_response(reservation.to_dict())

        @self.post("/admin/tenants/<tenant:str>/freeze")
        async def freeze(request: Request, tenant: str):
            self._require_admin(request)
            reclaimed = await self.service(request).freeze(tenant)
            return json_response(
                {"tenant": tenant, "frozen": True, "reclaimed": reclaimed}
            )

        @self.post("/admin/tenants/<tenant:str>/unfreeze")
        async def unfreeze(request: Request, tenant: str):
            self._require_admin(request)
            await self.service(request).unfreeze(tenant)
            return json_response({"tenant": tenant, "frozen": False})

        @self.put("/admin/tenants/<tenant:str>/limit")
        async def set_limit(request: Request, tenant: str):
            self._require_admin(request)
            from sanic.exceptions import BadRequest

            payload = request.json or {}
            limit = payload.get("limit")
            if limit is not None:
                try:
                    limit = int(limit)
                except (TypeError, ValueError):
                    raise BadRequest("limit 必须是非负整数或 null")
                if limit < 0:
                    raise BadRequest("limit 不能为负数")
            await self.service(request).set_limit(tenant, limit)
            return json_response({"tenant": tenant, "limit": limit})

        @self.get("/admin/global")
        async def global_balance(request: Request):
            self._require_admin(request)
            return json_response(await self.service(request).global_balance())


def attach_quota(
    app,
    service: QuotaService | None = None,
    *,
    manager: QuotaManager | None = None,
    name: str = "default",
    global_limit: int | None = None,
    default_tenant_limit: int | None = None,
    ttl: float | None = 60.0,
    reaper_interval: float | None = 5.0,
    default_estimate: int = 1,
    tenant_resolver: TenantResolver | None = None,
    estimate_resolver: EstimateResolver | None = None,
    admin_key: str | None = None,
    url_prefix: str = "/quota",
    enable_admin: bool = True,
) -> QuotaService:
    """把配额预留机制挂到 Sanic 应用。

    注册：请求预留中间件、响应结算/异常归还信号、reaper 生命周期
    监听器，以及（可选的）管理查询蓝图。返回所用的 ``QuotaService``。
    """
    if service is None:
        manager = manager or getattr(app.ctx, "quota_manager", None)
        if manager is None:
            manager = QuotaManager()
            app.ctx.quota_manager = manager
        service = manager.require(
            name,
            global_limit=global_limit,
            default_tenant_limit=default_tenant_limit,
            ttl=ttl,
            reaper_interval=reaper_interval,
        )
    app.ctx.quota = service

    tenant_resolver = tenant_resolver or default_tenant_resolver
    estimate_resolver = estimate_resolver or make_estimate_resolver(
        default_estimate
    )

    async def _resolve(value):
        return await _maybe_await(value)

    @app.on_request
    async def quota_reserve_middleware(request: Request):
        """请求进入时原子预留，凭证挂到 request.ctx 传递给处理器。"""
        # 配额自身的查询/管理接口不占用租户预算
        if request.path.startswith(url_prefix.rstrip("/") + "/") or (
            request.path == url_prefix.rstrip("/")
        ):
            return
        tenant = await _resolve(tenant_resolver(request))
        estimate = await _resolve(estimate_resolver(request))
        reservation = await service.reserve(
            tenant,
            estimate,
            request_id=str(request.id) if request.id else None,
        )
        setattr(request.ctx, RESERVATION_CTX_ATTR, reservation)
        # 登记到连接上下文：连接关闭（lifecycle.complete）时兜底归还。
        # keep-alive 连接承载多个请求，因此这里是一个累积列表。
        conn_info = getattr(request, "conn_info", None)
        if conn_info is not None:
            held_on_conn = getattr(conn_info.ctx, RESERVATION_CONN_ATTR, None)
            if held_on_conn is None:
                held_on_conn = []
                setattr(conn_info.ctx, RESERVATION_CONN_ATTR, held_on_conn)
            held_on_conn.append(reservation)

    @app.signal("http.lifecycle.response")
    async def quota_settle_signal(request, response):
        """响应生命周期：按实际用量结算；已终结则幂等跳过。"""
        reservation = get_reservation(request)
        if reservation is None or not reservation.held:
            return
        actual = reservation.estimate
        header_value = response.headers.getone(QUOTA_ACTUAL_HEADER, None)
        if header_value is not None:
            try:
                actual = max(int(header_value), 0)
            except (TypeError, ValueError):
                logger.warning(
                    "quota: 忽略非法的 %s 头: %r",
                    QUOTA_ACTUAL_HEADER,
                    header_value,
                )
        await service.settle(
            reservation, actual, reason="响应生命周期自动结算"
        )
        try:
            response.headers[QUOTA_RESERVATION_HEADER] = reservation.id
        except Exception:  # noqa: BLE001 - 响应头不可写不应影响结算
            logger.debug("quota: 无法写入预留凭证响应头", exc_info=True)

    @app.signal("http.lifecycle.exception")
    async def quota_release_signal(request, exception):
        """异常生命周期：归还预留（含客户端断开的 RequestCancelled）。"""
        reservation = get_reservation(request)
        if reservation is None or not reservation.held:
            return
        reason = f"请求异常: {type(exception).__name__}"
        await service.cancel(reservation, reason=reason)

    @app.signal("http.lifecycle.complete")
    async def quota_connection_reclaim(conn_info):
        """连接关闭：兜底归还该连接上仍持有的全部预留。

        正常请求已在 response 信号结算、异常请求已在 exception 信号
        归还，此处对它们全部幂等跳过；只有客户端直接断开、处理器
        遗漏终结等路径会真正归还（ASGI 不触发本信号，由 TTL 回收
        循环承担同等职责）。
        """
        held_on_conn = getattr(conn_info.ctx, RESERVATION_CONN_ATTR, None)
        if not held_on_conn:
            return
        for reservation in held_on_conn:
            if reservation.held:
                await service.cancel(
                    reservation, reason="连接关闭，归还在途预留"
                )
        held_on_conn.clear()

    @app.listener("before_server_start")
    async def quota_start_reaper(app):
        service.start_reaper()

    @app.listener("after_server_stop")
    async def quota_stop_reaper(app):
        await service.stop_reaper()

    if enable_admin:
        blueprint = QuotaBlueprint(
            name=f"quota-admin-{name}",
            url_prefix=url_prefix,
            admin_key=admin_key,
            tenant_resolver=tenant_resolver,
        )
        app.blueprint(blueprint)

    return service


__all__ = (
    "ADMIN_KEY_HEADER",
    "ESTIMATE_HEADER",
    "QUOTA_ACTUAL_HEADER",
    "QUOTA_RESERVATION_HEADER",
    "RESERVATION_CTX_ATTR",
    "TENANT_HEADER",
    "QuotaBlueprint",
    "attach_quota",
    "cancel_request",
    "default_tenant_resolver",
    "get_reservation",
    "make_estimate_resolver",
    "settle_request",
)
