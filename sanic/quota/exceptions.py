"""配额预留子系统的异常类型。"""

from __future__ import annotations

from sanic.exceptions import SanicException


class QuotaError(SanicException):
    """配额子系统异常基类。"""


class QuotaExceeded(QuotaError):
    """预留或结算时越过租户或全局上限。

    Attributes:
        tenant: 触发上限的租户标识；全局上限触发时可能为 None。
        scope: "tenant" 或 "global"，说明是哪一层上限被越过。
    """

    status_code = 429

    def __init__(
        self,
        message: str | None = None,
        *,
        scope: str = "tenant",
        tenant: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(message, **kwargs)
        self.scope = scope
        self.tenant = tenant


class TenantFrozen(QuotaError):
    """租户已被管理员冻结，拒绝新预留。"""

    status_code = 403


class ReservationClosed(QuotaError):
    """预留凭证已终结（已结算、已取消、已超时回收或已冻结回收）。"""

    status_code = 409


class ReservationNotFound(QuotaError):
    """预留凭证不存在或不属于当前调用方。

    为避免跨租户探测，查询/操作一个不属于调用方的凭证与"不存在"返回
    完全相同的错误。
    """

    status_code = 404


__all__ = (
    "QuotaError",
    "QuotaExceeded",
    "TenantFrozen",
    "ReservationClosed",
    "ReservationNotFound",
)
