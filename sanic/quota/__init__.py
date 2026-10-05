"""请求级配额预留子系统。

在请求进入时从租户与全局预算中**预留**估算额度，处理过程中全程持有
预留凭证，响应结束时按实际用量结算；异常时归还预留，客户端断开后由
超时回收兜底，管理员可随时冻结租户。
"""

from sanic.quota.exceptions import (
    QuotaExceeded,
    ReservationClosed,
    ReservationNotFound,
    TenantFrozen,
)
from sanic.quota.integration import (
    QuotaBlueprint,
    attach_quota,
    cancel_request,
    get_reservation,
    settle_request,
)
from sanic.quota.ledger import Ledger, LedgerEntry, Reservation
from sanic.quota.service import QuotaManager, QuotaService


__all__ = (
    "Ledger",
    "LedgerEntry",
    "QuotaBlueprint",
    "QuotaExceeded",
    "QuotaManager",
    "QuotaService",
    "Reservation",
    "ReservationClosed",
    "ReservationNotFound",
    "TenantFrozen",
    "attach_quota",
    "cancel_request",
    "get_reservation",
    "settle_request",
)
