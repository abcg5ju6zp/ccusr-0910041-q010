# Sanic 服务框架

本项目提供异步 HTTP 服务、路由、蓝图、中间件、信号和工作进程管理能力。生产源码位于 `sanic/`，核心回归测试位于 `tests/`。

## 安装

`python3 -m pip install -e . pytest sanic-testing pytest-asyncio`

## 测试

`python3 -m pytest -q tests/test_blueprints.py tests/test_blueprint_group.py tests/test_quota.py`

## 构建

`python3 -m compileall -q sanic`

## 使用

应用通过 `Sanic` 创建服务，通过蓝图组合路由，并可使用测试客户端完成本地 HTTP 验收。

### 请求级配额预留

`sanic/quota/` 在请求进入时按估算额度原子预留租户/全局预算，凭证经
`request.ctx.quota_reservation` 在中间件、处理器与响应生命周期之间传递，
响应结束时按实际用量结算；异常与断连归还，超时由 reaper 回收，管理员可
冻结租户。

```python
from sanic import Sanic
from sanic.response import json
from sanic.quota import attach_quota

app = Sanic("inference")
attach_quota(
    app,
    global_limit=1000,
    default_tenant_limit=100,
    ttl=60,
    admin_key="secret",
)

@app.post("/infer")
async def infer(request):
    reservation = request.ctx.quota_reservation  # 中间件已预留
    result = await run_model(request.json)
    # 用响应头声明实际用量，响应生命周期自动结算
    resp = json(result)
    resp.headers["X-Quota-Actual"] = str(result["tokens"])
    return resp
```

- 请求头：`X-Tenant-ID` 标识租户，`X-Quota-Estimate` 声明估算用量；
- 越限返回 429（`QuotaExceeded.scope` 区分 `tenant`/`global`），冻结返回 403；
- `GET /quota/balance` 查询本租户余额及可解释的变化流水；
- 管理接口（需 `X-Admin-Key`）：`/quota/admin/...` 冻结/解冻、设置上限、查全局余额；
- 跨租户查询凭证一律返回与“不存在”相同的 404，不泄漏其他租户。
