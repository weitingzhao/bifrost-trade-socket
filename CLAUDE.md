# CLAUDE.md — bifrost-trade-socket

> **RETIRED / reference-only (Wave 14G-F Phase 0–2 DONE)** — 见根目录 `ARCHIVED.md`。
> 权威 IB 总线：**Platform IB Gateway Plugin → `redis-ib`**。
> 默认勿启动本仓进程；逃生舱仅 `--profile legacy-ib`。
> 设计：`bifrost-trade-infra/docs/WAVE_14G_F_SOCKET_RETIREMENT.md`。
> 工作区事实基线见 `../AGENT_FACTS.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## Agent 纪律

- **勿启动** `scripts/run_ib_ingestor.py` / `run_ib_account_agent.py` / `run_ib_operator.py`（除非 Owner 显式要求 `--profile legacy-ib` 排障）。
- Trade 读路径：K3s ExternalName `redis-ib` ← Plugin 写入；与本仓并行写 Redis = **双写事故**（D-14GF.4）。
- D10 仍 BLOCKED — 本仓退役不解锁 live place_order。

## 职责范围（历史参考）

本 repo 曾是 IB WebSocket / RPC 边缘实现。生产路径已由 **Platform IB Gateway Plugin**（`data/redis-ib`）承接；本仓仅作契约考古与短过渡对照。

### IB 子域（`src/bifrost_socket/ib/`）— 参考实现

| 服务 | 入口 | 职责（历史） |
|------|------|----------|
| IB Ingestor | `scripts/run_ib_ingestor.py` | 订阅 watchlist 行情 → `ib:ingester:tick:*` |
| IB Account Agent | `scripts/run_ib_account_agent.py` | 账户/持仓 → `ib:account:*` |
| IB Operator | `scripts/run_ib_operator.py` | `ib:operator:cmd` RPC → 结果键 |

**架构约束**：Daemon 从不直连 IB；Redis 解耦。权威 producer = Plugin Gateway。

### Polygon Options WS（已迁出）

`massive/` 与 `scripts/run_massive_ws.py` **已退役**。Polygon Options WebSocket 由 **Market Data Plugin** 的 `polygon-ws-ingestor` 写入共享 `redis-massive`。

## 依赖

```
bifrost-core  ← pip 安装（共享配置、Redis 协议、健康检查）
ib_insync     ← IB API 包装
aioredis      ← 异步 Redis 客户端
```

## 命令

```bash
pip install -e ".[dev]"

# 仅 Owner 授权的 legacy-ib 排障（默认不要跑）:
# python scripts/run_ib_ingestor.py
# python scripts/run_ib_account_agent.py
# python scripts/run_ib_operator.py

pytest -m 'not ib'
```

## Redis 协议（契约与 Plugin 对齐；由 Plugin 生产）

### IB 子域（权威写方：Platform `redis-ib`）
- 行情：`ib:ingester:tick:{symbol}`
- 账户：`ib:account:{account_id}`
- RPC：`ib:operator:cmd` / `ib:operator:result:{request_id}`
- 健康：`bifrost:health:ws_ib_{service}`

### Polygon Options（Plugin `redis-massive`）
- 行情：`massive:{contract_key}` · Stream · subscriptions · health
