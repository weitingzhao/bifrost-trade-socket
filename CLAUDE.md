# CLAUDE.md — bifrost-trade-socket

> 本项目是 bifrost-trader-engine 重构的一部分。迁移进度见 `bifrost-trade-infra/docs/MIGRATION_TRACKING.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 职责范围

本 repo 包含所有通过 **WebSocket 长连接**向系统提供数据或执行服务的进程。按数据源分两个子域：

### IB 子域（`src/bifrost_socket/ib/`）

与 Interactive Brokers TWS/Gateway 的全部直接通信：

| 服务 | 入口 | 职责 |
|------|------|------|
| IB Ingestor | `scripts/run_ib_ingestor.py` | 订阅 watchlist 行情，写入 `ib:ingester:tick:*` Redis Stream |
| IB Account Agent | `scripts/run_ib_account_agent.py` | 订阅账户/持仓更新，写入 `ib:account:*` Redis |
| IB Operator | `scripts/run_ib_operator.py` | 监听 `ib:operator:cmd` RPC 队列，执行订单，写回结果 |

**架构约束**：Daemon（bifrost-trade-worker）不直接连接 IB，所有 IB 通信通过本 repo 的服务和 Redis 解耦。

### Massive 子域（`src/bifrost_socket/massive/`）

与 Polygon.io Options WebSocket 的全部直接通信：

| 服务 | 入口 | 职责 |
|------|------|------|
| Massive WS Ingestor | `scripts/run_massive_ws.py` | 订阅 watchlist 期权行情，写入 `massive:*` Redis |

## 依赖

```
bifrost-core  ← pip 安装（共享配置、Redis 协议、健康检查）
ib_insync     ← IB API 包装（仅 IB 子域使用）
websockets    ← Polygon WS 客户端（仅 Massive 子域使用）
aioredis      ← 异步 Redis 客户端
```

## 命令

```bash
pip install -e ".[dev]"

python scripts/run_ib_ingestor.py
python scripts/run_ib_account_agent.py
python scripts/run_ib_operator.py
python scripts/run_massive_ws.py

pytest -m 'not ib'   # 单元测试（不需要 IB 连接）
pytest -m ib         # 需要 TWS/Gateway 运行
```

## IB 连接配置

TWS/Gateway 运行在 Mac Mini 上，Linux 服务器（容器）通过 `IB_HOST` 连接：
- Paper 账户：TWS 7497 / Gateway 4002
- Live 账户：TWS 7496 / Gateway 4001

每个服务使用不同的 `client_id`（配置在 `.env` 中）。

## Redis 协议

### IB 子域
- 行情写入：`ib:ingester:tick:{symbol}` (Redis Stream)
- 账户写入：`ib:account:{account_id}` (Redis Hash)
- RPC 命令：`ib:operator:cmd` (Redis Stream, consumer group)
- RPC 结果：`ib:operator:result:{request_id}` (Redis Key, TTL 300s)
- 健康键：`bifrost:health:ws_ib_{service}` (定期刷新)

### Massive 子域
- 行情写入：`massive:{contract_key}` (Redis Key, JSON)
- 变更通知：`massive:stream` (Redis Stream, XADD)
- 订阅集合：`massive:meta:subscriptions` (Redis Set)
- 健康键：`bifrost:health:ws_massive_option` (定期刷新)
