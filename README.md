# bifrost-trade-socket

Bifrost Trade 系统的 WebSocket 边缘服务层（IB 参考实现）。

Polygon Options WS 已迁入 **Market Data Plugin**（`polygon-ws-ingestor` → `redis-massive`）。

## 服务列表

| 脚本 | 服务 | 数据源 |
|------|------|--------|
| `scripts/run_ib_ingestor.py` | IB Ingestor | Interactive Brokers 行情 |
| `scripts/run_ib_account_agent.py` | IB Account Agent | Interactive Brokers 账户/持仓 |
| `scripts/run_ib_operator.py` | IB Operator | Interactive Brokers 订单执行 RPC |
