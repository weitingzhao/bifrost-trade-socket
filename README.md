# bifrost-trade-socket

Bifrost Trade 系统的 WebSocket 边缘服务层，负责所有与外部系统的长连接通信。

## 服务列表

| 脚本 | 服务 | 数据源 |
|------|------|--------|
| `scripts/run_ib_ingestor.py` | IB Ingestor | Interactive Brokers 行情 |
| `scripts/run_ib_account_agent.py` | IB Account Agent | Interactive Brokers 账户/持仓 |
| `scripts/run_ib_operator.py` | IB Operator | Interactive Brokers 订单执行 RPC |
| `scripts/run_massive_ws.py` | Massive WS Ingestor | Polygon.io 期权行情 |
