"""Redis keys for IB Operator."""

IB_OPERATOR_LOG_STREAM_KEY = "bifrost:console:ws_ib_operator"
IB_OPERATOR_HEALTH_KEY = "bifrost:health:ws_ib_operator"
IB_OPERATOR_CMD_STREAM = "ib:operator:cmd"
IB_OPERATOR_CONSUMER_GROUP = "ib-operator"
IB_OPERATOR_RESULT_PREFIX = "ib:operator:result:"
IB_OPERATOR_RESULT_TTL_SEC = 300
