"""Entry point: IB Operator — RPC service for order execution via Redis Streams."""
import asyncio
from bifrost_socket.ib.operator.ib_operator import IbOperator


def main():
    operator = IbOperator()
    asyncio.run(operator.run())


if __name__ == "__main__":
    main()
