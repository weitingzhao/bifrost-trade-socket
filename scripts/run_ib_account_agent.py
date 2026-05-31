"""Entry point: IB Account Agent — subscribes to account updates, writes to Redis."""
import asyncio
from bifrost_socket.ib.account_agent.ib_account_agent import IbAccountAgent


def main():
    agent = IbAccountAgent()
    asyncio.run(agent.run())


if __name__ == "__main__":
    main()
