"""Entry point: IB Ingestor — subscribes to IB market data, writes to Redis."""
import asyncio
from bifrost_socket.ib.ingestor.ib_ingestor import IbIngestor


def main():
    ingestor = IbIngestor()
    asyncio.run(ingestor.run())


if __name__ == "__main__":
    main()
