"""Entry point: Massive WS Ingestor — subscribes to Polygon options WebSocket, writes to Redis."""
import asyncio
from bifrost_socket.massive.massive_ws_ingestor import MassiveWsIngestor


def main():
    ingestor = MassiveWsIngestor()
    asyncio.run(ingestor.run())


if __name__ == "__main__":
    main()
