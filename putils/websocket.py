import asyncio
import socket
import websockets
import logging
import json

from typing import Any, AsyncGenerator, Awaitable, Callable, Coroutine
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError


class WSClient:
    def __init__(self, url, **kwargs) -> None:
        self.url = url
        self.timeout = kwargs.get("timeout") or 20
        self.sleep = kwargs.get("sleep") or 2

        self._reconnect = False

    async def send(self, message) -> None:
        try:
            if self._connection.open:
                try:
                    await self._connection.send(json.dumps(message))
                except Exception as e:
                    logging.error(f"Failed to send message. Reason: {e}")
            else:
                logging.error("Connection is closed")
        except AttributeError:
            logging.error("No connection")

    async def connect_forever(
        self,
        on_message: Callable[[Any], Any],
        on_reconnect: Callable[[], Awaitable[None]],
        ping: Callable[[], Coroutine[Any, Any, None]],
    ) -> AsyncGenerator:
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    self._connection = ws
                    logging.info("Connected to websocket")

                    if self._reconnect:
                        self._reconnect = False
                        await on_reconnect()
                    self._ping_task = asyncio.create_task(ping())

                    while True:
                        message = await asyncio.wait_for(
                            ws.recv(), timeout=self.timeout
                        )

                        try:
                            message = json.loads(message)
                            yield on_message(message)
                        except json.JSONDecodeError:
                            logging.debug(f"Not valid JSON - Message: {message}")
            except asyncio.TimeoutError:
                logging.error("Timeout Error")
                self._reconnect = True
            except ConnectionRefusedError:
                logging.error(
                    "Nobody seems to listen to this endpoint. Please check the URL."
                )
                self._reconnect = True
            except socket.gaierror:
                logging.error("Socket Error")
                self._reconnect = True
            except ConnectionClosedOK:
                logging.error("Connection was closed by endpoint")
                self._reconnect = True
            except ConnectionClosedError:
                logging.error("Connection was closed because of an error")
                self._reconnect = True
            except asyncio.exceptions.CancelledError:
                logging.info("Disconnected from websocket")
                return
            finally:
                self._ping_task.cancel()

                if self._reconnect:
                    logging.info(f"Retrying connection in {self.sleep} seconds")
                    await asyncio.sleep(self.sleep)
