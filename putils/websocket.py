import asyncio
import socket
import websockets
import logging
import json

from typing import AsyncGenerator
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

class WSClient():
    def __init__(self, url, **kwargs) -> None:
        self.url = url

        self.timeout = kwargs.get('timeout') or 20
        self.sleep = kwargs.get('sleep') or 2

        self.on_message = kwargs.get('on_message')
        self.on_connect = kwargs.get('on_connect')
        self.on_disconnect = kwargs.get('on_disconnect')
        self.on_reconnect = kwargs.get('on_reconnect')
        self.ping = kwargs.get('ping')
    
    async def send(self, message) -> None:
        try:
            if self._connection.open:
                try:
                    await self._connection.send(json.dumps(message))
                except Exception as e:
                    logging.error(f'Failed to send message. Reason: {e}')
            else:
                logging.error('Connection is closed')
        except AttributeError:
            logging.error('No connection')

    async def connect_forever(self) -> AsyncGenerator:
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    self._connection = ws
                    logging.info('Connected to websocket')

                    if self.ping:
                        self._ping_task = asyncio.create_task(self.ping())

                    while True:
                        message = await asyncio.wait_for(ws.recv(), timeout=self.timeout)

                        try:
                            message = json.loads(message)
                            if self.on_message:
                                yield self.on_message(message)
                            else:
                                yield message
                        except json.JSONDecodeError:
                            logging.debug(f'Not valid JSON - Message: {message}')
            except asyncio.TimeoutError:
                logging.error('Timeout Error')
                await self._reconnect()
            except ConnectionRefusedError:
                logging.error('Nobody seems to listen to this endpoint. Please check the URL.')
                await self._reconnect()
            except socket.gaierror:
                logging.error('Socket Error')
                await self._reconnect()
            except ConnectionClosedOK:
                logging.error('Connection was closed by endpoint')
                await self._reconnect()
            except ConnectionClosedError:
                logging.error('Connection was closed because of an error')
                await self._reconnect()
            except asyncio.exceptions.CancelledError:
                logging.info('Disconnected from websocket')
                if self.on_disconnect:
                    self.on_disconnect()
                return
            finally:
                if self.ping:
                    self._ping_task.cancel()
    
    async def _reconnect(self) -> None:
        logging.info(f'Retrying connection in {self.sleep} seconds')
        await asyncio.sleep(self.sleep)
        if self.on_reconnect:
            self.on_reconnect()
