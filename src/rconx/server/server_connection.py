import asyncio
import contextlib
import socket
import threading
from typing import Callable, Generator, Optional

from typing_extensions import Unpack

from rconx.common.connection import AsyncConnection, Connection
from rconx.common.connection_options import ConnectionOptions


class ServerConnection(Connection):
	def __init__(self, sock: socket.socket, *, on_close: Callable[[Connection], None], **kwargs: Unpack[ConnectionOptions]):
		super().__init__(sock, **kwargs)
		self.__on_close: Optional[Callable[[Connection], None]] = on_close
		self.__close_lock = threading.Lock()

	def close(self):
		callback: Optional[Callable[[Connection], None]] = None
		try:
			with self.__close_lock:
				callback = self.__on_close
				self.__on_close = None
				super().close()
		except BaseException:
			with contextlib.suppress(BaseException):
				if callback is not None:
					callback(self)
			raise
		else:
			if callback is not None:
				callback(self)


class AsyncServerConnection(AsyncConnection):
	def __init__(
		self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *,
		on_close: Callable[[AsyncConnection], None], **kwargs: Unpack[ConnectionOptions],
	):
		super().__init__(reader, writer, **kwargs)
		self.__on_close: Optional[Callable[[AsyncConnection], None]] = on_close
		self.__closing = False

	async def aclose(self):
		if self.__closing:
			return

		self.__closing = True
		try:
			with self.__notify_on_close():
				await super().aclose()
		finally:
			self.__closing = False

	def abort(self):
		with self.__notify_on_close():
			super().abort()

	@contextlib.contextmanager
	def __notify_on_close(self) -> Generator[None, None, None]:
		try:
			yield
		except BaseException:
			with contextlib.suppress(BaseException):
				self.__notify_closed()
			raise
		else:
			self.__notify_closed()

	def __notify_closed(self):
		callback = self.__on_close
		self.__on_close = None
		if callback is not None:
			callback(self)
