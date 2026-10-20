import asyncio
import contextlib
import dataclasses
import socket
import threading
from typing import Callable, Dict, Generic, Optional, TypeVar, cast

from typing_extensions import Unpack

from rconx.common.connection import AsyncConnection, Connection, ConnectionConfig
from rconx.common.connection_options import ConnectionOptions
from rconx.common.exceptions import RconConnectionError, RconStateError, RconTimeoutError
from rconx.server.log import SERVER_LOGGER
from rconx.server.raw_options import RawRconServerOptions
from rconx.server.server_connection import AsyncServerConnection, ServerConnection

_ConnectionT = TypeVar('_ConnectionT', Connection, AsyncConnection)
_HAS_START_SERVING = hasattr(asyncio.AbstractServer, 'start_serving')


@dataclasses.dataclass(frozen=True)
class _RconServerConfig:
	host: str
	port: int
	backlog: int
	connection_config: ConnectionConfig

	@classmethod
	def create(cls, host: str, port: int, *, backlog: int = 5, **kwargs: Unpack[ConnectionOptions]) -> '_RconServerConfig':
		return cls(host, port, backlog, ConnectionConfig(**kwargs))

	def __post_init__(self):
		if isinstance(self.backlog, bool) or not isinstance(self.backlog, int):
			raise TypeError('backlog must be an integer')
		if self.backlog < 0:
			raise ValueError('backlog must be non-negative')


def _raise_close_error(error: Optional[BaseException]):
	if error is not None:
		if isinstance(error, (socket.timeout, TimeoutError)):
			raise RconTimeoutError('closing server resources timed out') from error
		if isinstance(error, OSError):
			raise RconConnectionError('failed to close server resources') from error
		raise error


def _close_listener(sock: socket.socket):
	try:
		with contextlib.suppress(OSError):
			sock.shutdown(socket.SHUT_RDWR)
	finally:
		sock.close()


class _BaseRawRconServer(Generic[_ConnectionT]):
	def __init__(self, host: str, port: int, handler: Callable[[_ConnectionT], None], **kwargs: Unpack[RawRconServerOptions]):
		self._config = _RconServerConfig.create(host, port, **kwargs)
		self.__handler_func: Callable[[_ConnectionT], None] = handler
		self._serve_called = False

	def _begin_serve(self):
		if self._serve_called:
			raise RconStateError('serve() may only be called once')
		self._serve_called = True

	def _handoff_connection(self, connection: _ConnectionT):
		try:
			self.__handler_func(connection)
		except Exception:
			SERVER_LOGGER.exception('Connection handler raised an exception')
			with contextlib.suppress(BaseException):
				self._close_connection(connection)
		except BaseException:
			with contextlib.suppress(BaseException):
				self._close_connection(connection)
			raise

	def _close_connection(self, connection: _ConnectionT):
		raise NotImplementedError


class RawRconServer(_BaseRawRconServer[Connection]):
	def __init__(self, host: str, port: int, handler: Callable[[Connection], None], **kwargs: Unpack[RawRconServerOptions]):
		"""
		Configure a TCP listener and connection callback

		:param host: Local host name or IP address
		:param port: Local TCP port
		:param handler: Connection callback executed in the serving thread
		:param kwargs: Options described by ``RawRconServerOptions``
		"""
		super().__init__(host, port, handler, **kwargs)
		self.__stopping = threading.Event()
		self.__sock: Optional[socket.socket] = None
		self.__connections: Dict[int, ServerConnection] = {}
		self.__lock = threading.Lock()

	def serve(self, *, on_started: Optional[Callable[[], None]] = None):
		"""
		Run this instance once, handing off accepted connections until shutdown

		:keyword on_started: Optional no-argument callback invoked after listening starts
		"""
		with self.__lock:
			self._begin_serve()
		try:
			sock = self.__create_listener()
			with self.__lock:
				self.__sock = sock
				started = not self.__stopping.is_set()
				try:
					listening_address = sock.getsockname()
				except (socket.timeout, TimeoutError) as err:
					raise RconTimeoutError('querying listener address timed out') from err
				except OSError as err:
					raise RconConnectionError('failed to query listener address') from err
			if started:
				SERVER_LOGGER.info('TCP listener ready: %s', listening_address)
				if on_started is not None:
					on_started()
				while not self.__stopping.is_set():
					try:
						client, _ = sock.accept()
					except OSError as err:
						if self.__stopping.is_set():
							break
						if isinstance(err, (socket.timeout, TimeoutError)):
							raise RconTimeoutError('accepting a connection timed out') from err
						raise RconConnectionError('failed to accept a connection') from err
					self.__handle_connection(client)
		except BaseException:
			with contextlib.suppress(BaseException):
				self.shutdown()
			raise
		else:
			self.shutdown()

	def shutdown(self):
		"""
		Stop accepting connections and close all tracked connections
		"""
		with self.__lock:
			if not self._serve_called:
				return
			self.__stopping.set()
			sock = self.__sock
			self.__sock = None
			connections = list(self.__connections.values())

		error: Optional[BaseException] = None
		if sock is not None:
			try:
				_close_listener(sock)
			except BaseException as err:
				error = err
		for connection in connections:
			try:
				connection.close()
			except BaseException as err:
				if error is None:
					error = err
		_raise_close_error(error)

	def __create_listener(self) -> socket.socket:
		try:
			family, socktype, proto, _, address = socket.getaddrinfo(
				self._config.host or None, self._config.port,
				type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP, flags=socket.AI_PASSIVE,
			)[0]
			sock = socket.socket(family, socktype, proto)
			try:
				sock.bind(address)
				sock.listen(self._config.backlog)
			except BaseException:
				with contextlib.suppress(BaseException):
					sock.close()
				raise
		except (socket.timeout, TimeoutError) as err:
			raise RconTimeoutError('starting server on {}:{} timed out'.format(self._config.host, self._config.port)) from err
		except OSError as err:
			raise RconConnectionError('failed to start server on {}:{}'.format(self._config.host, self._config.port)) from err
		return sock

	def __handle_connection(self, client: socket.socket):
		try:
			connection = ServerConnection(
				client, on_close=self.__connection_closed,
				max_send_packet_size=self._config.connection_config.max_send_packet_size,
				max_receive_packet_size=self._config.connection_config.max_receive_packet_size,
			)
		except BaseException:
			with contextlib.suppress(BaseException):
				client.close()
			raise
		with self.__lock:
			self.__connections[id(connection)] = connection
			stopping = self.__stopping.is_set()
		if stopping:
			connection.close()
			return
		self._handoff_connection(connection)

	def _close_connection(self, connection: Connection):
		connection.close()

	def __connection_closed(self, connection: Connection):
		with self.__lock:
			self.__connections.pop(id(connection), None)


class AsyncRawRconServer(_BaseRawRconServer[AsyncConnection]):
	def __init__(self, host: str, port: int, handler: Callable[[AsyncConnection], None], **kwargs: Unpack[RawRconServerOptions]):
		"""
		Configure TCP listeners and a connection callback

		:param host: Local host name or IP address
		:param port: Local TCP port
		:param handler: Non-coroutine connection callback executed in the serving event loop
		:param kwargs: Options described by ``RawRconServerOptions``
		"""
		super().__init__(host, port, handler, **kwargs)
		self.__stopping: Optional[asyncio.Event] = None
		self.__server: Optional[asyncio.AbstractServer] = None
		self.__connections: Dict[int, AsyncServerConnection] = {}

	async def serve(self, *, on_started: Optional[Callable[[], None]] = None):
		"""
		Run this instance once, handing off accepted connections until shutdown

		:keyword on_started: Optional no-argument, non-coroutine callback invoked after listening starts
		"""
		self._begin_serve()
		stopping = asyncio.Event()
		self.__stopping = stopping
		try:
			try:
				if _HAS_START_SERVING:
					self.__server = await asyncio.start_server(
						self.__handle_connection, self._config.host, self._config.port,
						backlog=self._config.backlog, start_serving=False,
					)
					if not stopping.is_set():
						await self.__server.start_serving()
				else:
					self.__server = await asyncio.start_server(
						self.__handle_connection, self._config.host, self._config.port,
						backlog=self._config.backlog,
					)
			except (socket.timeout, TimeoutError) as err:
				raise RconTimeoutError('starting server on {}:{} timed out'.format(self._config.host, self._config.port)) from err
			except OSError as err:
				raise RconConnectionError('failed to start server on {}:{}'.format(self._config.host, self._config.port)) from err
			if not stopping.is_set():
				try:
					listening_addresses = [sock.getsockname() for sock in cast(asyncio.base_events.Server, self.__server).sockets]
				except (socket.timeout, TimeoutError) as err:
					raise RconTimeoutError('querying listener addresses timed out') from err
				except OSError as err:
					raise RconConnectionError('failed to query listener addresses') from err
				SERVER_LOGGER.info('Async TCP listener ready: %s', listening_addresses)
				if on_started is not None:
					on_started()
				await stopping.wait()
		except BaseException:
			with contextlib.suppress(BaseException):
				await self.shutdown()
			raise
		else:
			await self.shutdown()

	async def shutdown(self):
		"""
		Stop accepting connections and abort all tracked connections
		"""
		stopping = self.__stopping
		if stopping is None:
			return
		stopping.set()
		server = self.__server
		connections = list(self.__connections.values())

		error: Optional[BaseException] = None
		if server is not None:
			try:
				server.close()
			except BaseException as err:
				error = err
		for connection in connections:
			try:
				connection.abort()
			except BaseException as err:
				if error is None:
					error = err
		_raise_close_error(error)
		if server is not None:
			try:
				await server.wait_closed()
			except (socket.timeout, TimeoutError) as err:
				raise RconTimeoutError('waiting for server closure timed out') from err
			except OSError as err:
				raise RconConnectionError('failed to wait for server closure') from err

	def __handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
		try:
			connection = AsyncServerConnection(
				reader, writer, on_close=self.__connection_closed,
				max_send_packet_size=self._config.connection_config.max_send_packet_size,
				max_receive_packet_size=self._config.connection_config.max_receive_packet_size,
			)
		except BaseException:
			with contextlib.suppress(BaseException):
				writer.transport.abort()
			raise
		self.__connections[id(connection)] = connection
		if cast(asyncio.Event, self.__stopping).is_set():
			connection.abort()
			return
		self._handoff_connection(connection)

	def _close_connection(self, connection: AsyncConnection):
		connection.abort()

	def __connection_closed(self, connection: AsyncConnection):
		self.__connections.pop(id(connection), None)
