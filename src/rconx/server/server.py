import asyncio
import contextlib
import hmac
import threading
from typing import Callable, Optional, Set, Union

from typing_extensions import Unpack

from rconx.common.connection import AsyncConnection, Connection
from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconStateError
from rconx.common.thread_pool import ThreadPool
from rconx.server.config import ServerConfig
from rconx.server.log import SERVER_LOGGER
from rconx.server.raw import AsyncRawRconServer, RawRconServer
from rconx.server.server_options import RconServerOptions
from rconx.server.session import AsyncAuthenticationHandler, AsyncCommandHandler, AsyncSession, AuthenticationHandler, CommandHandler, Session, encode_content


_Authentication = Union[str, bytes, AuthenticationHandler]
_AsyncAuthentication = Union[str, bytes, AsyncAuthenticationHandler]
_WORKER_IDLE_TIMEOUT = 60.0


def _create_fixed_authentication_handler(password: Union[str, bytes], encoding: str) -> AuthenticationHandler:
	expected = encode_content(password, encoding)
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		return hmac.compare_digest(content.content, expected)
	return authenticate


class RconServer:
	"""
	Synchronous RCON server following SRCDS authentication and response conventions
	"""

	def __init__(
		self, host: str, port: int, *,
		authentication: _Authentication,
		command_handler: CommandHandler,
		**kwargs: Unpack[RconServerOptions],
	):
		"""
		Configure the server without opening a listener

		Callbacks receive ``(RconContent, ConnectionInfo)`` and run serially per connection.
		Different connections may invoke them concurrently on different threads. Callback exceptions close the connection.

		:param host: Local host name or IP address
		:param port: Local TCP port
		:keyword authentication: Fixed ``str``/``bytes`` password, or callback returning ``bool``
		:keyword command_handler: Callback returning ``bytes`` or ``str``; encoded output must not contain NUL
		:param kwargs: Options described by ``RconServerOptions``
		"""
		config = ServerConfig(**kwargs)
		if isinstance(authentication, (str, bytes)):
			authentication = _create_fixed_authentication_handler(authentication, config.default_encoding)
		if not callable(authentication) or not callable(command_handler):
			raise TypeError('authentication must be str, bytes or callable, and command_handler must be callable')
		self.__config = config
		self.__authentication = authentication
		self.__command_handler = command_handler
		self.__pool = ThreadPool(max_workers=config.max_connections, worker_idle_timeout=_WORKER_IDLE_TIMEOUT)
		self.__raw = RawRconServer(
			host, port, self.__dispatch_connection,
			backlog=config.backlog, max_send_packet_size=config.max_send_packet_size, max_receive_packet_size=config.max_receive_packet_size,
		)
		self.__lock = threading.Lock()
		self.__serve_called = False
		self.__stopping = False

	def serve(self, *, on_started: Optional[Callable[[], None]] = None):
		"""
		Run this instance once, waiting for active callbacks to finish before exiting

		:keyword on_started: No-argument callback invoked in the serving thread after listening starts
		:raises RconStateError: If ``serve`` was already called, including a failed start
		"""
		with self.__lock:
			if self.__serve_called:
				raise RconStateError('serve() may only be called once')
			self.__serve_called = True

		def notify_started():
			with self.__lock:
				stopping = self.__stopping
			if stopping:
				self.__raw.shutdown()
				return
			if on_started is not None:
				on_started()

		try:
			self.__raw.serve(on_started=notify_started)
		finally:
			with self.__lock:
				self.__stopping = True
			self.__pool.shutdown(wait=True)
		SERVER_LOGGER.info('RCON server %r stopped', self)

	def shutdown(self):
		"""
		Stop accepting connections and close existing connections without waiting for callbacks

		May be called from another thread or a callback. Repeated calls are allowed;
		calls before ``serve`` have no effect. Wait for ``serve`` to exit for complete shutdown.
		"""
		with self.__lock:
			if not self.__serve_called:
				return
			self.__stopping = True
			self.__pool.shutdown(wait=False)
		self.__raw.shutdown()

	def __dispatch_connection(self, connection: Connection):
		with self.__lock:
			future = None
			if not self.__stopping:
				session = Session(connection, self.__config, self.__authentication, self.__command_handler)
				future = self.__pool.try_submit(session.run)
		if future is None:
			connection.close()


class AsyncRconServer:
	"""
	Asynchronous RCON server following SRCDS authentication and response conventions
	"""

	def __init__(
		self, host: str, port: int, *,
		authentication: _AsyncAuthentication,
		command_handler: AsyncCommandHandler,
		**kwargs: Unpack[RconServerOptions],
	):
		"""
		Configure the server without opening a listener

		Callbacks receive ``(RconContent, ConnectionInfo)`` and run serially per connection.
		Different connections may invoke them concurrently. Callback exceptions close the connection.

		:param host: Local host name or IP address
		:param port: Local TCP port
		:keyword authentication: Fixed ``str``/``bytes`` password, or async callback returning ``bool``
		:keyword command_handler: Async callback returning ``bytes`` or ``str``; encoded output must not contain NUL
		:param kwargs: Options described by ``RconServerOptions``
		"""
		config = ServerConfig(**kwargs)
		if isinstance(authentication, (str, bytes)):
			fixed = _create_fixed_authentication_handler(authentication, config.default_encoding)
			async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
				return fixed(content, info)
			authentication = authenticate
		if not callable(authentication) or not callable(command_handler):
			raise TypeError('authentication must be str, bytes or callable, and command_handler must be callable')
		self.__config = config
		self.__authentication = authentication
		self.__command_handler = command_handler
		self.__raw = AsyncRawRconServer(
			host, port, self.__dispatch_connection,
			backlog=config.backlog, max_send_packet_size=config.max_send_packet_size, max_receive_packet_size=config.max_receive_packet_size,
		)
		self.__tasks: Set[asyncio.Future[None]] = set()
		self.__serve_called = False
		self.__stopping = False

	async def serve(self, *, on_started: Optional[Callable[[], None]] = None):
		"""
		Run this instance once, waiting for active callbacks to finish before exiting

		Use ``serve`` and ``shutdown`` in the same event loop. Caller cancellation propagates after cleanup.

		:keyword on_started: No-argument, non-coroutine callback invoked in the serving event loop after listening starts
		:raises RconStateError: If ``serve`` was already called, including a failed start
		"""
		if self.__serve_called:
			raise RconStateError('serve() may only be called once')
		self.__serve_called = True

		try:
			await self.__raw.serve(on_started=on_started)
		except BaseException:
			with contextlib.suppress(asyncio.CancelledError):
				await self.__wait_for_sessions()
			raise
		else:
			await self.__wait_for_sessions()
		SERVER_LOGGER.info('Async RCON server %r stopped', self)

	async def shutdown(self):
		"""
		Stop accepting connections, close existing connections and request callback cancellation

		Does not wait for callbacks; await ``serve`` to exit for complete shutdown.
		May be called from a callback. Repeated calls are allowed; calls before ``serve`` have no effect.
		"""
		if not self.__serve_called:
			return
		self.__stop_sessions()
		await self.__raw.shutdown()

	def __stop_sessions(self):
		if not self.__stopping:
			self.__stopping = True
			current_task = getattr(asyncio, 'current_task', None)
			current = current_task() if current_task is not None else getattr(asyncio.Task, 'current_task')()
			for task in self.__tasks:
				if task is not current:
					task.cancel()

	async def __wait_for_sessions(self):
		self.__stop_sessions()
		if self.__tasks:
			waiting = asyncio.gather(*self.__tasks, return_exceptions=True)
			try:
				await asyncio.shield(waiting)
			except asyncio.CancelledError:
				while not waiting.done():
					try:
						await asyncio.shield(waiting)
					except asyncio.CancelledError:
						pass
				raise

	def __dispatch_connection(self, connection: AsyncConnection):
		maximum = self.__config.max_connections
		if self.__stopping or (maximum is not None and len(self.__tasks) >= maximum):
			connection.abort()
			return
		session = AsyncSession(connection, self.__config, self.__authentication, self.__command_handler)
		task = asyncio.ensure_future(session.run())
		self.__tasks.add(task)
		task.add_done_callback(self.__tasks.discard)
