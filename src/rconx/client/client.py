import codecs
import contextlib
import dataclasses
import functools
import inspect
import threading
from types import TracebackType
from typing import Any, Awaitable, Callable, Generator, Optional, Type, TypeVar, Union, cast

from typing_extensions import NotRequired, Unpack

from rconx.client.auth.base import AuthenticationHandler
from rconx.client.client_options import RconClientOptions
from rconx.client.command.base import CommandHandler
from rconx.client.operation import AsyncRconOperationContext, RequestIdCounter, RconOperationContext
from rconx.client.preset import RconPreset, RconPresetLiteral
from rconx.client.raw import AsyncRawRconClient, LocalAddress, RawRconClient
from rconx.common.connection import ConnectionConfig
from rconx.common.connection_options import ConnectionOptions
from rconx.common.content import RconContent
from rconx.common.exceptions import RconSizeLimitError, RconStateError
from rconx.common.protocol import LENGTH_HEADER, MIN_PACKET_BODY_SIZE
from rconx.common.utils import Deadline, validate_packet_size_limit

DEFAULT_MAX_RESPONSE_SIZE = 4 * 1024 * 1024
_DEFAULT_ENCODING = 'utf8'
_INT32_MAX = (1 << 31) - 1
_ClientMethod = TypeVar('_ClientMethod', bound=Callable[..., Any])


class _RconBaseClientOptions(ConnectionOptions):
	default_encoding: NotRequired[str]
	"""Text encoding for string input and response text; defaults to ``utf8``"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum accumulated response payload size in bytes, with a default of 4 MiB. ``None`` disables the limit"""


class _RconClientInitOptions(_RconBaseClientOptions):
	authentication_handler: AuthenticationHandler
	command_handler: CommandHandler


@dataclasses.dataclass(frozen=True)
class _RconHandlers:
	authentication_handler: AuthenticationHandler
	command_handler: CommandHandler


@dataclasses.dataclass(frozen=True)
class _RconClientConfig(ConnectionConfig):
	default_encoding: str = _DEFAULT_ENCODING
	max_response_size: Optional[int] = DEFAULT_MAX_RESPONSE_SIZE

	def __post_init__(self):
		super().__post_init__()

		try:
			codecs.lookup(self.default_encoding)
			''.encode(self.default_encoding)
		except LookupError as err:
			raise ValueError('default_encoding must name a text encoding, got {!r}'.format(self.default_encoding)) from err

		validate_packet_size_limit(self.max_response_size, 'max_response_size')


class _BaseRconClient:
	def __init__(self, *, authentication_handler: AuthenticationHandler, command_handler: CommandHandler, **kwargs: Unpack[_RconBaseClientOptions]):
		self._config = _RconClientConfig(**kwargs)

		if not isinstance(authentication_handler, AuthenticationHandler):
			raise TypeError('authentication_handler must be an AuthenticationHandler, got {}'.format(type(authentication_handler).__name__))
		if not isinstance(command_handler, CommandHandler):
			raise TypeError('command_handler must be a CommandHandler, got {}'.format(type(command_handler).__name__))

		self._authentication_handler = authentication_handler
		self._command_handler = command_handler
		self.__connected = False
		self.__authenticated = False
		self._request_id_counter = RequestIdCounter()
		self.__operation_lock = threading.Lock()

	@staticmethod
	def _resolve_preset(preset: Union[RconPreset, RconPresetLiteral]) -> RconPreset:
		if isinstance(preset, RconPreset):
			return preset
		if isinstance(preset, str):
			try:
				return RconPreset[preset]
			except KeyError:
				raise ValueError('unknown RCON preset {!r}'.format(preset)) from None
		raise TypeError('preset must be a RconPreset or str, got {}'.format(type(preset).__name__))

	@classmethod
	def _create_handlers(cls, preset: RconPreset) -> _RconHandlers:
		command_handler: CommandHandler
		if preset == RconPreset.srcds:
			from rconx.client.command.srcds_handler import SrcdsCommandHandler
			command_handler = SrcdsCommandHandler()
		elif preset == RconPreset.minecraft:
			from rconx.client.command.minecraft_handler import MinecraftCommandHandler
			command_handler = MinecraftCommandHandler()
		elif preset == RconPreset.cuberite:
			from rconx.client.command.cuberite_handler import CuberiteCommandHandler
			command_handler = CuberiteCommandHandler()
		elif preset == RconPreset.single_packet:
			from rconx.client.command.single_packet_handler import SinglePacketCommandHandler
			command_handler = SinglePacketCommandHandler()
		else:
			raise ValueError('unsupported RCON preset {!r}'.format(preset))

		from rconx.client.auth.source_handler import SourceAuthenticationHandler
		return _RconHandlers(SourceAuthenticationHandler(), command_handler)

	@staticmethod
	def _apply_preset_defaults(preset: RconPreset, options: _RconBaseClientOptions):
		if preset == RconPreset.minecraft:
			# Minecraft reads each complete request into a 1460-byte buffer.
			options.setdefault('max_send_packet_size', 1460)
		elif preset == RconPreset.cuberite:
			# Cuberite allows a 1500-byte packet body plus the 4-byte length header.
			options.setdefault('max_send_packet_size', 1504)

	@classmethod
	def _prepare_init_options(cls, options: RconClientOptions) -> _RconClientInitOptions:
		resolved_preset = cls._resolve_preset(options['preset'])
		handlers = cls._create_handlers(resolved_preset)
		config_options = dict(options)
		config_options.pop('preset')
		client_options = cast(_RconBaseClientOptions, config_options)
		cls._apply_preset_defaults(resolved_preset, client_options)

		return _RconClientInitOptions(
			authentication_handler=handlers.authentication_handler,
			command_handler=handlers.command_handler,
			**client_options,
		)

	@property
	def connected(self) -> bool:
		"""
		Whether the client holds a connection
		"""
		return self.__connected

	@property
	def authenticated(self) -> bool:
		"""
		Whether authentication has succeeded on the current connection
		"""
		return self.__authenticated

	@contextlib.contextmanager
	def _guard_operation(self, operation: str) -> Generator[None, None, None]:
		if not self.__operation_lock.acquire(blocking=False):
			raise RconStateError('cannot call {} while another client operation is in progress'.format(operation))

		try:
			yield
		finally:
			self.__operation_lock.release()

	def _prepare_payload(self, body: Union[str, bytes]) -> bytes:
		if isinstance(body, str):
			payload = body.encode(self._config.default_encoding)
		elif isinstance(body, bytes):
			payload = body
		else:
			raise TypeError('request body must be str or bytes, got {}'.format(type(body).__name__))
		if b'\x00' in payload:
			raise ValueError('request body contains a null byte at byte offset {}'.format(payload.index(b'\x00')))

		packet_body_size = MIN_PACKET_BODY_SIZE + len(payload)
		if packet_body_size > _INT32_MAX:
			raise ValueError('packet body size {} bytes exceeds the protocol size field maximum {} bytes'.format(packet_body_size, _INT32_MAX))
		packet_size = LENGTH_HEADER.size + packet_body_size
		maximum_size = self._config.max_send_packet_size
		if maximum_size is not None and packet_size > maximum_size:
			raise RconSizeLimitError('packet size {} exceeds max_send_packet_size {}'.format(packet_size, maximum_size))

		return payload

	def _ensure_disconnected(self):
		if self.__connected:
			raise RconStateError('client is already connected')

	def _ensure_authenticatable(self):
		if not self.__connected:
			raise RconStateError('client is not connected')
		if self.__authenticated:
			raise RconStateError('client is already authenticated')

	def _ensure_authenticated(self):
		if not self.__authenticated:
			raise RconStateError('client is not authenticated')

	def _mark_connected(self):
		self.__connected = True
		self.__authenticated = False
		self._request_id_counter = RequestIdCounter()

	def _mark_authenticated(self):
		self.__authenticated = True

	def _mark_disconnected(self):
		self.__connected = False
		self.__authenticated = False


def _exclusive_operation(method: _ClientMethod) -> _ClientMethod:
	if inspect.iscoroutinefunction(method):
		@functools.wraps(method)
		async def async_wrapper(self: _BaseRconClient, *args: Any, **kwargs: Any) -> Any:
			with self._guard_operation(method.__name__):
				return await method(self, *args, **kwargs)

		return cast(_ClientMethod, async_wrapper)

	@functools.wraps(method)
	def sync_wrapper(self: _BaseRconClient, *args: Any, **kwargs: Any) -> Any:
		with self._guard_operation(method.__name__):
			return method(self, *args, **kwargs)

	return cast(_ClientMethod, sync_wrapper)


class RconClient(_BaseRconClient):
	"""
	Synchronous single-session RCON client, created with ``create``

	Overlapping operations on one instance raise ``RconStateError``.
	"""
	def __init__(self, host: str, port: int, **kwargs: Unpack[_RconClientInitOptions]):
		"""
		**Not public API**
		"""
		super().__init__(**kwargs)

		self.__raw = RawRconClient(
			host, port,
			max_send_packet_size=self._config.max_send_packet_size,
			max_receive_packet_size=self._config.max_receive_packet_size,
		)

	@classmethod
	def create(
		cls, host: str, port: int,
		**kwargs: Unpack[RconClientOptions],
	) -> 'RconClient':
		"""
		Create a configured client without connecting

		:param host: The remote host name or IP address
		:param port: The remote TCP port
		:param kwargs: Options described by ``RconClientOptions``
		:return: A disconnected client
		"""
		return cls(host, port, **cls._prepare_init_options(kwargs))

	@_exclusive_operation
	def connect(self, *, timeout: Optional[float] = None, local_address: Optional[LocalAddress] = None):
		"""
		Establish a TCP connection to the configured target

		:keyword timeout: Timeout per connection attempt in seconds. ``None`` disables the timeout
		:keyword local_address: The local address to bind, or ``None`` for automatic selection
		"""
		self._ensure_disconnected()
		self.__raw.connect(timeout=timeout, local_address=local_address)
		self._mark_connected()

	@_exclusive_operation
	def authenticate(self, password: Union[str, bytes], *, timeout: Optional[float] = None):
		"""
		Authenticate the connected session

		:param password: Password bytes or text encoded with the client's configured encoding; null bytes are not allowed
		:keyword timeout: Total authentication budget in seconds. ``None`` disables the timeout
		"""
		deadline = Deadline(timeout)
		self._ensure_authenticatable()
		payload = self._prepare_payload(password)

		with self.__operation(deadline) as context:
			self._authentication_handler.authenticate(context, payload)
		self._mark_authenticated()

	def connect_and_authenticate(self, password: Union[str, bytes], *, timeout: Optional[float] = None, local_address: Optional[LocalAddress] = None):
		"""
		Establish an authenticated connection to the configured target

		:param password: Password bytes or text encoded with the client's configured encoding; null bytes are not allowed
		:keyword timeout: Shared connection and authentication budget in seconds. ``None`` disables the timeout
		:keyword local_address: The local address to bind, or ``None`` for automatic selection
		"""
		deadline = Deadline(timeout)
		self.connect(timeout=deadline.get_remaining_or_raise(), local_address=local_address)
		self.authenticate(password, timeout=deadline.get_remaining_or_raise())

	@_exclusive_operation
	def execute(self, command: Union[str, bytes], *, timeout: Optional[float] = None) -> RconContent:
		"""
		Execute a command on the authenticated session and collect its response using the selected preset

		:param command: Command bytes or text encoded with the client's configured encoding; null bytes are not allowed
		:keyword timeout: Total command exchange budget in seconds. ``None`` disables the timeout
		:return: Collected response payload with access to its decoded text
		"""
		deadline = Deadline(timeout)
		self._ensure_authenticated()
		payload = self._prepare_payload(command)

		with self.__operation(deadline) as context:
			self._command_handler.execute(context, payload)
			content = context.consume_response()

		return RconContent(content, default_encoding=self._config.default_encoding)

	@_exclusive_operation
	def close(self):
		"""
		Close the current connection and clear authentication state. Repeated calls are allowed
		"""
		self.__close()

	def __close(self):
		self._mark_disconnected()
		self.__raw.close()

	def __enter__(self) -> 'RconClient':
		"""
		Return this client without connecting or authenticating
		"""
		return self

	def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]):
		"""
		Close the current connection when leaving the context
		"""
		if exc_type is None:
			self.close()
		else:
			with contextlib.suppress(BaseException):
				self.close()

	@contextlib.contextmanager
	def __operation(self, deadline: Deadline) -> Generator[RconOperationContext, None, None]:
		# Reject an expired budget before the handler's failure-close scope begins.
		deadline.get_remaining_or_raise()
		context = RconOperationContext(self.__raw, self._request_id_counter, deadline, max_response_size=self._config.max_response_size)
		try:
			yield context
		except BaseException:
			with contextlib.suppress(BaseException):
				self.__close()
			raise


class AsyncRconClient(_BaseRconClient):
	"""
	Asynchronous single-session RCON client, created with ``create``

	Timeouts and cancellation are controlled by the caller through asyncio.
	Overlapping operations on one instance raise ``RconStateError``.
	"""
	def __init__(self, host: str, port: int, **kwargs: Unpack[_RconClientInitOptions]):
		"""
		**Not public API**
		"""
		super().__init__(**kwargs)

		self.__raw = AsyncRawRconClient(
			host, port,
			max_send_packet_size=self._config.max_send_packet_size,
			max_receive_packet_size=self._config.max_receive_packet_size,
		)

	@classmethod
	def create(
		cls, host: str, port: int,
		**kwargs: Unpack[RconClientOptions],
	) -> 'AsyncRconClient':
		"""
		Create a configured client without connecting

		:param host: The remote host name or IP address
		:param port: The remote TCP port
		:param kwargs: Options described by ``RconClientOptions``
		:return: A disconnected client
		"""
		return cls(host, port, **cls._prepare_init_options(kwargs))

	@_exclusive_operation
	async def connect(self, *, local_address: Optional[LocalAddress] = None):
		"""
		Establish a TCP connection to the configured target

		:keyword local_address: The local address to bind, or ``None`` for automatic selection
		"""
		self._ensure_disconnected()
		await self.__raw.connect(local_address=local_address)
		self._mark_connected()

	@_exclusive_operation
	async def authenticate(self, password: Union[str, bytes]):
		"""
		Authenticate the connected session

		:param password: Password bytes or text encoded with the client's configured encoding; null bytes are not allowed
		"""
		self._ensure_authenticatable()
		payload = self._prepare_payload(password)

		async with self.__operation() as context:
			await self._authentication_handler.authenticate_async(context, payload)
		self._mark_authenticated()

	async def connect_and_authenticate(self, password: Union[str, bytes], *, local_address: Optional[LocalAddress] = None):
		"""
		Establish an authenticated connection to the configured target

		:param password: Password bytes or text encoded with the client's configured encoding; null bytes are not allowed
		:keyword local_address: The local address to bind, or ``None`` for automatic selection
		"""
		await self.connect(local_address=local_address)
		await self.authenticate(password)

	@_exclusive_operation
	async def execute(self, command: Union[str, bytes]) -> RconContent:
		"""
		Execute a command on the authenticated session and collect its response using the selected preset

		:param command: Command bytes or text encoded with the client's configured encoding; null bytes are not allowed
		:return: Collected response payload with access to its decoded text
		"""
		self._ensure_authenticated()
		payload = self._prepare_payload(command)

		async with self.__operation() as context:
			await self._command_handler.execute_async(context, payload)
			content = context.consume_response()

		return RconContent(content, default_encoding=self._config.default_encoding)

	def __operation(self) -> '_AsyncOperation':
		context = AsyncRconOperationContext(self.__raw, self._request_id_counter, max_response_size=self._config.max_response_size)
		return _AsyncOperation(context, self.__aclose)

	@_exclusive_operation
	async def aclose(self):
		"""
		Close the current connection and clear authentication state. Repeated calls are allowed
		"""
		await self.__aclose()

	async def __aclose(self):
		self._mark_disconnected()
		await self.__raw.aclose()

	async def __aenter__(self) -> 'AsyncRconClient':
		"""
		Return this client without connecting or authenticating
		"""
		return self

	async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]):
		"""
		Close the current connection when leaving the asynchronous context
		"""
		if exc_type is None:
			await self.aclose()
		else:
			with contextlib.suppress(BaseException):
				await self.aclose()


# Use a class for Python 3.6 compatibility since contextlib.asynccontextmanager requires Python 3.7+.
class _AsyncOperation:
	def __init__(self, context: AsyncRconOperationContext, close: Callable[[], Awaitable[None]]):
		self.__context = context
		self.__close = close

	async def __aenter__(self) -> AsyncRconOperationContext:
		return self.__context

	async def __aexit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]):
		if exc_type is not None:
			with contextlib.suppress(BaseException):
				await self.__close()
