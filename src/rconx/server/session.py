import asyncio
import contextlib
from typing import Awaitable, Callable, Generic, Iterable, Iterator, List, TypeVar, Union

from rconx.common.async_utils import await_with_timeout
from rconx.common.connection import AsyncConnection, Connection
from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconProtocolError
from rconx.common.protocol import LENGTH_HEADER, MIN_PACKET_BODY_SIZE, Packet, PacketType
from rconx.common.utils import Deadline
from rconx.server.config import ServerConfig


_END_PAYLOAD = b'\x00\x00\x00\x01\x00\x00\x00\x00'
_ConnectionT = TypeVar('_ConnectionT', Connection, AsyncConnection)
AuthenticationHandler = Callable[[RconContent, ConnectionInfo], bool]
AsyncAuthenticationHandler = Callable[[RconContent, ConnectionInfo], Awaitable[bool]]
CommandHandler = Callable[[RconContent, ConnectionInfo], Union[bytes, str]]
AsyncCommandHandler = Callable[[RconContent, ConnectionInfo], Awaitable[Union[bytes, str]]]


def encode_content(value: Union[bytes, str], encoding: str) -> bytes:
	if isinstance(value, str):
		value = value.encode(encoding)
	elif not isinstance(value, bytes):
		raise TypeError('content must be bytes or str')
	if b'\x00' in value:
		raise ValueError('content must not contain null bytes')
	return value


class _BaseSession(Generic[_ConnectionT]):
	def __init__(self, connection: _ConnectionT, config: ServerConfig):
		self._authentication_deadline = Deadline(config.authentication_timeout)
		self._connection: _ConnectionT = connection
		self._config = config
		self._info = connection.get_info()
		self._authenticated = False

	def _validate_request(self, packet: Packet):
		if packet.request_id <= 0:
			raise RconProtocolError('request id must be positive')
		if not self._authenticated and packet.packet_type != PacketType.auth:
			raise RconProtocolError('authentication is required before other requests')
		if packet.packet_type in (PacketType.auth, PacketType.exec_command) and b'\x00' in packet.payload:
			raise RconProtocolError('request content contains null bytes')

	def _wrap_content(self, packet: Packet) -> RconContent:
		return RconContent(packet.payload, default_encoding=self._config.default_encoding)

	def _create_authentication_responses(self, request_id: int, accepted: bool) -> List[Packet]:
		if not isinstance(accepted, bool):
			raise TypeError('authentication callback must return bool')
		return [Packet(request_id, PacketType.response_value, b''), Packet(request_id if accepted else -1, PacketType.auth_response, b'')]

	def _generate_command_responses(self, request_id: int, result: Union[bytes, str]) -> Iterator[Packet]:
		payload = encode_content(result, self._config.default_encoding)
		maximum = self._config.max_response_size
		if maximum is not None and len(payload) > maximum:
			raise ValueError('command result exceeds max_response_size')
		packet_size = 4100
		if self._config.max_send_packet_size is not None:
			packet_size = min(packet_size, self._config.max_send_packet_size)
		chunk_size = max(1, packet_size - LENGTH_HEADER.size - MIN_PACKET_BODY_SIZE)
		if not payload:
			return iter([Packet(request_id, PacketType.response_value, b'')])
		return (Packet(request_id, PacketType.response_value, payload[offset:offset + chunk_size]) for offset in range(0, len(payload), chunk_size))

	@staticmethod
	def _create_probe_responses(request_id: int) -> List[Packet]:
		return [Packet(request_id, PacketType.response_value, b''), Packet(request_id, PacketType.response_value, _END_PAYLOAD)]


class Session(_BaseSession[Connection]):
	def __init__(self, connection: Connection, config: ServerConfig, authentication: AuthenticationHandler, command_handler: CommandHandler):
		super().__init__(connection, config)
		self.__authentication = authentication
		self.__command_handler = command_handler

	def run(self):
		try:
			self.__exchange()
		except asyncio.CancelledError:
			raise
		except Exception:
			pass
		finally:
			with contextlib.suppress(Exception):
				self._connection.close()

	def __exchange(self):
		while not self._connection.closed:
			authenticated = self._authenticated
			packet = self._connection.receive_packet(
				timeout=None if authenticated else self._authentication_deadline.get_remaining_or_raise(),
				first_byte_timeout=self._config.idle_timeout if authenticated else None,
				after_first_byte_timeout=self._config.receive_timeout,
			)
			self._validate_request(packet)
			packets: Iterable[Packet]
			if packet.packet_type == PacketType.auth:
				accepted = self.__authentication(self._wrap_content(packet), self._info)
				packets = self._create_authentication_responses(packet.request_id, accepted)
				self._authenticated = accepted
			elif packet.packet_type == PacketType.exec_command:
				result = self.__command_handler(self._wrap_content(packet), self._info)
				packets = self._generate_command_responses(packet.request_id, result)
			elif packet.packet_type == PacketType.response_value:
				packets = self._create_probe_responses(packet.request_id)
			else:
				continue
			for response in packets:
				self.__send(response)
			if not self._authenticated:
				return

	def __send(self, packet: Packet):
		self._connection.send_packet(packet, timeout=self._config.send_timeout)


class AsyncSession(_BaseSession[AsyncConnection]):
	def __init__(self, connection: AsyncConnection, config: ServerConfig, authentication: AsyncAuthenticationHandler, command_handler: AsyncCommandHandler):
		super().__init__(connection, config)
		self.__authentication = authentication
		self.__command_handler = command_handler

	async def run(self):
		try:
			await self.__exchange()
		except asyncio.CancelledError:
			raise
		except Exception:
			pass
		finally:
			with contextlib.suppress(Exception):
				self._connection.abort()

	async def __exchange(self):
		while not self._connection.closed:
			authenticated = self._authenticated
			remaining = None if authenticated else self._authentication_deadline.get_remaining_or_raise()
			packet = await self._connection.receive_packet(
				timeout=remaining,
				first_byte_timeout=self._config.idle_timeout if authenticated else None,
				after_first_byte_timeout=self._config.receive_timeout,
			)
			self._validate_request(packet)
			packets: Iterable[Packet]
			if packet.packet_type == PacketType.auth:
				accepted = await self.__authentication(self._wrap_content(packet), self._info)
				packets = self._create_authentication_responses(packet.request_id, accepted)
				self._authenticated = accepted
			elif packet.packet_type == PacketType.exec_command:
				result = await self.__command_handler(self._wrap_content(packet), self._info)
				packets = self._generate_command_responses(packet.request_id, result)
			elif packet.packet_type == PacketType.response_value:
				packets = self._create_probe_responses(packet.request_id)
			else:
				continue
			for response in packets:
				await self.__send(response)
			if not self._authenticated:
				await self._connection.aclose()
				return

	async def __send(self, packet: Packet):
		await await_with_timeout(self._connection.send_packet(packet), self._config.send_timeout)
