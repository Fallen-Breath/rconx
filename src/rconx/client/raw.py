import asyncio
import dataclasses
import socket
from typing import Optional, cast

from typing_extensions import Unpack

from rconx.common.connection import AsyncConnection, Connection, ConnectionConfig
from rconx.common.connection_options import ConnectionOptions
from rconx.common.exceptions import RconConnectionError, RconTimeoutError
from rconx.common.protocol import Packet
from rconx.common.utils import validate_timeout


@dataclasses.dataclass(frozen=True)
class LocalAddress:
	host: str
	port: int = 0


@dataclasses.dataclass(frozen=True)
class _RawRconClientConfig:
	host: str
	port: int
	connection_config: ConnectionConfig


class RawRconClient:
	def __init__(self, host: str, port: int, **kwargs: Unpack[ConnectionOptions]):
		"""
		Configure the target address and packet size limits

		:param host: The remote host name or IP address
		:param port: The remote TCP port
		:keyword max_send_packet_size: Maximum sent packet size in bytes, including the length header. ``None`` disables the limit
		:keyword max_receive_packet_size: Maximum received packet size in bytes, including the length header. ``None`` disables the limit; defaults to 4 MiB
		"""
		self.__config = _RawRconClientConfig(host, port, ConnectionConfig(**kwargs))
		self.__connection: Optional[Connection] = None

	def connect(self, *, timeout: Optional[float] = None, local_address: Optional[LocalAddress] = None):
		"""
		Establish a TCP connection to the configured target

		:keyword timeout: Timeout per connection attempt in seconds. ``None`` disables the timeout; ``0`` fails immediately
		:keyword local_address: The local binding address. ``None`` selects it automatically; port ``0`` selects an available port
		"""
		if self.__connection is not None and not self.__connection.closed:
			raise RconConnectionError('client is already connected')
		validate_timeout(timeout)
		if timeout == 0:
			raise RconTimeoutError('connection timed out')
		try:
			sock = socket.create_connection(
				(self.__config.host, self.__config.port),
				timeout=timeout,
				source_address=None if local_address is None else (local_address.host, local_address.port),
			)
		except socket.timeout as err:
			raise RconTimeoutError('connection timed out') from err
		except OSError as err:
			raise RconConnectionError('failed to connect to {}:{}'.format(self.__config.host, self.__config.port)) from err
		try:
			sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
			sock.settimeout(None)
			self.__connection = Connection(
				sock,
				max_send_packet_size=self.__config.connection_config.max_send_packet_size,
				max_receive_packet_size=self.__config.connection_config.max_receive_packet_size,
			)
		except OSError as err:
			sock.close()
			raise RconConnectionError('failed to configure TCP connection') from err
		except BaseException:
			sock.close()
			raise

	def send_packet(self, packet: Packet, *, timeout: Optional[float] = None):
		"""
		Send a complete RCON packet

		:param packet: The packet to send
		:keyword timeout: Total timeout in seconds. ``None`` disables the timeout; ``0`` expires immediately
		"""
		self.__get_connection().send_packet(packet, timeout=timeout)

	def receive_packet(self, *, timeout: Optional[float] = None) -> Packet:
		"""
		Receive the next complete RCON packet

		:keyword timeout: Total timeout in seconds. ``None`` disables the timeout; ``0`` expires immediately
		:return: The received packet
		"""
		return self.__get_connection().receive_packet(timeout=timeout)

	def try_receive_packet(self, *, first_byte_timeout: float, timeout: Optional[float] = None) -> Optional[Packet]:
		"""
		Receive a packet, allowing the first-byte wait to end without closing the connection

		Once a packet starts, receive failures close the connection. Exhausting the total budget also closes it.

		:keyword first_byte_timeout: Required finite non-negative first-byte waiting time in seconds; ``0`` checks immediately
		:keyword timeout: Total receive budget in seconds. ``None`` disables the timeout; ``0`` expires immediately
		:return: The received packet, or ``None`` if the first-byte wait expires while the total budget remains
		"""
		return self.__get_connection().try_receive_packet(first_byte_timeout=first_byte_timeout, timeout=timeout)

	def close(self):
		"""
		Close the current connection
		"""
		connection = self.__connection
		self.__connection = None
		if connection is not None:
			connection.close()

	def __enter__(self) -> 'RawRconClient':
		"""
		Return this client
		"""
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		"""
		Close the current connection when leaving the context
		"""
		self.close()

	def __get_connection(self) -> Connection:
		if self.__connection is None or self.__connection.closed:
			raise RconConnectionError('client is not connected')
		return self.__connection


class AsyncRawRconClient:
	def __init__(self, host: str, port: int, **kwargs: Unpack[ConnectionOptions]):
		"""
		Configure the target address and packet size limits

		:param host: The remote host name or IP address
		:param port: The remote TCP port
		:keyword max_send_packet_size: Maximum sent packet size in bytes, including the length header. ``None`` disables the limit
		:keyword max_receive_packet_size: Maximum received packet size in bytes, including the length header. ``None`` disables the limit; defaults to 4 MiB
		"""
		self.__config = _RawRconClientConfig(host, port, ConnectionConfig(**kwargs))
		self.__connection: Optional[AsyncConnection] = None

	async def connect(self, *, local_address: Optional[LocalAddress] = None):
		"""
		Establish a TCP connection to the configured target

		:keyword local_address: The local binding address. ``None`` selects it automatically; port ``0`` selects an available port
		"""
		if self.__connection is not None and not self.__connection.closed:
			raise RconConnectionError('client is already connected')

		try:
			reader, writer = await asyncio.open_connection(
				self.__config.host,
				self.__config.port,
				local_addr=None if local_address is None else (local_address.host, local_address.port),
			)
		except OSError as err:
			raise RconConnectionError('failed to connect to {}:{}'.format(self.__config.host, self.__config.port)) from err

		try:
			sock = writer.get_extra_info('socket')
			if sock is None:
				raise RconConnectionError('tcp connection does not expose a socket')
			cast(socket.socket, sock).setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		except OSError as err:
			writer.transport.abort()
			raise RconConnectionError('failed to configure TCP connection') from err
		except BaseException:
			writer.transport.abort()
			raise
		else:
			self.__connection = AsyncConnection(
				reader, writer,
				max_send_packet_size=self.__config.connection_config.max_send_packet_size,
				max_receive_packet_size=self.__config.connection_config.max_receive_packet_size,
			)

	async def send_packet(self, packet: Packet):
		"""
		Send a complete RCON packet

		:param packet: The packet to send
		"""
		await self.__get_connection().send_packet(packet)

	async def receive_packet(self) -> Packet:
		"""
		Receive the next complete RCON packet

		:return: The received packet
		"""
		return await self.__get_connection().receive_packet()

	async def try_receive_packet(self, *, first_byte_timeout: float) -> Optional[Packet]:
		"""
		Receive a packet, allowing the first-byte wait to end without closing the connection

		Receive failures and caller cancellation close the connection.

		:keyword first_byte_timeout: Required finite non-negative first-byte waiting time in seconds; ``0`` checks immediately
		:return: The received packet, or ``None`` if the first-byte wait expires before a packet starts
		"""
		return await self.__get_connection().try_receive_packet(first_byte_timeout=first_byte_timeout)

	async def aclose(self):
		"""
		Close the current connection
		"""
		connection = self.__connection
		self.__connection = None
		if connection is not None:
			await connection.aclose()

	async def __aenter__(self) -> 'AsyncRawRconClient':
		"""
		Return this client
		"""
		return self

	async def __aexit__(self, exc_type, exc_value, traceback):
		"""
		Close the current connection when leaving the asynchronous context
		"""
		await self.aclose()

	def __get_connection(self) -> AsyncConnection:
		if self.__connection is None or self.__connection.closed:
			raise RconConnectionError('client is not connected')
		return self.__connection
