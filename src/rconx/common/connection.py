import asyncio
import contextlib
import dataclasses
import socket
from typing import Generator, Optional, Union

from typing_extensions import Unpack

from rconx.common.connection_options import ConnectionOptions
from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError, RconTimeoutError
from rconx.common.protocol import LENGTH_HEADER, MIN_PACKET_BODY_SIZE, Packet
from rconx.common.utils import BytesBuffer, Deadline, validate_packet_size_limit, validate_timeout

DEFAULT_MAX_RECEIVE_PACKET_SIZE = 4 * 1024 * 1024
_READ_CHUNK_SIZE = 64 * 1024


@dataclasses.dataclass(frozen=True)
class ConnectionConfig:
	max_send_packet_size: Optional[int] = None
	max_receive_packet_size: Optional[int] = DEFAULT_MAX_RECEIVE_PACKET_SIZE

	def __post_init__(self):
		validate_packet_size_limit(self.max_send_packet_size, 'max_send_packet_size')
		validate_packet_size_limit(self.max_receive_packet_size, 'max_receive_packet_size')


class __BaseConnection:
	def __init__(self, **kwargs: Unpack[ConnectionOptions]):
		self.__config = ConnectionConfig(**kwargs)

	def _encode_packet(self, packet: Packet) -> bytes:
		data = packet.encode_framed()
		maximum_size = self.__config.max_send_packet_size
		if maximum_size is not None and len(data) > maximum_size:
			raise ValueError('packet size {} exceeds max_send_packet_size {}'.format(len(data), maximum_size))
		return data

	def _decode_packet_size(self, data: Union[bytes, bytearray]) -> int:
		packet_size = LENGTH_HEADER.unpack(data)[0]
		if packet_size < MIN_PACKET_BODY_SIZE:
			raise RconPacketDecodeError('packet size {} must be at least {}'.format(packet_size, MIN_PACKET_BODY_SIZE))
		framed_size = LENGTH_HEADER.size + packet_size
		maximum_size = self.__config.max_receive_packet_size
		if maximum_size is not None and framed_size > maximum_size:
			raise ValueError('packet size {} exceeds max_receive_packet_size {}'.format(framed_size, maximum_size))
		return packet_size


class Connection(__BaseConnection):
	def __init__(self, sock: socket.socket, **kwargs: Unpack[ConnectionOptions]):
		"""
		**Not public API**
		"""
		super().__init__(**kwargs)
		self.__socket: Optional[socket.socket] = sock

	@property
	def closed(self) -> bool:
		"""
		Whether this connection has been closed locally
		"""
		return self.__socket is None

	def send_packet(self, packet: Packet, *, timeout: Optional[float] = None):
		"""
		Send a complete RCON packet

		:param packet: The packet to send
		:keyword timeout: Total timeout in seconds. ``None`` disables the timeout; ``0`` expires immediately
		"""
		deadline = Deadline(timeout)
		sock = self.__get_socket()
		data = self._encode_packet(packet)
		with self.__close_on_error():
			try:
				sock.settimeout(deadline.get_remaining_or_raise())
				sock.sendall(data)
			except socket.timeout as err:
				raise RconTimeoutError('packet send timed out') from err
			except OSError as err:
				raise RconConnectionError('failed to send packet') from err
			deadline.get_remaining_or_raise()

	def receive_packet(self, *, timeout: Optional[float] = None) -> Packet:
		"""
		Receive the next complete RCON packet

		:keyword timeout: Total timeout in seconds. ``None`` disables the timeout; ``0`` expires immediately
		:return: The received packet
		"""
		deadline = Deadline(timeout)
		sock = self.__get_socket()
		with self.__close_on_error():
			return self.__receive_packet(sock, deadline)

	def try_receive_packet(self, *, first_byte_timeout: float, timeout: Optional[float] = None) -> Optional[Packet]:
		"""
		Receive a packet, allowing the first-byte wait to end without closing the connection

		Once a packet starts, receive failures close the connection. Exhausting the total budget also closes it.

		:keyword first_byte_timeout: Required finite non-negative first-byte waiting time in seconds; ``0`` checks immediately
		:keyword timeout: Total receive budget in seconds. ``None`` disables the timeout; ``0`` expires immediately
		:return: The received packet, or ``None`` if the first-byte wait expires while the total budget remains
		"""
		if first_byte_timeout is None:
			raise TypeError('first_byte_timeout must be a finite non-negative number; got None')
		validate_timeout(first_byte_timeout, name='first_byte_timeout')
		deadline = Deadline(timeout)
		sock = self.__get_socket()

		with self.__close_on_error():
			remaining = deadline.get_remaining_or_raise()
			wait_timeout = first_byte_timeout if remaining is None else min(first_byte_timeout, remaining)
			try:
				sock.settimeout(wait_timeout)
				first_byte = sock.recv(1)
			except (socket.timeout, BlockingIOError):
				# An exhausted operation budget is a failure even when no packet has started.
				deadline.get_remaining_or_raise()
				return None
			except OSError as err:
				raise RconConnectionError('failed to read the first byte of a packet') from err

			if not first_byte:
				raise RconConnectionError('peer closed the connection while reading the first byte of a packet')
			return self.__receive_packet(sock, deadline, first_byte)

	def close(self):
		"""
		Close the connection
		"""
		sock = self.__socket
		self.__socket = None
		if sock is not None:
			sock.close()

	def __enter__(self) -> 'Connection':
		"""
		Return this connection
		"""
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		"""
		Close the connection when leaving the context
		"""
		self.close()

	def __get_socket(self) -> socket.socket:
		if self.__socket is None:
			raise RconConnectionError('connection is closed')
		return self.__socket

	@contextlib.contextmanager
	def __close_on_error(self) -> Generator[None, None, None]:
		try:
			yield
		except BaseException:
			with contextlib.suppress(BaseException):
				self.close()
			raise

	def __receive_packet(self, sock: socket.socket, deadline: Deadline, header_prefix: bytes = b'') -> Packet:
		header = header_prefix + self.__read_exactly(sock, LENGTH_HEADER.size - len(header_prefix), deadline, stage='packet length header')
		packet_size = self._decode_packet_size(header)
		body = self.__read_exactly(sock, packet_size, deadline, stage='packet body')
		packet = Packet.decode(body)
		deadline.get_remaining_or_raise()
		return packet

	@classmethod
	def __read_exactly(cls, sock: socket.socket, size: int, deadline: Deadline, *, stage: str) -> bytes:
		buffer = BytesBuffer()
		while len(buffer) < size:
			try:
				sock.settimeout(deadline.get_remaining_or_raise())
				data = sock.recv(min(size - len(buffer), _READ_CHUNK_SIZE))
			except socket.timeout as err:
				raise RconTimeoutError('timed out while reading {}'.format(stage)) from err
			except OSError as err:
				raise RconConnectionError('failed to read {}'.format(stage)) from err
			if not data:
				raise RconConnectionError('peer closed the connection while reading {}'.format(stage))
			buffer.append(data)
		return buffer.consume()


@dataclasses.dataclass(frozen=True)
class _AsyncStream:
	reader: asyncio.StreamReader
	writer: asyncio.StreamWriter


class AsyncConnection(__BaseConnection):
	def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, **kwargs: Unpack[ConnectionOptions]):
		"""
		**Not public API**
		"""
		super().__init__(**kwargs)
		self.__stream: Optional[_AsyncStream] = _AsyncStream(reader, writer)

	@property
	def closed(self) -> bool:
		"""
		Whether this connection has been closed locally
		"""
		return self.__stream is None

	async def send_packet(self, packet: Packet):
		"""
		Send a complete RCON packet

		:param packet: The packet to send
		"""
		stream = self.__get_stream()
		data = self._encode_packet(packet)
		with self.__close_on_error():
			try:
				stream.writer.write(data)
				await stream.writer.drain()
			except OSError as err:
				raise RconConnectionError('failed to send packet') from err

	async def receive_packet(self) -> Packet:
		"""
		Receive the next complete RCON packet

		:return: The received packet
		"""
		stream = self.__get_stream()
		with self.__close_on_error():
			return await self.__receive_packet(stream.reader)

	async def try_receive_packet(self, *, first_byte_timeout: float) -> Optional[Packet]:
		"""
		Receive a packet, allowing the first-byte wait to end without closing the connection

		Receive failures and caller cancellation close the connection.

		:keyword first_byte_timeout: Required finite non-negative first-byte waiting time in seconds; ``0`` checks immediately
		:return: The received packet, or ``None`` if the first-byte wait expires before a packet starts
		"""
		if first_byte_timeout is None:
			raise TypeError('first_byte_timeout must be a finite non-negative number; got None')
		validate_timeout(first_byte_timeout, name='first_byte_timeout')
		stream = self.__get_stream()

		with self.__close_on_error():
			first_byte_read = asyncio.ensure_future(self.__read_exactly(stream.reader, 1, stage='the first byte of a packet'))
			try:
				await asyncio.wait([first_byte_read], timeout=first_byte_timeout)
				if not first_byte_read.done():
					first_byte_read.cancel()
					# Wait for the cancelled read to finish before another receive can use the reader.
					await asyncio.wait([first_byte_read])
					return None
				first_byte = first_byte_read.result()
			finally:
				first_byte_read.cancel()
				with contextlib.suppress(BaseException):
					await first_byte_read

			return await self.__receive_packet(stream.reader, first_byte)

	async def aclose(self):
		"""
		Close the connection
		"""
		stream = self.__stream
		self.__stream = None
		if stream is not None:
			try:
				stream.writer.close()
				await self.__wait_writer_closed(stream.writer)
			except BaseException:
				self.__abort_stream(stream)
				raise

	async def __aenter__(self) -> 'AsyncConnection':
		"""
		Return this connection
		"""
		return self

	async def __aexit__(self, exc_type, exc_value, traceback):
		"""
		Close the connection when leaving the asynchronous context
		"""
		await self.aclose()

	def __get_stream(self) -> _AsyncStream:
		if self.__stream is None:
			raise RconConnectionError('connection is closed')
		return self.__stream

	@contextlib.contextmanager
	def __close_on_error(self) -> Generator[None, None, None]:
		try:
			yield
		except BaseException:
			stream = self.__stream
			self.__stream = None
			if stream is not None:
				self.__abort_stream(stream)
			raise

	@classmethod
	def __abort_stream(cls, stream: _AsyncStream):
		with contextlib.suppress(BaseException):
			stream.writer.transport.abort()

	async def __receive_packet(self, reader: asyncio.StreamReader, header_prefix: bytes = b'') -> Packet:
		header = header_prefix + await self.__read_exactly(reader, LENGTH_HEADER.size - len(header_prefix), stage='packet length header')
		packet_size = self._decode_packet_size(header)
		body = await self.__read_exactly(reader, packet_size, stage='packet body')
		return Packet.decode(body)

	@classmethod
	async def __read_exactly(cls, reader: asyncio.StreamReader, size: int, *, stage: str) -> bytes:
		try:
			return await reader.readexactly(size)
		except asyncio.IncompleteReadError as err:
			raise RconConnectionError('peer closed the connection while reading {}'.format(stage)) from err
		except OSError as err:
			raise RconConnectionError('failed to read {}'.format(stage)) from err

	@classmethod
	async def __wait_writer_closed(cls, writer: asyncio.StreamWriter):
		wait_closed = getattr(writer, 'wait_closed', None)
		if wait_closed is not None:
			await wait_closed()
