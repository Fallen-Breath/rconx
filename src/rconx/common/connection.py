import asyncio
import contextlib
import dataclasses
import socket
from typing import Iterator, Optional, Union

from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError, RconTimeoutError
from rconx.common.protocol import LENGTH_HEADER, MIN_PACKET_BODY_SIZE, Packet
from rconx.common.utils import BytesBuffer, Deadline, validate_packet_size_limit

DEFAULT_MAX_RECEIVE_PACKET_SIZE = 4 * 1024 * 1024
_READ_CHUNK_SIZE = 64 * 1024


@dataclasses.dataclass(frozen=True)
class _ConnectionConfig:
	max_send_packet_size: Optional[int]
	max_receive_packet_size: Optional[int]

	def __post_init__(self):
		validate_packet_size_limit(self.max_send_packet_size, 'max_send_packet_size')
		validate_packet_size_limit(self.max_receive_packet_size, 'max_receive_packet_size')


class __BaseConnection:
	def __init__(self, *, max_send_packet_size: Optional[int], max_receive_packet_size: Optional[int]):
		self.__config = _ConnectionConfig(max_send_packet_size, max_receive_packet_size)

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
	def __init__(
		self,
		sock: socket.socket,
		*,
		max_send_packet_size: Optional[int] = None,
		max_receive_packet_size: Optional[int] = DEFAULT_MAX_RECEIVE_PACKET_SIZE,
	):
		super().__init__(max_send_packet_size=max_send_packet_size, max_receive_packet_size=max_receive_packet_size)
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
			header = self.__read_exactly(sock, LENGTH_HEADER.size, deadline)
			packet_size = self._decode_packet_size(header)
			body = self.__read_exactly(sock, packet_size, deadline)
			packet = Packet.decode(body)
			deadline.get_remaining_or_raise()
			return packet

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
	def __close_on_error(self) -> Iterator[None]:
		try:
			yield
		except BaseException:
			with contextlib.suppress(BaseException):
				self.close()
			raise

	@classmethod
	def __read_exactly(cls, sock: socket.socket, size: int, deadline: Deadline) -> bytes:
		buffer = BytesBuffer()
		while len(buffer) < size:
			try:
				sock.settimeout(deadline.get_remaining_or_raise())
				data = sock.recv(min(size - len(buffer), _READ_CHUNK_SIZE))
			except socket.timeout as err:
				raise RconTimeoutError('packet receive timed out') from err
			except OSError as err:
				raise RconConnectionError('failed to receive packet') from err
			if not data:
				raise RconConnectionError('peer closed the connection after {} of {} bytes'.format(len(buffer), size))
			buffer.append(data)
		return buffer.consume()


@dataclasses.dataclass(frozen=True)
class _AsyncStream:
	reader: asyncio.StreamReader
	writer: asyncio.StreamWriter


class AsyncConnection(__BaseConnection):
	def __init__(
		self,
		reader: asyncio.StreamReader,
		writer: asyncio.StreamWriter,
		*,
		max_send_packet_size: Optional[int] = None,
		max_receive_packet_size: Optional[int] = DEFAULT_MAX_RECEIVE_PACKET_SIZE,
	):
		super().__init__(max_send_packet_size=max_send_packet_size, max_receive_packet_size=max_receive_packet_size)
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
			header = await self.__read_exactly(stream.reader, LENGTH_HEADER.size)
			packet_size = self._decode_packet_size(header)
			body = await self.__read_exactly(stream.reader, packet_size)
			return Packet.decode(body)

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
	def __close_on_error(self) -> Iterator[None]:
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

	@classmethod
	async def __read_exactly(cls, reader: asyncio.StreamReader, size: int) -> bytes:
		try:
			return await reader.readexactly(size)
		except asyncio.IncompleteReadError as err:
			raise RconConnectionError('peer closed the connection after {} of {} bytes'.format(len(err.partial), size)) from err
		except OSError as err:
			raise RconConnectionError('failed to receive packet') from err

	@classmethod
	async def __wait_writer_closed(cls, writer: asyncio.StreamWriter):
		wait_closed = getattr(writer, 'wait_closed', None)
		if wait_closed is not None:
			await wait_closed()
