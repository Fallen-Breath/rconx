import asyncio
import contextlib
import dataclasses
import socket
import struct
from collections import deque
from typing import Any, Callable, Coroutine, Deque, Iterable, List, NoReturn, Optional, Tuple, TypeVar, Union

from typing_extensions import Protocol, SupportsIndex, Unpack

from rconx.client.client import AsyncRconClient, RconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset
from rconx.common.connection import AsyncConnection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from rconx.common.protocol import Packet, PacketType

_T = TypeVar('_T')
SocketAddress = Union[Tuple[str, int], Tuple[str, int, int, int]]


def make_frame(payload: bytes = b'hello', request_id: int = 17, packet_type: int = 2) -> bytes:
	return struct.pack('<iii', len(payload) + 10, request_id, packet_type) + payload + b'\x00\x00'


def read_exactly(sock: socket.socket, size: int) -> bytes:
	chunks: List[bytes] = []
	while size:
		data = sock.recv(size)
		if not data:
			raise EOFError('peer closed before all expected bytes arrived')
		chunks.append(data)
		size -= len(data)
	return b''.join(chunks)


def read_packet(sock: socket.socket) -> Packet:
	header = read_exactly(sock, 4)
	size = struct.unpack('<i', header)[0]
	return Packet.decode_framed(header + read_exactly(sock, size))


async def read_async_packet(reader: asyncio.StreamReader) -> Packet:
	header = await reader.readexactly(4)
	size = struct.unpack('<i', header)[0]
	return Packet.decode_framed(header + await reader.readexactly(size))


class Clock:
	def __init__(self):
		self.now = 100.0

	def __call__(self) -> float:
		return self.now

	def advance(self, seconds: float):
		self.now += seconds


class MemorySocket:
	def __init__(self, chunks: Iterable[Union[bytes, BaseException]] = ()):
		self.chunks: Deque[Union[bytes, BaseException]] = deque(chunks)
		self.sent = bytearray()
		self.closed = False
		self.timeout: Optional[float] = None
		self.send_error: Optional[BaseException] = None
		self.close_error: Optional[BaseException] = None
		self.on_receive: Optional[Callable[['MemorySocket'], None]] = None
		self.on_send: Optional[Callable[['MemorySocket'], None]] = None

	def settimeout(self, timeout: Optional[float]):
		self.timeout = timeout

	def recv(self, size: int) -> bytes:
		if self.closed:
			raise OSError('socket is closed')
		if self.on_receive is not None:
			self.on_receive(self)
		if not self.chunks:
			return b''
		data = self.chunks.popleft()
		if isinstance(data, BaseException):
			raise data
		if len(data) > size:
			self.chunks.appendleft(data[size:])
		return data[:size]

	def sendall(self, data: bytes):
		if self.closed:
			raise OSError('socket is closed')
		if self.on_send is not None:
			self.on_send(self)
		if self.send_error is not None:
			raise self.send_error
		self.sent.extend(data)

	def close(self):
		self.closed = True
		if self.close_error is not None:
			raise self.close_error


class MemoryTransport:
	def __init__(self, writer: 'BasicMemoryWriter'):
		self.__writer = writer
		self.abort_error: Optional[BaseException] = None

	def abort(self):
		self.__writer.closing = True
		self.__writer.released = True
		if self.abort_error is not None:
			raise self.abort_error


class BasicMemoryWriter:
	def __init__(self):
		self.sent = bytearray()
		self.closing = False
		self.released = False
		self.write_error: Optional[BaseException] = None
		self.drain_error: Optional[BaseException] = None
		self.drain_gate: Optional[asyncio.Event] = None
		self.drain_started = asyncio.Event()
		self.transport = MemoryTransport(self)

	def write(self, data: bytes):
		if self.write_error is not None:
			raise self.write_error
		self.sent.extend(data)

	async def drain(self):
		self.drain_started.set()
		if self.drain_gate is not None:
			await self.drain_gate.wait()
		if self.drain_error is not None:
			raise self.drain_error

	def close(self):
		self.closing = True


class MemoryWriter(BasicMemoryWriter):
	def __init__(self):
		super().__init__()
		self.close_gate: Optional[asyncio.Event] = None
		self.close_started = asyncio.Event()
		self.close_error: Optional[BaseException] = None

	async def wait_closed(self):
		self.close_started.set()
		if self.close_gate is not None:
			await self.close_gate.wait()
		if self.close_error is not None:
			raise self.close_error
		self.released = True


class FailingReader:
	def __init__(self, error: BaseException):
		self.__error = error

	async def readexactly(self, size: int) -> NoReturn:
		raise self.__error


class PendingReader(asyncio.StreamReader):
	def __init__(self):
		super().__init__()
		self.pending = asyncio.Event()
		self.__available = 0

	def feed_data(self, data: Iterable[SupportsIndex]):
		payload = bytes(data)
		super().feed_data(payload)
		self.__available += len(payload)

	async def readexactly(self, size: int) -> bytes:
		if size > self.__available:
			self.pending.set()
		data = await super().readexactly(size)
		self.__available -= len(data)
		return data


@dataclasses.dataclass
class AsyncEndpoint:
	connection: AsyncConnection
	reader: PendingReader
	writer: MemoryWriter


@dataclasses.dataclass(frozen=True)
class TcpAddress:
	host: str
	port: int


@dataclasses.dataclass
class AsyncPeer:
	reader: asyncio.StreamReader
	writer: asyncio.StreamWriter


class TcpListener:
	def __init__(self, family: int = socket.AF_INET):
		self.socket = socket.socket(family, socket.SOCK_STREAM)
		self.__sockets: List[socket.socket] = []
		self.__writers: List[asyncio.StreamWriter] = []
		try:
			host = '::1' if family == socket.AF_INET6 else '127.0.0.1'
			self.socket.bind((host, 0))
			bound: SocketAddress = self.socket.getsockname()
			self.address = TcpAddress(bound[0], bound[1])
			self.socket.listen()
		except BaseException:
			self.socket.close()
			raise

	def accept(self) -> socket.socket:
		self.socket.settimeout(3.0)
		peer, _ = self.socket.accept()
		peer.settimeout(3.0)
		self.__sockets.append(peer)
		return peer

	async def accept_async(self) -> AsyncPeer:
		self.socket.setblocking(False)
		loop = asyncio.get_event_loop()
		peer, _ = await loop.sock_accept(self.socket)
		self.__sockets.append(peer)
		reader, writer = await asyncio.open_connection(sock=peer)
		self.__writers.append(writer)
		return AsyncPeer(reader, writer)

	async def aclose(self):
		wait_closed: Optional[Callable[[], Coroutine[Any, Any, None]]]
		self.socket.close()
		for writer in self.__writers:
			writer.close()
		for writer in self.__writers:
			wait_closed = getattr(writer, 'wait_closed', None)
			if wait_closed is not None:
				with contextlib.suppress(OSError):
					await wait_closed()
		for sock in self.__sockets:
			sock.close()


class SocketFactory(Protocol):
	def __call__(self, chunks: Iterable[Union[bytes, BaseException]] = ()) -> MemorySocket:
		...


class AsyncConnectionFactory(Protocol):
	def __call__(
		self, data: bytes = b'', *, max_send_packet_size: Optional[int] = None,
		max_receive_packet_size: Optional[int] = DEFAULT_MAX_RECEIVE_PACKET_SIZE,
	) -> AsyncEndpoint:
		...


class TcpListenerFactory(Protocol):
	def __call__(self, family: int = socket.AF_INET) -> TcpListener:
		...


class RunAsync(Protocol):
	def __call__(self, coroutine: Coroutine[Any, Any, _T]) -> _T:
		...


@dataclasses.dataclass(frozen=True)
class PacketReply:
	payload: bytes = b''
	packet_type: int = PacketType.response_value
	request_id: Optional[int] = None
	id_offset: int = 0

	def for_request(self, request: Packet) -> Packet:
		request_id = request.request_id + self.id_offset if self.request_id is None else self.request_id
		return Packet(request_id, self.packet_type, self.payload)


class RconPeer:
	def __init__(self, preset: RconPreset):
		self.preset = preset
		self.requests: List[Packet] = []
		self.output = [b'first', b'second']
		self.auth_replies = [PacketReply(packet_type=PacketType.auth_response)]
		self.command_replies: Optional[List[PacketReply]] = None
		self.probe_replies: Optional[List[PacketReply]] = None
		self.result_before_ack = False
		self.first_response_received = False
		self.__command_id: Optional[int] = None
		self.__received = bytearray()

	def observe_receive(self, data: bytes):
		self.__received.extend(data)
		while len(self.__received) >= 4:
			size = struct.unpack_from('<i', self.__received)[0] + 4
			if len(self.__received) < size:
				return
			packet = Packet.decode_framed(self.__received[:size])
			del self.__received[:size]
			if packet.request_id == self.__command_id:
				self.first_response_received = True

	def respond(self, request: Packet) -> bytes:
		self.requests.append(request)
		if request.packet_type == PacketType.auth:
			replies = self.auth_replies
		elif request.packet_type == PacketType.exec_command:
			self.__command_id = request.request_id
			self.first_response_received = False
			if self.command_replies is not None:
				replies = self.command_replies
			elif self.preset == RconPreset.cuberite:
				replies = [PacketReply(packet_type=2), PacketReply(b''.join(self.output), 2)]
				if self.result_before_ack:
					replies.reverse()
			elif self.preset == RconPreset.single_packet:
				replies = [PacketReply(b''.join(self.output))]
			else:
				replies = [PacketReply(payload) for payload in self.output]
		else:
			assert request.packet_type == PacketType.response_value
			assert request.payload == b''
			assert request.request_id != self.__command_id
			if self.preset == RconPreset.minecraft:
				# Minecraft's probe is usable only after a complete command reply has been read.
				assert self.first_response_received
			else:
				assert self.preset == RconPreset.srcds
				assert not self.first_response_received
			if self.probe_replies is not None:
				replies = self.probe_replies
			elif self.preset == RconPreset.srcds:
				replies = [PacketReply(), PacketReply(b'arbitrary ending payload')]
			else:
				replies = [PacketReply(b'Unknown request 0')]

		return b''.join(reply.for_request(request).encode_framed() for reply in replies)


class RconSocket(MemorySocket):
	def __init__(self, peer: RconPeer):
		super().__init__()
		self.peer = peer
		self.on_close: Optional[Callable[[], None]] = None

	def setsockopt(self, level: int, option: int, value: int):
		pass

	def sendall(self, data: bytes):
		super().sendall(data)
		response = self.peer.respond(Packet.decode_framed(data))
		if response:
			self.chunks.append(response)

	def recv(self, size: int) -> bytes:
		if not self.closed and not self.chunks:
			raise socket.timeout('no reply available')
		data = super().recv(size)
		self.peer.observe_receive(data)
		return data

	def close(self):
		super().close()
		if self.on_close is not None:
			self.on_close()


class RconReader(PendingReader):
	def __init__(self, peer: RconPeer):
		super().__init__()
		self.peer = peer

	async def readexactly(self, size: int) -> bytes:
		data = await super().readexactly(size)
		self.peer.observe_receive(data)
		return data


class RconWriter(MemoryWriter):
	def __init__(self, peer: RconPeer, reader: RconReader):
		super().__init__()
		self.peer = peer
		self.reader = reader

	def get_extra_info(self, name: str) -> Any:
		return self if name == 'socket' else None

	def setsockopt(self, level: int, option: int, value: int):
		pass

	def write(self, data: bytes):
		super().write(data)
		response = self.peer.respond(Packet.decode_framed(data))
		if response:
			self.reader.feed_data(response)


@dataclasses.dataclass
class SyncRconEndpoint:
	client: RconClient
	peer: RconPeer
	sockets: List[RconSocket] = dataclasses.field(default_factory=list)

	@property
	def socket(self) -> RconSocket:
		return self.sockets[-1]


@dataclasses.dataclass
class AsyncRconEndpoint:
	client: AsyncRconClient
	peer: RconPeer
	readers: List[RconReader] = dataclasses.field(default_factory=list)
	writers: List[RconWriter] = dataclasses.field(default_factory=list)

	@property
	def reader(self) -> RconReader:
		return self.readers[-1]

	@property
	def writer(self) -> RconWriter:
		return self.writers[-1]


class SyncRconFactory(Protocol):
	def __call__(self, preset: RconPreset = RconPreset.single_packet, *, first_byte_timeout: Optional[float] = 0, **kwargs: Unpack[RconClientOptions]) -> SyncRconEndpoint:
		...


class AsyncRconFactory(Protocol):
	def __call__(self, preset: RconPreset = RconPreset.single_packet, *, first_byte_timeout: Optional[float] = 0, **kwargs: Unpack[RconClientOptions]) -> AsyncRconEndpoint:
		...
