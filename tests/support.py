import asyncio
import contextlib
import dataclasses
import socket
import struct
from collections import deque
from typing import Any, Callable, Coroutine, Deque, Iterable, List, NoReturn, Optional, Tuple, TypeVar, Union

from typing_extensions import Protocol

from rconx.common.connection import AsyncConnection, DEFAULT_MAX_RECEIVE_PACKET_SIZE

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


@dataclasses.dataclass
class AsyncEndpoint:
	connection: AsyncConnection
	reader: asyncio.StreamReader
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
