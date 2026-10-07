import asyncio
import contextlib
import socket
from typing import Any, Callable, Coroutine, Generator, Iterable, List, Optional, Set, TypeVar, Union, cast

import pytest

from rconx.common.connection import AsyncConnection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from tests.support import AsyncConnectionFactory, AsyncEndpoint, Clock, MemorySocket, MemoryWriter, PendingReader, RunAsync, SocketFactory, TcpListener, TcpListenerFactory

_T = TypeVar('_T')


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
	clock = Clock()
	monkeypatch.setattr('rconx.common.utils.time.monotonic', clock)
	return clock


@pytest.fixture
def socket_factory() -> Generator[SocketFactory, None, None]:
	sockets: List[MemorySocket] = []

	def create(chunks: Iterable[Union[bytes, BaseException]] = ()) -> MemorySocket:
		sock = MemorySocket(chunks)
		sockets.append(sock)
		return sock

	yield create
	for sock in sockets:
		with contextlib.suppress(BaseException):
			sock.close()


@pytest.fixture
def run_async() -> Generator[RunAsync, None, None]:
	loop = asyncio.new_event_loop()
	asyncio.set_event_loop(loop)

	def run(coroutine: Coroutine[Any, Any, _T]) -> _T:
		return loop.run_until_complete(asyncio.wait_for(coroutine, 5.0))

	yield run
	try:
		all_tasks: Optional[Callable[[Optional[asyncio.AbstractEventLoop]], Set[asyncio.Task[Any]]]] = getattr(asyncio, 'all_tasks', None)
		pending: List[asyncio.Task[Any]] = list(all_tasks(loop) if all_tasks is not None else getattr(asyncio.Task, 'all_tasks')(loop))
		for task in pending:
			task.cancel()
		if pending:
			loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
		loop.run_until_complete(loop.shutdown_asyncgens())
		loop.run_until_complete(asyncio.sleep(0))
	finally:
		asyncio.set_event_loop(None)
		loop.close()


@pytest.fixture
def async_connection_factory(run_async: RunAsync) -> Generator[AsyncConnectionFactory, None, None]:
	connections: List[AsyncConnection] = []

	def create(data: bytes = b'', *, max_send_packet_size: Optional[int] = None, max_receive_packet_size: Optional[int] = DEFAULT_MAX_RECEIVE_PACKET_SIZE) -> AsyncEndpoint:
		reader = PendingReader()
		reader.feed_data(data)
		writer = MemoryWriter()
		connection = AsyncConnection(reader, cast(asyncio.StreamWriter, writer), max_send_packet_size=max_send_packet_size, max_receive_packet_size=max_receive_packet_size)
		connections.append(connection)
		return AsyncEndpoint(connection, reader, writer)

	yield create
	for connection in connections:
		run_async(connection.aclose())


@pytest.fixture
def tcp_listener_factory(run_async: RunAsync) -> Generator[TcpListenerFactory, None, None]:
	listeners: List[TcpListener] = []

	def create(family: int = socket.AF_INET) -> TcpListener:
		try:
			listener = TcpListener(family)
		except OSError:
			if family == socket.AF_INET6:
				pytest.skip('ipv6 loopback is unavailable')
			raise
		listeners.append(listener)
		return listener

	yield create
	for listener in listeners:
		run_async(listener.aclose())
