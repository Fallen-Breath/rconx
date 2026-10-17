import asyncio
import contextlib
import socket
from asyncio.base_events import Server
from typing import Any, Callable, List, Optional, Tuple, cast

import pytest
from typing_extensions import Unpack

from rconx.common.connection import AsyncConnection
from rconx.common.connection_options import ConnectionOptions
from rconx.common.exceptions import RconConnectionError, RconStateError
from rconx.common.protocol import Packet
from rconx.server.raw import AsyncRawRconServer
from rconx.server.server_connection import AsyncServerConnection
from tests.support import RunAsync, TcpListenerFactory, read_async_packet


def listener_address(server: Server) -> Tuple[str, int]:
	sockets = server.sockets
	assert sockets
	address = sockets[0].getsockname()
	return address[0], address[1]


def test_exchange_and_handoff_context(run_async: RunAsync, async_listeners: List[Server], listening_host: str):
	async def scenario():
		ready = asyncio.Event()
		loop = asyncio.get_event_loop()
		workers: List[asyncio.Future[None]] = []
		async def worker(connection: AsyncConnection):
			async with connection:
				for _ in range(2):
					await connection.send_packet(await connection.receive_packet())
		def handler(connection: AsyncConnection):
			assert asyncio.get_event_loop() is loop
			workers.append(asyncio.ensure_future(worker(connection)))
		server = AsyncRawRconServer(listening_host, 0, handler)
		serve = asyncio.ensure_future(server.serve(on_started=ready.set))
		try:
			await ready.wait()
			reader, writer = await asyncio.open_connection(*listener_address(async_listeners[0]))
			try:
				for packet in [Packet(1, 2, b'first'), Packet(2, 0, b'\x00\xff')]:
					writer.write(packet.encode_framed())
					await writer.drain()
					assert await read_async_packet(reader) == packet
				assert await reader.read(1) == b''
				await asyncio.gather(*workers)
			finally:
				writer.close()
		finally:
			await server.shutdown()
			await serve
	run_async(scenario())


@pytest.mark.parametrize('cancel,close_error', [(False, False), (True, False), (True, True)], ids=['shutdown', 'cancel-serve', 'cancel-with-cleanup-failure'])
def test_stop_closes_long_lived_connections(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch, cancel: bool, close_error: bool):
	created: List[AsyncServerConnection] = []
	if close_error:
		def create(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, on_close: Callable[[AsyncConnection], None], **kwargs: Unpack[ConnectionOptions]) -> AsyncServerConnection:
			fail = len(created) == 1
			def notify(connection: AsyncConnection):
				on_close(connection)
				if fail:
					raise OSError('connection close failed')
			connection = AsyncServerConnection(reader, writer, on_close=notify, **kwargs)
			created.append(connection)
			return connection
		monkeypatch.setattr('rconx.server.raw.AsyncServerConnection', create)
	async def scenario():
		ready = asyncio.Event()
		accepted: asyncio.Queue[AsyncConnection] = asyncio.Queue()
		server = AsyncRawRconServer('127.0.0.1', 0, accepted.put_nowait)
		await server.shutdown()
		serve = asyncio.ensure_future(server.serve(on_started=ready.set))
		writers: List[asyncio.StreamWriter] = []
		try:
			await ready.wait()
			address = listener_address(async_listeners[0])
			connections: List[AsyncConnection] = []
			readers: List[asyncio.StreamReader] = []
			for _ in range(3):
				reader, writer = await asyncio.open_connection(*address)
				readers.append(reader)
				writers.append(writer)
				connections.append(await accepted.get())
			await connections[0].aclose()
			assert await readers[0].read(1) == b''
			async def receive(connection: AsyncConnection):
				with pytest.raises(RconConnectionError):
					await connection.receive_packet()
			workers = [asyncio.ensure_future(receive(connection)) for connection in connections[1:]]
			await asyncio.sleep(0)
			assert all(not worker.done() for worker in workers)
			if cancel:
				serve.cancel()
				with pytest.raises(asyncio.CancelledError):
					await serve
			else:
				await server.shutdown()
				await serve
			await asyncio.gather(*workers)
			assert all(connection.closed for connection in connections)
			for reader in readers[1:]:
				assert await reader.read(1) == b''
			with pytest.raises(OSError):
				await asyncio.open_connection(*address)
			await server.shutdown()
			with pytest.raises(RconStateError):
				await server.serve()
		finally:
			await server.shutdown()
			for writer in writers:
				writer.close()
			if not serve.cancelled():
				await serve
	run_async(scenario())


def test_bind_failure_consumes_serve(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	occupied = tcp_listener_factory()
	async def scenario():
		started: List[bool] = []
		server = AsyncRawRconServer(occupied.address.host, occupied.address.port, lambda connection: None)
		with pytest.raises(RconConnectionError) as caught:
			await server.serve(on_started=lambda: started.append(True))
		assert isinstance(caught.value.__cause__, OSError)
		assert not started
		await server.shutdown()
		with pytest.raises(RconStateError):
			await server.serve()
	run_async(scenario())


def test_start_callback_failure_closes_listeners(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		failure = RuntimeError('startup callback')
		def fail():
			raise failure
		server = AsyncRawRconServer('127.0.0.1', 0, lambda connection: None)
		with pytest.raises(RuntimeError) as caught:
			await server.serve(on_started=fail)
		assert caught.value is failure
		assert not async_listeners[0].sockets
		with pytest.raises(RconStateError):
			await server.serve()
	run_async(scenario())


def test_handoff_failure_keeps_serving(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		ready = asyncio.Event()
		connections: List[AsyncConnection] = []
		workers: List[asyncio.Future[None]] = []
		async def worker(connection: AsyncConnection):
			async with connection:
				await connection.send_packet(await connection.receive_packet())
		def handler(connection: AsyncConnection):
			connections.append(connection)
			if len(connections) == 1:
				raise RuntimeError('handoff failure')
			workers.append(asyncio.ensure_future(worker(connection)))
		server = AsyncRawRconServer('127.0.0.1', 0, handler)
		serve = asyncio.ensure_future(server.serve(on_started=ready.set))
		try:
			await ready.wait()
			address = listener_address(async_listeners[0])
			reader, writer = await asyncio.open_connection(*address)
			try:
				assert await reader.read(1) == b''
				assert connections[0].closed
			finally:
				writer.close()
			reader, writer = await asyncio.open_connection(*address)
			try:
				packet = Packet(1, 2, b'after failure')
				writer.write(packet.encode_framed())
				await writer.drain()
				assert await read_async_packet(reader) == packet
				await asyncio.gather(*workers)
			finally:
				writer.close()
		finally:
			await server.shutdown()
			await serve
	run_async(scenario())


def test_shutdown_during_startup(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch):
	start_server = asyncio.start_server
	async def scenario():
		entered = asyncio.Event()
		release = asyncio.Event()
		async def delayed_start(*args: Any, **kwargs: Any) -> asyncio.AbstractServer:
			server = await start_server(*args, **kwargs)
			entered.set()
			await release.wait()
			return server
		monkeypatch.setattr(asyncio, 'start_server', delayed_start)
		started: List[bool] = []
		handed_off: List[AsyncConnection] = []
		server = AsyncRawRconServer('127.0.0.1', 0, handed_off.append)
		serve = asyncio.ensure_future(server.serve(on_started=lambda: started.append(True)))
		try:
			await entered.wait()
			address = listener_address(async_listeners[0])
			await server.shutdown()
			release.set()
			await serve
			assert not started
			assert not handed_off
			assert not async_listeners[0].sockets
			with pytest.raises(OSError):
				await asyncio.open_connection(*address)
		finally:
			release.set()
			await serve
	run_async(scenario())


def test_cancel_when_listener_starts_closes_listener(run_async: RunAsync, monkeypatch: pytest.MonkeyPatch):
	async def scenario():
		listeners: List[socket.socket] = []
		addresses: List[Tuple[str, int]] = []
		class CancelOnListen(socket.socket):
			def listen(self, backlog: int = 5):
				super().listen(backlog)
				listeners.append(self)
				address = self.getsockname()
				addresses.append((address[0], address[1]))
				asyncio.get_event_loop().call_soon(serve.cancel)
		monkeypatch.setattr(socket, 'socket', CancelOnListen)
		server = AsyncRawRconServer('127.0.0.1', 0, lambda connection: None)
		serve = asyncio.ensure_future(server.serve())
		try:
			with pytest.raises(asyncio.CancelledError):
				await serve
			await server.shutdown()
			assert listeners
			assert all(listener.fileno() == -1 for listener in listeners)
			for address in addresses:
				with pytest.raises(OSError):
					await asyncio.open_connection(*address)
		finally:
			await server.shutdown()
			for listener in listeners:
				listener.close()
	run_async(scenario())


def test_shutdown_interrupts_pending_normal_close(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch):
	start_server = asyncio.start_server
	server_writers: List[asyncio.StreamWriter] = []
	async def start(callback: Callable[[asyncio.StreamReader, asyncio.StreamWriter], None], *args: Any, **kwargs: Any) -> Server:
		def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
			server_writers.append(writer)
			callback(reader, writer)
		return cast(Server, await start_server(accept, *args, **kwargs))
	monkeypatch.setattr(asyncio, 'start_server', start)
	async def scenario():
		ready = asyncio.Event()
		accepted: asyncio.Queue[AsyncConnection] = asyncio.Queue()
		server = AsyncRawRconServer('127.0.0.1', 0, accepted.put_nowait)
		serve = asyncio.ensure_future(server.serve(on_started=ready.set))
		waiting = asyncio.Event()
		release = asyncio.Event()
		aborted = asyncio.Event()
		writer = None
		closing: Optional[asyncio.Future[None]] = None
		try:
			await ready.wait()
			reader, writer = await asyncio.open_connection(*listener_address(async_listeners[0]))
			connection = await accepted.get()
			packet = Packet(1, 0, b'before close')
			await connection.send_packet(packet)
			assert await read_async_packet(reader) == packet
			server_writer = server_writers[0]
			drain = server_writer.drain
			abort = server_writer.transport.abort
			async def wait_to_close():
				await drain()
				waiting.set()
				await release.wait()
			def interrupt():
				abort()
				aborted.set()
				release.set()
			monkeypatch.setattr(server_writer, 'drain', wait_to_close)
			monkeypatch.setattr(server_writer.transport, 'abort', interrupt)
			closing = asyncio.ensure_future(connection.aclose())
			await waiting.wait()
			assert not closing.done()
			await server.shutdown()
			assert aborted.is_set()
			await serve
			await closing
			assert connection.closed
			with contextlib.suppress(ConnectionResetError):
				await reader.read()
		finally:
			release.set()
			await server.shutdown()
			if closing is not None:
				await closing
			if writer is not None:
				writer.close()
			await serve
	run_async(scenario())
