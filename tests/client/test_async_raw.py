import asyncio
import socket
from typing import Any, NoReturn, Type

import pytest

from rconx.client.raw import AsyncRawRconClient, LocalAddress
from rconx.common.exceptions import RconConnectionError, RconSizeLimitError, RconStateError, RconTimeoutError
from rconx.common.protocol import Packet
from tests.support import AsyncRawEndpoint, RunAsync, SocketAddress, TcpListenerFactory, make_frame


async def _assert_reconnect_works(endpoint: AsyncRawEndpoint):
	await endpoint.client.connect()
	await endpoint.client.send_packet(Packet(17, 2, b'next'))
	assert await endpoint.client.receive_packet() == Packet(17, 0, b'firstsecond')


@pytest.mark.parametrize('operation', ['send', 'receive', 'try-receive'])
def test_failed_operation_waits_for_release_and_allows_reconnect(run_async: RunAsync, async_raw_endpoint: AsyncRawEndpoint, operation: str):
	async def scenario():
		endpoint = async_raw_endpoint
		client = endpoint.client
		writer = endpoint.writer
		writer.defer_abort = True
		error = ConnectionResetError('connection reset')
		writer.close_error = error
		await client.connect()
		task: asyncio.Future[Any]
		if operation == 'send':
			writer.write_error = error
			task = asyncio.ensure_future(client.send_packet(Packet(17, 2, b'command')))
		else:
			endpoint.reader.set_exception(error)
			if operation == 'receive':
				task = asyncio.ensure_future(client.receive_packet())
			else:
				task = asyncio.ensure_future(client.try_receive_packet(first_byte_timeout=1))
		try:
			await writer.close_started.wait()
			assert not task.done() and not writer.released
			with pytest.raises(RconStateError):
				await client.connect()
		finally:
			writer.release()
			await asyncio.gather(task, return_exceptions=True)
		with pytest.raises(RconConnectionError):
			await task
		assert writer.released
		await _assert_reconnect_works(endpoint)

	run_async(scenario())


@pytest.mark.parametrize('cancel', [False, True], ids=['cleanup-error', 'cleanup-cancel'])
def test_failure_cleanup_propagates_new_failure(run_async: RunAsync, async_raw_endpoint: AsyncRawEndpoint, cancel: bool):
	async def scenario():
		endpoint = async_raw_endpoint
		client = endpoint.client
		writer = endpoint.writer
		writer.defer_abort = True
		await client.connect()
		endpoint.reader.set_exception(ConnectionResetError('connection reset'))
		task = asyncio.ensure_future(client.receive_packet())
		expected: Type[BaseException] = asyncio.CancelledError if cancel else RuntimeError
		try:
			await writer.close_started.wait()
			if cancel:
				task.cancel()
			else:
				writer.close_error = RuntimeError('close failure')
				writer.release()
			with pytest.raises(expected):
				await task
			with pytest.raises(RconStateError):
				await client.connect()
		finally:
			writer.release()
			await asyncio.gather(task, return_exceptions=True)
		await client.aclose()
		await _assert_reconnect_works(endpoint)

	run_async(scenario())


@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('configuration failure'), RconConnectionError, id='connection-error'),
	pytest.param(socket.timeout('configuration timeout'), RconTimeoutError, id='socket-timeout'),
	pytest.param(TimeoutError('configuration timeout'), RconTimeoutError, id='builtin-timeout'),
])
def test_configuration_failure_waits_for_release_and_allows_retry(
	run_async: RunAsync, async_raw_endpoint: AsyncRawEndpoint, monkeypatch: pytest.MonkeyPatch, error: OSError, expected: Type[BaseException],
):
	async def scenario():
		endpoint = async_raw_endpoint
		client = endpoint.client
		writer = endpoint.writer
		writer.defer_abort = True

		def fail_configuration(level: int, option: int, value: int):
			raise error

		monkeypatch.setattr(writer, 'setsockopt', fail_configuration)
		task = asyncio.ensure_future(client.connect())
		try:
			await writer.close_started.wait()
			assert not task.done() and not writer.released
			with pytest.raises(RconStateError):
				await client.connect()
		finally:
			writer.release()
			await asyncio.gather(task, return_exceptions=True)
		with pytest.raises(expected) as caught:
			await task
		assert caught.value.__cause__ is error
		assert writer.released
		await _assert_reconnect_works(endpoint)

	run_async(scenario())


def test_first_byte_window_allows_reuse(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			peer = await server.accept_async()
			assert await client.try_receive_packet(first_byte_timeout=0) is None
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.try_receive_packet(first_byte_timeout=1) == Packet(17, 2, b'hello')
			await client.send_packet(Packet(20, 2, b'next'))
			assert await peer.reader.readexactly(18) == make_frame(b'next', 20)

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 1, 8])
def test_try_receive_failure_allows_reconnect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, prefix_size: int):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			first = await server.accept_async()
			first.writer.write(make_frame()[:prefix_size])
			await first.writer.drain()
			first.writer.close()
			with pytest.raises(RconConnectionError):
				await client.try_receive_packet(first_byte_timeout=1)
			await client.connect()
			second = await server.accept_async()
			second.writer.write(make_frame())
			await second.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_packet_exchange(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, family: int):
	async def scenario():
		server = tcp_listener_factory(family)
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			peer = await server.accept_async()
			await client.send_packet(Packet(17, 2, b'hello'))
			assert await peer.reader.readexactly(19) == make_frame()
			peer.writer.write(make_frame(b'first') + make_frame(b'second', -1, 0))
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'first')
			assert await client.receive_packet() == Packet(-1, 0, b'second')
		assert await peer.reader.read(1) == b''

	run_async(scenario())


def test_context_requires_explicit_connect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		client = AsyncRawRconClient(server.address.host, server.address.port)
		async with client as entered:
			assert entered is client
			with pytest.raises(RconStateError):
				await client.send_packet(Packet(17, 2, b'hello'))
			with pytest.raises(RconStateError):
				await client.receive_packet()
			with pytest.raises(RconStateError):
				await client.try_receive_packet(first_byte_timeout=0)
			await client.connect()
			peer = await server.accept_async()
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('body_error', [False, True])
def test_context_closes_on_exit(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, body_error: bool):
	async def scenario():
		server = tcp_listener_factory()
		client = AsyncRawRconClient(server.address.host, server.address.port)
		if body_error:
			with pytest.raises(RuntimeError):
				async with client:
					await client.connect()
					peer = await server.accept_async()
					raise RuntimeError('context failure')
		else:
			async with client:
				await client.connect()
				peer = await server.accept_async()
		assert await peer.reader.read(1) == b''
		with pytest.raises(RconStateError):
			await client.send_packet(Packet(17, 2, b'hello'))

	run_async(scenario())


def test_repeated_close_and_reconnect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.aclose()
			await client.aclose()
			await client.connect()
			first = await server.accept_async()
			await client.aclose()
			await client.aclose()
			assert await first.reader.read(1) == b''
			with pytest.raises(RconStateError):
				await client.receive_packet()
			await client.connect()
			second = await server.accept_async()
			second.writer.write(make_frame())
			await second.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_duplicate_connect_preserves_connection(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			peer = await server.accept_async()
			with pytest.raises(RconStateError):
				await client.connect()
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('connection failure'), RconConnectionError, id='connection-error'),
	pytest.param(socket.timeout('connection timeout'), RconTimeoutError, id='socket-timeout'),
	pytest.param(TimeoutError('connection timeout'), RconTimeoutError, id='builtin-timeout'),
])
def test_connection_failure_allows_retry(
	run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, monkeypatch: pytest.MonkeyPatch, error: OSError, expected: Type[BaseException],
):
	async def scenario():
		server = tcp_listener_factory()

		async def failed_connection(*args: Any, **kwargs: Any) -> NoReturn:
			raise error

		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			with monkeypatch.context() as patched:
				patched.setattr(asyncio, 'open_connection', failed_connection)
				with pytest.raises(expected) as caught:
					await client.connect()
				assert caught.value.__cause__ is error
			await client.connect()
			peer = await server.accept_async()
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_cancelled_connect_allows_retry(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, monkeypatch: pytest.MonkeyPatch):
	async def scenario():
		server = tcp_listener_factory()
		entered = asyncio.Event()
		gate = asyncio.Event()

		async def pending_connection(*args: Any, **kwargs: Any) -> NoReturn:
			entered.set()
			await gate.wait()
			raise AssertionError('connection gate unexpectedly released')

		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			with monkeypatch.context() as patched:
				patched.setattr(asyncio, 'open_connection', pending_connection)
				task: asyncio.Future[None] = asyncio.ensure_future(client.connect())
				await entered.wait()
				task.cancel()
				with pytest.raises(asyncio.CancelledError):
					await task
			await client.connect()
			peer = await server.accept_async()
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 2, 8])
def test_receive_failure_allows_reconnect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, prefix_size: int):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			first = await server.accept_async()
			first.writer.write(make_frame()[:prefix_size])
			await first.writer.drain()
			first.writer.close()
			with pytest.raises(RconConnectionError):
				await client.receive_packet()
			with pytest.raises(RconStateError):
				await client.send_packet(Packet(17, 2, b'hello'))
			await client.connect()
			second = await server.accept_async()
			second.writer.write(make_frame())
			await second.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 2, 8])
def test_cancelled_receive_allows_reconnect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, prefix_size: int):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect()
			first = await server.accept_async()
			first.writer.write(make_frame()[:prefix_size])
			await first.writer.drain()
			task = asyncio.ensure_future(client.receive_packet())
			await asyncio.sleep(0)
			task.cancel()
			with pytest.raises(asyncio.CancelledError):
				await task
			with pytest.raises(RconStateError):
				await client.receive_packet()
			await client.connect()
			second = await server.accept_async()
			second.writer.write(make_frame())
			await second.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_send_limit_preserves_connection(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port, max_send_packet_size=18) as client:
			await client.connect()
			peer = await server.accept_async()
			with pytest.raises(RconSizeLimitError):
				await client.send_packet(Packet(17, 2, b'hello'))
			await client.send_packet(Packet(17, 2, b''))
			assert await peer.reader.readexactly(14) == make_frame(b'')
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_receive_limit_closes_and_allows_reconnect(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port, max_receive_packet_size=18) as client:
			await client.connect()
			first = await server.accept_async()
			first.writer.write(make_frame()[:4])
			await first.writer.drain()
			with pytest.raises(RconSizeLimitError):
				await client.receive_packet()
			await client.connect()
			second = await server.accept_async()
			second.writer.write(make_frame(b''))
			await second.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'')

	run_async(scenario())


@pytest.mark.parametrize('fixed_port', [False, True])
@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_local_binding(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, fixed_port: bool, family: int):
	async def scenario():
		server = tcp_listener_factory(family)
		port = 0
		if fixed_port:
			with socket.socket(family, socket.SOCK_STREAM) as reservation:
				reservation.bind((server.address.host, 0))
				port = reservation.getsockname()[1]
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			await client.connect(local_address=LocalAddress(server.address.host, port))
			peer = await server.accept_async()
			address: SocketAddress = peer.writer.get_extra_info('peername')
			assert address[0] == server.address.host
			assert address[1] == port if fixed_port else address[1] > 0
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_occupied_local_port_allows_retry(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	async def scenario():
		server = tcp_listener_factory()
		async with AsyncRawRconClient(server.address.host, server.address.port) as client:
			with pytest.raises(RconConnectionError):
				await client.connect(local_address=LocalAddress(server.address.host, server.address.port))
			await client.connect(local_address=LocalAddress(server.address.host))
			peer = await server.accept_async()
			peer.writer.write(make_frame())
			await peer.writer.drain()
			assert await client.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())
