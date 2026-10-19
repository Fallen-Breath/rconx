import asyncio
import socket
import struct
from typing import Any, Optional, cast

import pytest

from rconx.common.connection import AsyncConnection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError, RconTimeoutError
from rconx.common.protocol import Packet
from tests.support import AsyncConnectionFactory, RunAsync, BasicMemoryWriter, FailingReader, MemoryWriter, TcpListenerFactory, buffer_stream_output, make_frame, open_async_connection_pair, read_async_packet


@pytest.mark.parametrize('window', [0, 0.001])
def test_first_byte_window_preserves_reader(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, window: float):
	async def scenario():
		endpoint = async_connection_factory()
		for _ in range(2):
			assert await endpoint.connection.try_receive_packet(first_byte_timeout=window) is None
			assert not endpoint.connection.closed
		endpoint.reader.feed_data(make_frame())
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')
		endpoint.reader.feed_data(make_frame(b'next'))
		assert await endpoint.connection.try_receive_packet(first_byte_timeout=0) == Packet(17, 2, b'next')

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 1, 3, 4, 8, 18])
def test_try_receive_eof_closes(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, prefix_size: int):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:prefix_size])
		endpoint.reader.feed_eof()
		with pytest.raises(RconConnectionError):
			await endpoint.connection.try_receive_packet(first_byte_timeout=0)
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 1, 4, 8])
def test_cancel_try_receive_closes(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, prefix_size: int):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:prefix_size])
		task = asyncio.ensure_future(endpoint.connection.try_receive_packet(first_byte_timeout=10))
		await endpoint.reader.pending.wait()
		task.cancel()
		with pytest.raises(asyncio.CancelledError):
			await task
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('window', [None, -1, float('inf'), float('nan')])
def test_invalid_first_byte_window_preserves_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, window: Any):
	async def scenario():
		endpoint = async_connection_factory(make_frame())
		with pytest.raises((TypeError, ValueError)):
			await endpoint.connection.try_receive_packet(first_byte_timeout=window)
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_first_byte_window_does_not_limit_packet_body(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:1])
		task = asyncio.ensure_future(endpoint.connection.try_receive_packet(first_byte_timeout=0))
		await endpoint.reader.pending.wait()
		await asyncio.sleep(0)
		assert not task.done()
		endpoint.reader.feed_data(make_frame()[1:])
		assert await task == Packet(17, 2, b'hello')
		assert not endpoint.connection.closed

	run_async(scenario())


def test_try_receive_network_error_closes(run_async: RunAsync):
	async def scenario():
		writer = MemoryWriter()
		connection = AsyncConnection(cast(asyncio.StreamReader, FailingReader(OSError())), cast(asyncio.StreamWriter, writer))
		with pytest.raises(RconConnectionError):
			await connection.try_receive_packet(first_byte_timeout=1)
		assert connection.closed and writer.released

	run_async(scenario())


def test_try_receive_checks_packet_size(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory(make_frame(), max_receive_packet_size=18)
		with pytest.raises(ValueError):
			await endpoint.connection.try_receive_packet(first_byte_timeout=0)
		assert endpoint.connection.closed

	run_async(scenario())


@pytest.mark.parametrize('chunk_size', [1, 3, 1460, 65536])
def test_receive_fragmented_packet(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, chunk_size: int):
	async def scenario():
		endpoint = async_connection_factory()
		payload = bytes(range(256)) * 513
		frame = make_frame(payload)
		receive = asyncio.ensure_future(endpoint.connection.receive_packet())
		try:
			for index in range(0, len(frame), chunk_size):
				endpoint.reader.feed_data(frame[index:index + chunk_size])
				if index < 4 or index % 4096 == 0:
					await asyncio.sleep(0)
			assert await receive == Packet(17, 2, payload)
			assert not endpoint.connection.closed
		finally:
			if not receive.done():
				receive.cancel()
				await asyncio.gather(receive, return_exceptions=True)

	run_async(scenario())


def test_receive_consecutive_packets(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory(make_frame(b'first') + make_frame(b'second', -1, 0))
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'first')
		assert await endpoint.connection.receive_packet() == Packet(-1, 0, b'second')

	run_async(scenario())


@pytest.mark.parametrize('first_byte_ready', [False, True], ids=['body-budget-starts-after-first-byte', 'first-byte-budget-ends-at-first-byte'])
def test_receive_phase_budgets_are_independent(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, first_byte_ready: bool):
	async def scenario():
		frame = make_frame()
		endpoint = async_connection_factory(frame[:1] if first_byte_ready else b'')
		receiving = asyncio.ensure_future(endpoint.connection.receive_packet(
			first_byte_timeout=0.05 if first_byte_ready else None,
			after_first_byte_timeout=None if first_byte_ready else 0.05,
		))
		await endpoint.reader.pending.wait()
		await asyncio.sleep(0.1)
		endpoint.reader.feed_data(frame[1:] if first_byte_ready else frame)
		assert await receiving == Packet(17, 2, b'hello')
		assert not endpoint.connection.closed

	run_async(scenario())


@pytest.mark.parametrize('stage', ['total', 'first-byte', 'after-first-byte'])
def test_receive_timeout_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, stage: str):
	async def scenario():
		budget = 0.001
		endpoint = async_connection_factory(make_frame()[:1] if stage == 'after-first-byte' else b'')
		with pytest.raises(RconTimeoutError):
			if stage == 'total':
				await endpoint.connection.receive_packet(timeout=budget)
			elif stage == 'first-byte':
				await endpoint.connection.receive_packet(first_byte_timeout=budget, after_first_byte_timeout=10)
			else:
				await endpoint.connection.receive_packet(after_first_byte_timeout=budget)
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('total_budget, later_budget', [(None, 0.2), (0.2, 10)])
def test_receive_fragmented_packet_does_not_renew_budgets(
	run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, total_budget: Optional[float], later_budget: float,
):
	async def scenario():
		frame = make_frame()
		endpoint = async_connection_factory(frame[:1] if total_budget is None else b'')
		receiving = asyncio.ensure_future(endpoint.connection.receive_packet(timeout=total_budget, first_byte_timeout=10, after_first_byte_timeout=later_budget))
		await endpoint.reader.pending.wait()
		await asyncio.sleep(0.12)
		endpoint.reader.feed_data(frame[1:4] if total_budget is None else frame[:4])
		await asyncio.sleep(0.12)
		endpoint.reader.feed_data(frame[4:])
		with pytest.raises(RconTimeoutError):
			await receiving
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('stage', ['total', 'first-byte', 'after-first-byte'])
def test_invalid_receive_timeout_preserves_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, stage: str):
	async def scenario():
		endpoint = async_connection_factory(make_frame())
		with pytest.raises(ValueError):
			if stage == 'total':
				await endpoint.connection.receive_packet(timeout=-1)
			elif stage == 'first-byte':
				await endpoint.connection.receive_packet(first_byte_timeout=-1)
			else:
				await endpoint.connection.receive_packet(after_first_byte_timeout=-1)
		assert not endpoint.connection.closed
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('stage', ['total', 'first-byte', 'after-first-byte'])
def test_cancel_receive_with_budgets_closes(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, stage: str):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:1] if stage == 'after-first-byte' else b'')
		if stage == 'total':
			receiving = asyncio.ensure_future(endpoint.connection.receive_packet(timeout=10))
		else:
			receiving = asyncio.ensure_future(endpoint.connection.receive_packet(first_byte_timeout=10, after_first_byte_timeout=10))
		await endpoint.reader.pending.wait()
		receiving.cancel()
		with pytest.raises(asyncio.CancelledError):
			await receiving
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_get_info_returns_address_snapshot(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, family: int):
	listener = tcp_listener_factory(family)
	async def scenario():
		pair = await open_async_connection_pair(listener)
		try:
			info = pair.connection.get_info()
			local_address = pair.peer.writer.get_extra_info('peername')
			assert info.local_address.host == local_address[0]
			assert info.local_address.port == local_address[1]
			assert info.remote_address.host == listener.address.host
			assert info.remote_address.port == listener.address.port
			await pair.connection.aclose()
			assert info.local_address.host == local_address[0]
			assert info.local_address.port == local_address[1]
			assert info.remote_address.host == listener.address.host
			assert info.remote_address.port == listener.address.port
			with pytest.raises(RconConnectionError):
				pair.connection.get_info()
		finally:
			await pair.aclose()

	run_async(scenario())


@pytest.mark.parametrize('maximum', [None, 19, 20])
def test_send_size_limit_allows_packet(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, maximum: Optional[int]):
	async def scenario():
		endpoint = async_connection_factory(max_send_packet_size=maximum)
		await endpoint.connection.send_packet(Packet(17, 2, b'hello'))
		assert bytes(endpoint.writer.sent) == make_frame()
		assert not endpoint.connection.closed

	run_async(scenario())


@pytest.mark.parametrize('maximum', [0, 18])
def test_send_size_limit_preserves_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, maximum: Optional[int]):
	async def scenario():
		endpoint = async_connection_factory(make_frame(), max_send_packet_size=maximum)
		with pytest.raises(ValueError):
			await endpoint.connection.send_packet(Packet(17, 2, b'hello'))
		assert endpoint.writer.sent == b''
		assert not endpoint.connection.closed
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


def test_default_send_limit_is_unlimited(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory()
		payload = b'x' * DEFAULT_MAX_RECEIVE_PACKET_SIZE
		await endpoint.connection.send_packet(Packet(17, 2, payload))
		assert bytes(endpoint.writer.sent) == make_frame(payload)

	run_async(scenario())


@pytest.mark.parametrize('maximum', [None, 19, 20])
def test_receive_size_limit_allows_packet(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, maximum: Optional[int]):
	async def scenario():
		endpoint = async_connection_factory(make_frame(), max_receive_packet_size=maximum)
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('maximum', [0, 18])
def test_receive_size_limit_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, maximum: Optional[int]):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:4], max_receive_packet_size=maximum)
		with pytest.raises(ValueError):
			await endpoint.connection.receive_packet()
		assert endpoint.connection.closed
		assert endpoint.writer.released
		with pytest.raises(RconConnectionError):
			await endpoint.connection.receive_packet()

	run_async(scenario())


def test_default_receive_limit_includes_length_header(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory(struct.pack('<i', DEFAULT_MAX_RECEIVE_PACKET_SIZE))
		with pytest.raises(ValueError):
			await endpoint.connection.receive_packet()
		assert endpoint.connection.closed

	run_async(scenario())


@pytest.mark.parametrize('size', [-1, 0, 9])
def test_invalid_packet_size_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, size: int):
	async def scenario():
		endpoint = async_connection_factory(struct.pack('<i', size))
		with pytest.raises(RconPacketDecodeError):
			await endpoint.connection.receive_packet()
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 2, 4, 8, 18])
def test_eof_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, prefix_size: int):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:prefix_size])
		endpoint.reader.feed_eof()
		with pytest.raises(RconConnectionError):
			await endpoint.connection.receive_packet()
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('stage', ['write', 'drain', 'read'])
def test_network_error_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, stage: str):
	async def scenario():
		endpoint = async_connection_factory()
		error = OSError('network failure')
		if stage == 'read':
			connection = AsyncConnection(cast(asyncio.StreamReader, FailingReader(error)), cast(asyncio.StreamWriter, endpoint.writer))
		else:
			connection = endpoint.connection
			if stage == 'write':
				endpoint.writer.write_error = error
			else:
				endpoint.writer.drain_error = error
		with pytest.raises(RconConnectionError):
			if stage == 'read':
				await connection.receive_packet()
			else:
				await connection.send_packet(Packet(17, 2, b'hello'))
		assert connection.closed
		assert endpoint.writer.released

	run_async(scenario())


def test_cleanup_error_preserves_original_error(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory()
		primary = RuntimeError('send failure')
		endpoint.writer.write_error = primary
		endpoint.writer.transport.abort_error = OSError('cleanup failure')
		with pytest.raises(RuntimeError) as caught:
			await endpoint.connection.send_packet(Packet(17, 2, b'hello'))
		assert caught.value is primary
		assert endpoint.connection.closed

	run_async(scenario())


def test_encoding_error_preserves_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory(make_frame())
		with pytest.raises(Exception):
			await endpoint.connection.send_packet(Packet(2 ** 31, 2, b'hello'))
		assert endpoint.writer.sent == b''
		assert not endpoint.connection.closed
		await endpoint.connection.send_packet(Packet(17, 2, b'hello'))
		assert bytes(endpoint.writer.sent) == make_frame()
		assert await endpoint.connection.receive_packet() == Packet(17, 2, b'hello')

	run_async(scenario())


@pytest.mark.parametrize('prefix_size', [0, 2, 4, 8])
def test_cancel_receive_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, prefix_size: int):
	async def scenario():
		endpoint = async_connection_factory(make_frame()[:prefix_size])
		task = asyncio.ensure_future(endpoint.connection.receive_packet())
		await asyncio.sleep(0)
		task.cancel()
		with pytest.raises(asyncio.CancelledError):
			await task
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


def test_cancel_send_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory):
	async def scenario():
		endpoint = async_connection_factory()
		endpoint.writer.drain_gate = asyncio.Event()
		task: asyncio.Future[None] = asyncio.ensure_future(endpoint.connection.send_packet(Packet(17, 2, b'hello')))
		await endpoint.writer.drain_started.wait()
		task.cancel()
		with pytest.raises(asyncio.CancelledError):
			await task
		assert endpoint.connection.closed
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('body_error', [False, True])
def test_context_closes_connection(run_async: RunAsync, async_connection_factory: AsyncConnectionFactory, body_error: bool):
	async def scenario():
		endpoint = async_connection_factory()
		if body_error:
			with pytest.raises(RuntimeError):
				async with endpoint.connection as entered:
					assert entered is endpoint.connection
					raise RuntimeError('context failure')
		else:
			async with endpoint.connection as entered:
				assert entered is endpoint.connection
		assert endpoint.connection.closed
		assert endpoint.writer.released
		await endpoint.connection.aclose()
		with pytest.raises(RconConnectionError):
			await endpoint.connection.send_packet(Packet(17, 2, b'hello'))

	run_async(scenario())


def test_close_after_abort_waits_for_release(run_async: RunAsync, memory_writer_support: None):
	async def scenario():
		writer = MemoryWriter()
		writer.defer_abort = True
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		connection.abort()
		closing = asyncio.ensure_future(connection.aclose())
		try:
			await writer.close_started.wait()
			assert not closing.done() and not writer.released
		finally:
			writer.release()
			await closing
		assert writer.released

	run_async(scenario())


def test_cancelled_close_can_finish_cleanup(run_async: RunAsync, memory_writer_support: None):
	async def scenario():
		writer = MemoryWriter()
		writer.defer_abort = True
		writer.close_gate = asyncio.Event()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		closing = asyncio.ensure_future(connection.aclose())
		try:
			await writer.close_started.wait()
			closing.cancel()
			with pytest.raises(asyncio.CancelledError):
				await closing
			assert connection.closed and not writer.released
			closing = asyncio.ensure_future(connection.aclose())
			await asyncio.sleep(0)
			assert not closing.done()
		finally:
			writer.release()
			await asyncio.gather(closing, return_exceptions=True)
		await closing
		assert connection.closed
		assert writer.released
		await connection.aclose()

	run_async(scenario())


def test_close_failure_releases_transport_and_propagates_error(run_async: RunAsync, memory_writer_support: None):
	async def scenario():
		writer = MemoryWriter()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		primary = OSError('close failure')
		writer.close_error = primary
		with pytest.raises(OSError) as caught:
			await connection.aclose()
		assert caught.value is primary
		assert connection.closed
		assert writer.released
		await connection.aclose()

	run_async(scenario())


def test_close_without_wait_closed_support(run_async: RunAsync, monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr('rconx.common.connection._HAS_WAIT_CLOSED', False)
	async def scenario():
		writer = BasicMemoryWriter()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		await connection.aclose()
		assert connection.closed
		assert writer.closing
		await connection.aclose()

	run_async(scenario())


def test_normal_close_flushes_buffered_output(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory):
	listener = tcp_listener_factory()
	async def scenario():
		pair = await open_async_connection_pair(listener)
		packet = Packet(1, 2, b'x' * (256 * 1024))
		try:
			prefilled = await buffer_stream_output(pair.writer)
			pair.writer.transport.set_write_buffer_limits(high=0)
			sending = asyncio.ensure_future(pair.connection.send_packet(packet))
			await asyncio.sleep(0.05)
			assert not sending.done()
			closing = asyncio.ensure_future(pair.connection.aclose())
			await asyncio.sleep(0.05)
			assert not closing.done()
			cast(asyncio.Transport, pair.peer.writer.transport).resume_reading()
			assert await pair.peer.reader.readexactly(prefilled) == b'x' * prefilled
			assert await read_async_packet(pair.peer.reader) == packet
			await sending
			await closing
			assert await pair.peer.reader.read(1) == b''
		finally:
			await pair.aclose()
	run_async(scenario())


@pytest.mark.parametrize('cancel_close', [False, True], ids=['abort', 'cancel-close'])
def test_interrupt_concurrent_send_and_close(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, cancel_close: bool):
	listener = tcp_listener_factory()
	async def scenario():
		pair = await open_async_connection_pair(listener)
		try:
			await buffer_stream_output(pair.writer)
			pair.writer.transport.set_write_buffer_limits(high=0)
			sending = asyncio.ensure_future(pair.connection.send_packet(Packet(1, 2, b'x' * (256 * 1024))))
			await asyncio.sleep(0.05)
			assert not sending.done()
			closing = asyncio.ensure_future(pair.connection.aclose())
			await asyncio.sleep(0.05)
			assert not closing.done()
			if cancel_close:
				closing.cancel()
				with pytest.raises(asyncio.CancelledError):
					await closing
			else:
				pair.connection.abort()
				await closing
			await sending
			assert pair.connection.closed
		finally:
			await pair.aclose()
	run_async(scenario())


def test_close_after_completed_send_remains_abortable(run_async: RunAsync, memory_writer_support: None, monkeypatch: pytest.MonkeyPatch):
	async def scenario():
		writer = MemoryWriter()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		await connection.send_packet(Packet(1, 2, b'completed send'))
		waiting = asyncio.Event()
		release = asyncio.Event()
		writer.drain_started = waiting
		writer.close_started = waiting
		writer.drain_gate = release
		writer.close_gate = release
		abort = writer.transport.abort
		def interrupt():
			abort()
			release.set()
		monkeypatch.setattr(writer.transport, 'abort', interrupt)
		closing = asyncio.ensure_future(connection.aclose())
		try:
			await waiting.wait()
			assert not closing.done()
			connection.abort()
			assert writer.released
			await closing
			assert connection.closed
		finally:
			release.set()
			connection.abort()
			await closing
	run_async(scenario())
