import asyncio
import struct
from typing import Optional, cast

import pytest

from rconx.common.connection import AsyncConnection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError
from rconx.common.protocol import Packet
from tests.support import AsyncConnectionFactory, RunAsync, BasicMemoryWriter, FailingReader, MemoryWriter, make_frame


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


@pytest.mark.parametrize('cancel', [False, True])
def test_close_failure_releases_transport_and_propagates_error(run_async: RunAsync, cancel: bool):
	async def scenario():
		writer = MemoryWriter()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		if cancel:
			writer.close_gate = asyncio.Event()
			task: asyncio.Future[None] = asyncio.ensure_future(connection.aclose())
			await writer.close_started.wait()
			task.cancel()
			with pytest.raises(asyncio.CancelledError):
				await task
		else:
			primary = OSError('close failure')
			writer.close_error = primary
			with pytest.raises(OSError) as caught:
				await connection.aclose()
			assert caught.value is primary
		assert connection.closed
		assert writer.released
		await connection.aclose()

	run_async(scenario())


def test_close_without_wait_closed_support(run_async: RunAsync):
	async def scenario():
		writer = BasicMemoryWriter()
		connection = AsyncConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, writer))
		await connection.aclose()
		assert connection.closed
		assert writer.closing
		await connection.aclose()

	run_async(scenario())
