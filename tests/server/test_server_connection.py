import asyncio
import socket
import threading
from typing import List, cast

import pytest

from rconx.common.connection import AsyncConnection, Connection
from rconx.common.exceptions import RconConnectionError
from rconx.server.server_connection import AsyncServerConnection, ServerConnection
from tests.support import MemorySocket, MemoryWriter, PendingReader, RunAsync, ThreadCall


def test_sync_concurrent_close_notifies_after_completion():
	entered = threading.Event()
	release = threading.Event()
	class ClosingSocket(MemorySocket):
		def close(self):
			entered.set()
			assert release.wait(2)
			super().close()
	sock = ClosingSocket()
	notified: List[Connection] = []
	connection = ServerConnection(cast(socket.socket, sock), on_close=notified.append)
	first = ThreadCall(connection.close)
	second = None
	try:
		assert entered.wait(2)
		second = ThreadCall(connection.close)
		assert not notified
	finally:
		release.set()
		first.join()
		if second is not None:
			second.join()
	connection.close()
	assert notified == [connection]
	assert sock.closed


@pytest.mark.parametrize('io_error', [False, True], ids=['explicit-close', 'receive-failure'])
def test_sync_close_failure_still_notifies(io_error: bool):
	sock = MemorySocket()
	failure = OSError('close failed')
	sock.close_error = failure
	notified: List[Connection] = []
	connection = ServerConnection(cast(socket.socket, sock), on_close=notified.append)
	if io_error:
		with pytest.raises(RconConnectionError):
			connection.receive_packet()
	else:
		with pytest.raises(RconConnectionError) as caught:
			connection.close()
		assert caught.value.__cause__ is failure
	connection.close()
	assert notified == [connection]
	assert connection.closed


@pytest.mark.parametrize('wait_closed', [False, True], ids=['legacy-writer', 'modern-writer'])
def test_async_close_notifies_after_wait(run_async: RunAsync, monkeypatch: pytest.MonkeyPatch, wait_closed: bool):
	monkeypatch.setattr('rconx.common.connection._HAS_WAIT_CLOSED', wait_closed)
	async def scenario():
		writer = MemoryWriter()
		gate = asyncio.Event()
		if wait_closed:
			writer.close_gate = gate
			started = writer.close_started
		else:
			writer.drain_gate = gate
			started = writer.drain_started
		notified: List[AsyncConnection] = []
		connection = AsyncServerConnection(PendingReader(), cast(asyncio.StreamWriter, writer), on_close=notified.append)
		closing = asyncio.ensure_future(connection.aclose())
		closing_tasks = [closing]
		try:
			await started.wait()
			assert connection.closed
			closing_tasks.append(asyncio.ensure_future(connection.aclose()))
			await asyncio.sleep(0)
			assert not notified
			assert not closing.done()
		finally:
			gate.set()
			await asyncio.gather(*closing_tasks)
		connection.abort()
		await connection.aclose()
		assert notified == [connection]
	run_async(scenario())


def test_async_abort_notifies_during_pending_close(run_async: RunAsync, memory_writer_support: None):
	async def scenario():
		writer = MemoryWriter()
		gate = asyncio.Event()
		writer.close_gate = gate
		notified: List[AsyncConnection] = []
		connection = AsyncServerConnection(PendingReader(), cast(asyncio.StreamWriter, writer), on_close=notified.append)
		closing = asyncio.ensure_future(connection.aclose())
		try:
			await writer.close_started.wait()
			connection.abort()
			assert writer.released
			assert notified == [connection]
		finally:
			gate.set()
			await closing
		connection.abort()
		await connection.aclose()
		assert notified == [connection]
	run_async(scenario())


@pytest.mark.parametrize('io_error', [False, True], ids=['explicit-close', 'receive-failure'])
def test_async_failure_still_notifies(run_async: RunAsync, memory_writer_support: None, io_error: bool):
	async def scenario():
		writer = MemoryWriter()
		failure = OSError('close failed')
		writer.close_error = failure
		notified: List[AsyncConnection] = []
		reader = PendingReader()
		reader.feed_eof()
		connection = AsyncServerConnection(reader, cast(asyncio.StreamWriter, writer), on_close=notified.append)
		if io_error:
			with pytest.raises(RconConnectionError):
				await connection.receive_packet()
		else:
			with pytest.raises(RconConnectionError) as caught:
				await connection.aclose()
			assert caught.value.__cause__ is failure
		connection.abort()
		await connection.aclose()
		assert notified == [connection]
		assert connection.closed and writer.released
	run_async(scenario())
