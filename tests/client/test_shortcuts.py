import asyncio
import socket
from typing import Any, Tuple, cast

import pytest

from rconx.client.preset import RconPreset
from rconx.client.shortcuts import execute, execute_async
from rconx.common.exceptions import RconTimeoutError
from tests.support import Clock, RconPeer, RconReader, RconSocket, RconWriter, RunAsync


def test_execute_collects_response_and_closes_connection(monkeypatch: pytest.MonkeyPatch):
	peer = RconPeer(RconPreset.srcds)
	peer.output = [b'r\xe9', b'ponse']
	sock = RconSocket(peer)

	def connect(address: Tuple[str, int], **kwargs: Any) -> socket.socket:
		return cast(socket.socket, sock)

	monkeypatch.setattr(socket, 'create_connection', connect)
	response = execute('localhost', 25575, 'caf\xe9', password='s\xe9same', preset=RconPreset.srcds, default_encoding='latin1')

	assert response.content == b'r\xe9ponse'
	assert response.text == 'r\xe9ponse'
	assert peer.requests[0].payload == b's\xe9same'
	assert peer.requests[1].payload == b'caf\xe9'
	assert sock.closed


def test_execute_async_collects_response_and_closes_connection(monkeypatch: pytest.MonkeyPatch, run_async: RunAsync, memory_writer_support: None):
	async def scenario():
		peer = RconPeer(RconPreset.srcds)
		peer.output = [b'r\xe9', b'ponse']
		reader = RconReader(peer)
		writer = RconWriter(peer, reader)

		async def connect(host: str, port: int, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
			return reader, cast(asyncio.StreamWriter, writer)

		monkeypatch.setattr(asyncio, 'open_connection', connect)
		response = await execute_async('localhost', 25575, 'caf\xe9', password='s\xe9same', preset='srcds', default_encoding='latin1')

		assert response.content == b'r\xe9ponse'
		assert response.text == 'r\xe9ponse'
		assert peer.requests[0].payload == b's\xe9same'
		assert peer.requests[1].payload == b'caf\xe9'
		assert writer.released

	run_async(scenario())


def test_execute_shares_timeout_across_connection_authentication_and_command(monkeypatch: pytest.MonkeyPatch, clock: Clock):
	peer = RconPeer(RconPreset.single_packet)
	sock = RconSocket(peer)
	sock.on_send = lambda sock: clock.advance(2)

	def connect(address: Tuple[str, int], **kwargs: Any) -> socket.socket:
		clock.advance(2)
		return cast(socket.socket, sock)

	monkeypatch.setattr(socket, 'create_connection', connect)
	with pytest.raises(RconTimeoutError):
		execute('localhost', 25575, 'command', password='password', preset='single_packet', timeout=5)

	assert peer.requests[-1].payload == b'command'
	assert sock.closed
