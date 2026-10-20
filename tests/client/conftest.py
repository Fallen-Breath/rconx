import asyncio
import socket
from typing import Any, Generator, List, Tuple, Union, cast

import pytest
from typing_extensions import Unpack

from rconx.client.client import AsyncRconClient, RconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset, RconPresetLiteral
from rconx.client.raw import AsyncRawRconClient
from tests.support import AsyncRawEndpoint, AsyncRconEndpoint, AsyncRconFactory, RconPeer, RconReader, RconSocket, RconWriter, RunAsync, SyncRconEndpoint, SyncRconFactory


@pytest.fixture
def rcon_factory(monkeypatch: pytest.MonkeyPatch) -> Generator[SyncRconFactory, None, None]:
	endpoints: List[SyncRconEndpoint] = []

	def connect(address: Tuple[str, int], **kwargs: Any) -> socket.socket:
		endpoint = endpoints[address[1] - 1]
		sock = RconSocket(endpoint.peer)
		endpoint.sockets.append(sock)
		return cast(socket.socket, sock)

	def create(preset: Union[RconPreset, RconPresetLiteral] = RconPreset.single_packet, **kwargs: Unpack[RconClientOptions]) -> SyncRconEndpoint:
		client = RconClient.create('localhost', len(endpoints) + 1, preset, **kwargs)
		peer_preset = RconPreset[preset] if isinstance(preset, str) else preset
		endpoint = SyncRconEndpoint(client, RconPeer(peer_preset))
		endpoints.append(endpoint)
		return endpoint

	monkeypatch.setattr(socket, 'create_connection', connect)
	yield create
	for endpoint in endpoints:
		endpoint.client.close()


@pytest.fixture
def async_rcon_factory(monkeypatch: pytest.MonkeyPatch, run_async: RunAsync, memory_writer_support: None) -> Generator[AsyncRconFactory, None, None]:
	endpoints: List[AsyncRconEndpoint] = []

	async def connect(host: str, port: int, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
		endpoint = endpoints[port - 1]
		reader = RconReader(endpoint.peer)
		writer = RconWriter(endpoint.peer, reader)
		endpoint.readers.append(reader)
		endpoint.writers.append(writer)
		return reader, cast(asyncio.StreamWriter, writer)

	def create(preset: Union[RconPreset, RconPresetLiteral] = RconPreset.single_packet, **kwargs: Unpack[RconClientOptions]) -> AsyncRconEndpoint:
		client = AsyncRconClient.create('localhost', len(endpoints) + 1, preset, **kwargs)
		peer_preset = RconPreset[preset] if isinstance(preset, str) else preset
		endpoint = AsyncRconEndpoint(client, RconPeer(peer_preset))
		endpoints.append(endpoint)
		return endpoint

	monkeypatch.setattr(asyncio, 'open_connection', connect)
	yield create
	for endpoint in endpoints:
		run_async(endpoint.client.aclose())


@pytest.fixture
def async_raw_endpoint(monkeypatch: pytest.MonkeyPatch, run_async: RunAsync, memory_writer_support: None) -> Generator[AsyncRawEndpoint, None, None]:
	peer = RconPeer(RconPreset.single_packet)
	reader = RconReader(peer)
	writer = RconWriter(peer, reader)
	endpoint = AsyncRawEndpoint(AsyncRawRconClient('localhost', 1), reader, writer)
	writers = [writer]
	first_connection = True

	async def connect(host: str, port: int, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
		nonlocal first_connection
		if first_connection:
			first_connection = False
		else:
			endpoint.reader = RconReader(peer)
			endpoint.writer = RconWriter(peer, endpoint.reader)
			writers.append(endpoint.writer)
		return endpoint.reader, cast(asyncio.StreamWriter, endpoint.writer)

	monkeypatch.setattr(asyncio, 'open_connection', connect)
	yield endpoint
	for writer in writers:
		writer.release()
	run_async(endpoint.client.aclose())
