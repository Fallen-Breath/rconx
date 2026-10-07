import asyncio
import socket
from typing import Any, Generator, List, Optional, Tuple, cast

import pytest
from typing_extensions import Unpack

from rconx.client.client import AsyncRconClient, RconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset
from tests.support import AsyncRconEndpoint, AsyncRconFactory, RconPeer, RconReader, RconSocket, RconWriter, RunAsync, SyncRconEndpoint, SyncRconFactory


@pytest.fixture
def rcon_factory(monkeypatch: pytest.MonkeyPatch) -> Generator[SyncRconFactory, None, None]:
	endpoints: List[SyncRconEndpoint] = []

	def connect(address: Tuple[str, int], **kwargs: Any) -> socket.socket:
		endpoint = endpoints[address[1] - 1]
		sock = RconSocket(endpoint.peer)
		endpoint.sockets.append(sock)
		return cast(socket.socket, sock)

	def create(preset: RconPreset = RconPreset.single_packet, *, first_byte_timeout: Optional[float] = 0, **kwargs: Unpack[RconClientOptions]) -> SyncRconEndpoint:
		client = RconClient.create('localhost', len(endpoints) + 1, preset, first_byte_timeout=first_byte_timeout, **kwargs)
		endpoint = SyncRconEndpoint(client, RconPeer(preset))
		endpoints.append(endpoint)
		return endpoint

	monkeypatch.setattr(socket, 'create_connection', connect)
	yield create
	for endpoint in endpoints:
		endpoint.client.close()


@pytest.fixture
def async_rcon_factory(monkeypatch: pytest.MonkeyPatch, run_async: RunAsync) -> Generator[AsyncRconFactory, None, None]:
	endpoints: List[AsyncRconEndpoint] = []

	async def connect(host: str, port: int, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
		endpoint = endpoints[port - 1]
		reader = RconReader(endpoint.peer)
		writer = RconWriter(endpoint.peer, reader)
		endpoint.readers.append(reader)
		endpoint.writers.append(writer)
		return reader, cast(asyncio.StreamWriter, writer)

	def create(preset: RconPreset = RconPreset.single_packet, *, first_byte_timeout: Optional[float] = 0, **kwargs: Unpack[RconClientOptions]) -> AsyncRconEndpoint:
		client = AsyncRconClient.create('localhost', len(endpoints) + 1, preset, first_byte_timeout=first_byte_timeout, **kwargs)
		endpoint = AsyncRconEndpoint(client, RconPeer(preset))
		endpoints.append(endpoint)
		return endpoint

	monkeypatch.setattr(asyncio, 'open_connection', connect)
	yield create
	for endpoint in endpoints:
		run_async(endpoint.client.aclose())
