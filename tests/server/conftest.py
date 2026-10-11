import asyncio
import socket
from asyncio.base_events import Server
from typing import Any, Generator, List, Optional, cast

import pytest

from tests.support import RunAsync

_Socket = socket.socket


@pytest.fixture(params=['127.0.0.1', '::1'], ids=['ipv4', 'ipv6'])
def listening_host(request: pytest.FixtureRequest) -> str:
	host = str(request.param)
	if host == '::1':
		try:
			with _Socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
				probe.bind((host, 0))
		except OSError:
			pytest.skip('ipv6 loopback is unavailable')
	return host


@pytest.fixture
def sync_listeners(monkeypatch: pytest.MonkeyPatch) -> Generator[List[socket.socket], None, None]:
	create_socket = socket.socket
	sockets: List[socket.socket] = []
	def create(family: int = socket.AF_INET, type: int = socket.SOCK_STREAM, proto: int = 0, fileno: Optional[int] = None) -> socket.socket:
		sock = create_socket(family, type, proto, fileno)
		sockets.append(sock)
		return sock
	monkeypatch.setattr(socket, 'socket', create)
	yield sockets
	for sock in sockets:
		sock.close()


@pytest.fixture
def async_listeners(monkeypatch: pytest.MonkeyPatch, run_async: RunAsync) -> Generator[List[Server], None, None]:
	start_server = asyncio.start_server
	servers: List[Server] = []
	async def start(*args: Any, **kwargs: Any) -> Server:
		server = cast(Server, await start_server(*args, **kwargs))
		servers.append(server)
		return server
	monkeypatch.setattr(asyncio, 'start_server', start)
	yield servers
	for server in servers:
		server.close()
		run_async(server.wait_closed())
