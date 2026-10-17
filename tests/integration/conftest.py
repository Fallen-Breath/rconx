import asyncio
import contextlib
import socket
import threading
from typing import Any, Callable, ContextManager, Iterator, List, Union

import pytest

from rconx.server.server import AsyncRconServer, RconServer
from tests.support import TcpAddress, ThreadCall


@pytest.fixture
def run_server(monkeypatch: pytest.MonkeyPatch) -> Callable[[Union[RconServer, AsyncRconServer]], ContextManager[TcpAddress]]:
	addresses: List[TcpAddress] = []
	bind = socket.socket.bind
	def observe_bind(sock: socket.socket, address: Any):
		bind(sock, address)
		bound = sock.getsockname()
		addresses.append(TcpAddress(bound[0], bound[1]))
	# Record the OS-assigned port because the server has no public listener-address query.
	monkeypatch.setattr(socket.socket, 'bind', observe_bind)

	@contextlib.contextmanager
	def run(server: Union[RconServer, AsyncRconServer]) -> Iterator[TcpAddress]:
		ready = threading.Event()
		if isinstance(server, AsyncRconServer):
			loop = asyncio.new_event_loop()
			stop_requested: asyncio.Future[None] = loop.create_future()
			async def run_async_server():
				serving = asyncio.ensure_future(server.serve(on_started=ready.set))
				await asyncio.wait([serving, stop_requested], return_when=asyncio.FIRST_COMPLETED)
				await server.shutdown()
				await serving
			def serve():
				asyncio.set_event_loop(loop)
				try:
					loop.run_until_complete(run_async_server())
				finally:
					loop.close()
			def shutdown():
				loop.call_soon_threadsafe(stop_requested.set_result, None)
		else:
			def serve():
				server.serve(on_started=ready.set)
			shutdown = server.shutdown

		serving = ThreadCall(serve)
		try:
			if not ready.wait(2):
				serving.join()
				pytest.fail('server did not start')
			yield addresses[-1]
		finally:
			if not serving.done.is_set():
				shutdown()
			serving.join()

	return run
