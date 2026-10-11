import socket
import threading
from functools import partial
from queue import Queue
from typing import Callable, List, Tuple

import pytest
from typing_extensions import Unpack

from rconx.common.connection import Connection
from rconx.common.connection_options import ConnectionOptions
from rconx.common.exceptions import RconConnectionError, RconStateError
from rconx.common.protocol import Packet
from rconx.server.raw import RawRconServer
from rconx.server.server_connection import ServerConnection
from tests.support import ThreadCall, TcpListenerFactory, read_packet

_Socket = socket.socket


def test_exchange_and_handoff_thread(sync_listeners: List[socket.socket], listening_host: str):
	ready = threading.Event()
	handler_threads: List[int] = []
	def handler(connection: Connection):
		handler_threads.append(threading.get_ident())
		with connection:
			for _ in range(2):
				connection.send_packet(connection.receive_packet())
	server = RawRconServer(listening_host, 0, handler)
	call = ThreadCall(lambda: server.serve(on_started=ready.set))
	try:
		if not ready.wait(2):
			call.join()
			pytest.fail('listener did not start')
		with socket.create_connection(sync_listeners[0].getsockname()[:2], timeout=2) as peer:
			for packet in [Packet(1, 2, b'first'), Packet(2, 0, b'\x00\xff')]:
				peer.sendall(packet.encode_framed())
				assert read_packet(peer) == packet
			assert peer.recv(1) == b''
		assert handler_threads == [call.thread.ident]
	finally:
		server.shutdown()
		call.join()


@pytest.mark.parametrize('close_error', [False, True], ids=['normal-close', 'close-failure'])
def test_shutdown_closes_long_lived_connections(sync_listeners: List[socket.socket], monkeypatch: pytest.MonkeyPatch, close_error: bool):
	ready = threading.Event()
	entered = threading.Event()
	release = threading.Event()
	accepted: Queue[Connection] = Queue()
	created: List[ServerConnection] = []
	failure = OSError('connection close failed')
	if close_error:
		def create(sock: socket.socket, *, on_close: Callable[[Connection], None], **kwargs: Unpack[ConnectionOptions]) -> ServerConnection:
			fail = len(created) == 1
			def notify(connection: Connection):
				on_close(connection)
				if fail:
					raise failure
			connection = ServerConnection(sock, on_close=notify, **kwargs)
			created.append(connection)
			return connection
		monkeypatch.setattr('rconx.server.raw.ServerConnection', create)
	def handler(connection: Connection):
		accepted.put(connection)
		if close_error and len(created) == 3:
			entered.set()
			assert release.wait(2)
	server = RawRconServer('127.0.0.1', 0, handler)
	server.shutdown()
	call = ThreadCall(lambda: server.serve(on_started=ready.set))
	peers: List[socket.socket] = []
	workers: List[ThreadCall] = []
	try:
		assert ready.wait(2)
		address = sync_listeners[0].getsockname()[:2]
		connections: List[Connection] = []
		for _ in range(3):
			peers.append(socket.create_connection(address, timeout=2))
			connections.append(accepted.get(timeout=2))
		connections[0].close()
		assert peers[0].recv(1) == b''
		def receive(connection: Connection):
			with pytest.raises(RconConnectionError):
				connection.receive_packet()
		for connection in connections[1:]:
			workers.append(ThreadCall(partial(receive, connection)))
		for worker in workers:
			assert not worker.done.wait(0.05)
		if close_error:
			assert entered.wait(2)
			with pytest.raises(RconConnectionError) as caught:
				server.shutdown()
			assert caught.value.__cause__ is failure
		else:
			server.shutdown()
		release.set()
		call.join()
		for worker in workers:
			worker.join()
		assert all(connection.closed for connection in connections)
		assert all(peer.recv(1) == b'' for peer in peers[1:])
		with pytest.raises(OSError):
			socket.create_connection(address, timeout=1)
		server.shutdown()
		with pytest.raises(RconStateError):
			server.serve()
	finally:
		release.set()
		server.shutdown()
		for peer in peers:
			peer.close()
		call.join()
		for worker in workers:
			worker.join()


def test_bind_failure_consumes_serve(tcp_listener_factory: TcpListenerFactory):
	occupied = tcp_listener_factory()
	started: List[bool] = []
	server = RawRconServer(occupied.address.host, occupied.address.port, lambda connection: None)
	with pytest.raises(RconConnectionError) as caught:
		server.serve(on_started=lambda: started.append(True))
	assert isinstance(caught.value.__cause__, OSError)
	assert not started
	server.shutdown()
	with pytest.raises(RconStateError):
		server.serve()


def test_start_callback_failure_closes_listener(sync_listeners: List[socket.socket]):
	failure = RuntimeError('startup callback')
	def fail():
		raise failure
	server = RawRconServer('127.0.0.1', 0, lambda connection: None)
	with pytest.raises(RuntimeError) as caught:
		server.serve(on_started=fail)
	assert caught.value is failure
	assert sync_listeners[0].fileno() == -1
	with pytest.raises(RconStateError):
		server.serve()


def test_handoff_failure_keeps_serving(sync_listeners: List[socket.socket]):
	ready = threading.Event()
	connections: List[Connection] = []
	def handler(connection: Connection):
		connections.append(connection)
		if len(connections) == 1:
			raise RuntimeError('handoff failure')
		with connection:
			connection.send_packet(connection.receive_packet())
	server = RawRconServer('127.0.0.1', 0, handler)
	call = ThreadCall(lambda: server.serve(on_started=ready.set))
	try:
		assert ready.wait(2)
		address = sync_listeners[0].getsockname()[:2]
		with socket.create_connection(address, timeout=2) as first:
			assert first.recv(1) == b''
		assert connections[0].closed
		with socket.create_connection(address, timeout=2) as second:
			packet = Packet(1, 2, b'after failure')
			second.sendall(packet.encode_framed())
			assert read_packet(second) == packet
	finally:
		server.shutdown()
		call.join()


def test_shutdown_during_startup(sync_listeners: List[socket.socket], monkeypatch: pytest.MonkeyPatch):
	resolve = socket.getaddrinfo
	entered = threading.Event()
	release = threading.Event()
	started: List[bool] = []
	def delayed_resolve(*args, **kwargs):
		entered.set()
		assert release.wait(2)
		return resolve(*args, **kwargs)
	monkeypatch.setattr(socket, 'getaddrinfo', delayed_resolve)
	server = RawRconServer('127.0.0.1', 0, lambda connection: None)
	call = ThreadCall(lambda: server.serve(on_started=lambda: started.append(True)))
	try:
		assert entered.wait(2)
		server.shutdown()
	finally:
		release.set()
		call.join()
	assert not started
	assert sync_listeners[0].fileno() == -1


def test_shutdown_during_accept(monkeypatch: pytest.MonkeyPatch):
	accepted = threading.Event()
	release = threading.Event()
	ready = threading.Event()
	listeners: List[socket.socket] = []
	class DelayedAccept(_Socket):
		def listen(self, backlog: int = 5):
			super().listen(backlog)
			listeners.append(self)
		def accept(self) -> Tuple[socket.socket, Tuple[str, int]]:
			client, address = super().accept()
			accepted.set()
			assert release.wait(2)
			return client, address
	monkeypatch.setattr(socket, 'socket', DelayedAccept)
	handed_off: List[Connection] = []
	server = RawRconServer('127.0.0.1', 0, handed_off.append)
	call = ThreadCall(lambda: server.serve(on_started=ready.set))
	peer = None
	try:
		assert ready.wait(2)
		peer = socket.create_connection(listeners[0].getsockname()[:2], timeout=2)
		assert accepted.wait(2)
		server.shutdown()
		release.set()
		call.join()
		assert peer.recv(1) == b''
		assert not handed_off
	finally:
		release.set()
		server.shutdown()
		if peer is not None:
			peer.close()
		call.join()
