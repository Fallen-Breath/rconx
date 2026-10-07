import socket
from typing import Any, NoReturn, Type, Union

import pytest

from rconx.client.raw import AsyncRawRconClient, LocalAddress, RawRconClient
from rconx.common.exceptions import RconConnectionError, RconTimeoutError
from rconx.common.protocol import Packet
from tests.support import SocketAddress, TcpListenerFactory, make_frame, read_exactly


def test_first_byte_window_allows_reuse(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		peer = server.accept()
		assert client.try_receive_packet(first_byte_timeout=0, timeout=1) is None
		peer.sendall(make_frame())
		assert client.try_receive_packet(first_byte_timeout=1, timeout=1) == Packet(17, 2, b'hello')
		client.send_packet(Packet(20, 2, b'next'), timeout=1)
		assert read_exactly(peer, 18) == make_frame(b'next', 20)


@pytest.mark.parametrize('prefix_size', [0, 1, 8])
def test_try_receive_failure_allows_reconnect(tcp_listener_factory: TcpListenerFactory, prefix_size: int):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		first = server.accept()
		first.sendall(make_frame()[:prefix_size])
		first.shutdown(socket.SHUT_WR)
		with pytest.raises(RconConnectionError):
			client.try_receive_packet(first_byte_timeout=1, timeout=1)
		client.connect(timeout=1)
		second = server.accept()
		second.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_packet_exchange(tcp_listener_factory: TcpListenerFactory, family: int):
	server = tcp_listener_factory(family)
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		peer = server.accept()
		client.send_packet(Packet(17, 2, b'hello'), timeout=1)
		assert read_exactly(peer, 19) == make_frame()
		peer.sendall(make_frame(b'first') + make_frame(b'second', -1, 0))
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'first')
		assert client.receive_packet(timeout=1) == Packet(-1, 0, b'second')
	assert peer.recv(1) == b''


def test_context_requires_explicit_connect(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	client = RawRconClient(server.address.host, server.address.port)
	with client as entered:
		assert entered is client
		with pytest.raises(RconConnectionError):
			client.send_packet(Packet(17, 2, b'hello'))
		with pytest.raises(RconConnectionError):
			client.receive_packet()
		client.connect(timeout=1)
		peer = server.accept()
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('body_error', [False, True])
def test_context_closes_on_exit(tcp_listener_factory: TcpListenerFactory, body_error: bool):
	server = tcp_listener_factory()
	client = RawRconClient(server.address.host, server.address.port)
	if body_error:
		with pytest.raises(RuntimeError):
			with client:
				client.connect(timeout=1)
				peer = server.accept()
				raise RuntimeError('context failure')
	else:
		with client:
			client.connect(timeout=1)
			peer = server.accept()
	assert peer.recv(1) == b''
	with pytest.raises(RconConnectionError):
		client.send_packet(Packet(17, 2, b'hello'))


def test_repeated_close_and_reconnect(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.close()
		client.close()
		client.connect(timeout=1)
		first = server.accept()
		client.close()
		client.close()
		assert first.recv(1) == b''
		with pytest.raises(RconConnectionError):
			client.receive_packet()
		client.connect(timeout=1)
		second = server.accept()
		second.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


def test_duplicate_connect_preserves_connection(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		peer = server.accept()
		with pytest.raises(RconConnectionError):
			client.connect(timeout=1)
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('connection failure'), RconConnectionError, id='connection-error'),
	pytest.param(socket.timeout('connection timeout'), RconTimeoutError, id='timeout'),
])
def test_connection_failure_allows_retry(tcp_listener_factory: TcpListenerFactory, monkeypatch: pytest.MonkeyPatch, error: BaseException, expected: Type[BaseException]):
	server = tcp_listener_factory()

	def timeout(*args: Any, **kwargs: Any) -> NoReturn:
		raise error

	with RawRconClient(server.address.host, server.address.port) as client:
		with monkeypatch.context() as patched:
			patched.setattr(socket, 'create_connection', timeout)
			with pytest.raises(expected):
				client.connect(timeout=1)
		client.connect(timeout=1)
		peer = server.accept()
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('timeout, error', [
	pytest.param(0, RconTimeoutError, id='zero'),
	pytest.param(-1, ValueError, id='negative'),
	pytest.param(float('inf'), ValueError, id='infinite'),
	pytest.param(float('nan'), ValueError, id='nan'),
	pytest.param('1', TypeError, id='wrong-type'),
])
def test_invalid_connect_timeout_allows_retry(tcp_listener_factory: TcpListenerFactory, timeout: Any, error: Type[BaseException]):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		with pytest.raises(error):
			client.connect(timeout=timeout)
		client.connect(timeout=1)
		peer = server.accept()
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('prefix_size', [0, 2, 8])
def test_receive_failure_allows_reconnect(tcp_listener_factory: TcpListenerFactory, prefix_size: int):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		first = server.accept()
		first.sendall(make_frame()[:prefix_size])
		first.shutdown(socket.SHUT_WR)
		with pytest.raises(RconConnectionError):
			client.receive_packet(timeout=1)
		with pytest.raises(RconConnectionError):
			client.send_packet(Packet(17, 2, b'hello'))
		client.connect(timeout=1)
		second = server.accept()
		second.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('operation', ['send', 'receive'])
def test_zero_io_timeout_allows_reconnect(tcp_listener_factory: TcpListenerFactory, operation: str):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1)
		first = server.accept()
		with pytest.raises(RconTimeoutError):
			if operation == 'send':
				client.send_packet(Packet(17, 2, b'hello'), timeout=0)
			else:
				client.receive_packet(timeout=0)
		assert first.recv(1) == b''
		client.connect(timeout=1)
		second = server.accept()
		second.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


def test_send_limit_preserves_connection(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port, max_send_packet_size=18) as client:
		client.connect(timeout=1)
		peer = server.accept()
		with pytest.raises(ValueError):
			client.send_packet(Packet(17, 2, b'hello'), timeout=1)
		client.send_packet(Packet(17, 2, b''), timeout=1)
		assert read_exactly(peer, 14) == make_frame(b'')
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


def test_receive_limit_closes_and_allows_reconnect(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port, max_receive_packet_size=18) as client:
		client.connect(timeout=1)
		first = server.accept()
		first.sendall(make_frame()[:4])
		with pytest.raises(ValueError):
			client.receive_packet(timeout=1)
		client.connect(timeout=1)
		second = server.accept()
		second.sendall(make_frame(b''))
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'')


@pytest.mark.parametrize('fixed_port', [False, True])
@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_local_binding(tcp_listener_factory: TcpListenerFactory, fixed_port: bool, family: int):
	server = tcp_listener_factory(family)
	port = 0
	if fixed_port:
		with socket.socket(family, socket.SOCK_STREAM) as reservation:
			reservation.bind((server.address.host, 0))
			port = reservation.getsockname()[1]
	with RawRconClient(server.address.host, server.address.port) as client:
		client.connect(timeout=1, local_address=LocalAddress(server.address.host, port))
		peer = server.accept()
		address: SocketAddress = peer.getpeername()
		assert address[0] == server.address.host
		assert address[1] == port if fixed_port else address[1] > 0
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


def test_occupied_local_port_allows_retry(tcp_listener_factory: TcpListenerFactory):
	server = tcp_listener_factory()
	with RawRconClient(server.address.host, server.address.port) as client:
		with pytest.raises(RconConnectionError):
			client.connect(timeout=1, local_address=LocalAddress(server.address.host, server.address.port))
		client.connect(timeout=1, local_address=LocalAddress(server.address.host))
		peer = server.accept()
		peer.sendall(make_frame())
		assert client.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('client_type', [RawRconClient, AsyncRawRconClient])
@pytest.mark.parametrize('limit, error', [
	pytest.param(-1, ValueError, id='negative'),
	pytest.param(True, TypeError, id='boolean'),
	pytest.param(1.0, TypeError, id='float'),
])
@pytest.mark.parametrize('direction', ['send', 'receive'])
def test_invalid_packet_size_configuration(client_type: Union[Type[RawRconClient], Type[AsyncRawRconClient]], limit: Any, error: Type[BaseException], direction: str):
	with pytest.raises(error):
		if direction == 'send':
			client_type('127.0.0.1', 1, max_send_packet_size=limit)
		else:
			client_type('127.0.0.1', 1, max_receive_packet_size=limit)
