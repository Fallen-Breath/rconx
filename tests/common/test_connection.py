import socket
import struct
from typing import Any, Optional, Type, cast

import pytest

from rconx.common.connection import Connection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError, RconTimeoutError
from rconx.common.protocol import Packet
from tests.support import Clock, MemorySocket, SocketFactory, make_frame


@pytest.mark.parametrize('chunk_size', [1, 3, 1460, 65536])
def test_receive_fragmented_packet(socket_factory: SocketFactory, chunk_size: int):
	payload = bytes(range(256)) * 513
	frame = make_frame(payload)
	sock = socket_factory(frame[index:index + chunk_size] for index in range(0, len(frame), chunk_size))
	connection = Connection(cast(socket.socket, sock))
	assert connection.receive_packet() == Packet(17, 2, payload)
	assert not connection.closed


def test_receive_consecutive_packets(socket_factory: SocketFactory):
	sock = socket_factory([make_frame(b'first') + make_frame(b'second', -1, 0)])
	connection = Connection(cast(socket.socket, sock))
	assert connection.receive_packet() == Packet(17, 2, b'first')
	assert connection.receive_packet() == Packet(-1, 0, b'second')


def test_send_complete_packet(socket_factory: SocketFactory):
	sock = socket_factory()
	connection = Connection(cast(socket.socket, sock))
	connection.send_packet(Packet(17, 2, b'hello'))
	assert bytes(sock.sent) == make_frame()
	assert not connection.closed


@pytest.mark.parametrize('maximum', [None, 19, 20])
def test_send_size_limit_allows_packet(socket_factory: SocketFactory, maximum: Optional[int]):
	sock = socket_factory()
	connection = Connection(cast(socket.socket, sock), max_send_packet_size=maximum)
	connection.send_packet(Packet(17, 2, b'hello'))
	assert bytes(sock.sent) == make_frame()


@pytest.mark.parametrize('maximum', [0, 18])
def test_send_size_limit_rejects_without_losing_connection(socket_factory: SocketFactory, maximum: Optional[int]):
	sock = socket_factory([make_frame()])
	connection = Connection(cast(socket.socket, sock), max_send_packet_size=maximum)
	with pytest.raises(ValueError):
		connection.send_packet(Packet(17, 2, b'hello'))
	assert sock.sent == b''
	assert not connection.closed
	assert connection.receive_packet() == Packet(17, 2, b'hello')


def test_default_send_limit_is_unlimited(socket_factory: SocketFactory):
	sock = socket_factory()
	connection = Connection(cast(socket.socket, sock))
	payload = b'x' * DEFAULT_MAX_RECEIVE_PACKET_SIZE
	connection.send_packet(Packet(17, 2, payload))
	assert bytes(sock.sent) == make_frame(payload)


@pytest.mark.parametrize('maximum', [None, 19, 20])
def test_receive_size_limit_allows_packet(socket_factory: SocketFactory, maximum: Optional[int]):
	connection = Connection(cast(socket.socket, socket_factory([make_frame()])), max_receive_packet_size=maximum)
	assert connection.receive_packet() == Packet(17, 2, b'hello')


@pytest.mark.parametrize('maximum', [0, 18])
def test_receive_size_limit_rejects_and_closes(socket_factory: SocketFactory, maximum: Optional[int]):
	sock = socket_factory([make_frame()[:4]])
	connection = Connection(cast(socket.socket, sock), max_receive_packet_size=maximum)
	with pytest.raises(ValueError):
		connection.receive_packet()
	assert connection.closed
	assert sock.closed
	with pytest.raises(RconConnectionError):
		connection.receive_packet()


def test_default_receive_limit_includes_length_header(socket_factory: SocketFactory):
	sock = socket_factory([struct.pack('<i', DEFAULT_MAX_RECEIVE_PACKET_SIZE)])
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(ValueError):
		connection.receive_packet()
	assert connection.closed


@pytest.mark.parametrize('size', [-1, 0, 9])
def test_invalid_packet_size_closes(socket_factory: SocketFactory, size: int):
	connection = Connection(cast(socket.socket, socket_factory([struct.pack('<i', size)])))
	with pytest.raises(RconPacketDecodeError):
		connection.receive_packet()
	assert connection.closed


@pytest.mark.parametrize('prefix_size', [0, 2, 4, 8, 18])
def test_eof_closes_connection(socket_factory: SocketFactory, prefix_size: int):
	sock = socket_factory([make_frame()[:prefix_size]])
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconConnectionError):
		connection.receive_packet()
	assert connection.closed
	assert sock.closed


@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('network failure'), RconConnectionError, id='network-error'),
	pytest.param(socket.timeout('timeout'), RconTimeoutError, id='timeout'),
	pytest.param(KeyboardInterrupt(), KeyboardInterrupt, id='interruption'),
])
@pytest.mark.parametrize('operation', ['send', 'receive'])
def test_io_error_closes_connection(socket_factory: SocketFactory, error: BaseException, expected: Type[BaseException], operation: str):
	sock = socket_factory([error])
	sock.send_error = error
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(expected):
		if operation == 'send':
			connection.send_packet(Packet(17, 2, b'hello'))
		else:
			connection.receive_packet()
	assert connection.closed
	assert sock.closed


def test_cleanup_error_preserves_original_error(socket_factory: SocketFactory):
	primary = RuntimeError('send failure')
	sock = socket_factory()
	sock.send_error = primary
	sock.close_error = OSError('close failure')
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RuntimeError) as caught:
		connection.send_packet(Packet(17, 2, b'hello'))
	assert caught.value is primary
	assert connection.closed


def test_encoding_error_preserves_connection(socket_factory: SocketFactory):
	sock = socket_factory([make_frame()])
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(Exception):
		connection.send_packet(Packet(2 ** 31, 2, b'hello'))
	assert sock.sent == b''
	assert not connection.closed
	connection.send_packet(Packet(17, 2, b'hello'))
	assert bytes(sock.sent) == make_frame()
	assert connection.receive_packet() == Packet(17, 2, b'hello')


@pytest.mark.parametrize('timeout', [-1, float('inf'), float('nan')])
@pytest.mark.parametrize('operation', ['send', 'receive'])
def test_invalid_timeout_preserves_connection(socket_factory: SocketFactory, timeout: Any, operation: str):
	connection = Connection(cast(socket.socket, socket_factory([make_frame()])))
	with pytest.raises(ValueError):
		if operation == 'send':
			connection.send_packet(Packet(17, 2, b'hello'), timeout=timeout)
		else:
			connection.receive_packet(timeout=timeout)
	assert not connection.closed
	assert connection.receive_packet() == Packet(17, 2, b'hello')


@pytest.mark.parametrize('operation', ['send', 'receive'])
def test_zero_timeout_closes_connection(socket_factory: SocketFactory, operation: str):
	sock = socket_factory([make_frame()])
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		if operation == 'send':
			connection.send_packet(Packet(17, 2, b'hello'), timeout=0)
		else:
			connection.receive_packet(timeout=0)
	assert connection.closed
	assert sock.closed


def test_receive_uses_one_total_timeout(socket_factory: SocketFactory, clock: Clock):
	sock = socket_factory([make_frame()])

	def delayed_receive(sock: MemorySocket):
		delay = 0.6
		if sock.timeout is not None and sock.timeout <= delay:
			clock.advance(sock.timeout)
			raise socket.timeout('receive timeout')
		clock.advance(delay)

	sock.on_receive = delayed_receive
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		connection.receive_packet(timeout=1)
	assert connection.closed


def test_send_checks_total_timeout_after_io(socket_factory: SocketFactory, clock: Clock):
	sock = socket_factory()
	sock.on_send = lambda sock: clock.advance(2)
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		connection.send_packet(Packet(17, 2, b'hello'), timeout=1)
	assert connection.closed


@pytest.mark.parametrize('body_error', [False, True])
def test_context_closes_connection(socket_factory: SocketFactory, body_error: bool):
	sock = socket_factory()
	connection = Connection(cast(socket.socket, sock))
	if body_error:
		with pytest.raises(RuntimeError):
			with connection as entered:
				assert entered is connection
				raise RuntimeError('context failure')
	else:
		with connection as entered:
			assert entered is connection
	assert connection.closed
	assert sock.closed
	connection.close()
	with pytest.raises(RconConnectionError):
		connection.send_packet(Packet(17, 2, b'hello'))
