import socket
import struct
import threading
from typing import Any, Optional, Type, cast

import pytest

from rconx.common.connection import Connection, DEFAULT_MAX_RECEIVE_PACKET_SIZE
from rconx.common.exceptions import RconConnectionError, RconPacketDecodeError, RconSizeLimitError, RconStateError, RconTimeoutError
from rconx.common.protocol import Packet
from tests.support import Clock, MemorySocket, SocketFactory, TcpListenerFactory, ThreadCall, fill_socket_send_buffer, make_frame


@pytest.mark.parametrize('window', [0, 0.5])
@pytest.mark.parametrize('error', [socket.timeout(), BlockingIOError()])
def test_first_byte_window_preserves_connection(socket_factory: SocketFactory, window: float, error: BaseException):
	sock = socket_factory([error, make_frame()])
	connection = Connection(cast(socket.socket, sock))
	assert connection.try_receive_packet(first_byte_timeout=window, timeout=1) is None
	assert not connection.closed
	assert connection.receive_packet(timeout=1) == Packet(17, 2, b'hello')


@pytest.mark.parametrize('chunk_size', [1, 3, 19])
def test_try_receive_complete_packet(socket_factory: SocketFactory, chunk_size: int):
	frame = make_frame()
	connection = Connection(cast(socket.socket, socket_factory(frame[index:index + chunk_size] for index in range(0, len(frame), chunk_size))))
	assert connection.try_receive_packet(first_byte_timeout=0) == Packet(17, 2, b'hello')
	assert not connection.closed


@pytest.mark.parametrize('prefix_size', [0, 1, 3, 4, 8, 18])
@pytest.mark.parametrize('error, expected', [
	pytest.param(socket.timeout(), RconTimeoutError, id='timeout'),
	pytest.param(TimeoutError(), RconTimeoutError, id='os-timeout'),
	pytest.param(OSError(), RconConnectionError, id='network-error'),
	pytest.param(b'', RconConnectionError, id='eof'),
])
def test_try_receive_failure_closes(socket_factory: SocketFactory, prefix_size: int, error: Any, expected: Type[BaseException]):
	chunks = [make_frame()[:prefix_size], error] if prefix_size else [error]
	connection = Connection(cast(socket.socket, socket_factory(chunks)))
	if prefix_size == 0 and isinstance(error, socket.timeout):
		assert connection.try_receive_packet(first_byte_timeout=0.1) is None
		assert not connection.closed
	else:
		with pytest.raises(expected):
			connection.try_receive_packet(first_byte_timeout=0.1)
		assert connection.closed


@pytest.mark.parametrize('window', [None, -1, float('inf'), float('nan')])
def test_invalid_first_byte_window_preserves_connection(socket_factory: SocketFactory, window: Any):
	connection = Connection(cast(socket.socket, socket_factory([make_frame()])))
	with pytest.raises((TypeError, ValueError)):
		connection.try_receive_packet(first_byte_timeout=window)
	assert connection.receive_packet() == Packet(17, 2, b'hello')


def test_first_byte_window_does_not_limit_packet_body(socket_factory: SocketFactory, clock: Clock):
	sock = socket_factory([make_frame()])
	reads = 0

	def receive(sock: MemorySocket):
		nonlocal reads
		reads += 1
		if reads > 1:
			clock.advance(0.2)

	sock.on_receive = receive
	connection = Connection(cast(socket.socket, sock))
	assert connection.try_receive_packet(first_byte_timeout=0.1, timeout=1) == Packet(17, 2, b'hello')


def test_try_receive_total_budget_closes(socket_factory: SocketFactory, clock: Clock):
	budget = 0.5
	sock = socket_factory([socket.timeout()])
	sock.on_receive = lambda sock: clock.advance(budget)
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		connection.try_receive_packet(first_byte_timeout=1, timeout=budget)
	assert connection.closed


def test_try_receive_checks_packet_size(socket_factory: SocketFactory):
	connection = Connection(cast(socket.socket, socket_factory([make_frame()])), max_receive_packet_size=18)
	with pytest.raises(RconSizeLimitError):
		connection.try_receive_packet(first_byte_timeout=0)
	assert connection.closed


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
	with pytest.raises(RconSizeLimitError):
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
	with pytest.raises(RconSizeLimitError):
		connection.receive_packet()
	assert connection.closed
	assert sock.closed
	with pytest.raises(RconStateError):
		connection.receive_packet()


def test_default_receive_limit_includes_length_header(socket_factory: SocketFactory):
	sock = socket_factory([struct.pack('<i', DEFAULT_MAX_RECEIVE_PACKET_SIZE)])
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconSizeLimitError):
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
	pytest.param(TimeoutError('timeout'), RconTimeoutError, id='os-timeout'),
	pytest.param(KeyboardInterrupt(), KeyboardInterrupt, id='interruption'),
])
@pytest.mark.parametrize('operation', ['send', 'receive'])
def test_io_error_closes_connection(socket_factory: SocketFactory, error: BaseException, expected: Type[BaseException], operation: str):
	sock = socket_factory([error])
	sock.send_error = error
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(expected) as caught:
		if operation == 'send':
			connection.send_packet(Packet(17, 2, b'hello'))
		else:
			connection.receive_packet()
	if isinstance(error, OSError):
		assert caught.value.__cause__ is error
	else:
		assert caught.value is error
	assert connection.closed
	assert sock.closed


def test_closed_connection_rejects_try_receive(socket_factory: SocketFactory):
	connection = Connection(cast(socket.socket, socket_factory()))
	connection.close()
	with pytest.raises(RconStateError):
		connection.try_receive_packet(first_byte_timeout=0)


@pytest.mark.parametrize('method', ['getsockname', 'getpeername'])
@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('query failed'), RconConnectionError, id='io-error'),
	pytest.param(socket.timeout('query timed out'), RconTimeoutError, id='socket-timeout'),
	pytest.param(TimeoutError('query timed out'), RconTimeoutError, id='os-timeout'),
])
def test_address_query_error_preserves_connection(
	socket_factory: SocketFactory, monkeypatch: pytest.MonkeyPatch, method: str, error: OSError, expected: Type[BaseException],
):
	sock = socket_factory([make_frame()])
	monkeypatch.setattr(sock, 'getsockname', lambda: ('127.0.0.1', 12345), raising=False)
	monkeypatch.setattr(sock, 'getpeername', lambda: ('127.0.0.1', 25575), raising=False)

	def fail_query():
		raise error

	monkeypatch.setattr(sock, method, fail_query)
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(expected) as caught:
		connection.get_info()
	assert caught.value.__cause__ is error
	assert not connection.closed
	assert connection.receive_packet() == Packet(17, 2, b'hello')


@pytest.mark.parametrize('error, expected', [
	pytest.param(OSError('close failed'), RconConnectionError, id='io-error'),
	pytest.param(socket.timeout('close timed out'), RconTimeoutError, id='socket-timeout'),
	pytest.param(TimeoutError('close timed out'), RconTimeoutError, id='os-timeout'),
	pytest.param(RuntimeError('close failed'), RuntimeError, id='other-error'),
])
def test_close_error_preserves_cause_and_closed_state(socket_factory: SocketFactory, error: BaseException, expected: Type[BaseException]):
	sock = socket_factory()
	sock.close_error = error
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(expected) as caught:
		connection.close()
	if isinstance(error, OSError):
		assert caught.value.__cause__ is error
	else:
		assert caught.value is error
	assert connection.closed
	assert sock.closed
	connection.close()


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


@pytest.mark.parametrize('first_delay, later_delay, first_budget, later_budget', [
	pytest.param(0.8, 0.1, None, 0.3, id='body-budget-starts-after-first-byte'),
	pytest.param(0.1, 0.3, 0.2, None, id='first-byte-budget-ends-at-first-byte'),
])
def test_receive_phase_budgets_are_independent(
	socket_factory: SocketFactory, clock: Clock, first_delay: float, later_delay: float, first_budget: Optional[float], later_budget: Optional[float],
):
	sock = socket_factory([make_frame()])
	delays = iter([first_delay])

	def receive(sock: MemorySocket):
		delay = next(delays, later_delay)
		if sock.timeout is not None and sock.timeout <= delay:
			clock.advance(sock.timeout)
			raise socket.timeout()
		clock.advance(delay)

	sock.on_receive = receive
	connection = Connection(cast(socket.socket, sock))
	assert connection.receive_packet(first_byte_timeout=first_budget, after_first_byte_timeout=later_budget) == Packet(17, 2, b'hello')
	assert not connection.closed


def test_receive_first_byte_timeout_closes(socket_factory: SocketFactory, clock: Clock):
	sock = socket_factory([make_frame()])
	def receive(sock: MemorySocket):
		delay = 0.2
		if sock.timeout is not None and sock.timeout <= delay:
			clock.advance(sock.timeout)
			raise socket.timeout()
		clock.advance(delay)

	sock.on_receive = receive
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		connection.receive_packet(first_byte_timeout=0.1, after_first_byte_timeout=10)
	assert connection.closed


@pytest.mark.parametrize('total_budget, later_budget', [(None, 1), (1, 10)])
def test_receive_fragmented_packet_does_not_renew_budgets(socket_factory: SocketFactory, clock: Clock, total_budget: Optional[float], later_budget: float):
	frame = make_frame()
	sock = socket_factory(frame[index:index + 1] for index in range(len(frame)))

	def receive(sock: MemorySocket):
		delay = 0.2
		if sock.timeout is not None and sock.timeout <= delay:
			clock.advance(sock.timeout)
			raise socket.timeout()
		clock.advance(delay)

	sock.on_receive = receive
	connection = Connection(cast(socket.socket, sock))
	with pytest.raises(RconTimeoutError):
		connection.receive_packet(timeout=total_budget, first_byte_timeout=10, after_first_byte_timeout=later_budget)
	assert connection.closed


@pytest.mark.parametrize('first_byte', [False, True], ids=['after-first-byte', 'first-byte'])
def test_invalid_receive_phase_timeout_preserves_connection(socket_factory: SocketFactory, first_byte: bool):
	connection = Connection(cast(socket.socket, socket_factory([make_frame()])))
	with pytest.raises(ValueError):
		if first_byte:
			connection.receive_packet(first_byte_timeout=-1)
		else:
			connection.receive_packet(after_first_byte_timeout=-1)
	assert not connection.closed
	assert connection.receive_packet() == Packet(17, 2, b'hello')


@pytest.mark.parametrize('family', [socket.AF_INET, socket.AF_INET6], ids=['ipv4', 'ipv6'])
def test_get_info_returns_read_only_address_snapshot(tcp_listener_factory: TcpListenerFactory, family: int):
	listener = tcp_listener_factory(family)
	with socket.create_connection((listener.address.host, listener.address.port), timeout=2) as peer:
		connection = Connection(listener.accept())
		try:
			info = connection.get_info()
			remote_address = peer.getsockname()
			assert info.local_address.host == listener.address.host
			assert info.local_address.port == listener.address.port
			assert info.remote_address.host == remote_address[0]
			assert info.remote_address.port == remote_address[1]
			with pytest.raises(AttributeError):
				setattr(info, 'local_address', info.remote_address)
			with pytest.raises(AttributeError):
				setattr(info.local_address, 'host', 'different')
			with pytest.raises(AttributeError):
				setattr(info.local_address, 'port', 0)
			connection.close()
			assert info.local_address.host == listener.address.host
			assert info.local_address.port == listener.address.port
			assert info.remote_address.host == remote_address[0]
			assert info.remote_address.port == remote_address[1]
			with pytest.raises(RconStateError):
				connection.get_info()
		finally:
			connection.close()


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
	with pytest.raises(RconStateError):
		connection.send_packet(Packet(17, 2, b'hello'))


@pytest.mark.parametrize('send', [False, True], ids=['blocked-receive', 'blocked-send'])
def test_close_interrupts_socket_io(tcp_listener_factory: TcpListenerFactory, send: bool):
	listener = tcp_listener_factory()
	with socket.create_connection((listener.address.host, listener.address.port), timeout=2) as peer:
		peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
		sock = listener.accept()
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
		if send:
			fill_socket_send_buffer(sock)
		connection = Connection(sock)
		entered = threading.Event()
		def operation():
			entered.set()
			with pytest.raises(RconConnectionError):
				if send:
					connection.send_packet(Packet(1, 2, b'x' * (4 * 1024 * 1024)))
				else:
					connection.receive_packet()
		worker = ThreadCall(operation)
		try:
			assert entered.wait(2)
			assert not worker.done.wait(0.1)
			connection.close()
			worker.join()
			assert connection.closed
		finally:
			connection.close()
			peer.close()
			worker.join()
