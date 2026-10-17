import socket
import threading
import time
from typing import Any, List, Optional, Tuple, Type, Union, cast

import pytest

from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconConnectionError, RconStateError
from rconx.common.protocol import Packet, PacketType
from rconx.server.server import AsyncRconServer, RconServer
from rconx.server.server_options import RconServerOptions
from tests.server.highlevel_support import COMMAND_RESULT, PROBE_END, ProtocolCase, create_authentication_responses, get_protocol_cases
from tests.support import TcpListenerFactory, ThreadCall, read_packet


_Socket = socket.socket


class _RunningServer:
	def __init__(self, server: RconServer, listeners: List[socket.socket]):
		self.server = server
		self.listeners = listeners
		self.peers: List[socket.socket] = []
		self.serving: ThreadCall
		self.address: Tuple[str, int]

	def __enter__(self) -> '_RunningServer':
		ready = threading.Event()
		self.serving = ThreadCall(lambda: self.server.serve(on_started=ready.set))
		if not ready.wait(2):
			self.serving.join()
			pytest.fail('server did not start')
		address = self.listeners[0].getsockname()
		self.address = address[0], address[1]
		return self

	def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any):
		for peer in self.peers:
			peer.close()
		self.server.shutdown()
		self.serving.join()

	def connect(self) -> socket.socket:
		peer = socket.create_connection(self.address, timeout=2)
		self.peers.append(peer)
		return peer


def _send(peer: socket.socket, *packets: Packet):
	peer.sendall(b''.join(packet.encode_framed() for packet in packets))


def _authenticate(peer: socket.socket, password: bytes = b'password'):
	_send(peer, Packet(1, PacketType.auth, password))
	for expected in create_authentication_responses(1):
		assert read_packet(peer) == expected


def _assert_closed(peer: socket.socket):
	try:
		assert peer.recv(1) == b''
	except ConnectionResetError:
		pass


def _receive_result(peer: socket.socket, request_id: int = 2, probe_id: int = 3) -> List[Packet]:
	packets: List[Packet] = []
	while True:
		packet = read_packet(peer)
		if packet.request_id == probe_id:
			assert packet == Packet(probe_id, PacketType.response_value, b'')
			assert read_packet(peer) == Packet(probe_id, PacketType.response_value, PROBE_END)
			assert packets
			return packets
		assert packet.request_id == request_id
		assert packet.packet_type == PacketType.response_value
		packets.append(packet)


def _echo(content: RconContent, info: ConnectionInfo) -> bytes:
	return content.content


@pytest.mark.parametrize('case', get_protocol_cases(), ids=lambda case: case.name)
def test_protocol(sync_listeners: List[socket.socket], case: ProtocolCase):
	commands: List[bytes] = []
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		commands.append(content.content)
		return COMMAND_RESULT
	server = RconServer('127.0.0.1', 0, authentication=case.authentication, command_handler=command)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_send(peer, *case.requests)
		for expected in case.responses:
			assert read_packet(peer) == expected
		if case.closes:
			_assert_closed(peer)
		assert commands == case.commands


def test_custom_callbacks_receive_content_and_addresses(sync_listeners: List[socket.socket], listening_host: str):
	inputs: List[RconContent] = []
	infos: List[ConnectionInfo] = []
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		inputs.append(content)
		infos.append(info)
		return content.text == 'caf\xe9'
	def command(content: RconContent, info: ConnectionInfo) -> str:
		inputs.append(content)
		infos.append(info)
		return content.text + '!'
	server = RconServer(listening_host, 0, authentication=authenticate, command_handler=command, default_encoding='latin1')
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer, b'caf\xe9')
		_send(peer, Packet(2, PacketType.exec_command, b'caf\xe9'))
		assert read_packet(peer) == Packet(2, PacketType.response_value, b'caf\xe9!')
		assert [content.content for content in inputs] == [b'caf\xe9', b'caf\xe9']
		address = peer.getsockname()
		for info in infos:
			assert (info.local_address.host, info.local_address.port) == running.address
			assert (info.remote_address.host, info.remote_address.port) == (address[0], address[1])


@pytest.mark.parametrize('result,packet_limit', [
	pytest.param(b'', None, id='empty-bytes'),
	pytest.param('', None, id='empty-text'),
	pytest.param(b'abcdef' * 2000, None, id='protocol-size'),
	pytest.param('caf\xe9' * 3000, 31, id='configured-size-text'),
])
def test_command_result_encoding_and_splitting(sync_listeners: List[socket.socket], result: Union[bytes, str], packet_limit: Optional[int]):
	def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		return result
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, max_send_packet_size=packet_limit)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		_send(peer, Packet(2, PacketType.exec_command, b''), Packet(3, PacketType.response_value, b''))
		packets = _receive_result(peer)
		assert all(len(packet.encode_framed()) <= min(4100, packet_limit or 4100) for packet in packets)
		assert b''.join(packet.payload for packet in packets) == (result.encode('utf8') if isinstance(result, str) else result)


@pytest.mark.parametrize('failure', ['auth-result', 'auth-error', 'command-result', 'command-error', 'encoding', 'nul-bytes', 'nul-text'])
def test_callback_failure_only_ends_current_session(sync_listeners: List[socket.socket], failure: str):
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		if content.content == b'bad' and failure.startswith('auth'):
			if failure == 'auth-error':
				raise RuntimeError('authentication failed')
			return cast(bool, 1)
		return True
	def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		if content.content != b'bad':
			return b'ok'
		if failure == 'command-error':
			raise RuntimeError('command failed')
		if failure == 'encoding':
			return '\xe9'
		if failure == 'nul-bytes':
			return b'bad\x00result'
		if failure == 'nul-text':
			return 'bad\x00result'
		return cast(bytes, None)
	server = RconServer('127.0.0.1', 0, authentication=authenticate, command_handler=command, default_encoding='ascii')
	with _RunningServer(server, sync_listeners) as running:
		good = running.connect()
		_authenticate(good)
		bad = running.connect()
		if failure.startswith('auth'):
			_send(bad, Packet(1, PacketType.auth, b'bad'))
		else:
			_authenticate(bad)
			_send(bad, Packet(2, PacketType.exec_command, b'bad'))
		_assert_closed(bad)
		for peer in (good, running.connect()):
			if peer is not good:
				_authenticate(peer)
			_send(peer, Packet(2, PacketType.exec_command, b'good'))
			assert read_packet(peer) == Packet(2, PacketType.response_value, b'ok')


@pytest.mark.parametrize('maximum,result,allowed', [
	pytest.param(0, b'', True, id='zero-empty'),
	pytest.param(0, b'x', False, id='zero-nonempty'),
	pytest.param(3, b'abc', True, id='exact-size'),
	pytest.param(3, b'abcd', False, id='oversized'),
	pytest.param(None, b'x' * (4 * 1024 * 1024 + 1), True, id='above-default-limit'),
	pytest.param(3, 'éé', False, id='encoded-oversized'),
	pytest.param(4, 'éé', True, id='encoded-exact-size'),
])
def test_result_size_limit(sync_listeners: List[socket.socket], maximum: Optional[int], result: Union[bytes, str], allowed: bool):
	def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		return result
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, max_response_size=maximum)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		_send(peer, Packet(2, PacketType.exec_command, b'command'))
		if allowed:
			_send(peer, Packet(3, PacketType.response_value, b''))
			packets = _receive_result(peer)
			assert b''.join(packet.payload for packet in packets) == (result.encode('utf8') if isinstance(result, str) else result)
		else:
			_assert_closed(peer)


def test_receive_packet_size_boundary(sync_listeners: List[socket.socket]):
	server = RconServer('127.0.0.1', 0, authentication=b'pw', command_handler=_echo, max_receive_packet_size=16)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer, b'pw')
		_send(peer, Packet(2, PacketType.exec_command, b'xx'))
		assert read_packet(peer) == Packet(2, PacketType.response_value, b'xx')
		_send(peer, Packet(3, PacketType.exec_command, b'xxx'))
		_assert_closed(peer)


def test_malformed_packet_only_ends_current_session(sync_listeners: List[socket.socket]):
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo)
	with _RunningServer(server, sync_listeners) as running:
		bad = running.connect()
		bad.sendall(b'\x09\x00\x00\x00')
		_assert_closed(bad)
		good = running.connect()
		_authenticate(good)
		_send(good, Packet(2, PacketType.exec_command, b'good'))
		assert read_packet(good) == Packet(2, PacketType.response_value, b'good')


@pytest.mark.parametrize('limit_send', [False, True], ids=['receive', 'send'])
@pytest.mark.parametrize('maximum', [0, 13])
def test_undersized_packet_limit_fails_during_exchange(sync_listeners: List[socket.socket], limit_send: bool, maximum: int):
	server = RconServer(
		'127.0.0.1', 0, authentication=b'', command_handler=_echo,
		max_send_packet_size=maximum if limit_send else None, max_receive_packet_size=None if limit_send else maximum,
	)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_send(peer, Packet(1, PacketType.auth, b''))
		_assert_closed(peer)


@pytest.mark.parametrize('server_type', [RconServer, AsyncRconServer])
@pytest.mark.parametrize('options,error', [
	pytest.param(RconServerOptions(max_connections=0), ValueError, id='zero-connections'),
	pytest.param(RconServerOptions(max_connections=True), TypeError, id='boolean-connections'),
	pytest.param(RconServerOptions(max_response_size=-1), ValueError, id='negative-result-limit'),
	pytest.param(RconServerOptions(max_receive_packet_size=-1), ValueError, id='negative-packet-limit'),
	pytest.param(RconServerOptions(default_encoding='missing-encoding'), ValueError, id='unknown-encoding'),
	pytest.param(RconServerOptions(default_encoding='base64_codec'), ValueError, id='non-text-encoding'),
	pytest.param(RconServerOptions(authentication_timeout=-1), ValueError, id='negative-timeout'),
	pytest.param(RconServerOptions(send_timeout=float('inf')), ValueError, id='infinite-timeout'),
])
def test_invalid_configuration(server_type: Union[Type[RconServer], Type[AsyncRconServer]], options: RconServerOptions, error: Type[Exception]):
	with pytest.raises(error):
		server_type('127.0.0.1', 0, authentication=b'', command_handler=cast(Any, _echo), **options)


@pytest.mark.parametrize('server_type', [RconServer, AsyncRconServer])
@pytest.mark.parametrize('password', [b'a\x00b', 'a\x00b'])
def test_fixed_password_rejects_null(server_type: Union[Type[RconServer], Type[AsyncRconServer]], password: Union[bytes, str]):
	with pytest.raises(ValueError):
		server_type('127.0.0.1', 0, authentication=password, command_handler=cast(Any, _echo))


def test_lifecycle_and_shutdown_before_serve(sync_listeners: List[socket.socket]):
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo)
	server.shutdown()
	with _RunningServer(server, sync_listeners) as running:
		_authenticate(running.connect())
		with pytest.raises(RconStateError):
			server.serve()
	server.shutdown()
	with pytest.raises(RconStateError):
		server.serve()
	with pytest.raises(OSError):
		socket.create_connection(running.address, timeout=1)


def test_failed_start_consumes_serve(tcp_listener_factory: TcpListenerFactory):
	occupied = tcp_listener_factory()
	server = RconServer(occupied.address.host, occupied.address.port, authentication=b'', command_handler=_echo)
	with pytest.raises(RconConnectionError):
		server.serve()
	server.shutdown()
	with pytest.raises(RconStateError):
		server.serve()


def test_callback_can_shutdown_server(sync_listeners: List[socket.socket]):
	stopped = threading.Event()
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		server.shutdown()
		stopped.set()
		return b''
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		_send(peer, Packet(2, PacketType.exec_command, b'stop'))
		assert stopped.wait(2)
		running.serving.join()
		_assert_closed(peer)


def test_shutdown_returns_while_serve_waits_for_callback(sync_listeners: List[socket.socket]):
	entered = threading.Event()
	release = threading.Event()
	finished = threading.Event()
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		entered.set()
		assert release.wait(2)
		finished.set()
		return b'done'
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
	with _RunningServer(server, sync_listeners) as running:
		try:
			peer = running.connect()
			_authenticate(peer)
			_send(peer, Packet(2, PacketType.exec_command, b'wait'))
			assert entered.wait(2)
			server.shutdown()
			_assert_closed(peer)
			assert not running.serving.done.wait(0.05)
			assert not finished.is_set()
			release.set()
			running.serving.join()
			assert finished.is_set()
		finally:
			release.set()


@pytest.mark.parametrize('waiting_for', ['callback', 'response'])
def test_connection_requests_are_serial_and_connections_concurrent(sync_listeners: List[socket.socket], monkeypatch: pytest.MonkeyPatch, waiting_for: str):
	entered = threading.Event()
	release = threading.Event()
	after = threading.Event()
	blocked_remote: Optional[Tuple[str, int]] = None
	if waiting_for == 'response':
		class BlockedSocket(_Socket):
			def listen(self, backlog: int = 5):
				super().listen(backlog)
				sync_listeners.append(self)
			def sendall(self, data: Any, flags: int = 0):
				if self.getpeername()[:2] == blocked_remote:
					entered.set()
					assert release.wait(2)
				return super().sendall(data, flags)
		monkeypatch.setattr(socket, 'socket', BlockedSocket)
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		if content.content == b'slow' and waiting_for == 'callback':
			entered.set()
			assert release.wait(2)
		elif content.content == b'after':
			after.set()
		return content.content
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
	with _RunningServer(server, sync_listeners) as running:
		try:
			first = running.connect()
			_authenticate(first)
			blocked_remote = first.getsockname()[:2]
			_send(first, Packet(2, PacketType.exec_command, b'slow'), Packet(3, PacketType.exec_command, b'after'))
			assert entered.wait(2)
			second = running.connect()
			_authenticate(second)
			_send(second, Packet(2, PacketType.exec_command, b'fast'))
			assert read_packet(second) == Packet(2, PacketType.response_value, b'fast')
			assert not after.is_set()
			release.set()
			assert read_packet(first) == Packet(2, PacketType.response_value, b'slow')
			assert read_packet(first) == Packet(3, PacketType.response_value, b'after')
		finally:
			release.set()


def test_capacity_includes_authentication_and_disconnected_callback(sync_listeners: List[socket.socket]):
	working = threading.Event()
	release = threading.Event()
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		working.set()
		assert release.wait(2)
		return b'done'
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, max_connections=1)
	with _RunningServer(server, sync_listeners) as running:
		try:
			first = running.connect()
			_assert_closed(running.connect())
			_authenticate(first)
			_send(first, Packet(2, PacketType.exec_command, b'wait'))
			assert working.wait(2)
			first.close()
			_assert_closed(running.connect())
			release.set()
			deadline = time.monotonic() + 2
			while True:
				peer = running.connect()
				try:
					_authenticate(peer)
					break
				except (EOFError, ConnectionResetError, BrokenPipeError):
					assert time.monotonic() < deadline, 'session capacity was not released'
					peer.close()
					time.sleep(0.005)
		finally:
			release.set()


def test_unlimited_capacity_exceeds_default_limit(sync_listeners: List[socket.socket]):
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, max_connections=None)
	with _RunningServer(server, sync_listeners) as running:
		peers: List[socket.socket] = []
		for _ in range(101):
			peer = running.connect()
			_authenticate(peer)
			peers.append(peer)
		for peer in (peers[0], peers[-1]):
			_send(peer, Packet(2, PacketType.exec_command, b'active'), Packet(3, PacketType.response_value, b''))
			assert b''.join(packet.payload for packet in _receive_result(peer)) == b'active'


@pytest.mark.parametrize('prefix', [b'', Packet(1, PacketType.auth, b'password').encode_framed()[:1], Packet(1, PacketType.auth, b'password').encode_framed()[:-1]], ids=['silent', 'first-byte', 'incomplete-body'])
def test_authentication_deadline_requires_complete_packet(sync_listeners: List[socket.socket], prefix: bytes):
	called = threading.Event()
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		called.set()
		return True
	server = RconServer('127.0.0.1', 0, authentication=authenticate, command_handler=_echo, authentication_timeout=0.05)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		peer.sendall(prefix)
		_assert_closed(peer)
		assert not called.is_set()


def test_authentication_deadline_excludes_callback(sync_listeners: List[socket.socket]):
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		time.sleep(0.12)
		return True
	server = RconServer('127.0.0.1', 0, authentication=authenticate, command_handler=_echo, authentication_timeout=0.05, receive_timeout=0.05)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		_send(peer, Packet(2, PacketType.exec_command, b'after authentication'))
		assert read_packet(peer) == Packet(2, PacketType.response_value, b'after authentication')


def test_idle_timeout_excludes_callback_and_resumes_after_result(sync_listeners: List[socket.socket]):
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		if content.content == b'wait':
			time.sleep(0.12)
		return b'done'
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, idle_timeout=0.05)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		_send(peer, Packet(2, PacketType.exec_command, b'wait'))
		assert read_packet(peer) == Packet(2, PacketType.response_value, b'done')
		_send(peer, Packet(3, PacketType.exec_command, b'next'))
		assert read_packet(peer) == Packet(3, PacketType.response_value, b'done')
		_assert_closed(peer)


def test_receive_timeout_starts_after_first_byte(sync_listeners: List[socket.socket]):
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, receive_timeout=0.05)
	with _RunningServer(server, sync_listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		time.sleep(0.12)
		_send(peer, Packet(2, PacketType.exec_command, b'still open'))
		assert read_packet(peer) == Packet(2, PacketType.response_value, b'still open')
		peer.sendall(Packet(3, PacketType.exec_command, b'incomplete').encode_framed()[:1])
		_assert_closed(peer)


@pytest.mark.parametrize('delay,expires', [(0.02, False), (0.1, True)], ids=['per-packet-budget', 'blocked-packet'])
def test_send_timeout_applies_per_packet(monkeypatch: pytest.MonkeyPatch, delay: float, expires: bool):
	listeners: List[socket.socket] = []
	delaying = threading.Event()
	class DelayedSocket(_Socket):
		def listen(self, backlog: int = 5):
			super().listen(backlog)
			listeners.append(self)
		def sendall(self, data: Any, flags: int = 0):
			if delaying.is_set() and self.getsockname()[1] == listeners[0].getsockname()[1]:
				time.sleep(delay)
			return super().sendall(data, flags)
	monkeypatch.setattr(socket, 'socket', DelayedSocket)
	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, send_timeout=0.05, max_send_packet_size=31)
	with _RunningServer(server, listeners) as running:
		peer = running.connect()
		_authenticate(peer)
		delaying.set()
		_send(peer, Packet(2, PacketType.exec_command, b'x' * 60), Packet(3, PacketType.exec_command, b'next'))
		chunks: List[bytes] = []
		while True:
			try:
				packet = read_packet(peer)
			except (EOFError, ConnectionResetError):
				assert expires
				assert b''.join(chunks) != b'x' * 60
				break
			if packet.request_id == 3:
				assert not expires
				assert packet == Packet(3, PacketType.response_value, b'next')
				assert b''.join(chunks) == b'x' * 60
				break
			assert packet.request_id == 2
			chunks.append(packet.payload)
