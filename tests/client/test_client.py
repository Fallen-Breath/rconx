import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Type, Union

import pytest

from rconx.client.client import AsyncRconClient, RconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset
from rconx.common.exceptions import RconAuthenticationError, RconConnectionError, RconProtocolError, RconStateError, RconTimeoutError
from rconx.common.protocol import PacketType
from tests.support import Clock, PacketReply, RconPeer, SyncRconFactory, TcpListenerFactory, read_packet


def test_context_cleanup_preserves_body_exception(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory()
	endpoint.client.connect()
	primary = RuntimeError('body failure')
	endpoint.socket.close_error = OSError('close failure')
	with pytest.raises(RuntimeError) as caught:
		with endpoint.client:
			raise primary
	assert caught.value is primary
	assert not endpoint.client.connected and endpoint.socket.closed


@pytest.mark.parametrize('preset', list(RconPreset))
def test_tcp_preset_exchange(tcp_listener_factory: TcpListenerFactory, preset: RconPreset):
	listener = tcp_listener_factory()
	client = RconClient.create(listener.address.host, listener.address.port, preset, first_byte_timeout=0.02)
	model = RconPeer(preset)

	def serve():
		peer = listener.accept()
		request_count = 5 if preset in (RconPreset.srcds, RconPreset.minecraft) else 3
		for _ in range(request_count):
			request = read_packet(peer)
			peer.sendall(model.respond(request))
			if preset == RconPreset.minecraft and request.packet_type == PacketType.exec_command:
				model.first_response_received = True

	with ThreadPoolExecutor(max_workers=1) as executor:
		server = executor.submit(serve)
		with client:
			client.connect(timeout=1)
			client.authenticate('password', timeout=1)
			assert client.execute('command', timeout=1).content == b'firstsecond'
			assert client.execute('next', timeout=1).content == b'firstsecond'
		server.result(timeout=3)


@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('output', [[b'first', b'', b'second'], [b'']])
@pytest.mark.parametrize('auth_prefix', [False, True])
def test_presets_collect_complete_results(rcon_factory: SyncRconFactory, preset: RconPreset, output: List[bytes], auth_prefix: bool):
	endpoint = rcon_factory(preset)
	endpoint.peer.output = output
	if auth_prefix:
		endpoint.peer.auth_replies.insert(0, PacketReply())
	client = endpoint.client
	assert not client.connected and not client.authenticated
	client.connect()
	assert client.connected and not client.authenticated
	client.authenticate('password')
	assert client.authenticated

	for command in ('first command', b'second command'):
		response = client.execute(command)
		assert response.content == b''.join(output)
		assert response.text == response.content.decode('utf8')
		assert client.connected and client.authenticated
	commands = [packet for packet in endpoint.peer.requests if packet.packet_type == PacketType.exec_command]
	assert [packet.payload for packet in commands] == [b'first command', b'second command']
	assert commands[0].request_id != commands[1].request_id


@pytest.mark.parametrize('result_before_ack', [False, True])
def test_cuberite_accepts_both_reply_orders(rcon_factory: SyncRconFactory, result_before_ack: bool):
	endpoint = rcon_factory(RconPreset.cuberite)
	endpoint.peer.result_before_ack = result_before_ack
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert endpoint.client.execute('command').content == b'firstsecond'
	assert endpoint.client.execute('next').content == b'firstsecond'


@pytest.mark.parametrize('replies, error', [
	pytest.param([PacketReply(packet_type=2, request_id=-1)], RconAuthenticationError, id='rejected'),
	pytest.param([PacketReply(), PacketReply(packet_type=2, request_id=-1)], RconAuthenticationError, id='rejected-after-prefix'),
	pytest.param([PacketReply(packet_type=2, id_offset=1)], RconProtocolError, id='wrong-result-id'),
	pytest.param([PacketReply(b'bad', 2)], RconProtocolError, id='nonempty-result'),
	pytest.param([PacketReply(packet_type=7)], RconProtocolError, id='wrong-type'),
	pytest.param([PacketReply(b'bad')], RconProtocolError, id='nonempty-prefix'),
	pytest.param([PacketReply(id_offset=1)], RconProtocolError, id='wrong-prefix-id'),
	pytest.param([PacketReply(), PacketReply(), PacketReply(packet_type=2)], RconProtocolError, id='duplicate-prefix'),
])
def test_authentication_failure_closes_and_allows_retry(rcon_factory: SyncRconFactory, replies: List[PacketReply], error: Type[BaseException]):
	endpoint = rcon_factory()
	endpoint.peer.auth_replies = replies
	client = endpoint.client
	client.connect()
	with pytest.raises(error):
		client.authenticate('password')
	assert not client.connected and not client.authenticated
	assert endpoint.socket.closed

	endpoint.peer.auth_replies = [PacketReply(packet_type=2)]
	client.connect()
	client.authenticate('password')
	assert client.execute('command').content == b'firstsecond'


@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('kind', ['wrong-id', 'wrong-type', 'session-rejected'])
def test_invalid_command_response_closes(rcon_factory: SyncRconFactory, preset: RconPreset, kind: str):
	endpoint = rcon_factory(preset)
	if kind == 'wrong-id':
		reply = PacketReply(packet_type=2 if preset == RconPreset.cuberite else 0, id_offset=100)
	elif kind == 'wrong-type':
		reply = PacketReply(packet_type=7)
	else:
		reply = PacketReply(packet_type=2, request_id=-1)
	endpoint.peer.command_replies = [reply]
	client = endpoint.client
	client.connect()
	client.authenticate('password')
	with pytest.raises(RconAuthenticationError if kind == 'session-rejected' else RconProtocolError):
		client.execute('command')
	assert not client.connected and not client.authenticated
	assert endpoint.socket.closed


@pytest.mark.parametrize('preset, replies', [
	pytest.param(RconPreset.srcds, [PacketReply(b'bad'), PacketReply()], id='srcds-nonempty-first-ending'),
	pytest.param(RconPreset.srcds, [PacketReply(), PacketReply(id_offset=1)], id='srcds-wrong-second-id'),
	pytest.param(RconPreset.srcds, [PacketReply(), PacketReply(packet_type=7)], id='srcds-wrong-second-type'),
	pytest.param(RconPreset.minecraft, [PacketReply(id_offset=1)], id='minecraft-wrong-ending-id'),
])
def test_invalid_ending_response_closes(rcon_factory: SyncRconFactory, preset: RconPreset, replies: List[PacketReply]):
	endpoint = rcon_factory(preset)
	endpoint.peer.probe_replies = replies
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	with pytest.raises(RconProtocolError):
		endpoint.client.execute('command')
	assert not endpoint.client.connected and endpoint.socket.closed


def test_cuberite_requires_empty_acknowledgement(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory(RconPreset.cuberite)
	endpoint.peer.command_replies = [PacketReply(b'one', 2), PacketReply(b'two', 2)]
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	with pytest.raises(RconProtocolError):
		endpoint.client.execute('command')
	assert not endpoint.client.authenticated


@pytest.mark.parametrize('preset', [RconPreset.srcds, RconPreset.minecraft, RconPreset.cuberite, RconPreset.idle_timeout])
def test_incomplete_exchange_is_not_success(rcon_factory: SyncRconFactory, preset: RconPreset):
	endpoint = rcon_factory(preset)
	if preset == RconPreset.cuberite:
		endpoint.peer.command_replies = [PacketReply(packet_type=2)]
	elif preset == RconPreset.idle_timeout:
		endpoint.peer.command_replies = []
	else:
		endpoint.peer.probe_replies = []
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	with pytest.raises(RconTimeoutError):
		endpoint.client.execute('command', timeout=1)
	assert not endpoint.client.connected and endpoint.socket.closed


def test_lifecycle_state_checks_and_reconnect(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory()
	client = endpoint.client
	client.close()
	client.close()
	with pytest.raises(RconStateError):
		client.authenticate('password')
	with pytest.raises(RconStateError):
		client.execute('command')
	client.connect()
	with pytest.raises(RconStateError):
		client.connect()
	with pytest.raises(RconStateError):
		client.execute('command')
	client.authenticate('password')
	with pytest.raises(RconStateError):
		client.authenticate('password')
	assert client.execute('command').content == b'firstsecond'
	client.close()
	assert not client.connected and not client.authenticated and endpoint.socket.closed
	client.close()
	client.connect()
	client.authenticate('password')
	assert client.execute('command').content == b'firstsecond'


@pytest.mark.parametrize('body_error', [False, True])
def test_context_lifecycle(rcon_factory: SyncRconFactory, body_error: bool):
	endpoint = rcon_factory()
	client = endpoint.client
	with client as entered:
		assert entered is client and not client.connected
	client.connect()
	client.authenticate('password')
	if body_error:
		with pytest.raises(RuntimeError):
			with client:
				raise RuntimeError('body failure')
	else:
		with client:
			assert client.execute('command').content == b'firstsecond'
	assert not client.connected and not client.authenticated
	assert endpoint.socket.closed


@pytest.mark.parametrize('operation', ['authenticate', 'execute'])
@pytest.mark.parametrize('body, error', [
	pytest.param('bad\x00value', ValueError, id='null-text'),
	pytest.param(b'bad\x00value', ValueError, id='null-bytes'),
	pytest.param('\u4e2d', UnicodeEncodeError, id='encoding'),
	pytest.param(1, TypeError, id='wrong-type'),
	pytest.param(b'x' * 100, ValueError, id='send-limit'),
])
def test_input_failure_preserves_session(rcon_factory: SyncRconFactory, operation: str, body: Any, error: Type[BaseException]):
	endpoint = rcon_factory(default_encoding='ascii', max_send_packet_size=30)
	client = endpoint.client
	client.connect()
	if operation == 'execute':
		client.authenticate('password')
	requests = len(endpoint.peer.requests)
	with pytest.raises(error):
		getattr(client, operation)(body)
	assert len(endpoint.peer.requests) == requests
	assert client.connected and client.authenticated == (operation == 'execute')
	if operation == 'authenticate':
		client.authenticate('password')
	assert client.execute('command').content == b'firstsecond'


@pytest.mark.parametrize('preset, maximum', [pytest.param(RconPreset.minecraft, 1460), pytest.param(RconPreset.cuberite, 1504)])
def test_preset_send_limit_counts_encoded_bytes(rcon_factory: SyncRconFactory, preset: RconPreset, maximum: int):
	endpoint = rcon_factory(preset)
	endpoint.client.connect()
	with pytest.raises(ValueError):
		endpoint.client.authenticate(b'x' * (maximum - 13))
	endpoint.client.authenticate(b'x' * (maximum - 14))
	assert endpoint.peer.requests[-1].payload == b'x' * (maximum - 14)
	with pytest.raises(ValueError):
		endpoint.client.execute('\u4e2d' * ((maximum - 14) // 3 + 1))
	assert endpoint.client.execute(b'x' * (maximum - 14)).content == b'firstsecond'


@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('maximum', [None, 14, 100, 4096])
def test_send_limit_override(rcon_factory: SyncRconFactory, preset: RconPreset, maximum: Optional[int]):
	endpoint = rcon_factory(preset, max_send_packet_size=maximum)
	endpoint.client.connect()
	payload = b'x' * (4096 if maximum is None else maximum - 14)
	endpoint.client.authenticate(payload)
	if maximum is not None:
		with pytest.raises(ValueError):
			endpoint.client.execute(payload + b'x')
	assert endpoint.client.execute(payload).content == b'firstsecond'


@pytest.mark.parametrize('preset', [RconPreset.srcds, RconPreset.single_packet, RconPreset.idle_timeout])
def test_other_presets_default_send_limit_is_unlimited(rcon_factory: SyncRconFactory, preset: RconPreset):
	endpoint = rcon_factory(preset)
	endpoint.client.connect()
	endpoint.client.authenticate(b'x' * 4096)
	assert endpoint.client.execute(b'x' * 4096).content == b'firstsecond'


@pytest.mark.parametrize('maximum, output, accepted', [
	pytest.param(0, [b''], True, id='zero-empty'),
	pytest.param(0, [b'x'], False, id='zero-nonempty'),
	pytest.param(4, [b'ab', b'cd'], True, id='exact'),
	pytest.param(3, [b'ab', b'cd'], False, id='aggregate-overflow'),
	pytest.param(None, [b'ab', b'cd'], True, id='unlimited'),
])
@pytest.mark.parametrize('preset', list(RconPreset))
def test_response_size_limit(rcon_factory: SyncRconFactory, preset: RconPreset, maximum: Optional[int], output: List[bytes], accepted: bool):
	endpoint = rcon_factory(preset, max_response_size=maximum)
	endpoint.peer.output = output
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	if accepted:
		assert endpoint.client.execute('command').content == b''.join(output)
		assert endpoint.client.authenticated
	else:
		with pytest.raises(ValueError):
			endpoint.client.execute('command')
		assert not endpoint.client.connected and not endpoint.client.authenticated
		assert endpoint.socket.closed


def test_receive_packet_limit_closes_session(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory(max_receive_packet_size=18)
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	with pytest.raises(ValueError):
		endpoint.client.execute('command')
	assert not endpoint.client.connected and endpoint.socket.closed


@pytest.mark.parametrize('unlimited', [False, True])
def test_default_aggregate_response_limit(rcon_factory: SyncRconFactory, unlimited: bool):
	options: RconClientOptions = {'max_response_size': None} if unlimited else {}
	endpoint = rcon_factory(RconPreset.srcds, **options)
	endpoint.peer.output = [b'x' * (2 * 1024 * 1024), b'y' * (2 * 1024 * 1024)]
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert len(endpoint.client.execute('command').content) == 4 * 1024 * 1024
	endpoint.peer.output.append(b'z')
	if unlimited:
		assert len(endpoint.client.execute('next').content) == 4 * 1024 * 1024 + 1
	else:
		with pytest.raises(ValueError):
			endpoint.client.execute('next')
		assert not endpoint.client.connected


@pytest.mark.parametrize('unlimited', [False, True])
def test_default_receive_packet_limit(rcon_factory: SyncRconFactory, unlimited: bool):
	options: RconClientOptions = {'max_response_size': None}
	if unlimited:
		options['max_receive_packet_size'] = None
	endpoint = rcon_factory(**options)
	endpoint.peer.output = [b'x' * (4 * 1024 * 1024 - 14)]
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert len(endpoint.client.execute('command').content) == 4 * 1024 * 1024 - 14
	endpoint.peer.output.append(b'y')
	if unlimited:
		assert len(endpoint.client.execute('next').content) == 4 * 1024 * 1024 - 13
	else:
		with pytest.raises(ValueError):
			endpoint.client.execute('next')
		assert not endpoint.client.connected and endpoint.socket.closed


def test_zero_send_limit_rejects_empty_input(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory(RconPreset.minecraft, max_send_packet_size=0)
	endpoint.client.connect()
	with pytest.raises(ValueError):
		endpoint.client.authenticate(b'')
	assert endpoint.client.connected and not endpoint.client.authenticated
	assert endpoint.peer.requests == []


@pytest.mark.parametrize('operation', ['authenticate', 'execute'])
def test_interruption_closes_session(rcon_factory: SyncRconFactory, operation: str):
	endpoint = rcon_factory()
	endpoint.client.connect()
	if operation == 'execute':
		endpoint.client.authenticate('password')
	endpoint.socket.send_error = KeyboardInterrupt()
	with pytest.raises(KeyboardInterrupt):
		getattr(endpoint.client, operation)('value')
	assert not endpoint.client.connected and not endpoint.client.authenticated
	assert endpoint.socket.closed
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert endpoint.client.execute('next').content == b'firstsecond'


def test_failed_connect_allows_retry(rcon_factory: SyncRconFactory, monkeypatch: pytest.MonkeyPatch):
	endpoint = rcon_factory()
	with monkeypatch.context() as patched:
		def fail(*args: Any, **kwargs: Any) -> socket.socket:
			raise OSError('connection failure')

		patched.setattr(socket, 'create_connection', fail)
		with pytest.raises(RconConnectionError):
			endpoint.client.connect()
	assert not endpoint.client.connected and not endpoint.client.authenticated
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert endpoint.client.execute('next').content == b'firstsecond'


def test_response_encoding_and_read_only_content(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory(default_encoding='latin1')
	endpoint.peer.output = [b'caf\xe9']
	endpoint.client.connect()
	endpoint.client.authenticate('caf\xe9')
	response = endpoint.client.execute('caf\xe9')
	assert endpoint.peer.requests[-1].payload == b'caf\xe9'
	assert response.content == b'caf\xe9' and response.text == 'caf\xe9'
	with pytest.raises(AttributeError):
		setattr(response, 'content', b'changed')
	endpoint.client.close()
	assert response.text == 'caf\xe9'


def test_response_decode_error_preserves_session(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory()
	endpoint.peer.output = [b'\xff']
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	response = endpoint.client.execute('command')
	assert response.content == b'\xff'
	with pytest.raises(UnicodeDecodeError):
		response.text
	endpoint.peer.output = [b'valid']
	assert endpoint.client.authenticated and endpoint.client.execute('next').text == 'valid'


@pytest.mark.parametrize('operation', ['authenticate', 'execute'])
def test_expired_budget_preserves_session(rcon_factory: SyncRconFactory, operation: str):
	endpoint = rcon_factory()
	endpoint.client.connect()
	if operation == 'execute':
		endpoint.client.authenticate('password')
	requests = len(endpoint.peer.requests)
	with pytest.raises(RconTimeoutError):
		getattr(endpoint.client, operation)('value', timeout=0)
	assert endpoint.client.connected and len(endpoint.peer.requests) == requests
	if operation == 'authenticate':
		endpoint.client.authenticate('password')
	assert endpoint.client.execute('command').content == b'firstsecond'


def test_command_uses_one_total_budget(rcon_factory: SyncRconFactory, clock: Clock):
	endpoint = rcon_factory(RconPreset.srcds)
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	endpoint.socket.on_send = lambda sock: clock.advance(0.3)
	endpoint.socket.on_receive = lambda sock: clock.advance(0.15)
	with pytest.raises(RconTimeoutError):
		endpoint.client.execute('command', timeout=1)
	assert not endpoint.client.connected and endpoint.socket.closed


def test_cleanup_preserves_primary_exception(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory()
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	primary = RuntimeError('send failure')
	endpoint.socket.send_error = primary
	endpoint.socket.close_error = OSError('close failure')
	with pytest.raises(RuntimeError) as caught:
		endpoint.client.execute('command')
	assert caught.value is primary
	assert not endpoint.client.connected
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert endpoint.client.execute('next').content == b'firstsecond'


@pytest.mark.parametrize('active', ['connect', 'authenticate', 'execute', 'close'])
def test_overlapping_operations_are_rejected(rcon_factory: SyncRconFactory, monkeypatch: pytest.MonkeyPatch, active: str):
	endpoint = rcon_factory()
	client = endpoint.client
	started = threading.Event()
	release = threading.Event()

	def gate():
		started.set()
		assert release.wait(3)

	if active == 'connect':
		connect = socket.create_connection

		def blocked_connect(*args: Any, **kwargs: Any) -> socket.socket:
			gate()
			return connect(*args, **kwargs)

		monkeypatch.setattr(socket, 'create_connection', blocked_connect)
	else:
		client.connect()
		if active != 'authenticate':
			client.authenticate('password')
		if active == 'close':
			endpoint.socket.on_close = gate
		else:
			endpoint.socket.on_send = lambda sock: gate()

	with ThreadPoolExecutor(max_workers=1) as executor:
		future = executor.submit(getattr(client, active), *(['value'] if active in ('authenticate', 'execute') else []))
		try:
			assert started.wait(3)
			for operation in ('connect', 'authenticate', 'execute', 'close'):
				with pytest.raises(RconStateError):
					getattr(client, operation)(*(['value'] if operation in ('authenticate', 'execute') else []))
		finally:
			release.set()
		future.result(timeout=3)

	if client.connected:
		endpoint.socket.on_send = None
		endpoint.socket.on_close = None
	else:
		client.connect()
	if not client.authenticated:
		client.authenticate('password')
	assert client.execute('next').content == b'firstsecond'


def test_guard_covers_failure_cleanup(rcon_factory: SyncRconFactory):
	endpoint = rcon_factory()
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	started = threading.Event()
	release = threading.Event()
	endpoint.socket.send_error = OSError('send failure')

	def close():
		started.set()
		assert release.wait(3)

	endpoint.socket.on_close = close
	with ThreadPoolExecutor(max_workers=1) as executor:
		future = executor.submit(endpoint.client.execute, 'command')
		try:
			assert started.wait(3)
			with pytest.raises(RconStateError):
				endpoint.client.connect()
		finally:
			release.set()
		with pytest.raises(RconConnectionError):
			future.result(timeout=3)
	endpoint.client.connect()
	endpoint.client.authenticate('password')
	assert endpoint.client.execute('next').content == b'firstsecond'


@pytest.mark.parametrize('client_type', [RconClient, AsyncRconClient])
@pytest.mark.parametrize('options, error', [
	pytest.param({'default_encoding': 'missing-encoding'}, ValueError, id='unknown-encoding'),
	pytest.param({'default_encoding': 'base64_codec'}, ValueError, id='non-text-encoding'),
	pytest.param({'max_send_packet_size': -1}, ValueError, id='negative-send'),
	pytest.param({'max_receive_packet_size': -1}, ValueError, id='negative-receive'),
	pytest.param({'max_response_size': -1}, ValueError, id='negative-result'),
	pytest.param({'max_response_size': True}, TypeError, id='boolean-result'),
])
def test_invalid_configuration(client_type: Union[Type[RconClient], Type[AsyncRconClient]], options: RconClientOptions, error: Type[BaseException]):
	with pytest.raises(error):
		client_type.create('localhost', 1, RconPreset.single_packet, **options)


@pytest.mark.parametrize('client_type', [RconClient, AsyncRconClient])
@pytest.mark.parametrize('window, error', [
	pytest.param(None, TypeError, id='missing'),
	pytest.param(-1, ValueError, id='negative'),
	pytest.param(float('inf'), ValueError, id='infinite'),
	pytest.param(float('nan'), ValueError, id='nan'),
])
def test_idle_preset_requires_valid_window(client_type: Union[Type[RconClient], Type[AsyncRconClient]], window: Any, error: Type[BaseException]):
	with pytest.raises(error):
		client_type.create('localhost', 1, RconPreset.idle_timeout, first_byte_timeout=window)


@pytest.mark.parametrize('client_type', [RconClient, AsyncRconClient])
@pytest.mark.parametrize('preset', [None, 'minecraft', 1])
def test_invalid_preset(client_type: Union[Type[RconClient], Type[AsyncRconClient]], preset: Any):
	with pytest.raises(TypeError):
		client_type.create('localhost', 1, preset)


@pytest.mark.parametrize('client_type', [RconClient, AsyncRconClient])
@pytest.mark.parametrize('preset', [RconPreset.srcds, RconPreset.minecraft, RconPreset.cuberite, RconPreset.single_packet])
def test_non_idle_presets_ignore_window(client_type: Union[Type[RconClient], Type[AsyncRconClient]], preset: RconPreset):
	client = client_type.create('localhost', 1, preset, first_byte_timeout=-1)
	assert not client.connected and not client.authenticated
