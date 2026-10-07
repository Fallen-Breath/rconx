import asyncio
from typing import Any, List, Optional, Tuple, Type

import pytest

from rconx.client.client import AsyncRconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset
from rconx.common.exceptions import RconAuthenticationError, RconConnectionError, RconProtocolError, RconStateError
from rconx.common.protocol import PacketType
from tests.support import AsyncRconFactory, PacketReply, RconPeer, RunAsync, TcpListenerFactory, read_async_packet


@pytest.mark.parametrize('active', ['connect', 'authenticate', 'execute', 'aclose'])
def test_overlapping_operations_are_rejected(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, monkeypatch: pytest.MonkeyPatch, active: str):
	async def scenario():
		endpoint = async_rcon_factory()
		client = endpoint.client
		started = asyncio.Event()
		release = asyncio.Event()
		if active == 'connect':
			connect = asyncio.open_connection

			async def blocked_connect(*args: Any, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
				started.set()
				await release.wait()
				return await connect(*args, **kwargs)

			monkeypatch.setattr(asyncio, 'open_connection', blocked_connect)
		else:
			await client.connect()
			if active != 'authenticate':
				await client.authenticate('password')
			if active == 'aclose':
				endpoint.writer.close_gate = release
				started = endpoint.writer.close_started
			else:
				endpoint.writer.drain_gate = release
				endpoint.writer.drain_started.clear()
				started = endpoint.writer.drain_started

		task = asyncio.ensure_future(getattr(client, active)(*(['value'] if active in ('authenticate', 'execute') else [])))
		try:
			await started.wait()
			for operation in ('connect', 'authenticate', 'execute', 'aclose'):
				with pytest.raises(RconStateError):
					await getattr(client, operation)(*(['value'] if operation in ('authenticate', 'execute') else []))
		finally:
			release.set()
			await task

		if client.connected:
			endpoint.writer.drain_gate = None
		else:
			await client.connect()
		if not client.authenticated:
			await client.authenticate('password')
		assert (await client.execute('next')).content == b'firstsecond'

	run_async(scenario())


def test_guard_covers_failure_cleanup(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		endpoint.peer.command_replies = [PacketReply(id_offset=1)]
		endpoint.writer.close_gate = asyncio.Event()
		task = asyncio.ensure_future(endpoint.client.execute('command'))
		try:
			await endpoint.writer.close_started.wait()
			with pytest.raises(RconStateError):
				await endpoint.client.connect()
		finally:
			endpoint.writer.close_gate.set()
			with pytest.raises(RconProtocolError):
				await task
		assert endpoint.writer.released
		endpoint.peer.command_replies = None
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert (await endpoint.client.execute('next')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('operation', ['authenticate', 'execute'])
@pytest.mark.parametrize('stage', ['send', 'receive'])
def test_cancelled_operation_closes_and_allows_retry(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, operation: str, stage: str):
	async def scenario():
		endpoint = async_rcon_factory()
		client = endpoint.client
		await client.connect()
		if operation == 'execute':
			await client.authenticate('password')
		if stage == 'send':
			endpoint.writer.drain_gate = asyncio.Event()
			endpoint.writer.drain_started.clear()
			started = endpoint.writer.drain_started
		else:
			if operation == 'authenticate':
				endpoint.peer.auth_replies = []
			else:
				endpoint.peer.command_replies = []
			endpoint.reader.pending.clear()
			started = endpoint.reader.pending

		task = asyncio.ensure_future(getattr(client, operation)('value'))
		await started.wait()
		task.cancel()
		with pytest.raises(asyncio.CancelledError):
			await task
		assert not client.connected and not client.authenticated
		assert endpoint.writer.released
		endpoint.peer.auth_replies = [PacketReply(packet_type=2)]
		endpoint.peer.command_replies = None
		await client.connect()
		await client.authenticate('password')
		assert (await client.execute('next')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset', [RconPreset.srcds, RconPreset.minecraft, RconPreset.cuberite, RconPreset.idle_timeout])
def test_cancelled_incomplete_exchange_is_not_success(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		if preset == RconPreset.cuberite:
			endpoint.peer.command_replies = [PacketReply(packet_type=2)]
		elif preset == RconPreset.idle_timeout:
			endpoint.peer.command_replies = []
		else:
			endpoint.peer.probe_replies = []
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		endpoint.reader.pending.clear()
		task = asyncio.ensure_future(endpoint.client.execute('command'))
		await endpoint.reader.pending.wait()
		assert not task.done()
		task.cancel()
		with pytest.raises(asyncio.CancelledError):
			await task
		assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


def test_outer_timeout_closes_session(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		endpoint.peer.command_replies = []
		task = asyncio.ensure_future(endpoint.client.execute('command'))
		with pytest.raises(asyncio.TimeoutError):
			await asyncio.wait_for(task, 0.001)
		# Python 3.6 wait_for can return before the cancelled operation has finished cleaning up.
		with pytest.raises(asyncio.CancelledError):
			await task
		assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


def test_context_cleanup_preserves_body_exception(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		await endpoint.client.connect()
		primary = RuntimeError('body failure')
		endpoint.writer.close_error = OSError('close failure')
		with pytest.raises(RuntimeError) as caught:
			async with endpoint.client:
				raise primary
		assert caught.value is primary
		assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('unlimited', [False, True])
def test_default_aggregate_response_limit(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, unlimited: bool):
	async def scenario():
		options: RconClientOptions = {'max_response_size': None} if unlimited else {}
		endpoint = async_rcon_factory(RconPreset.srcds, **options)
		endpoint.peer.output = [b'x' * (2 * 1024 * 1024), b'y' * (2 * 1024 * 1024)]
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert len((await endpoint.client.execute('command')).content) == 4 * 1024 * 1024
		endpoint.peer.output.append(b'z')
		if unlimited:
			assert len((await endpoint.client.execute('next')).content) == 4 * 1024 * 1024 + 1
		else:
			with pytest.raises(ValueError):
				await endpoint.client.execute('next')
			assert not endpoint.client.connected

	run_async(scenario())


@pytest.mark.parametrize('unlimited', [False, True])
def test_default_receive_packet_limit(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, unlimited: bool):
	async def scenario():
		options: RconClientOptions = {'max_response_size': None}
		if unlimited:
			options['max_receive_packet_size'] = None
		endpoint = async_rcon_factory(**options)
		endpoint.peer.output = [b'x' * (4 * 1024 * 1024 - 14)]
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert len((await endpoint.client.execute('command')).content) == 4 * 1024 * 1024 - 14
		endpoint.peer.output.append(b'y')
		if unlimited:
			assert len((await endpoint.client.execute('next')).content) == 4 * 1024 * 1024 - 13
		else:
			with pytest.raises(ValueError):
				await endpoint.client.execute('next')
			assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


def test_zero_send_limit_rejects_empty_input(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory(RconPreset.minecraft, max_send_packet_size=0)
		await endpoint.client.connect()
		with pytest.raises(ValueError):
			await endpoint.client.authenticate(b'')
		assert endpoint.client.connected and not endpoint.client.authenticated
		assert endpoint.peer.requests == []

	run_async(scenario())


@pytest.mark.parametrize('cancel', [False, True])
def test_failed_connect_allows_retry(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, monkeypatch: pytest.MonkeyPatch, cancel: bool):
	async def scenario():
		endpoint = async_rcon_factory()
		started = asyncio.Event()
		with monkeypatch.context() as patched:
			async def fail(*args: Any, **kwargs: Any) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
				if cancel:
					started.set()
					await asyncio.Event().wait()
				raise OSError('connection failure')

			patched.setattr(asyncio, 'open_connection', fail)
			if cancel:
				task = asyncio.ensure_future(endpoint.client.connect())
				await started.wait()
				task.cancel()
				with pytest.raises(asyncio.CancelledError):
					await task
			else:
				with pytest.raises(RconConnectionError):
					await endpoint.client.connect()
		assert not endpoint.client.connected and not endpoint.client.authenticated
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert (await endpoint.client.execute('next')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset', list(RconPreset))
def test_tcp_preset_exchange(run_async: RunAsync, tcp_listener_factory: TcpListenerFactory, preset: RconPreset):
	async def scenario():
		listener = tcp_listener_factory()
		client = AsyncRconClient.create(listener.address.host, listener.address.port, preset, first_byte_timeout=0.02)
		model = RconPeer(preset)

		async def serve():
			peer = await listener.accept_async()
			request_count = 5 if preset in (RconPreset.srcds, RconPreset.minecraft) else 3
			for _ in range(request_count):
				request = await read_async_packet(peer.reader)
				peer.writer.write(model.respond(request))
				await peer.writer.drain()
				if preset == RconPreset.minecraft and request.packet_type == PacketType.exec_command:
					model.first_response_received = True

		server = asyncio.ensure_future(serve())
		try:
			async with client:
				await client.connect()
				await client.authenticate('password')
				assert (await client.execute('command')).content == b'firstsecond'
				assert (await client.execute('next')).content == b'firstsecond'
			await server
		finally:
			if not server.done():
				server.cancel()
				await asyncio.gather(server, return_exceptions=True)

	run_async(scenario())



@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('output', [[b'first', b'', b'second'], [b'']])
@pytest.mark.parametrize('auth_prefix', [False, True])
def test_presets_collect_complete_results(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, output: List[bytes], auth_prefix: bool):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		endpoint.peer.output = output
		if auth_prefix:
			endpoint.peer.auth_replies.insert(0, PacketReply())
		client = endpoint.client
		assert not client.connected and not client.authenticated
		await client.connect()
		assert client.connected and not client.authenticated
		await client.authenticate('password')
		assert client.authenticated

		for command in ('first command', b'second command'):
			response = (await client.execute(command))
			assert response.content == b''.join(output)
			assert response.text == response.content.decode('utf8')
			assert client.connected and client.authenticated
		commands = [packet for packet in endpoint.peer.requests if packet.packet_type == PacketType.exec_command]
		assert [packet.payload for packet in commands] == [b'first command', b'second command']
		assert commands[0].request_id != commands[1].request_id

	run_async(scenario())


@pytest.mark.parametrize('result_before_ack', [False, True])
def test_cuberite_accepts_both_reply_orders(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, result_before_ack: bool):
	async def scenario():
		endpoint = async_rcon_factory(RconPreset.cuberite)
		endpoint.peer.result_before_ack = result_before_ack
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert (await endpoint.client.execute('command')).content == b'firstsecond'
		assert (await endpoint.client.execute('next')).content == b'firstsecond'

	run_async(scenario())


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
def test_authentication_failure_closes_and_allows_retry(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, replies: List[PacketReply], error: Type[BaseException]):
	async def scenario():
		endpoint = async_rcon_factory()
		endpoint.peer.auth_replies = replies
		client = endpoint.client
		await client.connect()
		with pytest.raises(error):
			await client.authenticate('password')
		assert not client.connected and not client.authenticated
		assert endpoint.writer.released

		endpoint.peer.auth_replies = [PacketReply(packet_type=2)]
		await client.connect()
		await client.authenticate('password')
		assert (await client.execute('command')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('kind', ['wrong-id', 'wrong-type', 'session-rejected'])
def test_invalid_command_response_closes(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, kind: str):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		if kind == 'wrong-id':
			reply = PacketReply(packet_type=2 if preset == RconPreset.cuberite else 0, id_offset=100)
		elif kind == 'wrong-type':
			reply = PacketReply(packet_type=7)
		else:
			reply = PacketReply(packet_type=2, request_id=-1)
		endpoint.peer.command_replies = [reply]
		client = endpoint.client
		await client.connect()
		await client.authenticate('password')
		with pytest.raises(RconAuthenticationError if kind == 'session-rejected' else RconProtocolError):
			await client.execute('command')
		assert not client.connected and not client.authenticated
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('preset, replies', [
	pytest.param(RconPreset.srcds, [PacketReply(b'bad'), PacketReply()], id='srcds-nonempty-first-ending'),
	pytest.param(RconPreset.srcds, [PacketReply(), PacketReply(id_offset=1)], id='srcds-wrong-second-id'),
	pytest.param(RconPreset.srcds, [PacketReply(), PacketReply(packet_type=7)], id='srcds-wrong-second-type'),
	pytest.param(RconPreset.minecraft, [PacketReply(id_offset=1)], id='minecraft-wrong-ending-id'),
])
def test_invalid_ending_response_closes(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, replies: List[PacketReply]):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		endpoint.peer.probe_replies = replies
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		with pytest.raises(RconProtocolError):
			await endpoint.client.execute('command')
		assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


def test_cuberite_requires_empty_acknowledgement(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory(RconPreset.cuberite)
		endpoint.peer.command_replies = [PacketReply(b'one', 2), PacketReply(b'two', 2)]
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		with pytest.raises(RconProtocolError):
			await endpoint.client.execute('command')
		assert not endpoint.client.authenticated

	run_async(scenario())


def test_lifecycle_state_checks_and_reconnect(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		client = endpoint.client
		await client.aclose()
		await client.aclose()
		with pytest.raises(RconStateError):
			await client.authenticate('password')
		with pytest.raises(RconStateError):
			await client.execute('command')
		await client.connect()
		with pytest.raises(RconStateError):
			await client.connect()
		with pytest.raises(RconStateError):
			await client.execute('command')
		await client.authenticate('password')
		with pytest.raises(RconStateError):
			await client.authenticate('password')
		assert (await client.execute('command')).content == b'firstsecond'
		await client.aclose()
		assert not client.connected and not client.authenticated and endpoint.writer.released
		await client.aclose()
		await client.connect()
		await client.authenticate('password')
		assert (await client.execute('command')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('body_error', [False, True])
def test_context_lifecycle(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, body_error: bool):
	async def scenario():
		endpoint = async_rcon_factory()
		client = endpoint.client
		async with client as entered:
			assert entered is client and not client.connected
		await client.connect()
		await client.authenticate('password')
		if body_error:
			with pytest.raises(RuntimeError):
				async with client:
					raise RuntimeError('body failure')
		else:
			async with client:
				assert (await client.execute('command')).content == b'firstsecond'
		assert not client.connected and not client.authenticated
		assert endpoint.writer.released

	run_async(scenario())


@pytest.mark.parametrize('operation', ['authenticate', 'execute'])
@pytest.mark.parametrize('body, error', [
	pytest.param('bad\x00value', ValueError, id='null-text'),
	pytest.param(b'bad\x00value', ValueError, id='null-bytes'),
	pytest.param('\u4e2d', UnicodeEncodeError, id='encoding'),
	pytest.param(1, TypeError, id='wrong-type'),
	pytest.param(b'x' * 100, ValueError, id='send-limit'),
])
def test_input_failure_preserves_session(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, operation: str, body: Any, error: Type[BaseException]):
	async def scenario():
		endpoint = async_rcon_factory(default_encoding='ascii', max_send_packet_size=30)
		client = endpoint.client
		await client.connect()
		if operation == 'execute':
			await client.authenticate('password')
		requests = len(endpoint.peer.requests)
		with pytest.raises(error):
			await getattr(client, operation)(body)
		assert len(endpoint.peer.requests) == requests
		assert client.connected and client.authenticated == (operation == 'execute')
		if operation == 'authenticate':
			await client.authenticate('password')
		assert (await client.execute('command')).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset, maximum', [pytest.param(RconPreset.minecraft, 1460), pytest.param(RconPreset.cuberite, 1504)])
def test_preset_send_limit_counts_encoded_bytes(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, maximum: int):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		await endpoint.client.connect()
		with pytest.raises(ValueError):
			await endpoint.client.authenticate(b'x' * (maximum - 13))
		await endpoint.client.authenticate(b'x' * (maximum - 14))
		assert endpoint.peer.requests[-1].payload == b'x' * (maximum - 14)
		with pytest.raises(ValueError):
			await endpoint.client.execute('\u4e2d' * ((maximum - 14) // 3 + 1))
		assert (await endpoint.client.execute(b'x' * (maximum - 14))).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset', list(RconPreset))
@pytest.mark.parametrize('maximum', [None, 14, 100, 4096])
def test_send_limit_override(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, maximum: Optional[int]):
	async def scenario():
		endpoint = async_rcon_factory(preset, max_send_packet_size=maximum)
		await endpoint.client.connect()
		payload = b'x' * (4096 if maximum is None else maximum - 14)
		await endpoint.client.authenticate(payload)
		if maximum is not None:
			with pytest.raises(ValueError):
				await endpoint.client.execute(payload + b'x')
		assert (await endpoint.client.execute(payload)).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('preset', [RconPreset.srcds, RconPreset.single_packet, RconPreset.idle_timeout])
def test_other_presets_default_send_limit_is_unlimited(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset):
	async def scenario():
		endpoint = async_rcon_factory(preset)
		await endpoint.client.connect()
		await endpoint.client.authenticate(b'x' * 4096)
		assert (await endpoint.client.execute(b'x' * 4096)).content == b'firstsecond'

	run_async(scenario())


@pytest.mark.parametrize('maximum, output, accepted', [
	pytest.param(0, [b''], True, id='zero-empty'),
	pytest.param(0, [b'x'], False, id='zero-nonempty'),
	pytest.param(4, [b'ab', b'cd'], True, id='exact'),
	pytest.param(3, [b'ab', b'cd'], False, id='aggregate-overflow'),
	pytest.param(None, [b'ab', b'cd'], True, id='unlimited'),
])
@pytest.mark.parametrize('preset', list(RconPreset))
def test_response_size_limit(run_async: RunAsync, async_rcon_factory: AsyncRconFactory, preset: RconPreset, maximum: Optional[int], output: List[bytes], accepted: bool):
	async def scenario():
		endpoint = async_rcon_factory(preset, max_response_size=maximum)
		endpoint.peer.output = output
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		if accepted:
			assert (await endpoint.client.execute('command')).content == b''.join(output)
			assert endpoint.client.authenticated
		else:
			with pytest.raises(ValueError):
				await endpoint.client.execute('command')
			assert not endpoint.client.connected and not endpoint.client.authenticated
			assert endpoint.writer.released

	run_async(scenario())


def test_receive_packet_limit_closes_session(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory(max_receive_packet_size=18)
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		with pytest.raises(ValueError):
			await endpoint.client.execute('command')
		assert not endpoint.client.connected and endpoint.writer.released

	run_async(scenario())


def test_response_encoding_and_read_only_content(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory(default_encoding='latin1')
		endpoint.peer.output = [b'caf\xe9']
		await endpoint.client.connect()
		await endpoint.client.authenticate('caf\xe9')
		response = (await endpoint.client.execute('caf\xe9'))
		assert endpoint.peer.requests[-1].payload == b'caf\xe9'
		assert response.content == b'caf\xe9' and response.text == 'caf\xe9'
		with pytest.raises(AttributeError):
			setattr(response, 'content', b'changed')
		await endpoint.client.aclose()
		assert response.text == 'caf\xe9'

	run_async(scenario())


def test_response_decode_error_preserves_session(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		endpoint.peer.output = [b'\xff']
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		response = (await endpoint.client.execute('command'))
		assert response.content == b'\xff'
		with pytest.raises(UnicodeDecodeError):
			response.text
		endpoint.peer.output = [b'valid']
		assert endpoint.client.authenticated and (await endpoint.client.execute('next')).text == 'valid'

	run_async(scenario())


def test_cleanup_preserves_primary_exception(run_async: RunAsync, async_rcon_factory: AsyncRconFactory):
	async def scenario():
		endpoint = async_rcon_factory()
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		primary = RuntimeError('send failure')
		endpoint.writer.write_error = primary
		endpoint.writer.transport.abort_error = OSError('close failure')
		with pytest.raises(RuntimeError) as caught:
			await endpoint.client.execute('command')
		assert caught.value is primary
		assert not endpoint.client.connected
		await endpoint.client.connect()
		await endpoint.client.authenticate('password')
		assert (await endpoint.client.execute('next')).content == b'firstsecond'

	run_async(scenario())
