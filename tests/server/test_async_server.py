import asyncio
import contextlib
import struct
from asyncio.base_events import Server
from typing import Any, List, Optional, Tuple, Union, cast

import pytest

from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconStateError
from rconx.common.protocol import Packet, PacketType
from rconx.server.server import AsyncRconServer
from tests.server.highlevel_support import COMMAND_RESULT, PROBE_END, ProtocolCase, create_authentication_responses, get_protocol_cases
from tests.support import AsyncPeer, RunAsync, read_async_packet


class _RunningServer:
	def __init__(self, server: AsyncRconServer, listeners: List[Server]):
		self.server = server
		self.listeners = listeners
		self.peers: List[AsyncPeer] = []
		self.serving: asyncio.Future[None]
		self.address: Tuple[str, int]

	async def __aenter__(self) -> '_RunningServer':
		ready = asyncio.Event()
		self.serving = asyncio.ensure_future(self.server.serve(on_started=ready.set))
		await ready.wait()
		sockets = self.listeners[-1].sockets
		assert sockets
		address = sockets[0].getsockname()
		self.address = address[0], address[1]
		return self

	async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any):
		for peer in self.peers:
			peer.writer.close()
		await self.server.shutdown()
		with contextlib.suppress(asyncio.CancelledError):
			await self.serving

	async def connect(self) -> AsyncPeer:
		reader, writer = await asyncio.open_connection(*self.address)
		peer = AsyncPeer(reader, writer)
		self.peers.append(peer)
		return peer


async def _send(peer: AsyncPeer, *packets: Packet):
	peer.writer.write(b''.join(packet.encode_framed() for packet in packets))
	await peer.writer.drain()


async def _authenticate(peer: AsyncPeer, password: bytes = b'password'):
	await _send(peer, Packet(1, PacketType.auth, password))
	for expected in create_authentication_responses(1):
		assert await read_async_packet(peer.reader) == expected


async def _receive_result(peer: AsyncPeer, request_id: int = 2, probe_id: int = 3) -> List[Packet]:
	packets: List[Packet] = []
	while True:
		packet = await read_async_packet(peer.reader)
		if packet.request_id == probe_id:
			assert packet == Packet(probe_id, PacketType.response_value, b'')
			assert await read_async_packet(peer.reader) == Packet(probe_id, PacketType.response_value, PROBE_END)
			assert packets
			return packets
		assert packet.request_id == request_id
		assert packet.packet_type == PacketType.response_value
		packets.append(packet)


async def _assert_closed(peer: AsyncPeer):
	try:
		assert await peer.reader.read(1) == b''
	except ConnectionResetError:
		pass


async def _echo(content: RconContent, info: ConnectionInfo) -> bytes:
	return content.content


@pytest.mark.parametrize('case', get_protocol_cases(), ids=lambda case: case.name)
def test_protocol(run_async: RunAsync, async_listeners: List[Server], case: ProtocolCase):
	async def scenario():
		commands: List[bytes] = []
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			commands.append(content.content)
			return COMMAND_RESULT
		server = AsyncRconServer('127.0.0.1', 0, authentication=case.authentication, command_handler=command)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			requests, responses = case.requests, case.responses
			if case.closes and len(requests) > 1:
				# A later invalid request may abort output still buffered from authentication.
				await _send(peer, requests[0])
				for expected in responses[:2]:
					assert await read_async_packet(peer.reader) == expected
				requests, responses = requests[1:], responses[2:]
			await _send(peer, *requests)
			for expected in responses:
				assert await read_async_packet(peer.reader) == expected
			if case.closes:
				await _assert_closed(peer)
			assert commands == case.commands
	run_async(scenario())


def test_custom_callbacks_receive_content_and_addresses(run_async: RunAsync, async_listeners: List[Server], listening_host: str):
	async def scenario():
		inputs: List[RconContent] = []
		infos: List[ConnectionInfo] = []
		async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
			inputs.append(content)
			infos.append(info)
			return content.text == 'caf\xe9'
		async def command(content: RconContent, info: ConnectionInfo) -> str:
			inputs.append(content)
			infos.append(info)
			return content.text + '!'
		server = AsyncRconServer(listening_host, 0, authentication=authenticate, command_handler=command, default_encoding='latin1')
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer, b'caf\xe9')
			await _send(peer, Packet(2, PacketType.exec_command, b'caf\xe9'))
			assert await read_async_packet(peer.reader) == Packet(2, PacketType.response_value, b'caf\xe9!')
			assert [content.content for content in inputs] == [b'caf\xe9', b'caf\xe9']
			address = peer.writer.get_extra_info('sockname')
			for info in infos:
				assert (info.local_address.host, info.local_address.port) == running.address
				assert (info.remote_address.host, info.remote_address.port) == (address[0], address[1])
	run_async(scenario())


@pytest.mark.parametrize('result,packet_limit', [
	pytest.param(b'', None, id='empty-bytes'),
	pytest.param('', None, id='empty-text'),
	pytest.param(b'abcdef' * 2000, None, id='split-bytes-protocol-size'),
	pytest.param('caf\xe9' * 3000, 31, id='split-text-configured-size'),
])
def test_command_result_encoding_and_splitting(run_async: RunAsync, async_listeners: List[Server], result: Union[bytes, str], packet_limit: Optional[int]):
	async def scenario():
		async def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
			return result
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, max_send_packet_size=packet_limit)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			await _send(peer, Packet(2, PacketType.exec_command, b''), Packet(3, PacketType.response_value, b''))
			packets = await _receive_result(peer)
			for packet in packets:
				assert len(packet.encode_framed()) <= min(4100, packet_limit or 4100)
			assert b''.join(packet.payload for packet in packets) == (result.encode('utf8') if isinstance(result, str) else result)
	run_async(scenario())


@pytest.mark.parametrize('failure', ['auth-result', 'auth-error', 'command-result', 'command-error', 'encoding', 'nul-bytes', 'nul-text'])
def test_callback_failure_only_ends_current_session(run_async: RunAsync, async_listeners: List[Server], failure: str):
	async def scenario():
		async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
			if content.content == b'bad' and failure.startswith('auth'):
				if failure == 'auth-error':
					raise RuntimeError('authentication failed')
				return cast(bool, 1)
			return True
		async def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
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
		server = AsyncRconServer('127.0.0.1', 0, authentication=authenticate, command_handler=command, default_encoding='ascii')
		async with _RunningServer(server, async_listeners) as running:
			good = await running.connect()
			await _authenticate(good)
			bad = await running.connect()
			if failure.startswith('auth'):
				await _send(bad, Packet(1, PacketType.auth, b'bad'))
			else:
				await _authenticate(bad)
				await _send(bad, Packet(2, PacketType.exec_command, b'bad'))
			await _assert_closed(bad)
			await _send(good, Packet(2, PacketType.exec_command, b'good'))
			assert await read_async_packet(good.reader) == Packet(2, PacketType.response_value, b'ok')
			new = await running.connect()
			await _authenticate(new)
			await _send(new, Packet(2, PacketType.exec_command, b'good'))
			assert await read_async_packet(new.reader) == Packet(2, PacketType.response_value, b'ok')
	run_async(scenario())


def test_malformed_packet_only_ends_current_session(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo)
		async with _RunningServer(server, async_listeners) as running:
			bad = await running.connect()
			await _authenticate(bad)
			bad.writer.write(struct.pack('<i', 9))
			await bad.writer.drain()
			await _assert_closed(bad)
			good = await running.connect()
			await _authenticate(good)
			await _send(good, Packet(2, PacketType.exec_command, b'after malformed packet'))
			assert await read_async_packet(good.reader) == Packet(2, PacketType.response_value, b'after malformed packet')
	run_async(scenario())


@pytest.mark.parametrize('maximum,result,allowed', [
	(0, b'', True), (0, b'x', False), (3, b'abc', True), (3, b'abcd', False),
	pytest.param(None, b'x' * (4 * 1024 * 1024 + 1), True, id='unlimited-above-default'),
	(3, 'éé', False), (4, 'éé', True),
])
def test_result_size_limit(run_async: RunAsync, async_listeners: List[Server], maximum: Optional[int], result: Union[bytes, str], allowed: bool):
	async def scenario():
		async def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
			return result
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, max_response_size=maximum)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			await _send(peer, Packet(2, PacketType.exec_command, b'command'), Packet(3, PacketType.response_value, b''))
			if allowed:
				packets = await _receive_result(peer)
				assert b''.join(packet.payload for packet in packets) == (result.encode('utf8') if isinstance(result, str) else result)
			else:
				await _assert_closed(peer)
	run_async(scenario())


def test_receive_packet_size_boundary(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'pw', command_handler=_echo, max_receive_packet_size=16)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer, b'pw')
			await _send(peer, Packet(2, PacketType.exec_command, b'xx'))
			assert await read_async_packet(peer.reader) == Packet(2, PacketType.response_value, b'xx')
			await _send(peer, Packet(3, PacketType.exec_command, b'xxx'))
			await _assert_closed(peer)
	run_async(scenario())


@pytest.mark.parametrize('limit_send', [False, True], ids=['receive', 'send'])
@pytest.mark.parametrize('maximum', [0, 13])
def test_undersized_packet_limit_fails_during_exchange(run_async: RunAsync, async_listeners: List[Server], limit_send: bool, maximum: int):
	async def scenario():
		server = AsyncRconServer(
			'127.0.0.1', 0, authentication=b'', command_handler=_echo,
			max_send_packet_size=maximum if limit_send else None, max_receive_packet_size=None if limit_send else maximum,
		)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _send(peer, Packet(1, PacketType.auth, b''))
			await _assert_closed(peer)
	run_async(scenario())


def test_lifecycle_and_shutdown_before_serve(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo)
		assert not async_listeners
		await server.shutdown()
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			with pytest.raises(RconStateError):
				await server.serve()
		await server.shutdown()
		with pytest.raises(RconStateError):
			await server.serve()
		with pytest.raises(OSError):
			await asyncio.open_connection(*running.address)
	run_async(scenario())


def test_failed_start_consumes_serve(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		failure = RuntimeError('startup')
		addresses: List[Tuple[str, int]] = []
		def fail():
			sockets = async_listeners[-1].sockets
			assert sockets
			address = sockets[0].getsockname()
			addresses.append((address[0], address[1]))
			raise failure
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo)
		with pytest.raises(RuntimeError) as caught:
			await server.serve(on_started=fail)
		assert caught.value is failure
		with pytest.raises(OSError):
			await asyncio.open_connection(*addresses[0])
		await server.shutdown()
		with pytest.raises(RconStateError):
			await server.serve()
	run_async(scenario())


def test_callback_can_shutdown_server(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		stopped = asyncio.Event()
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			await server.shutdown()
			stopped.set()
			return b''
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			await _send(peer, Packet(2, PacketType.exec_command, b'stop'))
			await stopped.wait()
			await running.serving
			await _assert_closed(peer)
	run_async(scenario())


@pytest.mark.parametrize('stop', ['shutdown', 'cancel'])
def test_serve_waits_for_callback_cleanup(run_async: RunAsync, async_listeners: List[Server], stop: str):
	async def scenario():
		entered = asyncio.Event()
		cleaning = asyncio.Event()
		release = asyncio.Event()
		finished = asyncio.Event()
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			try:
				entered.set()
				await asyncio.Event().wait()
			finally:
				cleaning.set()
				await release.wait()
				finished.set()
			return b''
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
		async with _RunningServer(server, async_listeners) as running:
			try:
				peer = await running.connect()
				await _authenticate(peer)
				await _send(peer, Packet(2, PacketType.exec_command, b'wait'))
				await entered.wait()
				if stop == 'shutdown':
					await server.shutdown()
				else:
					running.serving.cancel()
				await cleaning.wait()
				assert not running.serving.done()
				assert not finished.is_set()
				release.set()
				if stop == 'shutdown':
					await running.serving
				else:
					with pytest.raises(asyncio.CancelledError):
						await running.serving
				assert finished.is_set()
				await _assert_closed(peer)
			finally:
				release.set()
	run_async(scenario())


@pytest.mark.parametrize('waiting_for', ['callback', 'response'])
def test_connection_requests_are_serial_and_connections_concurrent(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch, waiting_for: str):
	async def scenario():
		entered = asyncio.Event()
		release = asyncio.Event()
		after = asyncio.Event()
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			if content.content == b'slow':
				if waiting_for == 'callback':
					entered.set()
					await release.wait()
			elif content.content == b'after':
				after.set()
			return content.content
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
		async with _RunningServer(server, async_listeners) as running:
			try:
				first = await running.connect()
				await _authenticate(first)
				if waiting_for == 'response':
					drain = asyncio.StreamWriter.drain
					client_address = first.writer.get_extra_info('sockname')
					async def blocked_drain(writer: asyncio.StreamWriter):
						await drain(writer)
						if writer.get_extra_info('peername') == client_address:
							entered.set()
							await release.wait()
					monkeypatch.setattr(asyncio.StreamWriter, 'drain', blocked_drain)
				await _send(first, Packet(2, PacketType.exec_command, b'slow'), Packet(3, PacketType.exec_command, b'after'))
				await entered.wait()
				second = await running.connect()
				await _authenticate(second)
				await _send(second, Packet(2, PacketType.exec_command, b'fast'))
				assert await read_async_packet(second.reader) == Packet(2, PacketType.response_value, b'fast')
				assert not after.is_set()
				release.set()
				assert await read_async_packet(first.reader) == Packet(2, PacketType.response_value, b'slow')
				assert await read_async_packet(first.reader) == Packet(3, PacketType.response_value, b'after')
			finally:
				release.set()
	run_async(scenario())


def test_capacity_includes_authentication_and_disconnected_callback(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		authenticating = asyncio.Event()
		auth_release = asyncio.Event()
		working = asyncio.Event()
		work_release = asyncio.Event()
		async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
			authenticating.set()
			await auth_release.wait()
			return True
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			working.set()
			await work_release.wait()
			return b'done'
		server = AsyncRconServer('127.0.0.1', 0, authentication=authenticate, command_handler=command, max_connections=1)
		async with _RunningServer(server, async_listeners) as running:
			try:
				first = await running.connect()
				await _send(first, Packet(1, PacketType.auth, b'password'))
				await authenticating.wait()
				await _assert_closed(await running.connect())
				auth_release.set()
				for expected in create_authentication_responses(1):
					assert await read_async_packet(first.reader) == expected
				await _send(first, Packet(2, PacketType.exec_command, b'wait'))
				await working.wait()
				first.writer.close()
				await _assert_closed(await running.connect())
				work_release.set()
				# Admission itself observes that the previous callback has fully left its session.
				while True:
					peer = await running.connect()
					try:
						await _send(peer, Packet(1, PacketType.auth, b'password'))
						packet = await read_async_packet(peer.reader)
					except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
						await asyncio.sleep(0)
						continue
					assert packet == create_authentication_responses(1)[0]
					assert await read_async_packet(peer.reader) == create_authentication_responses(1)[1]
					break
			finally:
				auth_release.set()
				work_release.set()
	run_async(scenario())


def test_unlimited_capacity_exceeds_default_limit(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, max_connections=None)
		async with _RunningServer(server, async_listeners) as running:
			peers: List[AsyncPeer] = []
			for _ in range(101):
				peer = await running.connect()
				peers.append(peer)
				await _authenticate(peer)
			for peer in (peers[0], peers[-1]):
				await _send(peer, Packet(2, PacketType.exec_command, b'still open'), Packet(3, PacketType.response_value, b''))
				packets = await _receive_result(peer)
				assert b''.join(packet.payload for packet in packets) == b'still open'
	run_async(scenario())


@pytest.mark.parametrize('prefix', [b'', Packet(1, PacketType.auth, b'password').encode_framed()[:1], Packet(1, PacketType.auth, b'password').encode_framed()[:-1]], ids=['silent', 'first-byte', 'incomplete-body'])
def test_authentication_deadline_requires_complete_packet(run_async: RunAsync, async_listeners: List[Server], prefix: bytes):
	async def scenario():
		called = asyncio.Event()
		async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
			called.set()
			return True
		server = AsyncRconServer('127.0.0.1', 0, authentication=authenticate, command_handler=_echo, authentication_timeout=0.05)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			peer.writer.write(prefix)
			await peer.writer.drain()
			await _assert_closed(peer)
			assert not called.is_set()
	run_async(scenario())


def test_authentication_deadline_excludes_callback(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
			await asyncio.sleep(0.12)
			return True
		server = AsyncRconServer('127.0.0.1', 0, authentication=authenticate, command_handler=_echo, authentication_timeout=0.05, receive_timeout=0.05)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			await _send(peer, Packet(2, PacketType.exec_command, b'after authentication'))
			assert await read_async_packet(peer.reader) == Packet(2, PacketType.response_value, b'after authentication')
	run_async(scenario())


@pytest.mark.parametrize('waiting_for', ['callback', 'response'])
def test_idle_timeout_excludes_processing_and_resumes_after_result(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch, waiting_for: str):
	async def scenario():
		release = asyncio.Event()
		async def command(content: RconContent, info: ConnectionInfo) -> bytes:
			if waiting_for == 'callback':
				await asyncio.sleep(0.12)
			return b'done'
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=command, idle_timeout=0.05)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			if waiting_for == 'response':
				drain = asyncio.StreamWriter.drain
				async def delayed_drain(writer: asyncio.StreamWriter):
					await drain(writer)
					address = writer.get_extra_info('sockname')
					if address is not None and address[1] == running.address[1]:
						await release.wait()
				monkeypatch.setattr(asyncio.StreamWriter, 'drain', delayed_drain)
			await _send(peer, Packet(2, PacketType.exec_command, b'wait'))
			assert await read_async_packet(peer.reader) == Packet(2, PacketType.response_value, b'done')
			if waiting_for == 'response':
				closed = asyncio.ensure_future(peer.reader.read(1))
				await asyncio.wait([closed], timeout=0.12)
				assert not closed.done()
				release.set()
				assert await closed == b''
			else:
				await _assert_closed(peer)
	run_async(scenario())


def test_receive_timeout_starts_after_first_byte(run_async: RunAsync, async_listeners: List[Server]):
	async def scenario():
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, receive_timeout=0.05)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			await asyncio.sleep(0.12)
			await _send(peer, Packet(2, PacketType.exec_command, b'still open'))
			assert await read_async_packet(peer.reader) == Packet(2, PacketType.response_value, b'still open')
			peer.writer.write(Packet(3, PacketType.exec_command, b'incomplete').encode_framed()[:1])
			await peer.writer.drain()
			await _assert_closed(peer)
	run_async(scenario())


@pytest.mark.parametrize('delay,expires', [(0.02, False), (0.1, True)], ids=['per-packet-budget', 'blocked-packet'])
def test_send_timeout_applies_per_packet(run_async: RunAsync, async_listeners: List[Server], monkeypatch: pytest.MonkeyPatch, delay: float, expires: bool):
	async def scenario():
		drain = asyncio.StreamWriter.drain
		server = AsyncRconServer('127.0.0.1', 0, authentication=b'password', command_handler=_echo, send_timeout=0.05, max_send_packet_size=31)
		async with _RunningServer(server, async_listeners) as running:
			peer = await running.connect()
			await _authenticate(peer)
			async def delayed_drain(writer: asyncio.StreamWriter):
				await drain(writer)
				address = writer.get_extra_info('sockname')
				if address is not None and address[1] == running.address[1]:
					await asyncio.sleep(delay)
			monkeypatch.setattr(asyncio.StreamWriter, 'drain', delayed_drain)
			await _send(peer, Packet(2, PacketType.exec_command, b'x' * 60), Packet(3, PacketType.exec_command, b'next'))
			chunks: List[bytes] = []
			while True:
				try:
					packet = await read_async_packet(peer.reader)
				except (asyncio.IncompleteReadError, ConnectionResetError):
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
	run_async(scenario())
