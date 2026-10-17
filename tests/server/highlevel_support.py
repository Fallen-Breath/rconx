import dataclasses
from typing import List, Union

from rconx.common.protocol import Packet, PacketType

COMMAND_RESULT = b'result'
PROBE_END = b'\x00\x00\x00\x01\x00\x00\x00\x00'


def create_authentication_responses(request_id: int, accepted: bool = True) -> List[Packet]:
	return [Packet(request_id, PacketType.response_value, b''), Packet(request_id if accepted else -1, PacketType.auth_response, b'')]


@dataclasses.dataclass(frozen=True)
class ProtocolCase:
	name: str
	authentication: Union[str, bytes]
	requests: List[Packet]
	responses: List[Packet]
	commands: List[bytes] = dataclasses.field(default_factory=list)
	closes: bool = False


def get_protocol_cases() -> List[ProtocolCase]:
	auth = Packet(1, PacketType.auth, b'password')
	command = Packet(2, PacketType.exec_command, b'command')
	result = Packet(2, PacketType.response_value, COMMAND_RESULT)
	cases = [
		ProtocolCase('string-password', 'password', [auth, command], create_authentication_responses(1) + [result], [b'command']),
		ProtocolCase('bytes-password', b'password', [auth, command], create_authentication_responses(1) + [result], [b'command']),
		ProtocolCase('unicode-password', '密码', [Packet(1, PacketType.auth, '密码'.encode('utf8')), command], create_authentication_responses(1) + [result], [b'command']),
		ProtocolCase('empty-password', '', [Packet(1, PacketType.auth, b''), command], create_authentication_responses(1) + [result], [b'command']),
		ProtocolCase('empty-password-rejects-nonempty', '', [auth], create_authentication_responses(1, False), closes=True),
		ProtocolCase('wrong-password', 'other', [auth], create_authentication_responses(1, False), closes=True),
		ProtocolCase('reauthenticate', 'password', [auth, Packet(3, PacketType.auth, b'password'), command], create_authentication_responses(1) + create_authentication_responses(3) + [result], [b'command']),
		ProtocolCase('reauthentication-rejected', 'password', [auth, Packet(3, PacketType.auth, b'wrong')], create_authentication_responses(1) + create_authentication_responses(3, False), closes=True),
		ProtocolCase('empty-command', 'password', [auth, Packet(2, PacketType.exec_command, b'')], create_authentication_responses(1) + [result], [b'']),
		ProtocolCase('null-password', 'password', [Packet(1, PacketType.auth, b'pass\x00word')], [], closes=True),
		ProtocolCase('null-command', 'password', [auth, Packet(2, PacketType.exec_command, b'a\x00b')], create_authentication_responses(1), closes=True),
	]
	for packet_type in (PacketType.exec_command, PacketType.response_value, 99):
		cases.append(ProtocolCase('authentication-required-{}'.format(packet_type), 'password', [Packet(1, packet_type, b'')], [], closes=True))
	for request_id in (0, -1):
		cases.append(ProtocolCase('invalid-id-{}'.format(request_id), 'password', [auth, Packet(request_id, PacketType.exec_command, b'command')], create_authentication_responses(1), closes=True))
	for payload in (b'', b'opaque\x00payload'):
		requests = [auth, command, Packet(3, PacketType.response_value, payload), Packet(4, 99, b'ignored'), Packet(5, PacketType.exec_command, b'next')]
		responses = create_authentication_responses(1) + [result, Packet(3, PacketType.response_value, b''), Packet(3, PacketType.response_value, PROBE_END), Packet(5, PacketType.response_value, COMMAND_RESULT)]
		cases.append(ProtocolCase('probe-{}'.format('nonempty' if payload else 'empty'), 'password', requests, responses, [b'command', b'next']))
	return cases
