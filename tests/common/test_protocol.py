import struct
from typing import Any, Callable, Type, Union

import pytest

from rconx.common.exceptions import RconError, RconOperationError, RconPacketDecodeError, RconProtocolError
from rconx.common.protocol import Packet, PacketType


def test_encode_matches_wire_format():
	packet = Packet(0x12345678, -1, b'\x00\xffA')
	body = bytes.fromhex('78 56 34 12 ff ff ff ff 00 ff 41 00 00')
	assert packet.encode() == body
	assert packet.encode_framed() == bytes.fromhex('0d 00 00 00') + body


@pytest.mark.parametrize('data_type', [bytes, bytearray])
def test_decode_known_wire_format(data_type: Union[Type[bytes], Type[bytearray]]):
	body = bytes.fromhex('78 56 34 12 ff ff ff ff 00 ff 41 00 00')
	packet = Packet(0x12345678, -1, b'\x00\xffA')
	assert Packet.decode(data_type(body)) == packet
	assert Packet.decode_framed(data_type(bytes.fromhex('0d 00 00 00') + body)) == packet


@pytest.mark.parametrize('payload', [b'', b'command', b'\x00\xff\x80\x00', '中文'.encode('utf8')])
@pytest.mark.parametrize('value', [-2 ** 31, 0, 2 ** 31 - 1])
def test_round_trip(payload: bytes, value: int):
	packet = Packet(value, value, payload)
	assert Packet.decode(packet.encode()) == packet
	assert Packet.decode_framed(packet.encode_framed()) == packet


@pytest.mark.parametrize('packet_type', list(PacketType))
def test_encode_accepts_packet_types(packet_type: PacketType):
	packet = Packet(17, packet_type, b'command')
	expected_packet = Packet(17, int(packet_type), b'command')
	assert packet.encode() == expected_packet.encode()
	assert packet.encode_framed() == expected_packet.encode_framed()


@pytest.mark.parametrize('encode', [Packet.encode, Packet.encode_framed], ids=['body', 'framed'])
@pytest.mark.parametrize('request_id', [False, True], ids=['packet-type', 'request-id'])
@pytest.mark.parametrize('value', [None, 1.0, '1'])
def test_encode_rejects_non_integer_fields(encode: Callable[[Packet], bytes], request_id: bool, value: Any):
	packet = Packet(value, 0, b'') if request_id else Packet(0, value, b'')
	with pytest.raises(TypeError):
		encode(packet)


@pytest.mark.parametrize('encode', [Packet.encode, Packet.encode_framed], ids=['body', 'framed'])
@pytest.mark.parametrize('request_id', [False, True], ids=['packet-type', 'request-id'])
@pytest.mark.parametrize('value', [-2 ** 31 - 1, 2 ** 31])
def test_encode_rejects_fields_outside_integer_range(encode: Callable[[Packet], bytes], request_id: bool, value: int):
	packet = Packet(value, 0, b'') if request_id else Packet(0, value, b'')
	with pytest.raises(ValueError):
		encode(packet)


@pytest.mark.parametrize('size', range(8))
def test_truncated_packet_header(size: int):
	with pytest.raises(RconPacketDecodeError):
		Packet.decode(b'\x00' * size)


@pytest.mark.parametrize('data', [
	b'', b'\x00', b'\x00\x00\x00',
	struct.pack('<i', -1), struct.pack('<i', 0), struct.pack('<i', 9),
	struct.pack('<i', 10) + b'\x00' * 9,
	struct.pack('<i', 10) + b'\x00' * 11,
])
def test_invalid_frame(data: bytes):
	with pytest.raises(RconPacketDecodeError):
		Packet.decode_framed(data)


def test_decode_error_is_a_protocol_operation_error():
	with pytest.raises(RconPacketDecodeError) as caught:
		Packet.decode_framed(b'')
	assert isinstance(caught.value, RconError)
	assert isinstance(caught.value, RconOperationError)
	assert isinstance(caught.value, RconProtocolError)
	assert not isinstance(caught.value, ValueError)


def test_decode_accepts_compatible_trailing_bytes():
	body = struct.pack('<ii', 3, 2) + b'command' + b'xy'
	expected = Packet(3, 2, b'command')
	assert Packet.decode(body) == expected
	assert Packet.decode_framed(struct.pack('<i', len(body)) + body) == expected
