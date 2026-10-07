import struct
from typing import Type, Union

import pytest

from rconx.common.exceptions import RconError, RconPacketDecodeError
from rconx.common.protocol import Packet


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


def test_decode_error_is_a_library_error_and_value_error():
	with pytest.raises(RconPacketDecodeError) as caught:
		Packet.decode_framed(b'')
	assert isinstance(caught.value, RconError)
	assert isinstance(caught.value, ValueError)


def test_decode_accepts_compatible_trailing_bytes():
	body = struct.pack('<ii', 3, 2) + b'command' + b'xy'
	expected = Packet(3, 2, b'command')
	assert Packet.decode(body) == expected
	assert Packet.decode_framed(struct.pack('<i', len(body)) + body) == expected
