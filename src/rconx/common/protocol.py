import dataclasses
import operator
import struct
from enum import IntEnum
from typing import Union

from rconx.common.exceptions import RconPacketDecodeError

LENGTH_HEADER = struct.Struct('<i')
_PACKET_HEADER = struct.Struct('<ii')
_FRAMED_PACKET_HEADER = struct.Struct('<iii')
_TERMINATOR = b'\x00\x00'
_INT32_MAX = (1 << 31) - 1
MIN_PACKET_BODY_SIZE = _PACKET_HEADER.size + len(_TERMINATOR)


class PacketType(IntEnum):
	response_value = 0
	exec_command = 2
	auth_response = 2
	auth = 3


@dataclasses.dataclass(frozen=True)
class Packet:
	request_id: int
	packet_type: int
	payload: bytes

	def encode(self) -> bytes:
		return self.__encode(_PACKET_HEADER.pack(self.__int32(self.request_id, 'request_id'), self.__int32(self.packet_type, 'packet_type')))

	def encode_framed(self) -> bytes:
		packet_size = _PACKET_HEADER.size + len(self.payload) + len(_TERMINATOR)
		return self.__encode(_FRAMED_PACKET_HEADER.pack(
			self.__int32(packet_size, 'packet_size'), self.__int32(self.request_id, 'request_id'), self.__int32(self.packet_type, 'packet_type'),
		))

	@classmethod
	def decode(cls, data: Union[bytes, bytearray]) -> 'Packet':
		return cls.__decode(data, 0)

	@classmethod
	def decode_framed(cls, data: Union[bytes, bytearray]) -> 'Packet':
		try:
			packet_size = LENGTH_HEADER.unpack_from(data)[0]
		except struct.error as err:
			raise RconPacketDecodeError(str(err)) from err
		minimum_size = MIN_PACKET_BODY_SIZE
		if packet_size < minimum_size:
			raise RconPacketDecodeError('packet size {} must be at least {}'.format(packet_size, minimum_size))
		remaining_size = len(data) - LENGTH_HEADER.size
		if packet_size != remaining_size:
			raise RconPacketDecodeError('declared packet body size {} does not match actual packet body size {}'.format(packet_size, remaining_size))
		return cls.__decode(data, LENGTH_HEADER.size)

	def __encode(self, header: bytes) -> bytes:
		return b''.join([header, self.payload, _TERMINATOR])

	@staticmethod
	def __int32(value: int, name: str) -> int:
		value = operator.index(value)
		if value < -_INT32_MAX - 1 or value > _INT32_MAX:
			raise ValueError('{} {} is outside the signed 32-bit integer range'.format(name, value))
		return value

	@classmethod
	def __decode(cls, data: Union[bytes, bytearray], offset: int) -> 'Packet':
		try:
			request_id, packet_type = _PACKET_HEADER.unpack_from(data, offset)
		except struct.error as err:
			raise RconPacketDecodeError(str(err)) from err
		return cls(
			request_id=request_id,
			packet_type=packet_type,
			payload=bytes(memoryview(data)[offset + _PACKET_HEADER.size:-len(_TERMINATOR)]),
		)
