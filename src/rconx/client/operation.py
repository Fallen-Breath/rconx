from typing import Optional

from rconx.client.raw import AsyncRawRconClient, RawRconClient
from rconx.common.protocol import Packet
from rconx.common.utils import BytesBuffer, Deadline


_INT32_MAX = (1 << 31) - 1


class RequestIdCounter:
	def __init__(self):
		self.__next_request_id = 1

	def get_next(self) -> int:
		request_id = self.__next_request_id
		self.__next_request_id = 1 if request_id == _INT32_MAX else request_id + 1
		return request_id


class _BaseOperationContext:
	def __init__(self, request_id_counter: RequestIdCounter, max_response_size: Optional[int]):
		self.__request_id_counter = request_id_counter
		self.__max_response_size = max_response_size

		self.__buffer = BytesBuffer()
		self.__response_size = 0

	def prepare_request(self, packet_type: int, payload: bytes) -> Packet:
		if not isinstance(packet_type, int):
			raise TypeError('packet type must be an integer; got {}'.format(type(packet_type).__name__))
		if packet_type < -_INT32_MAX - 1 or packet_type > _INT32_MAX:
			raise ValueError('packet type {} is outside the protocol type field range [{}, {}]'.format(packet_type, -_INT32_MAX - 1, _INT32_MAX))

		return Packet(self.__request_id_counter.get_next(), packet_type, payload)

	def append_response(self, payload: bytes):
		response_size = self.__response_size + len(payload)
		maximum_size = self.__max_response_size
		if maximum_size is not None and response_size > maximum_size:
			raise ValueError('accumulated command response size {} bytes exceeds max_response_size {} bytes'.format(response_size, maximum_size))

		self.__buffer.append(payload)
		self.__response_size = response_size

	def consume_response(self) -> bytes:
		return self.__buffer.consume()


class RconOperationContext(_BaseOperationContext):
	def __init__(self, raw: RawRconClient, request_id_counter: RequestIdCounter, deadline: Deadline, *, max_response_size: Optional[int]):
		super().__init__(request_id_counter, max_response_size)

		self.__raw = raw
		self.__deadline = deadline

	def send_packet(self, packet: Packet):
		remaining = self.__deadline.get_remaining_or_raise()
		self.__raw.send_packet(packet, timeout=remaining)

	def receive_packet(self) -> Packet:
		remaining = self.__deadline.get_remaining_or_raise()
		return self.__raw.receive_packet(timeout=remaining)

	def try_receive_packet(self, *, first_byte_timeout: float) -> Optional[Packet]:
		remaining = self.__deadline.get_remaining_or_raise()
		return self.__raw.try_receive_packet(first_byte_timeout=first_byte_timeout, timeout=remaining)


class AsyncRconOperationContext(_BaseOperationContext):
	def __init__(self, raw: AsyncRawRconClient, request_id_counter: RequestIdCounter, *, max_response_size: Optional[int]):
		super().__init__(request_id_counter, max_response_size)

		self.__raw = raw

	async def send_packet(self, packet: Packet):
		await self.__raw.send_packet(packet)

	async def receive_packet(self) -> Packet:
		return await self.__raw.receive_packet()

	async def try_receive_packet(self, *, first_byte_timeout: float) -> Optional[Packet]:
		return await self.__raw.try_receive_packet(first_byte_timeout=first_byte_timeout)
