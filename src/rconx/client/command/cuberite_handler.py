from rconx.client.command.base import CommandHandler
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconAuthenticationError, RconProtocolError
from rconx.common.protocol import Packet, PacketType


_RESPONSE_PACKET_TYPE = 2


class _CuberiteResponseProcessor:
	def __init__(self, request_id: int):
		self.__request_id = request_id
		self.__received_packet_count = 0
		self.__received_empty_response = False

	@property
	def is_complete(self) -> bool:
		return self.__received_packet_count == 2 and self.__received_empty_response

	def accept(self, packet: Packet) -> None:
		if packet.packet_type == _RESPONSE_PACKET_TYPE and packet.request_id == -1 and not packet.payload:
			raise RconAuthenticationError('server rejected the authenticated session')
		if packet.packet_type != _RESPONSE_PACKET_TYPE:
			raise RconProtocolError(
				'unexpected Cuberite command response type {}; expected {}, request id {}'.format(packet.packet_type, _RESPONSE_PACKET_TYPE, packet.request_id)
			)
		if packet.request_id != self.__request_id:
			raise RconProtocolError('unexpected Cuberite command response request id {}; expected {}'.format(packet.request_id, self.__request_id))

		# Cuberite sends an empty acknowledgement and one complete result, both with type 2.
		self.__received_packet_count += 1
		if not packet.payload:
			self.__received_empty_response = True
		if self.__received_packet_count == 2:
			if not self.__received_empty_response:
				raise RconProtocolError(
					'received {} Cuberite command responses without an empty acknowledgement (request id {})'.format(self.__received_packet_count, self.__request_id)
				)


class CuberiteCommandHandler(CommandHandler):
	"""
	**Not public API**

	Command response collection for Cuberite's type-2 acknowledgement and result packets.
	Consumes both packets without sending an ending probe.
	"""
	def execute(self, context: RconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		response_processor = _CuberiteResponseProcessor(request.request_id)

		context.send_packet(request)
		while True:
			packet = context.receive_packet()
			response_processor.accept(packet)
			context.append_response(packet.payload)
			if response_processor.is_complete:
				return

	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		response_processor = _CuberiteResponseProcessor(request.request_id)

		await context.send_packet(request)
		while True:
			packet = await context.receive_packet()
			response_processor.accept(packet)
			context.append_response(packet.payload)
			if response_processor.is_complete:
				return
