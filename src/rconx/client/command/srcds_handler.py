from rconx.client.command.base import CommandHandler
from rconx.client.command.common import validate_response_value
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconProtocolError
from rconx.common.protocol import Packet, PacketType


class _SrcdsResponseProcessor:
	def __init__(self, request_id: int, ending_request_id: int):
		self.__request_id = request_id
		self.__ending_request_id = ending_request_id
		self.__received_ending_empty_response = False
		self.__is_complete = False

	@property
	def is_complete(self) -> bool:
		return self.__is_complete

	def accept(self, packet: Packet) -> None:
		validate_response_value(packet)

		# SRCDS sends an empty reply and then a second reply for the ending request; both must be consumed.
		if self.__received_ending_empty_response:
			if packet.request_id != self.__ending_request_id:
				raise RconProtocolError('unexpected request id {} for the second ending probe response; expected {}'.format(packet.request_id, self.__ending_request_id))
			self.__is_complete = True
			return

		if packet.request_id == self.__ending_request_id:
			if packet.payload:
				raise RconProtocolError('first ending probe response payload length is {}; expected 0 (request id {})'.format(len(packet.payload), packet.request_id))
			self.__received_ending_empty_response = True
		elif packet.request_id != self.__request_id:
			raise RconProtocolError(
				'unexpected SRCDS response request id {}; expected command id {} or ending probe id {}'.format(
					packet.request_id, self.__request_id, self.__ending_request_id,
				)
			)


class SrcdsCommandHandler(CommandHandler):
	"""
	**Not public API**

	Command response collection for SRCDS servers using the documented ending-probe exchange.
	Sends an ending probe immediately after the command and consumes both ending replies.
	"""
	def execute(self, context: RconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		ending_request = context.prepare_request(PacketType.response_value, b'')
		response_processor = _SrcdsResponseProcessor(request.request_id, ending_request.request_id)

		context.send_packet(request)
		context.send_packet(ending_request)

		while True:
			packet = context.receive_packet()
			response_processor.accept(packet)
			if response_processor.is_complete:
				return

			if packet.request_id == request.request_id:
				context.append_response(packet.payload)

	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		ending_request = context.prepare_request(PacketType.response_value, b'')
		response_processor = _SrcdsResponseProcessor(request.request_id, ending_request.request_id)

		await context.send_packet(request)
		await context.send_packet(ending_request)

		while True:
			packet = await context.receive_packet()
			response_processor.accept(packet)
			if response_processor.is_complete:
				return

			if packet.request_id == request.request_id:
				context.append_response(packet.payload)
