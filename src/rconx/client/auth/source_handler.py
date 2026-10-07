from rconx.client.auth.base import AuthenticationHandler
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconAuthenticationError, RconProtocolError
from rconx.common.protocol import Packet, PacketType


class _AuthenticationResponseProcessor:
	def __init__(self, request_id: int):
		self.__request_id = request_id
		self.__received_empty_response = False
		self.__is_complete = False

	@property
	def is_complete(self) -> bool:
		return self.__is_complete

	def accept(self, packet: Packet) -> None:
		if packet.payload:
			raise RconProtocolError('authentication response payload length is {}; expected 0 (request id {})'.format(len(packet.payload), packet.request_id))

		if packet.packet_type == PacketType.auth_response:
			if packet.request_id == -1:
				raise RconAuthenticationError('server rejected authentication for request id {}'.format(self.__request_id))
			if packet.request_id != self.__request_id:
				raise RconProtocolError('unexpected authentication result request id {}; expected {}'.format(packet.request_id, self.__request_id))

			self.__is_complete = True
			return

		# Allow at most one empty response-value packet before the authentication result.
		if packet.packet_type != PacketType.response_value:
			raise RconProtocolError(
				'unexpected authentication response type {}; expected {} (response value) or {} (authentication result)'.format(
					packet.packet_type, PacketType.response_value, PacketType.auth_response,
				)
			)
		if packet.request_id != self.__request_id:
			raise RconProtocolError('unexpected empty authentication response request id {}; expected {}'.format(packet.request_id, self.__request_id))

		if self.__received_empty_response:
			raise RconProtocolError('received a second empty response-value packet before the authentication result (request id {})'.format(packet.request_id))

		self.__received_empty_response = True


class SourceAuthenticationHandler(AuthenticationHandler):
	"""
	**Not public API**

	Source RCON authentication for SRCDS, Minecraft and Cuberite.
	Accepts an authentication result directly or after one empty response-value packet.
	"""
	def authenticate(self, context: RconOperationContext, password: bytes):
		request = context.prepare_request(PacketType.auth, password)
		response_processor = _AuthenticationResponseProcessor(request.request_id)

		context.send_packet(request)

		while not response_processor.is_complete:
			response_processor.accept(context.receive_packet())

	async def authenticate_async(self, context: AsyncRconOperationContext, password: bytes):
		request = context.prepare_request(PacketType.auth, password)
		response_processor = _AuthenticationResponseProcessor(request.request_id)

		await context.send_packet(request)

		while not response_processor.is_complete:
			response_processor.accept(await context.receive_packet())
