from rconx.client.command.base import CommandHandler
from rconx.client.command.common import validate_response_value
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconProtocolError
from rconx.common.protocol import PacketType


class SinglePacketCommandHandler(CommandHandler):
	"""
	**Not public API**

	Single-packet collection for servers with Factorio-style complete type-0 responses.
	Requires the entire command result to fit in one matching response packet.
	"""
	def execute(self, context: RconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		context.send_packet(request)

		packet = context.receive_packet()
		validate_response_value(packet)
		if packet.request_id != request.request_id:
			raise RconProtocolError('unexpected command response request id {}; expected {}'.format(packet.request_id, request.request_id))

		context.append_response(packet.payload)

	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		await context.send_packet(request)

		packet = await context.receive_packet()
		validate_response_value(packet)
		if packet.request_id != request.request_id:
			raise RconProtocolError('unexpected command response request id {}; expected {}'.format(packet.request_id, request.request_id))

		context.append_response(packet.payload)
