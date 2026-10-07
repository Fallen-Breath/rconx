from typing import Optional

from rconx.client.command.base import CommandHandler
from rconx.client.command.common import validate_response_value
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconProtocolError
from rconx.common.protocol import Packet, PacketType
from rconx.common.utils import validate_timeout


class IdleTimeoutCommandHandler(CommandHandler):
	"""
	**Not public API**

	Quiet-window collection for servers without a usable ending probe, such as Project Zomboid.
	Waits for the first reply, then ends on a quiet window between packets; completion is heuristic.
	"""
	def __init__(self, *, first_byte_timeout: float):
		if first_byte_timeout is None:
			raise TypeError('first_byte_timeout must be a finite non-negative number; got None')
		validate_timeout(first_byte_timeout, name='first_byte_timeout')
		self.__first_byte_timeout = first_byte_timeout

	def execute(self, context: RconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		context.send_packet(request)

		# The first reply uses the normal operation budget; the quiet window applies only between replies.
		packet: Optional[Packet] = context.receive_packet()
		while packet is not None:
			validate_response_value(packet)
			if packet.request_id != request.request_id:
				raise RconProtocolError('unexpected command response request id {}; expected {}'.format(packet.request_id, request.request_id))

			context.append_response(packet.payload)
			packet = context.try_receive_packet(first_byte_timeout=self.__first_byte_timeout)

	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		await context.send_packet(request)

		# The first reply uses the normal operation budget; the quiet window applies only between replies.
		packet: Optional[Packet] = await context.receive_packet()
		while packet is not None:
			validate_response_value(packet)
			if packet.request_id != request.request_id:
				raise RconProtocolError('unexpected command response request id {}; expected {}'.format(packet.request_id, request.request_id))

			context.append_response(packet.payload)
			packet = await context.try_receive_packet(first_byte_timeout=self.__first_byte_timeout)
