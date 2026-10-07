from rconx.client.command.base import CommandHandler
from rconx.client.command.common import validate_response_value
from rconx.client.operation import AsyncRconOperationContext, RconOperationContext
from rconx.common.exceptions import RconProtocolError
from rconx.common.protocol import PacketType


class MinecraftCommandHandler(CommandHandler):
	"""
	**Not public API**

	Command response collection for vanilla Minecraft, CraftBukkit, Paper and Folia.
	Waits for the first command reply before sending an ending probe, then completes on its single reply.
	"""
	def execute(self, context: RconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		ending_request = context.prepare_request(PacketType.response_value, b'')
		ending_probe_sent = False

		context.send_packet(request)
		while True:
			packet = context.receive_packet()
			validate_response_value(packet)
			if ending_probe_sent and packet.request_id == ending_request.request_id:
				return
			if packet.request_id != request.request_id:
				if not ending_probe_sent:
					raise RconProtocolError('unexpected response request id {} before sending the ending probe; expected command id {}'.format(packet.request_id, request.request_id))
				raise RconProtocolError(
					'unexpected response request id {} after sending the ending probe; expected command id {} or ending probe id {}'.format(
						packet.request_id, request.request_id, ending_request.request_id,
					)
				)

			context.append_response(packet.payload)
			if not ending_probe_sent:
				# Wait for the first reply to keep Minecraft from reading the command and probe together.
				context.send_packet(ending_request)
				ending_probe_sent = True

	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		request = context.prepare_request(PacketType.exec_command, command)
		ending_request = context.prepare_request(PacketType.response_value, b'')
		ending_probe_sent = False

		await context.send_packet(request)
		while True:
			packet = await context.receive_packet()
			validate_response_value(packet)
			if ending_probe_sent and packet.request_id == ending_request.request_id:
				return
			if packet.request_id != request.request_id:
				if not ending_probe_sent:
					raise RconProtocolError('unexpected response request id {} before sending the ending probe; expected command id {}'.format(packet.request_id, request.request_id))
				raise RconProtocolError(
					'unexpected response request id {} after sending the ending probe; expected command id {} or ending probe id {}'.format(
						packet.request_id, request.request_id, ending_request.request_id,
					)
				)

			context.append_response(packet.payload)
			if not ending_probe_sent:
				# Wait for the first reply to keep Minecraft from reading the command and probe together.
				await context.send_packet(ending_request)
				ending_probe_sent = True
