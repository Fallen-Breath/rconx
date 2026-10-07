from rconx.common.exceptions import RconAuthenticationError, RconProtocolError
from rconx.common.protocol import Packet, PacketType


def validate_response_value(packet: Packet):
	if packet.packet_type == PacketType.auth_response and packet.request_id == -1 and not packet.payload:
		raise RconAuthenticationError('server rejected the authenticated session')
	if packet.packet_type != PacketType.response_value:
		raise RconProtocolError(
			'unexpected response type {}; expected {} (response value), request id {}'.format(packet.packet_type, PacketType.response_value, packet.request_id)
		)
