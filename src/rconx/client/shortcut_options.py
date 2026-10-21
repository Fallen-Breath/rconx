from typing import Optional, Union

from typing_extensions import NotRequired, TypedDict

from rconx.client.preset import RconPreset, RconPresetLiteral
from rconx.client.raw import LocalAddress


class __RconExecuteOptions(TypedDict):
	password: Union[str, bytes]
	preset: Union[RconPreset, RconPresetLiteral]
	local_address: NotRequired[Optional[LocalAddress]]

	max_send_packet_size: NotRequired[Optional[int]]
	"""Maximum sent packet size in bytes, including the length header. ``None`` disables the limit"""

	max_receive_packet_size: NotRequired[Optional[int]]
	"""Maximum received packet size in bytes, including the length header (default: 4 MiB). ``None`` disables the limit"""

	default_encoding: NotRequired[str]
	"""Text encoding for string input and response text; defaults to ``utf8``"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum accumulated response payload size in bytes, with a default of 4 MiB. ``None`` disables the limit"""


class SyncRconExecuteOptions(__RconExecuteOptions):
	timeout: NotRequired[Optional[float]]


class AsyncRconExecuteOptions(__RconExecuteOptions):
	pass
