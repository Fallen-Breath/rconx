from typing import Optional, Union

from typing_extensions import NotRequired, TypedDict

from rconx.client.preset import RconPreset, RconPresetLiteral
from rconx.client.raw import LocalAddress


class __RconExecuteOptions(TypedDict):
	password: Union[str, bytes]
	"""Required password bytes or text encoded with ``default_encoding``; null bytes are not allowed"""

	preset: Union[RconPreset, RconPresetLiteral]
	"""Required preset for authentication, command exchange and default size limits, supplied as a ``RconPreset`` member or its exact name"""

	local_address: NotRequired[Optional[LocalAddress]]
	"""Local address to bind, or ``None`` (the default) for automatic selection"""

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
	"""Shared budget in seconds for connecting, authenticating and executing the command. ``None`` (the default) disables the timeout"""


class AsyncRconExecuteOptions(__RconExecuteOptions):
	pass
