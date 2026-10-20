from typing import Optional

from typing_extensions import NotRequired

from rconx.server.raw_options import RawRconServerOptions


class RconServerOptions(RawRconServerOptions):
	"""
	Options for high-level RCON servers, including ``RawRconServerOptions``

	Both packet-size limits default to 4100 bytes, including the length header.
	Timeouts are in seconds. ``None`` disables the corresponding timeout.
	"""

	max_connections: NotRequired[Optional[int]]
	"""Maximum concurrent sessions, including unauthenticated ones (default: ``100``). ``None`` disables the limit"""

	default_encoding: NotRequired[str]
	"""Encoding for string passwords and results, and for decoding input text (default: ``utf8``)"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum encoded command result size in bytes (default: 4 MiB). ``None`` disables the limit"""

	authentication_timeout: NotRequired[Optional[float]]
	"""Timeout from accepting a connection until its first complete authentication packet arrives (default: ``10``)"""

	idle_timeout: NotRequired[Optional[float]]
	"""Timeout while an authenticated connection waits for the next request's first byte (default: ``None``)"""

	receive_timeout: NotRequired[Optional[float]]
	"""Timeout for receiving the remainder of each packet after its first byte arrives (default: ``None``)"""

	send_timeout: NotRequired[Optional[float]]
	"""Timeout for sending each response packet (default: ``None``)"""
