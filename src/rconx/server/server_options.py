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
	"""Maximum concurrent sessions, including unauthenticated ones; defaults to ``100``. ``None`` disables the limit; excess connections are rejected"""

	default_encoding: NotRequired[str]
	"""Text encoding for string passwords, string results and input text access; defaults to ``utf8``"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum encoded command result size in bytes; defaults to 4 MiB. ``None`` disables the limit; ``0`` permits only empty results"""

	authentication_timeout: NotRequired[Optional[float]]
	"""Time from accepting a connection to receiving its first complete authentication packet; defaults to ``10``"""

	idle_timeout: NotRequired[Optional[float]]
	"""Timeout for an authenticated connection waiting for the next request's first byte; defaults to ``None``"""

	receive_timeout: NotRequired[Optional[float]]
	"""Timeout for receiving the rest of each packet after its first byte; defaults to ``None``"""

	send_timeout: NotRequired[Optional[float]]
	"""Timeout for sending each response packet; defaults to ``None``"""
