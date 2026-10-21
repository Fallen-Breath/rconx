from typing import Optional

from typing_extensions import NotRequired, TypedDict


class RconServerOptions(TypedDict):
	max_send_packet_size: NotRequired[Optional[int]]
	"""Maximum sent packet size in bytes, including the length header (default: 4100 bytes). ``None`` disables the limit"""

	max_receive_packet_size: NotRequired[Optional[int]]
	"""Maximum received packet size in bytes, including the length header (default: 4100 bytes). ``None`` disables the limit"""

	backlog: NotRequired[int]
	"""Non-negative pending connection queue size with a default of ``5``"""

	max_connections: NotRequired[Optional[int]]
	"""Maximum concurrent sessions, including unauthenticated ones (default: ``100``). ``None`` disables the limit"""

	default_encoding: NotRequired[str]
	"""Encoding for string passwords and results, and for decoding input text (default: ``utf8``)"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum encoded command result size in bytes (default: 4 MiB). ``None`` disables the limit"""

	authentication_timeout: NotRequired[Optional[float]]
	"""Timeout in seconds from accepting a connection until its first complete authentication packet arrives (default: ``10``). ``None`` disables the timeout"""

	idle_timeout: NotRequired[Optional[float]]
	"""Timeout in seconds while an authenticated connection waits for the next request's first byte (default: ``None``). ``None`` disables the timeout"""

	receive_timeout: NotRequired[Optional[float]]
	"""Timeout in seconds for receiving the remainder of each packet after its first byte arrives (default: ``None``). ``None`` disables the timeout"""

	send_timeout: NotRequired[Optional[float]]
	"""Timeout in seconds for sending each response packet (default: ``None``). ``None`` disables the timeout"""
