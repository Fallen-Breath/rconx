from typing import Optional

from typing_extensions import NotRequired, TypedDict


class RawRconServerOptions(TypedDict):
	"""
	Optional server keyword arguments, including ``ConnectionOptions``
	"""

	max_send_packet_size: NotRequired[Optional[int]]
	"""Maximum sent packet size in bytes, including the length header. ``None`` disables the limit"""

	max_receive_packet_size: NotRequired[Optional[int]]
	"""Maximum received packet size in bytes, including the length header (default: 4 MiB). ``None`` disables the limit"""

	backlog: NotRequired[int]
	"""Non-negative pending connection queue size with a default of ``5``"""
