from typing import Optional

from typing_extensions import NotRequired, TypedDict


class ConnectionOptions(TypedDict):
	"""
	Optional connection keyword arguments
	"""

	max_send_packet_size: NotRequired[Optional[int]]
	"""Maximum sent packet size in bytes, including the length header. ``None`` disables the limit; high-level client presets may supply different defaults"""

	max_receive_packet_size: NotRequired[Optional[int]]
	"""Maximum received packet size in bytes, including the length header. ``None`` disables the limit; defaults to 4 MiB"""
