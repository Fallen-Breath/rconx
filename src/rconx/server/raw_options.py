from typing_extensions import NotRequired

from rconx.common.connection_options import ConnectionOptions


class RawRconServerOptions(ConnectionOptions):
	"""
	Optional server keyword arguments, including ``ConnectionOptions``
	"""

	backlog: NotRequired[int]
	"""Non-negative pending connection queue size with a default of ``5``"""
