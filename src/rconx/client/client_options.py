from typing import Optional

from typing_extensions import NotRequired

from rconx.common.connection_options import ConnectionOptions


class RconClientOptions(ConnectionOptions):
	"""
	Optional high-level client keyword arguments, including the fields of ``ConnectionOptions``
	"""

	default_encoding: NotRequired[str]
	"""Text encoding for string input and response text; defaults to ``utf8``"""

	max_response_size: NotRequired[Optional[int]]
	"""Maximum accumulated response payload size in bytes. ``None`` disables the limit; defaults to 4 MiB"""
