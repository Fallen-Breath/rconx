from typing import Optional

from typing_extensions import NotRequired

from rconx.server.raw_options import RawRconServerOptions


class RconServerOptions(RawRconServerOptions):
	max_connections: NotRequired[Optional[int]]
	default_encoding: NotRequired[str]
	max_response_size: NotRequired[Optional[int]]
	authentication_timeout: NotRequired[Optional[float]]
	idle_timeout: NotRequired[Optional[float]]
	receive_timeout: NotRequired[Optional[float]]
	send_timeout: NotRequired[Optional[float]]
