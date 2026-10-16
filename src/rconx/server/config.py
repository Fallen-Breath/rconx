import codecs
import dataclasses
from typing import Optional

from rconx.common.connection import ConnectionConfig
from rconx.common.utils import validate_packet_size_limit, validate_timeout


@dataclasses.dataclass(frozen=True)
class ServerConfig(ConnectionConfig):
	max_send_packet_size: Optional[int] = 4100
	max_receive_packet_size: Optional[int] = 4100
	backlog: int = 5
	max_connections: Optional[int] = 100
	default_encoding: str = 'utf8'
	max_response_size: Optional[int] = 4 * 1024 * 1024
	authentication_timeout: Optional[float] = 10.0
	idle_timeout: Optional[float] = None
	receive_timeout: Optional[float] = None
	send_timeout: Optional[float] = None

	def __post_init__(self):
		super().__post_init__()
		validate_packet_size_limit(self.max_connections, 'max_connections')
		if self.max_connections == 0:
			raise ValueError('max_connections must be positive or None')
		validate_packet_size_limit(self.max_response_size, 'max_response_size')
		try:
			codecs.lookup(self.default_encoding)
			''.encode(self.default_encoding)
		except LookupError as err:
			raise ValueError('default_encoding must name a text encoding') from err
		for name in ('authentication_timeout', 'idle_timeout', 'receive_timeout', 'send_timeout'):
			validate_timeout(getattr(self, name), name=name)
