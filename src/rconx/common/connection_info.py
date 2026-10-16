import dataclasses


@dataclasses.dataclass(frozen=True)
class ConnectionAddress:
	"""
	Read-only connection endpoint with a host address and TCP port
	"""

	host: str
	port: int


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
	"""
	Read-only snapshot of the local and remote connection addresses
	"""

	local_address: ConnectionAddress
	remote_address: ConnectionAddress
