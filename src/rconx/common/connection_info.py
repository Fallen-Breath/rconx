import dataclasses


@dataclasses.dataclass(frozen=True)
class ConnectionAddress:
	host: str
	port: int


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
	local_address: ConnectionAddress
	remote_address: ConnectionAddress
