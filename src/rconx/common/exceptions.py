class RconError(Exception):
	pass


class RconConnectionError(RconError):
	pass


class RconTimeoutError(RconError):
	pass


class RconPacketDecodeError(RconError, ValueError):
	pass


class RconAuthenticationError(RconError):
	pass
