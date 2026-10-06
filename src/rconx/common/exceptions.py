class RconError(Exception):
	pass


class RconConnectionError(RconError):
	pass


class RconAuthenticationError(RconError):
	pass
