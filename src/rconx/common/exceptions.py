class RconError(Exception):
	"""
	Base class for rconx errors
	"""


class RconStateError(RconError):
	"""
	The operation is invalid for the current state
	"""


class RconOperationError(RconError):
	"""
	Base class for RCON operation failures
	"""


class RconConnectionError(RconOperationError):
	"""
	A connection or transport operation failed
	"""


class RconTimeoutError(RconOperationError):
	"""
	An RCON operation timed out
	"""


class RconAuthenticationError(RconOperationError):
	"""
	The server rejected authentication or authenticated access
	"""


class RconSizeLimitError(RconOperationError):
	"""
	A packet or command result exceeded a configured size limit
	"""


class RconProtocolError(RconOperationError):
	"""
	An RCON protocol requirement was violated
	"""


class RconPacketDecodeError(RconProtocolError):
	"""
	RCON packet decoding failed
	"""
