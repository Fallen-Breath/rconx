from typing import Type

import pytest

from rconx.common.exceptions import (
	RconAuthenticationError,
	RconConnectionError,
	RconError,
	RconOperationError,
	RconPacketDecodeError,
	RconProtocolError,
	RconSizeLimitError,
	RconStateError,
	RconTimeoutError,
)


@pytest.mark.parametrize('error_type', [
	RconConnectionError,
	RconTimeoutError,
	RconAuthenticationError,
	RconSizeLimitError,
	RconProtocolError,
	RconPacketDecodeError,
])
def test_operation_errors_share_catch_groups(error_type: Type[RconOperationError]):
	assert issubclass(error_type, RconOperationError)
	assert issubclass(error_type, RconError)
	assert not issubclass(error_type, RconStateError)


def test_state_errors_are_separate_from_operation_failures():
	assert issubclass(RconStateError, RconError)
	assert not issubclass(RconStateError, RconOperationError)


def test_packet_decode_errors_are_protocol_failures():
	assert issubclass(RconPacketDecodeError, RconProtocolError)
	assert not issubclass(RconPacketDecodeError, ValueError)


def test_size_limit_errors_are_separate_from_argument_errors():
	assert not issubclass(RconSizeLimitError, ValueError)
