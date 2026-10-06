from typing import Any

import pytest

from rconx.common.exceptions import RconTimeoutError
from rconx.common.utils import BytesBuffer, Deadline, validate_packet_size_limit, validate_timeout
from tests.support import Clock


@pytest.mark.parametrize('value', [None, 0, 1, 4 * 1024 * 1024])
def test_valid_packet_size_limit(value: Any):
	validate_packet_size_limit(value, 'limit')


@pytest.mark.parametrize('value', [True, False, 1.0, '1', object()])
def test_invalid_packet_size_limit_type(value: Any):
	with pytest.raises(TypeError):
		validate_packet_size_limit(value, 'limit')


def test_negative_packet_size_limit():
	with pytest.raises(ValueError):
		validate_packet_size_limit(-1, 'limit')


@pytest.mark.parametrize('value', [None, 0, 0.5, 10])
def test_valid_timeout(value: Any):
	validate_timeout(value)


@pytest.mark.parametrize('value', [-1, float('inf'), -float('inf'), float('nan')])
def test_invalid_timeout_value(value: Any):
	with pytest.raises(ValueError):
		validate_timeout(value)
	with pytest.raises(ValueError):
		Deadline(value)


@pytest.mark.parametrize('value', ['1', object()])
def test_invalid_timeout_type(value: Any):
	with pytest.raises(TypeError):
		validate_timeout(value)
	with pytest.raises(TypeError):
		Deadline(value)


def test_deadline_without_timeout(clock: Clock):
	deadline = Deadline(None)
	clock.advance(100000)
	assert deadline.get_remaining_or_raise() is None


def test_deadline_remaining_and_expiry(clock: Clock):
	deadline = Deadline(2)
	assert deadline.get_remaining_or_raise() == pytest.approx(2)
	clock.advance(0.5)
	assert deadline.get_remaining_or_raise() == pytest.approx(1.5)
	clock.advance(1.5)
	with pytest.raises(RconTimeoutError):
		deadline.get_remaining_or_raise()


def test_zero_deadline(clock: Clock):
	with pytest.raises(RconTimeoutError):
		Deadline(0).get_remaining_or_raise()


@pytest.mark.parametrize('count', [0, 1, 7, 30001])
def test_buffer_consume_and_reuse(count: int):
	buffer = BytesBuffer()
	chunks = [bytes([index % 256]) for index in range(count)]
	for chunk in chunks:
		buffer.append(chunk)
	buffer.append(b'')
	assert len(buffer) == count
	data = buffer.consume()
	assert isinstance(data, bytes)
	assert data == b''.join(chunks)
	assert len(buffer) == 0
	assert buffer.consume() == b''
	buffer.append(b'next')
	buffer.append(b' packet')
	assert len(buffer) == 11
	assert buffer.consume() == b'next packet'
	assert len(buffer) == 0
