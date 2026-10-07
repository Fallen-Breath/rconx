import math
import time
from typing import List, Optional

from rconx.common.exceptions import RconTimeoutError


def validate_packet_size_limit(value: Optional[int], name: str):
	if value is not None:
		if isinstance(value, bool) or not isinstance(value, int):
			raise TypeError('{} must be an integer or None'.format(name))
		if value < 0:
			raise ValueError('{} must be non-negative'.format(name))


def validate_timeout(timeout: Optional[float], *, name: str = 'timeout'):
	if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
		raise ValueError('{} must be finite and non-negative; got {!r}'.format(name, timeout))


class Deadline:
	def __init__(self, timeout: Optional[float]):
		validate_timeout(timeout)
		self.__deadline: Optional[float] = None if timeout is None else time.monotonic() + timeout

	def get_remaining_or_raise(self) -> Optional[float]:
		if self.__deadline is None:
			return None
		remaining = self.__deadline - time.monotonic()
		if remaining <= 0:
			raise RconTimeoutError('operation timed out')
		return remaining


class BytesBuffer:
	__MERGE_THRESHOLD = 10000

	def __init__(self):
		self.__pending_chunks: List[bytes] = []
		self.__merged_chunks: List[bytes] = []
		self.__size = 0

	def __len__(self) -> int:
		return self.__size

	def append(self, data: bytes):
		if not data:
			return
		self.__pending_chunks.append(data)
		self.__size += len(data)
		if len(self.__pending_chunks) >= self.__MERGE_THRESHOLD:
			self.__merged_chunks.append(b''.join(self.__pending_chunks))
			self.__pending_chunks.clear()

	def consume(self) -> bytes:
		if self.__merged_chunks:
			data = b''.join(self.__merged_chunks + self.__pending_chunks)
		else:
			data = b''.join(self.__pending_chunks)
		self.__pending_chunks.clear()
		self.__merged_chunks.clear()
		self.__size = 0
		return data
