import asyncio
from typing import Awaitable, Optional, TypeVar

from rconx.common.exceptions import RconTimeoutError
from rconx.common.utils import Deadline


_ResultT = TypeVar('_ResultT')


async def await_with_timeout(awaitable: Awaitable[_ResultT], timeout: Optional[float]) -> _ResultT:
	if timeout is None:
		return await awaitable
	deadline = Deadline(timeout)
	operation = asyncio.ensure_future(awaitable)
	try:
		await asyncio.wait([operation], timeout=deadline.get_remaining_or_raise())
		if not operation.done():
			raise RconTimeoutError('operation timed out')
		return operation.result()
	finally:
		if not operation.done():
			operation.cancel()
			# Python 3.6 wait_for() does not wait for cancellation cleanup.
			await asyncio.wait([operation])
		if not operation.cancelled():
			operation.exception()
