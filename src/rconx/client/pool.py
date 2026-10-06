from rconx.client.client import AsyncRconClient, RconClient


class RconClientPool:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def acquire(self, *args, **kwargs) -> "_RconClientLease":
		raise NotImplementedError

	def execute(self, *args, **kwargs):
		raise NotImplementedError

	def close(self, *args, **kwargs):
		raise NotImplementedError

	def __enter__(self, *args, **kwargs):
		raise NotImplementedError

	def __exit__(self, *args, **kwargs):
		raise NotImplementedError


class AsyncRconClientPool:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def acquire(self, *args, **kwargs) -> "_AsyncRconClientLease":
		raise NotImplementedError

	async def execute(self, *args, **kwargs):
		raise NotImplementedError

	async def aclose(self, *args, **kwargs):
		raise NotImplementedError

	async def __aenter__(self, *args, **kwargs):
		raise NotImplementedError

	async def __aexit__(self, *args, **kwargs):
		raise NotImplementedError


class _RconClientLease:
	def __enter__(self, *args, **kwargs) -> RconClient:
		raise NotImplementedError

	def __exit__(self, *args, **kwargs):
		raise NotImplementedError


class _AsyncRconClientLease:
	async def __aenter__(self, *args, **kwargs) -> AsyncRconClient:
		raise NotImplementedError

	async def __aexit__(self, *args, **kwargs):
		raise NotImplementedError
