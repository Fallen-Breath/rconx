class RconClient:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def connect(self, *args, **kwargs):
		raise NotImplementedError

	def authenticate(self, *args, **kwargs):
		raise NotImplementedError

	def execute(self, *args, **kwargs):
		raise NotImplementedError

	def close(self, *args, **kwargs):
		raise NotImplementedError

	def __enter__(self, *args, **kwargs):
		raise NotImplementedError

	def __exit__(self, *args, **kwargs):
		raise NotImplementedError


class AsyncRconClient:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	async def connect(self, *args, **kwargs):
		raise NotImplementedError

	async def authenticate(self, *args, **kwargs):
		raise NotImplementedError

	async def execute(self, *args, **kwargs):
		raise NotImplementedError

	async def aclose(self, *args, **kwargs):
		raise NotImplementedError

	async def __aenter__(self, *args, **kwargs):
		raise NotImplementedError

	async def __aexit__(self, *args, **kwargs):
		raise NotImplementedError
