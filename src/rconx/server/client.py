class RconServer:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def start(self, *args, **kwargs):
		raise NotImplementedError

	def serve_forever(self, *args, **kwargs):
		raise NotImplementedError

	def close(self, *args, **kwargs):
		raise NotImplementedError

	def authenticate(self, *args, **kwargs):
		raise NotImplementedError

	def handle_command(self, *args, **kwargs):
		raise NotImplementedError


class AsyncRconServer:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	async def start(self, *args, **kwargs):
		raise NotImplementedError

	async def serve_forever(self, *args, **kwargs):
		raise NotImplementedError

	async def aclose(self, *args, **kwargs):
		raise NotImplementedError

	async def authenticate(self, *args, **kwargs):
		raise NotImplementedError

	async def handle_command(self, *args, **kwargs):
		raise NotImplementedError
