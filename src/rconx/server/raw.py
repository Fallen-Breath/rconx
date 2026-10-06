class RawRconServer:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def start(self, *args, **kwargs):
		raise NotImplementedError

	def serve_forever(self, *args, **kwargs):
		raise NotImplementedError

	def close(self, *args, **kwargs):
		raise NotImplementedError

	def on_connect(self, *args, **kwargs):
		raise NotImplementedError

	def on_disconnect(self, *args, **kwargs):
		raise NotImplementedError

	def handle_packet(self, *args, **kwargs):
		raise NotImplementedError


class AsyncRawRconServer:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	async def start(self, *args, **kwargs):
		raise NotImplementedError

	async def serve_forever(self, *args, **kwargs):
		raise NotImplementedError

	async def aclose(self, *args, **kwargs):
		raise NotImplementedError

	async def on_connect(self, *args, **kwargs):
		raise NotImplementedError

	async def on_disconnect(self, *args, **kwargs):
		raise NotImplementedError

	async def handle_packet(self, *args, **kwargs):
		raise NotImplementedError
