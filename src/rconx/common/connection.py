class Connection:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def send_packet(self, *args, **kwargs):
		raise NotImplementedError

	def receive_packet(self, *args, **kwargs):
		raise NotImplementedError

	def close(self, *args, **kwargs):
		raise NotImplementedError

	def __enter__(self, *args, **kwargs):
		raise NotImplementedError

	def __exit__(self, *args, **kwargs):
		raise NotImplementedError


class AsyncConnection:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	async def send_packet(self, *args, **kwargs):
		raise NotImplementedError

	async def receive_packet(self, *args, **kwargs):
		raise NotImplementedError

	async def aclose(self, *args, **kwargs):
		raise NotImplementedError

	async def __aenter__(self, *args, **kwargs):
		raise NotImplementedError

	async def __aexit__(self, *args, **kwargs):
		raise NotImplementedError
