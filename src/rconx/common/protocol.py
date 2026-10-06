class Packet:
	def __init__(self, *args, **kwargs):
		raise NotImplementedError

	def encode(self, *args, **kwargs):
		raise NotImplementedError

	@classmethod
	def decode(cls, *args, **kwargs):
		raise NotImplementedError
