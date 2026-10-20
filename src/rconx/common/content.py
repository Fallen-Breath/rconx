class RconContent:
	"""
	Read-only byte content with decoded text access

	:param content: Raw payload bytes
	:keyword default_encoding: Encoding used for text access, defaulting to ``utf8``
	"""

	def __init__(self, content: bytes, *, default_encoding: str = 'utf8'):
		self.__content = content
		self.__default_encoding = default_encoding

	@property
	def content(self) -> bytes:
		return self.__content

	@property
	def text(self) -> str:
		"""
		Decode the content using the configured encoding, with strict error handling
		"""
		return self.__content.decode(self.__default_encoding)

	def __str__(self) -> str:
		return self.text

	def __repr__(self) -> str:
		return '{}({!r})'.format(type(self).__name__, self.__content)
