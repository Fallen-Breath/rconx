class RconContent:
	def __init__(self, content: bytes, *, default_encoding: str = 'utf8'):
		self.__content = content
		self.__default_encoding = default_encoding

	@property
	def content(self) -> bytes:
		return self.__content

	@property
	def text(self) -> str:
		return self.__content.decode(self.__default_encoding)

	def __str__(self) -> str:
		return self.text

	def __repr__(self) -> str:
		return '{}({!r})'.format(type(self).__name__, self.__content)
