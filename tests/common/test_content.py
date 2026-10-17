import pytest

from rconx.common.content import RconContent


@pytest.mark.parametrize('payload', [b'', b'hello', b'\x00\xff'])
def test_content_preserves_bytes(payload: bytes):
	assert RconContent(payload).content == payload


def test_default_text_encoding():
	content = RconContent('中文'.encode('utf8'))
	assert content.text == '中文'
	assert str(content) == content.text


def test_configured_text_encoding():
	content = RconContent(b'\xe9', default_encoding='latin1')
	assert content.text == 'é'
	assert str(content) == content.text


@pytest.mark.parametrize('attribute', ['content', 'text'])
def test_content_is_read_only(attribute: str):
	content = RconContent(b'original')
	with pytest.raises(AttributeError):
		setattr(content, attribute, 'replacement')
	assert content.content == b'original'
	assert content.text == 'original'


def test_invalid_text_is_rejected_on_access():
	content = RconContent(b'\xff')
	assert content.content == b'\xff'
	with pytest.raises(UnicodeDecodeError):
		_ = content.text
	with pytest.raises(UnicodeDecodeError):
		str(content)
	assert content.content == b'\xff'
