from typing import Callable, ContextManager, List, Union

import pytest

from rconx.client.client import RconClient
from rconx.client.preset import RconPreset
from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconAuthenticationError, RconConnectionError
from rconx.server.server import AsyncRconServer, RconServer
from tests.support import TcpAddress

_RunServer = Callable[[Union[RconServer, AsyncRconServer]], ContextManager[TcpAddress]]


@pytest.mark.parametrize('asynchronous_server', [False, True], ids=['sync-server', 'async-server'])
def test_complete_session(run_server: _RunServer, asynchronous_server: bool):
	large_result = '\U0001f642' * 3000
	def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		return large_result if content.content == b'large' else content.content

	server: Union[RconServer, AsyncRconServer]
	if asynchronous_server:
		async def async_command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
			return command(content, info)
		server = AsyncRconServer('127.0.0.1', 0, authentication='password', command_handler=async_command)
	else:
		server = RconServer('127.0.0.1', 0, authentication='password', command_handler=command)

	with run_server(server) as address:
		with RconClient.create(address.host, address.port, RconPreset.srcds) as client:
			client.connect(timeout=2)
			client.authenticate('password', timeout=2)
			assert client.connected and client.authenticated
			assert client.execute('ordinary', timeout=2).content == b'ordinary'
			assert client.execute('', timeout=2).content == b''
			response = client.execute(b'large', timeout=2)
			assert response.content == large_result.encode('utf8')
			assert response.text == large_result
			assert client.execute(b'next', timeout=2).content == b'next'


def test_non_default_encoding(run_server: _RunServer):
	passwords: List[RconContent] = []
	commands: List[RconContent] = []
	def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		passwords.append(content)
		return content.text == 's\xe9same'
	def command(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		commands.append(content)
		return 'r\xe9ponse' if content.text == 'caf\xe9' else b'th\xe9'

	server = RconServer('127.0.0.1', 0, authentication=authenticate, command_handler=command, default_encoding='latin1')
	with run_server(server) as address:
		with RconClient.create(address.host, address.port, RconPreset.srcds, default_encoding='latin1') as client:
			client.connect(timeout=2)
			client.authenticate('s\xe9same', timeout=2)
			text_response = client.execute('caf\xe9', timeout=2)
			bytes_response = client.execute(b'th\xe9', timeout=2)
			assert text_response.content == b'r\xe9ponse'
			assert text_response.text == 'r\xe9ponse'
			assert bytes_response.content == b'th\xe9'
			assert bytes_response.text == 'th\xe9'
			assert [content.content for content in passwords] == [b's\xe9same']
			assert [content.text for content in passwords] == ['s\xe9same']
			assert [content.content for content in commands] == [b'caf\xe9', b'th\xe9']
			assert [content.text for content in commands] == ['caf\xe9', 'th\xe9']


def test_authentication_rejection_allows_reconnect(run_server: _RunServer):
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		return content.content

	server = RconServer('127.0.0.1', 0, authentication=b'password', command_handler=command)
	with run_server(server) as address:
		with RconClient.create(address.host, address.port, RconPreset.srcds) as client:
			client.connect(timeout=2)
			with pytest.raises(RconAuthenticationError):
				client.authenticate('wrong', timeout=2)
			assert not client.connected and not client.authenticated
			client.connect(timeout=2)
			client.authenticate('password', timeout=2)
			assert client.connected and client.authenticated
			assert client.execute('reconnected', timeout=2).content == b'reconnected'


def test_callback_failure_only_ends_current_session(run_server: _RunServer):
	def command(content: RconContent, info: ConnectionInfo) -> bytes:
		if content.content == b'bad':
			raise RuntimeError('command failed')
		return content.content

	server = RconServer('127.0.0.1', 0, authentication='password', command_handler=command)
	with run_server(server) as address:
		with RconClient.create(address.host, address.port, RconPreset.srcds) as good, RconClient.create(address.host, address.port, RconPreset.srcds) as bad:
			good.connect(timeout=2)
			good.authenticate('password', timeout=2)
			bad.connect(timeout=2)
			bad.authenticate('password', timeout=2)
			with pytest.raises(RconConnectionError):
				bad.execute('bad', timeout=2)
			assert not bad.connected and not bad.authenticated
			assert good.execute('still running', timeout=2).content == b'still running'
			assert good.connected and good.authenticated
