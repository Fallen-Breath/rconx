from typing import Callable, ContextManager, List, Union

import pytest

from rconx.client.client import AsyncRconClient
from rconx.client.preset import RconPreset
from rconx.common.connection_info import ConnectionInfo
from rconx.common.content import RconContent
from rconx.common.exceptions import RconAuthenticationError, RconConnectionError
from rconx.server.server import AsyncRconServer, RconServer
from tests.support import RunAsync, TcpAddress


_RunServer = Callable[[Union[RconServer, AsyncRconServer]], ContextManager[TcpAddress]]


@pytest.mark.parametrize('asynchronous_server', [False, True])
def test_complete_session(run_server: _RunServer, run_async: RunAsync, asynchronous_server: bool):
	large_result = '\U0001f642' * 3000

	def handle(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		return large_result if content.content == b'large' else content.content

	async def handle_async(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		return handle(content, info)

	server: Union[RconServer, AsyncRconServer]
	if asynchronous_server:
		server = AsyncRconServer('127.0.0.1', 0, authentication='password', command_handler=handle_async)
	else:
		server = RconServer('127.0.0.1', 0, authentication='password', command_handler=handle)

	with run_server(server) as address:
		async def scenario():
			async with AsyncRconClient.create(address.host, address.port, RconPreset.srcds) as client:
				await client.connect()
				await client.authenticate('password')
				assert client.authenticated
				response = await client.execute('ordinary')
				assert response.content == b'ordinary' and response.text == 'ordinary'
				response = await client.execute(b'')
				assert response.content == b'' and response.text == ''
				response = await client.execute('large')
				assert response.content == large_result.encode('utf-8') and response.text == large_result
				response = await client.execute(b'next')
				assert response.content == b'next' and response.text == 'next'
				assert client.authenticated

		run_async(scenario())


def test_non_default_encoding(run_server: _RunServer, run_async: RunAsync):
	authentications: List[RconContent] = []
	commands: List[RconContent] = []

	async def authenticate(content: RconContent, info: ConnectionInfo) -> bool:
		authentications.append(content)
		return content.text == 's\xe9cret'

	async def handle(content: RconContent, info: ConnectionInfo) -> Union[bytes, str]:
		commands.append(content)
		return 'r\xe9ponse' if content.text == 'caf\xe9' else b'voil\xe0'

	server = AsyncRconServer('127.0.0.1', 0, authentication=authenticate, command_handler=handle, default_encoding='latin-1')
	with run_server(server) as address:
		async def scenario():
			async with AsyncRconClient.create(address.host, address.port, RconPreset.srcds, default_encoding='latin-1') as client:
				await client.connect()
				await client.authenticate('s\xe9cret')
				response = await client.execute('caf\xe9')
				assert response.content == b'r\xe9ponse' and response.text == 'r\xe9ponse'
				response = await client.execute(b'd\xe9j\xe0')
				assert response.content == b'voil\xe0' and response.text == 'voil\xe0'

		run_async(scenario())
	assert [content.content for content in authentications] == [b's\xe9cret']
	assert [content.text for content in authentications] == ['s\xe9cret']
	assert [content.content for content in commands] == [b'caf\xe9', b'd\xe9j\xe0']
	assert [content.text for content in commands] == ['caf\xe9', 'd\xe9j\xe0']


def test_authentication_rejection_allows_reconnect(run_server: _RunServer, run_async: RunAsync):
	async def handle(content: RconContent, info: ConnectionInfo) -> bytes:
		return content.content

	server = AsyncRconServer('127.0.0.1', 0, authentication='password', command_handler=handle)
	with run_server(server) as address:
		async def scenario():
			async with AsyncRconClient.create(address.host, address.port, RconPreset.srcds) as client:
				await client.connect()
				with pytest.raises(RconAuthenticationError):
					await client.authenticate('wrong')
				assert not client.connected and not client.authenticated
				await client.connect()
				await client.authenticate('password')
				assert client.authenticated
				assert (await client.execute('next')).content == b'next'

		run_async(scenario())


def test_callback_failure_only_ends_current_session(run_server: _RunServer, run_async: RunAsync):
	async def handle(content: RconContent, info: ConnectionInfo) -> bytes:
		if content.content == b'bad':
			raise RuntimeError('command failed')
		return content.content

	server = AsyncRconServer('127.0.0.1', 0, authentication='password', command_handler=handle)
	with run_server(server) as address:
		async def scenario():
			async with AsyncRconClient.create(address.host, address.port, RconPreset.srcds) as failing:
				async with AsyncRconClient.create(address.host, address.port, RconPreset.srcds) as healthy:
					await failing.connect()
					await failing.authenticate('password')
					await healthy.connect()
					await healthy.authenticate('password')
					with pytest.raises(RconConnectionError):
						await failing.execute('bad')
					assert not failing.connected and not failing.authenticated
					assert (await healthy.execute('next')).content == b'next'
					assert healthy.authenticated

		run_async(scenario())
