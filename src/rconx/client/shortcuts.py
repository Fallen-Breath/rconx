from typing import Mapping, Union, cast

from typing_extensions import Unpack

from rconx.client.client import AsyncRconClient, RconClient
from rconx.client.client_options import RconClientOptions
from rconx.client.shortcut_options import AsyncRconExecuteOptions, SyncRconExecuteOptions
from rconx.common.content import RconContent
from rconx.common.utils import Deadline


def execute(host: str, port: int, command: Union[str, bytes], **kwargs: Unpack[SyncRconExecuteOptions]) -> RconContent:
	"""
	Connect, authenticate and execute one command, closing the connection on success or failure

	Connection, authentication and command exchange share the ``timeout`` budget.

	:param host: The remote host name or IP address
	:param port: The remote TCP port
	:param command: Command bytes or text encoded with the client's configured encoding; null bytes are not allowed
	:param kwargs: Options described by ``SyncRconExecuteOptions``
	:return: Collected response payload with access to its decoded text
	"""
	deadline = Deadline(kwargs.get('timeout'))
	password = kwargs['password']
	with RconClient.create(host, port, **__extract_client_options(kwargs)) as client:
		client.connect_and_authenticate(password, timeout=deadline.get_remaining_or_raise(), local_address=kwargs.get('local_address'))
		return client.execute(command, timeout=deadline.get_remaining_or_raise())


async def execute_async(host: str, port: int, command: Union[str, bytes], **kwargs: Unpack[AsyncRconExecuteOptions]) -> RconContent:
	"""
	Connect, authenticate and execute one command, closing the connection on success or failure

	Timeouts and cancellation are controlled by the caller through asyncio.

	:param host: The remote host name or IP address
	:param port: The remote TCP port
	:param command: Command bytes or text encoded with the client's configured encoding; null bytes are not allowed
	:param kwargs: Options described by ``AsyncRconExecuteOptions``
	:return: Collected response payload with access to its decoded text
	"""
	password = kwargs['password']
	async with AsyncRconClient.create(host, port, **__extract_client_options(kwargs)) as client:
		await client.connect_and_authenticate(password, local_address=kwargs.get('local_address'))
		return await client.execute(command)


def __extract_client_options(kwargs: Mapping[str, object]) -> RconClientOptions:
	return cast(RconClientOptions, {
		key: value
		for key, value in kwargs.items()
		if key in RconClientOptions.__annotations__
	})
