from typing import Optional, Union

from typing_extensions import NotRequired

from rconx.client.client_options import RconClientOptions
from rconx.client.preset import RconPreset, RconPresetLiteral
from rconx.client.raw import LocalAddress


class __RconExecuteOptions(RconClientOptions):
	password: Union[str, bytes]
	preset: Union[RconPreset, RconPresetLiteral]
	local_address: NotRequired[Optional[LocalAddress]]


class SyncRconExecuteOptions(__RconExecuteOptions):
	timeout: NotRequired[Optional[float]]


class AsyncRconExecuteOptions(__RconExecuteOptions):
	pass
