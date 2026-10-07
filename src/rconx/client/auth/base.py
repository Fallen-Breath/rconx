from abc import ABC, abstractmethod

from rconx.client.operation import AsyncRconOperationContext, RconOperationContext


class AuthenticationHandler(ABC):
	@abstractmethod
	def authenticate(self, context: RconOperationContext, password: bytes):
		raise NotImplementedError

	@abstractmethod
	async def authenticate_async(self, context: AsyncRconOperationContext, password: bytes):
		raise NotImplementedError
