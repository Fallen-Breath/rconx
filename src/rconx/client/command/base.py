from abc import ABC, abstractmethod

from rconx.client.operation import AsyncRconOperationContext, RconOperationContext


class CommandHandler(ABC):
	@abstractmethod
	def execute(self, context: RconOperationContext, command: bytes):
		raise NotImplementedError

	@abstractmethod
	async def execute_async(self, context: AsyncRconOperationContext, command: bytes):
		raise NotImplementedError
