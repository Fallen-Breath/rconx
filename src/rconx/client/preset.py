from enum import Enum, auto

from typing_extensions import Literal


RconPresetLiteral = Literal['srcds', 'minecraft', 'cuberite', 'single_packet']


class RconPreset(Enum):
	"""
	Authentication, command exchange and default limits for high-level clients
	"""

	srcds = auto()
	"""Servers: SRCDS. Sends an ending probe immediately after the command and consumes both ending replies."""

	minecraft = auto()
	"""Servers: vanilla Minecraft, CraftBukkit, Paper, Folia. Sends an ending probe after the first command reply. Default send limit: 1460 bytes."""

	cuberite = auto()
	"""Servers: Cuberite. Consumes a type-2 empty acknowledgement and result packet. Default send limit: 1504 bytes."""

	single_packet = auto()
	"""Servers: Factorio, ARK: Survival Evolved, Conan Exiles (untested). Requires the complete result in one type-0 packet."""
