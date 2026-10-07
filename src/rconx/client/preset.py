from enum import Enum, auto


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

	idle_timeout = auto()
	"""Servers: Project Zomboid (untested). Uses a quiet window between response packets; completion is heuristic."""
