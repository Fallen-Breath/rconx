# rconx

[![License](https://img.shields.io/badge/license-LGPL--3.0--only-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/rconx.svg)](https://pypi.org/project/rconx/)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#installation)

rconx is a Python library for communicating with TCP servers that use the [Source RCON](https://developer.valvesoftware.com/wiki/Source_RCON_Protocol) packet format. It provides synchronous and asynchronous clients for authentication and command execution, together with servers for exposing an application's own command handlers over RCON.

The clients collect command results using selectable presets, including SRCDS, Minecraft, and Cuberite. Low-level clients and servers are available for applications that need to exchange individual RCON packets. The command-line interface executes commands from a terminal or script.

## Installation

rconx supports Python 3.8 and later, with best-effort compatibility for Python 3.6 and 3.7. The examples below use Python 3.8 or later.

rconx is available on [PyPI](https://pypi.org/project/rconx/):

```sh
python -m pip install rconx
```

## Quick start

Application code should import public APIs directly from `rconx`. Only these public exports are guaranteed to remain stable across releases. Internal modules such as `rconx.client` are outside this guarantee.

These examples execute `list` on a Minecraft server with RCON enabled.

### Synchronous client

```python
from rconx import RconClient

with RconClient.create('127.0.0.1', 25575, 'minecraft') as client:
    client.connect_and_authenticate('your-rcon-password')
    response = client.execute('list')
    print(response.text)  # e.g. There are 1 of a max of 20 players online: Steve
```

### Asynchronous client

```python
import asyncio

from rconx import AsyncRconClient


async def main():
    async with AsyncRconClient.create('127.0.0.1', 25575, 'minecraft') as client:
        await client.connect_and_authenticate('your-rcon-password')
        response = await client.execute('list')
        print(response.text)


asyncio.run(main())
```

## Documentation

- The [client guide](https://github.com/Fallen-Breath/rconx/blob/master/docs/client.md) describes client usage, presets, command results, and error handling. It also records compatibility and testing status for individual servers.
- The [server guide](https://github.com/Fallen-Breath/rconx/blob/master/docs/server.md) describes business callbacks, connection handling, and service lifecycle.
- The [command-line guide](https://github.com/Fallen-Breath/rconx/blob/master/docs/cli.md) describes installation, command execution, interactive mode, and output handling.

## License

rconx is licensed under [LGPL-3.0-only](LICENSE).
