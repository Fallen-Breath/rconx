# Server guide

`RconServer` and `AsyncRconServer` expose application commands through Source RCON over TCP. They implement SRCDS authentication and command responses. rconx clients connect using the [`srcds` preset](client.md#presets-and-server-compatibility).

## Running a server

The application supplies an authentication policy and a command callback. These examples expose a `status` command with a fixed password.

### Synchronous server

```python
from rconx import ConnectionInfo, RconContent, RconServer


def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.text == 'status':
        return 'ready'
    return 'Unknown command'


server = RconServer(
    '127.0.0.1', 27015,
    authentication='example-password',
    command_handler=execute,
)

try:
    server.serve()
except KeyboardInterrupt:
    pass
```

### Asynchronous server

```python
import asyncio

from rconx import AsyncRconServer, ConnectionInfo, RconContent


async def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.text == 'status':
        return 'ready'
    return 'Unknown command'


async def main():
    server = AsyncRconServer(
        '127.0.0.1', 27015,
        authentication='example-password',
        command_handler=execute,
    )
    await server.serve()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
```

Further synchronous snippets reuse the imports and `execute` callback from the first example unless they define their own.

## Authentication and commands

The server authenticates each client connection before processing its commands. The `authentication` argument accepts a fixed `str` or `bytes` password, or a callback for an application-defined policy. Rejection closes the connection.

The callbacks use regular functions with `RconServer` and asynchronous functions with `AsyncRconServer`:

| Callback          | Arguments                                    | Result                                |
|-------------------|----------------------------------------------|---------------------------------------|
| `authentication`  | Password content and connection information. | `bool`: whether to accept the client. |
| `command_handler` | Command content and connection information.  | `str` or `bytes`: the command output. |

A custom authentication callback can replace the fixed password:

```python
def authenticate(password: RconContent, info: ConnectionInfo) -> bool:
    return password.content == b'example-password'


server = RconServer(
    '127.0.0.1', 27015,
    authentication=authenticate,
    command_handler=execute,
)
```

### Callback inputs

Both callbacks receive content as `RconContent` and connection information as `ConnectionInfo`:

| Property                        | Meaning                                             |
|---------------------------------|-----------------------------------------------------|
| `RconContent.content`           | The submitted password or command as bytes.         |
| `RconContent.text`              | The submitted content decoded strictly as text.     |
| `ConnectionInfo.local_address`  | The server endpoint, with `host` and `port` fields. |
| `ConnectionInfo.remote_address` | The client endpoint, with `host` and `port` fields. |

The `default_encoding` option defaults to `utf8`. It controls text decoding through `RconContent.text` and the encoding of string passwords and command results. Byte values are used unchanged.

This command handler recognizes `peer` from the raw bytes and otherwise returns the decoded text:

```python
def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.content == b'peer':
        return '{}:{}'.format(info.remote_address.host, info.remote_address.port)
    return command.text
```

### Command output

The server sends the command callback's return value to the client. It splits larger results into response packets and handles the SRCDS completion exchange automatically. The callback can return an empty string or `b''` for an empty result. `None` is not a valid result.

Commands and encoded results must not contain embedded NUL bytes. Invalid callback results end the affected connection.

This handler returns multiple lines for `status` and an empty result for other commands:

```python
def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.text == 'status':
        return 'players: 0\nstate: ready'
    return ''
```

## Server lifecycle

A server instance can run once. Starting another run after shutdown or a startup failure requires a new instance.

| Method       | Behavior                                                                                                                     |
|--------------|------------------------------------------------------------------------------------------------------------------------------|
| `serve()`    | Accepts connections and processes commands until stopped. It exits after the server's connection handlers have finished.     |
| `shutdown()` | Stops accepting connections and closes current connections. It requests stopping without waiting for all handlers to finish. |

The methods are awaited on `AsyncRconServer`. Complete shutdown requires waiting for `serve` to exit after requesting `shutdown`.

The two server classes handle running business callbacks differently during stopping:

| Server            | Running callbacks                              |
|-------------------|------------------------------------------------|
| `RconServer`      | Waits for callbacks to return.                 |
| `AsyncRconServer` | Cancels callbacks and waits for their cleanup. |

Cancelling asynchronous `serve` also stops the server and propagates cancellation after its connection handlers have finished.

The command callback can stop the server. Shutdown closes the client connection, so `stop` may not receive a reply:

```python
def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.text == 'stop':
        server.shutdown()
        return ''
    return 'ready'


server = RconServer(
    '127.0.0.1', 27015,
    authentication='example-password',
    command_handler=execute,
)
server.serve()
```

## Resource controls

### Concurrent connections

Requests on one connection are processed serially. Callbacks for different connections may run concurrently: `RconServer` uses worker threads, and `AsyncRconServer` uses asyncio tasks.

| Setting           | Default | Scope                                                                                                |
|-------------------|---------|------------------------------------------------------------------------------------------------------|
| `max_connections` | `100`   | Concurrent connection handlers, including clients awaiting authentication. `None` removes the limit. |
| `backlog`         | `5`     | The listener's pending TCP connection queue.                                                         |

When handler capacity is unavailable, the server closes new connections.

This server accepts up to 20 concurrent connection handlers:

```python
server = RconServer(
    '127.0.0.1', 27015,
    authentication='example-password',
    command_handler=execute,
    max_connections=20,
)
```

### Packet and result sizes

Packet limits count each complete encoded packet, including its length header. The result limit counts the command output before splitting.

| Setting                   | Default | Scope                              |
|---------------------------|---------|------------------------------------|
| `max_receive_packet_size` | `4100`  | Each incoming packet.              |
| `max_send_packet_size`    | `4100`  | Each outgoing packet.              |
| `max_response_size`       | 4 MiB   | The encoded result of one command. |

These settings accept byte counts, with `None` removing an additional limit. Exceeding a limit closes the affected connection.

This server limits encoded command output to 64 KiB:

```python
server = RconServer(
    '127.0.0.1', 27015,
    authentication='example-password',
    command_handler=execute,
    max_response_size=64 * 1024,
)
```

### Communication timeouts

Timeout settings are in seconds. `None` disables a timeout. Expiration closes the affected connection.

| Setting                  | Default | Scope                                                                                                                                             |
|--------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `authentication_timeout` | `10.0`  | From accepting a connection to receiving its first complete authentication request. It excludes the authentication callback and response sending. |
| `idle_timeout`           | `None`  | An authenticated connection's wait for its next request to start.                                                                                 |
| `receive_timeout`        | `None`  | Receiving the rest of a packet after its first byte.                                                                                              |
| `send_timeout`           | `None`  | Sending one complete response packet.                                                                                                             |

These timeouts cover communication. The library does not impose a business callback timeout.

This server allows a 60-second wait for an authenticated client's next request:

```python
server = RconServer(
    '127.0.0.1', 27015,
    authentication='example-password',
    command_handler=execute,
    idle_timeout=60.0,
)
```

## Failures and logging

### Failure handling

The effect of a failure depends on whether it occurs while handling a client or running the server:

| Failure | Effect |
| --- | --- |
| A client encounters a protocol, communication, or callback error. | Its connection closes. Other clients continue to be served. |
| The `serve` operation fails. | The server closes its connections and propagates the error to its caller. |

The server does not convert application exceptions into RCON error messages.

This callback raises an application error for the `fail` command:

```python
def execute(command: RconContent, info: ConnectionInfo) -> str:
    if command.text == 'fail':
        raise RuntimeError('Command failed')
    return 'ready'
```

### Logging

rconx provides two loggers through Python's logging system:

| Logger         | Records                                                  |
|----------------|----------------------------------------------------------|
| `rconx.server` | Listener and connection diagnostics.                     |
| `rconx.access` | Authentication decisions and command result preparation. |

rconx does not manage logging configuration. The library relies on the application to configure logging when output is needed.

For basic console output, configure logging before starting the server:

```python
import logging


logging.basicConfig(level=logging.INFO)
```

## Low-level servers

`RawRconServer` and `AsyncRawRconServer` deliver accepted connections to a regular `handler(connection)` function. The handler receives a `Connection` or `AsyncConnection`, respectively. Their packet methods and closing behavior are described under [Connections](client.md#connections).

The application implements authentication and command exchanges. For concurrent handling, it also arranges and manages worker threads or tasks. Processing an entire connection inside a synchronous delivery handler delays acceptance of other clients.

The server tracks delivered connections and closes them during shutdown. The application remains responsible for waiting for any work it started in the handler.

Low-level servers accept the same packet-size settings as high-level servers. Their outgoing limit defaults to `None`, and their incoming limit defaults to 4 MiB.

This synchronous packet echo reads one packet, sends it back, and closes the connection. The delivery handler processes connections serially in the serving thread:

```python
from rconx import Connection, RawRconServer


def handle_connection(connection: Connection):
    with connection:
        packet = connection.receive_packet()
        connection.send_packet(packet)


server = RawRconServer('127.0.0.1', 27015, handle_connection)
try:
    server.serve()
except KeyboardInterrupt:
    pass
```
