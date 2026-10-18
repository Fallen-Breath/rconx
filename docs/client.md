# Client guide

`RconClient` and `AsyncRconClient` are high-level clients for authenticating with an RCON server, executing commands, and collecting results. Applications that implement their own protocol interactions can use the [low-level clients](#low-level-clients).

## Using a client

The following examples connect to a Minecraft server and execute a command.

### Synchronous client

```python
from rconx import RconClient, RconPreset

with RconClient.create("127.0.0.1", 25575, RconPreset.minecraft) as client:
	client.connect_and_authenticate("your-rcon-password")
	response = client.execute("list")
	print(response.text)
```

### Asynchronous client

```python
import asyncio

from rconx import AsyncRconClient, RconPreset


async def main():
	async with AsyncRconClient.create("127.0.0.1", 25575, RconPreset.minecraft) as client:
		await client.connect_and_authenticate("your-rcon-password")
		response = await client.execute("list")
		print(response.text)


asyncio.run(main())
```

The shorter high-level examples below use an authenticated `RconClient` named `client` unless they create a client.

### Client state and lifetime

Call `connect_and_authenticate` on a disconnected client to establish an authenticated connection. Use `connect` and `authenticate` to manage connection and authentication separately.

`RconClient` and `AsyncRconClient` expose these read-only properties:

| Property | Meaning |
| --- | --- |
| `connected` | The client has an established connection. |
| `authenticated` | Authentication has succeeded for the client's current connection. |

An authenticated client reuses its connection for subsequent commands. Leaving the context closes that connection and clears authentication. Applications that manage a client without a context can close it with `close()` or `await aclose()`. Closing may be repeated, and the same client can later connect and authenticate again.

```python
print(client.connected, client.authenticated)  # True True
client.close()
print(client.connected, client.authenticated)  # False False
```

A client runs one operation at a time. Overlapping calls raise `RconStateError`.

## Command results

Passwords and commands accept `str` or `bytes`. The `default_encoding` keyword argument to `create` sets the encoding for string inputs and defaults to `utf8`. Byte inputs are sent unchanged.

`execute` returns an `RconContent` containing the collected command output:

| Property | Meaning |
| --- | --- |
| `content` | The result as bytes. |
| `text` | The result decoded strictly using the client's configured encoding. |

```python
response = client.execute("list")
print(response.text)
print(response.content)
```

Accessing `text` can raise `UnicodeDecodeError`. Command execution has already completed at that point, and the decoding error leaves the client connected. The original bytes remain available through `content`.

The selected preset determines when result collection finishes.

## Presets and server compatibility

RCON commands can produce multiple response packets. The `preset` argument to `create` accepts a `RconPreset` value that determines when the client has collected a complete result.

| Preset | Result collection |
| --- | --- |
| `srcds` | Collects command replies through the SRCDS completion exchange. |
| `minecraft` | Waits for the first command reply before requesting an end-of-result marker. The server must send at least one reply for each command. |
| `cuberite` | Reads Cuberite's acknowledgement and complete command result. |
| `single_packet` | Treats the first command reply as the complete result. |
| `idle_timeout` | Collects replies until no new packet starts within a configured interval. |

The client handles the preset's auxiliary responses and excludes them from the returned command output.

```python
client = RconClient.create("127.0.0.1", 27015, RconPreset.srcds)
```

### Collection by idle timeout

The `RconPreset.idle_timeout` preset requires the `first_byte_timeout` keyword argument when calling `RconClient.create` or `AsyncRconClient.create`. This argument sets the waiting interval, in seconds, for another packet to start after the first complete response. Expiration ends collection and keeps the connection open.

```python
client = RconClient.create(
	"127.0.0.1", 27015, RconPreset.idle_timeout,
	first_byte_timeout=0.2,
)
```

The interval can end before delayed output arrives. That output is excluded from the current result and may interfere with the next command, so this preset provides a heuristic completion condition.

### Server compatibility

Candidate entries indicate that the preset's suitability for that server is not established.

| Server | Preset | Live-server testing |
| --- | --- | --- |
| SRCDS | `srcds` | Not tested |
| Vanilla Minecraft | `minecraft` | Not tested |
| CraftBukkit | `minecraft` | Not tested |
| Paper | `minecraft` | Not tested |
| Folia | `minecraft` | Not tested |
| Cuberite | `cuberite` | Not tested |
| Factorio | `single_packet` (candidate) | Not tested |
| ARK: Survival Evolved | `single_packet` (candidate) | Not tested |
| Conan Exiles | `single_packet` (candidate) | Not tested |
| Project Zomboid | `idle_timeout` (candidate) | Not tested |

## Timeouts and size limits

### Timeouts

`RconClient` accepts a `timeout` in seconds on its communication methods:

| Method | Timeout scope |
| --- | --- |
| `connect` | Each underlying connection attempt. |
| `authenticate` | The complete authentication operation. |
| `connect_and_authenticate` | A shared connection and authentication budget. |
| `execute` | The complete command exchange, including all response packets. |

`connect_and_authenticate` uses the time remaining after connecting for authentication. Connection establishment retains the timeout scope of `connect`.

The default `None` disables the timeout. A timeout of zero expires immediately. Receiving another packet does not renew an authentication or command timeout.

```python
response = client.execute("list", timeout=5)
```

`AsyncRconClient` uses caller-controlled asyncio timeouts, such as `asyncio.wait_for`. Cancellation during authentication or command execution closes the connection and propagates to the caller.

### Size limits

These settings are keyword arguments to `create`:

| Setting | Default | Scope |
| --- | --- | --- |
| `max_send_packet_size` | 1460 bytes for `minecraft`, 1504 bytes for `cuberite`, otherwise `None` | Each outgoing packet. |
| `max_receive_packet_size` | 4 MiB | Each incoming packet. |
| `max_response_size` | 4 MiB | The accumulated output of one command. |

Packet limits count encoded bytes, including the length header. `max_response_size` counts only command output. These limits are independent, and `None` removes an additional limit.

```python
client = RconClient.create(
	"127.0.0.1", 25575, RconPreset.minecraft,
	max_response_size=8 * 1024 * 1024,
)
```

## Errors and recovery

rconx classifies client failures with the following exceptions. All inherit from `RconError`.

| Exception | Meaning |
| --- | --- |
| `RconConnectionError` | Connection establishment or transport failed. |
| `RconTimeoutError` | A timeout managed by rconx expired. |
| `RconPacketDecodeError` | A received packet could not be decoded. |
| `RconAuthenticationError` | The server explicitly refused authentication or authenticated access. |
| `RconProtocolError` | A response did not match the selected preset's interaction. |
| `RconStateError` | The operation is invalid for the client state, or another operation is active. |

Argument and size validation can also raise `TypeError` or `ValueError`. Caller-controlled asyncio timeouts use the selected utility's timeout exception.

`RconClient` and `AsyncRconClient` handle failures as follows:

| Failure | Client state |
| --- | --- |
| A connection attempt fails. | Disconnected and unauthenticated. |
| An `authenticate` or `execute` call is rejected before its exchange begins. | The existing state is preserved. |
| An authentication or command exchange fails. | The connection closes and authentication is cleared. |

After a communication failure, the same client can connect and authenticate again. rconx does not automatically reconnect or resend commands. A command may have executed before its response was lost, so the application decides whether repeating it is appropriate.

The application can restore the connection after a transport failure:

```python
from rconx import RconConnectionError

try:
	client.execute("list")
except RconConnectionError:
	client.connect_and_authenticate("your-rcon-password")
```

## Low-level clients

`RawRconClient(host, port)` and `AsyncRawRconClient(host, port)` exchange `Packet` objects. The application supplies request IDs and implements authentication and response collection.

Both low-level clients use `connect` and support context managers for closing. The synchronous client uses `with`, and the asynchronous client uses `async with`.

This example sends a Minecraft authentication request and reads its response as a packet:

```python
from rconx import Packet, PacketType, RawRconClient

with RawRconClient("127.0.0.1", 25575) as client:
	client.connect()
	client.send_packet(Packet(1, PacketType.auth, b"your-rcon-password"))
	response = client.receive_packet()
	print(response)
```

| Method | Behavior |
| --- | --- |
| `send_packet(packet)` | Sends one complete packet. |
| `receive_packet()` | Receives one complete packet. |
| `try_receive_packet(first_byte_timeout=...)` | Receives a complete packet, or returns `None` if no packet starts within the specified interval. |

Once a packet starts, receiving must complete. A partial-packet failure closes the connection. A `None` result from `try_receive_packet` describes only the waiting interval and does not establish the end of a command result.

Synchronous low-level calls use a separate `timeout` for each send or receive. Asynchronous calls use caller-controlled timeouts.

Low-level clients accept the same packet-size keywords as high-level clients. Their outgoing limit defaults to `None`, and their incoming limit defaults to 4 MiB. Authentication state and accumulated result limits are managed by the application.

### Connections

[Low-level servers](server.md#low-level-servers) deliver `Connection` or `AsyncConnection` objects with the same packet methods.

| Class | Closing |
| --- | --- |
| `Connection` | `close()` closes the connection and interrupts pending communication. |
| `AsyncConnection` | `await aclose()` allows buffered writes to finish. `abort()` terminates communication and discards pending output. |

A closed connection object cannot reconnect. A low-level client can establish a new connection after closing or a communication failure.

A synchronous connection handler can use a context to close the delivered connection after reading one packet:

```python
from rconx import Connection


def handle(connection: Connection):
	with connection:
		connection.receive_packet()
```
