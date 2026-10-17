# Command-line guide

The rconx command-line interface executes commands on an RCON server from a terminal or script. It supports multiple commands in one invocation and an interactive mode.

## Installation and invocation

The `cli` extra installs the `rconx` console command:

```sh
python -m pip install "rconx[cli]"
```

The base `rconx` package also provides `python -m rconx`, which accepts the same arguments:

```sh
python -m rconx --help
```

## Running commands

The following example connects to a Minecraft server, authenticates, and executes `list`. The default preset is `minecraft`.

```sh
rconx -p "your-rcon-password" 127.0.0.1:25575 list
```

Addresses use `HOST:PORT`, with brackets around IPv6 addresses: `[IPv6]:PORT`.

All CLI options must appear before the address. Each argument after the address is treated as one complete RCON command, including values such as `--help`. Commands containing spaces need shell quoting:

```sh
rconx -p "your-rcon-password" 127.0.0.1:25575 list "say Server maintenance starts soon"
```

Commands execute in order on the same authenticated connection. A failure ends the invocation and stops the remaining commands. When interactive mode is disabled, omitting commands checks only the connection and authentication, then exits.

### Authentication

The CLI requires exactly one password source:

| Option | Password source |
| --- | --- |
| `-p PASSWORD`, `--password PASSWORD` | The supplied argument. |
| `--password-env NAME` | The value of the named environment variable. |

The following example reads the password from an existing `RCON_PASSWORD` environment variable:

```sh
rconx --password-env RCON_PASSWORD 127.0.0.1:25575 list
```

## Interactive mode

The `-i` or `--interactive` option enables interactive mode. The CLI first executes any commands supplied on the command line, then reads further commands from standard input. This example executes `list` before entering interactive mode:

```sh
rconx -i -p "your-rcon-password" 127.0.0.1:25575 list
```

The CLI sends each nonempty input line as one command and ignores empty lines. End-of-file ends the invocation. Ctrl+C also exits normally while the CLI is waiting for input.

## Output and encoding

The CLI uses separate streams for results and diagnostics:

| Stream | Content |
| --- | --- |
| Standard output (`stdout`) | Command results. |
| Standard error (`stderr`) | Interactive prompts and error diagnostics. |

### Text output

The CLI decodes each command result as text and appends a newline when writing it. Output can be redirected to a file:

```sh
rconx -p "your-rcon-password" 127.0.0.1:25575 list > players.txt
```

The `--encoding NAME` option sets the RCON text encoding for passwords, commands, and responses. It defaults to `utf8`. A response that cannot be decoded ends the invocation.

### Raw output

The `--raw` option writes the collected response bytes without text decoding or added newlines. Results from multiple commands are concatenated without separators. This option cannot be combined with interactive mode.

```sh
rconx --raw -p "your-rcon-password" 127.0.0.1:25575 list > response.bin
```

## Presets and limits

### Presets

The `-P` or `--preset` option selects how the CLI collects a complete command result. The available presets are `minecraft` (the default), `srcds`, `cuberite`, `single_packet`, and `idle_timeout`. The client guide describes their [collection behavior and server compatibility](client.md#presets-and-server-compatibility).

```sh
rconx -P srcds -p "your-rcon-password" 127.0.0.1:27015 status
```

The `idle_timeout` preset requires the `--idle-window SECONDS` option, which is accepted only with that preset. After the first complete response, this interval sets how long to wait for another response packet to begin. Expiration ends collection, so delayed output can be omitted.

```sh
rconx -P idle_timeout --idle-window 0.2 -p "your-rcon-password" 127.0.0.1:27015 status
```

### Timeouts

The `-t` or `--timeout` option defaults to 10 seconds. It applies to each connection attempt and to the complete authentication exchange. Each command also receives its own timeout budget, covering all response packets.

```sh
rconx --timeout 5 -p "your-rcon-password" 127.0.0.1:25575 list
```

The value `none` disables the timeout. A timeout of zero expires immediately.

### Size limits

Size options accept byte counts, or `none` to remove a limit:

| Option | Default | Scope |
| --- | --- | --- |
| `--max-send-packet-size` | 1460 bytes for `minecraft`, 1504 bytes for `cuberite`, otherwise `none` | Each outgoing packet. |
| `--max-receive-packet-size` | 4 MiB | Each incoming packet. |
| `--max-response-size` | 4 MiB | The accumulated output of one command. |

Packet limits include the length header. The response limit counts command output before text decoding. Exceeding a limit ends the invocation.

This example raises the response limit to 8 MiB:

```sh
rconx --max-response-size 8388608 -p "your-rcon-password" 127.0.0.1:25575 list
```
