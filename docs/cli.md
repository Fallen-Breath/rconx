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

The command format is:

```text
rconx [OPTIONS] HOST:PORT [COMMAND ...]
```

Addresses use `HOST:PORT`, with brackets around IPv6 addresses: `[IPv6]:PORT`.

All CLI options must appear before the address. Each argument after the address is treated as one complete RCON command, including values such as `--help`. Commands containing spaces need shell quoting.

The CLI reads the password from the `RCONX_PASSWORD` environment variable by default.

The default preset is `minecraft`. These examples show common invocations:

```sh
# Execute one command
rconx 127.0.0.1:25575 list

# Execute multiple commands in order
rconx 127.0.0.1:25575 list "say Server maintenance starts soon"

# Check the connection and authentication
rconx 127.0.0.1:25575

# Enter interactive mode without startup commands
rconx -i 127.0.0.1:25575

# Connect over IPv6
rconx "[::1]:25575" list
```

Commands execute in order on the same authenticated connection. A failure ends the invocation and stops the remaining commands. When interactive mode is disabled, omitting commands checks only the connection and authentication, then exits.

## Authentication

To set the default password in Bash:

```sh
export RCONX_PASSWORD="your-rcon-password"
```

The password source is selected as follows:

| Password selection | Source |
| --- | --- |
| No explicit password option is supplied | The value of environment variable `RCONX_PASSWORD`. |
| `-p PASSWORD`, `--password PASSWORD` | The supplied argument. |
| `--password-env NAME` | The value of the named environment variable. |
| `--password-file PATH` | The complete contents of the file as bytes. |

On multi-user systems, command-line passwords may be visible to other users through `ps`, so using environment variable is suggested.

The three explicit password options are mutually exclusive. If the selected environment variable is not set or the selected file cannot be read, the invocation fails without falling back to another source. Empty password strings and empty files are allowed.

These examples override the default source. The second reads an existing `MY_RCON_PASSWORD` environment variable:

```sh
rconx -p "another-rcon-password" 127.0.0.1:25575 list
rconx --password-env MY_RCON_PASSWORD 127.0.0.1:25575 list
rconx --password-file rcon-password.secret 127.0.0.1:25575 list
```

## Interactive mode

The `-i` or `--interactive` option enables interactive mode. The CLI first executes any commands supplied on the command line, then reads further commands from standard input. This example executes `list` before entering interactive mode:

```sh
rconx -i 127.0.0.1:25575 list
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
rconx 127.0.0.1:25575 list > players.txt
```

The `--encoding NAME` option sets the RCON text encoding for text passwords, commands, and responses. It defaults to `utf8`. It does not convert file password bytes. A response that cannot be decoded ends the invocation.

### Raw output

The `--raw` option writes the collected response bytes without text decoding or added newlines. Results from multiple commands are concatenated without separators. This option cannot be combined with interactive mode.

```sh
rconx --raw 127.0.0.1:25575 list > response.bin
```

## Presets and limits

### Presets

The `-P` or `--preset` option selects how the CLI collects a complete command result and defaults to `minecraft`. The client guide describes [collection behavior and tested server versions](client.md#presets-and-server-compatibility).

```sh
rconx -P srcds 127.0.0.1:27015 status
```

### Timeouts

The `-t` or `--timeout` option defaults to 10 seconds. It applies to each connection attempt and to the complete authentication exchange. Each command also receives its own timeout budget, covering all response packets.

```sh
rconx --timeout 5 127.0.0.1:25575 list
```

The value `none` disables the timeout.

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
rconx --max-response-size 8388608 127.0.0.1:25575 list
```
