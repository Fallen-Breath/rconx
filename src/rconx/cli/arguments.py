import argparse
import dataclasses
import os
import sys
from typing import List, Optional

from rconx.client.preset import RconPreset
from rconx.common.connection_info import ConnectionAddress
from rconx.common.utils import validate_packet_size_limit, validate_timeout


def __get_self_version() -> str:
	try:
		if sys.version_info >= (3, 8):
			from importlib.metadata import version
		else:
			from importlib_metadata import version
		return version('rconx')
	except Exception:
		return 'unknown'


@dataclasses.dataclass(frozen=True)
class SizeLimit:
	value: Optional[int]


@dataclasses.dataclass(frozen=True)
class CliOptions:
	address: ConnectionAddress
	password: str = dataclasses.field(repr=False)
	commands: List[str]
	preset: RconPreset
	interactive: bool
	timeout: Optional[float]
	idle_window: Optional[float]
	encoding: Optional[str]
	raw: bool
	max_send_packet_size: Optional[SizeLimit]
	max_receive_packet_size: Optional[SizeLimit]
	max_response_size: Optional[SizeLimit]


def compute_address(value: str) -> ConnectionAddress:
	if value.startswith('['):
		closing = value.find(']')
		if closing < 0 or value[closing + 1:closing + 2] != ':':
			raise ValueError('expected HOST:PORT or [IPv6]:PORT')
		host = value[1:closing]
		port_text = value[closing + 2:]
		if ':' not in host or '[' in host:
			raise ValueError('expected [IPv6]:PORT')
	else:
		host, separator, port_text = value.rpartition(':')
		if not separator or ':' in host or '[' in host or ']' in host:
			raise ValueError('expected HOST:PORT or [IPv6]:PORT')
	if not host:
		raise ValueError('host must not be empty')
	if not port_text or any(character < '0' or character > '9' for character in port_text):
		raise ValueError('port must be a decimal integer from 1 to 65535')
	port = int(port_text)
	if not 1 <= port <= 65535:
		raise ValueError('port must be from 1 to 65535')
	return ConnectionAddress(host, port)


def seconds(value: str) -> float:
	seconds = float(value)
	validate_timeout(seconds)
	return seconds


def timeout(value: str) -> Optional[float]:
	return None if value == 'none' else seconds(value)


def size_limit(value: str) -> SizeLimit:
	size = None if value == 'none' else int(value)
	validate_packet_size_limit(size, 'size limit')
	return SizeLimit(size)


def parse_args() -> CliOptions:
	parser = argparse.ArgumentParser(
		prog='rconx',
		usage='%(prog)s [OPTIONS] HOST:PORT [COMMAND ...]',
		description='Put every local option before HOST:PORT; use [IPv6]:PORT for IPv6 addresses.',
		epilog='Each COMMAND is one complete command string. After HOST:PORT, all arguments are commands, including --help, -i and --. '
			'Without -i, zero commands check the connection and authentication. With -i, interaction follows the startup commands.',
	)
	password_source = parser.add_mutually_exclusive_group(required=True)
	password_source.add_argument('-p', '--password', metavar='PASSWORD', help='password; an explicitly empty value is allowed')
	password_source.add_argument('--password-env', metavar='NAME', help='read the password from environment variable NAME')
	parser.add_argument('-P', '--preset', choices=[preset.name for preset in RconPreset], default='minecraft', help='RCON exchange preset (default: %(default)s)')
	parser.add_argument('-i', '--interactive', action='store_true', help='read commands interactively after the startup commands')
	parser.add_argument('-t', '--timeout', type=timeout, default=10.0, metavar='SECONDS|none', help='timeout for each connect, authentication or command operation (default: 10 seconds)')
	parser.add_argument('--idle-window', type=seconds, metavar='SECONDS', help='response quiet window; required and valid only for the idle_timeout preset')
	parser.add_argument('--encoding', metavar='NAME', help='password, command and response text encoding (default: core utf8)')
	parser.add_argument('--raw', action='store_true', help='write response bytes; incompatible with -i')
	parser.add_argument('--max-send-packet-size', type=size_limit, metavar='BYTES|none', help='send packet size limit; none disables it (default: selected preset)')
	parser.add_argument('--max-receive-packet-size', type=size_limit, metavar='BYTES|none', help='receive packet size limit; none disables it (default: core limit)')
	parser.add_argument('--max-response-size', type=size_limit, metavar='BYTES|none', help='accumulated response size limit; none disables it (default: core limit)')
	parser.add_argument('--version', action='version', version='%(prog)s {}'.format(__get_self_version()))
	parser.add_argument('items', nargs=argparse.REMAINDER, metavar='HOST:PORT [COMMAND ...]', help='target address followed by zero or more complete command strings')
	arguments = parser.parse_args()

	if not arguments.items:
		raise ValueError('HOST:PORT is required')
	if (arguments.preset == 'idle_timeout') != (arguments.idle_window is not None):
		raise ValueError('--idle-window is required and valid only for idle_timeout')
	if arguments.raw and arguments.interactive:
		raise ValueError('--raw cannot be combined with --interactive')
	password = os.environ[arguments.password_env] if arguments.password_env is not None else arguments.password

	return CliOptions(
		address=compute_address(arguments.items[0]),
		password=password,
		commands=arguments.items[1:],
		preset=RconPreset[arguments.preset],
		interactive=arguments.interactive,
		timeout=arguments.timeout,
		idle_window=arguments.idle_window,
		encoding=arguments.encoding,
		raw=arguments.raw,
		max_send_packet_size=arguments.max_send_packet_size,
		max_receive_packet_size=arguments.max_receive_packet_size,
		max_response_size=arguments.max_response_size,
	)
