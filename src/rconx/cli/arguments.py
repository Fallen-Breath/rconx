import argparse
import codecs
import dataclasses
import os
import sys
from pathlib import Path
from typing import List, Optional, Union

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
	password: Union[str, bytes] = dataclasses.field(repr=False)
	commands: List[str]
	preset: RconPreset
	interactive: bool
	timeout: Optional[float]
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
	if value == 'none':
		return None
	try:
		return seconds(value)
	except ValueError as err:
		raise argparse.ArgumentTypeError("expected a finite non-negative number of seconds or 'none'; got {!r}".format(value)) from err


def size_limit(value: str) -> SizeLimit:
	try:
		size = None if value == 'none' else int(value)
		validate_packet_size_limit(size, 'size limit')
	except ValueError as err:
		raise argparse.ArgumentTypeError("expected a non-negative integer number of bytes or 'none'; got {!r}".format(value)) from err
	return SizeLimit(size)


def text_encoding(value: str) -> str:
	if not value:
		raise argparse.ArgumentTypeError('text encoding name must not be empty')
	try:
		codecs.lookup(value)
	except LookupError as err:
		raise argparse.ArgumentTypeError('unknown text encoding {!r}'.format(value)) from err
	try:
		''.encode(value)
	except LookupError as err:
		raise argparse.ArgumentTypeError('{!r} is not a text encoding'.format(value)) from err
	return value


def parse_args() -> CliOptions:
	parser = argparse.ArgumentParser(
		prog='rconx',
		usage='%(prog)s [OPTIONS] HOST:PORT [COMMAND ...]',
		description='Put every local option before HOST:PORT; use [IPv6]:PORT for IPv6 addresses.',
		epilog='Each COMMAND is one complete command string. After HOST:PORT, all arguments are commands, including --help, -i and --. '
			'Without -i, zero commands check the connection and authentication. With -i, interaction follows the startup commands.',
	)
	password_source = parser.add_mutually_exclusive_group()
	password_source.add_argument('-p', '--password', metavar='PASSWORD', help='password; an explicitly empty value is allowed')
	password_source.add_argument('--password-env', metavar='NAME', help='read the password from environment variable NAME (default source: RCONX_PASSWORD)')
	password_source.add_argument('--password-file', metavar='PATH', help='read the complete password bytes from file PATH')
	parser.add_argument('-P', '--preset', choices=[preset.name for preset in RconPreset], default='minecraft', help='RCON exchange preset (default: %(default)s)')
	output_mode = parser.add_mutually_exclusive_group()
	output_mode.add_argument('-i', '--interactive', action='store_true', help='read commands interactively after the startup commands')
	parser.add_argument('-t', '--timeout', type=timeout, default=10.0, metavar='SECONDS|none', help='timeout for each connect, authentication or command operation (default: 10 seconds)')
	parser.add_argument('--encoding', type=text_encoding, metavar='NAME', help='text password, command and response encoding (default: core utf8)')
	output_mode.add_argument('--raw', action='store_true', help='write response bytes; incompatible with -i')
	parser.add_argument('--max-send-packet-size', type=size_limit, metavar='BYTES|none', help='send packet size limit; none disables it (default: selected preset)')
	parser.add_argument('--max-receive-packet-size', type=size_limit, metavar='BYTES|none', help='receive packet size limit; none disables it (default: core limit)')
	parser.add_argument('--max-response-size', type=size_limit, metavar='BYTES|none', help='accumulated response size limit; none disables it (default: core limit)')
	parser.add_argument('--version', action='version', version='%(prog)s {}'.format(__get_self_version()))
	parser.add_argument('items', nargs=argparse.REMAINDER, metavar='HOST:PORT [COMMAND ...]', help='target address followed by zero or more complete command strings')
	if len(sys.argv) == 1:
		parser.print_help(file=sys.stderr)
		parser.exit(2)
	arguments = parser.parse_args()

	if not arguments.items:
		parser.error('HOST:PORT is required')
	try:
		address = compute_address(arguments.items[0])
	except ValueError as err:
		parser.error('invalid address: {}'.format(err))
	password: Union[str, bytes]
	if arguments.password is not None:
		password = arguments.password
	elif arguments.password_file is not None:
		if not arguments.password_file:
			parser.error('--password-file: file path must not be empty')
		try:
			password = Path(arguments.password_file).read_bytes()
		except OSError as err:
			parser.error('--password-file: cannot read {!r}: {}'.format(arguments.password_file, err.strerror))
	else:
		password_env = arguments.password_env if arguments.password_env is not None else 'RCONX_PASSWORD'
		if not password_env:
			parser.error('--password-env: environment variable name must not be empty')
		try:
			password = os.environ[password_env]
		except KeyError:
			if arguments.password_env is not None:
				parser.error('--password-env: environment variable {!r} is not set'.format(password_env))
			parser.error("password environment variable 'RCONX_PASSWORD' is not set; set it or specify --password, --password-env or --password-file")

	return CliOptions(
		address=address,
		password=password,
		commands=arguments.items[1:],
		preset=RconPreset[arguments.preset],
		interactive=arguments.interactive,
		timeout=arguments.timeout,
		encoding=arguments.encoding,
		raw=arguments.raw,
		max_send_packet_size=arguments.max_send_packet_size,
		max_receive_packet_size=arguments.max_receive_packet_size,
		max_response_size=arguments.max_response_size,
	)
