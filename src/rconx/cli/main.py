import sys
from typing import Optional

from rconx.cli.arguments import CliOptions, parse_args
from rconx.client.client import RconClient
from rconx.client.client_options import RconClientOptions
from rconx.common.content import RconContent


def main():
	options = parse_args()
	with __create_client(options) as client:
		__run(client, options)


def __create_client(options: CliOptions) -> RconClient:
	client_options: RconClientOptions = {'preset': options.preset}
	if options.encoding is not None:
		client_options['default_encoding'] = options.encoding
	if options.max_send_packet_size is not None:
		client_options['max_send_packet_size'] = options.max_send_packet_size.value
	if options.max_receive_packet_size is not None:
		client_options['max_receive_packet_size'] = options.max_receive_packet_size.value
	if options.max_response_size is not None:
		client_options['max_response_size'] = options.max_response_size.value
	return RconClient.create(
		options.address.host, options.address.port,
		**client_options,
	)


def __run(client: RconClient, options: CliOptions):
	client.connect(timeout=options.timeout)
	client.authenticate(options.password, timeout=options.timeout)

	for option_command in options.commands:
		__execute_command(client, option_command, options)
	if not options.interactive:
		return

	while True:
		user_command = __read_command()
		if user_command is None:
			return
		if user_command:
			__execute_command(client, user_command, options)


def __execute_command(client: RconClient, command: str, options: CliOptions):
	response = client.execute(command, timeout=options.timeout)
	__write_result(response, options)


def __read_command() -> Optional[str]:
	sys.stderr.write('rconx> ')
	sys.stderr.flush()
	try:
		line = sys.stdin.readline()
		if not line:
			return None
		if line.endswith('\n'):
			line = line[:-1]
		if line.endswith('\r'):
			line = line[:-1]
	except (KeyboardInterrupt, EOFError):
		sys.exit(0)
	return line


def __write_result(response: RconContent, options: CliOptions):
	if options.raw:
		sys.stdout.buffer.write(response.content)
		sys.stdout.buffer.flush()
	else:
		sys.stdout.write(response.text)
		sys.stdout.write('\n')
		sys.stdout.flush()
