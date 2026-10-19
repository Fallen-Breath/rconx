import io
import sys
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, Mock, call

import pytest

from rconx.cli.main import main
from rconx.client.client import RconClient
from rconx.client.preset import RconPreset
from rconx.common.content import RconContent
from rconx.common.exceptions import RconAuthenticationError, RconConnectionError, RconTimeoutError


@pytest.fixture
def create_client(monkeypatch: pytest.MonkeyPatch) -> Mock:
	create = Mock()
	monkeypatch.setattr(RconClient, 'create', create)
	return create


@pytest.fixture
def client(create_client: Mock, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
	client = MagicMock(spec=RconClient)
	client.__enter__.return_value = client
	client.execute.return_value = RconContent(b'response')
	create_client.return_value = client
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', 'localhost:25575'])
	monkeypatch.setattr(sys, 'stdin', io.StringIO())
	return client


def test_zero_commands_checks_connection_and_authentication(
	client: MagicMock, create_client: Mock, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
	stdin = Mock(wraps=io.StringIO('unused\n'))
	monkeypatch.setattr(sys, 'stdin', stdin)
	main()
	assert create_client.call_count == 1
	assert create_client.call_args[0] == ('localhost', 25575, RconPreset.minecraft)
	for option in ('default_encoding', 'max_send_packet_size', 'max_receive_packet_size', 'max_response_size'):
		assert option not in create_client.call_args[1]
	assert client.mock_calls == [
		call.__enter__(),
		call.connect(timeout=10.0),
		call.authenticate('password', timeout=10.0),
		call.__exit__(None, None, None),
	]
	stdin.readline.assert_not_called()
	output = capsys.readouterr()
	assert output.out == ''
	assert output.err == ''


def test_authenticates_with_default_password_variable(client: MagicMock, monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv('RCONX_PASSWORD', 'environment password')
	monkeypatch.setattr(sys, 'argv', ['rconx', '127.0.0.1:25575', 'list'])
	main()
	client.authenticate.assert_called_once_with('environment password', timeout=10.0)
	client.execute.assert_called_once_with('list', timeout=10.0)


def test_authenticates_with_complete_file_bytes(client: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
	password = b' \xfffirst\r\nsecond\n'
	password_file = tmp_path / 'password.bin'
	password_file.write_bytes(password)
	monkeypatch.delenv('RCONX_PASSWORD', raising=False)
	monkeypatch.setattr(sys, 'argv', ['rconx', '--password-file', str(password_file), '--encoding', 'utf8', '127.0.0.1:25575', 'list'])
	main()
	client.authenticate.assert_called_once_with(password, timeout=10.0)
	client.execute.assert_called_once_with('list', timeout=10.0)


def test_explicit_client_configuration(client: MagicMock, create_client: Mock, monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', [
		'rconx', '-p', 'password', '-P', 'srcds', '--encoding', 'latin1', '-t', '0.75',
		'--max-send-packet-size', '101', '--max-receive-packet-size', '202', '--max-response-size', '303',
		'[::1]:12345', 'command',
	])
	main()
	assert create_client.call_count == 1
	assert create_client.call_args[0] == ('::1', 12345, RconPreset.srcds)
	options = create_client.call_args[1]
	assert options['default_encoding'] == 'latin1'
	assert options['max_send_packet_size'] == 101
	assert options['max_receive_packet_size'] == 202
	assert options['max_response_size'] == 303
	client.connect.assert_called_once_with(timeout=0.75)
	client.authenticate.assert_called_once_with('password', timeout=0.75)
	client.execute.assert_called_once_with('command', timeout=0.75)
	client.__exit__.assert_called_once_with(None, None, None)


def test_explicit_none_disables_limits_and_timeout(client: MagicMock, create_client: Mock, monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', [
		'rconx', '-p', 'password', '-t', 'none', '--max-send-packet-size', 'none', '--max-receive-packet-size', 'none',
		'--max-response-size', 'none', 'localhost:25575', 'command',
	])
	main()
	for option in ('max_send_packet_size', 'max_receive_packet_size', 'max_response_size'):
		assert option in create_client.call_args[1]
		assert create_client.call_args[1][option] is None
	client.connect.assert_called_once_with(timeout=None)
	client.authenticate.assert_called_once_with('password', timeout=None)
	client.execute.assert_called_once_with('command', timeout=None)


def test_startup_commands_include_empty_strings_and_do_not_read_input(
	client: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
	commands = ['', 'second command', '--help', '-i']
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', 'localhost:25575'] + commands)
	stdin = Mock(wraps=io.StringIO('unused\n'))
	monkeypatch.setattr(sys, 'stdin', stdin)
	client.execute.side_effect = [RconContent(command.encode('utf8')) for command in commands]
	main()
	assert client.execute.call_args_list == [call(command, timeout=10.0) for command in commands]
	stdin.readline.assert_not_called()
	client.__exit__.assert_called_once_with(None, None, None)
	output = capsys.readouterr()
	assert output.out == '\nsecond command\n--help\n-i\n'
	assert output.err == ''


@pytest.mark.parametrize('startup_commands', [[], ['first', '', 'second']], ids=['no-startup-command', 'startup-commands'])
def test_interaction_follows_startup_commands(client: MagicMock, monkeypatch: pytest.MonkeyPatch, startup_commands: List[str]):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '-i', 'localhost:25575'] + startup_commands)
	stdin = Mock(wraps=io.StringIO('interactive\n'))
	client.attach_mock(stdin.readline, 'read_input')
	monkeypatch.setattr(sys, 'stdin', stdin)
	main()
	assert client.mock_calls == [
		call.__enter__(),
		call.connect(timeout=10.0),
		call.authenticate('password', timeout=10.0),
	] + [call.execute(command, timeout=10.0) for command in startup_commands] + [
		call.read_input(),
		call.execute('interactive', timeout=10.0),
		call.read_input(),
		call.__exit__(None, None, None),
	]


def test_interactive_lines_preserve_content_and_stop_at_eof(
	client: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '-i', 'localhost:25575'])
	monkeypatch.setattr(sys, 'stdin', io.StringIO('\n\r\n  \r\nlast'))
	main()
	assert client.execute.call_args_list == [call('  ', timeout=10.0), call('last', timeout=10.0)]
	client.__exit__.assert_called_once_with(None, None, None)
	output = capsys.readouterr()
	assert output.out == 'response\nresponse\n'
	assert 'rconx> ' in output.err


def test_ctrl_c_while_reading_exits_successfully(client: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '-i', 'localhost:25575'])
	stdin = Mock()
	stdin.readline.side_effect = KeyboardInterrupt()
	monkeypatch.setattr(sys, 'stdin', stdin)
	with pytest.raises(SystemExit) as caught:
		main()
	assert caught.value.code == 0
	client.execute.assert_not_called()
	client.__exit__.assert_called_once()
	assert client.__exit__.call_args[0][1] is caught.value
	output = capsys.readouterr()
	assert output.out == ''
	assert output.err == 'rconx> '


@pytest.mark.parametrize('content, expected', [
	pytest.param(RconContent(b''), '\n', id='empty'),
	pytest.param(RconContent(b'response'), 'response\n', id='text'),
	pytest.param(RconContent(b'with newline\n'), 'with newline\n\n', id='trailing-newline'),
	pytest.param(RconContent('中文'.encode('utf8')), '中文\n', id='unicode'),
	pytest.param(RconContent(b'caf\xe9', default_encoding='latin1'), 'café\n', id='configured-encoding'),
])
def test_text_output_uses_response_encoding_and_appends_newline(
	client: MagicMock, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], content: RconContent, expected: str,
):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', 'localhost:25575', 'command'])
	client.execute.return_value = content
	main()
	output = capsys.readouterr()
	assert output.out == expected
	assert output.err == ''


@pytest.mark.parametrize('content', [b'', b'plain\n', b'\x00\xff\r\n'])
def test_raw_output_preserves_bytes(client: MagicMock, monkeypatch: pytest.MonkeyPatch, capsysbinary: pytest.CaptureFixture[bytes], content: bytes):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '--raw', 'localhost:25575', 'command'])
	client.execute.return_value = RconContent(content)
	main()
	output = capsysbinary.readouterr()
	assert output.out == content
	assert output.err == b''


@pytest.mark.parametrize('operation, error', [
	pytest.param('connect', RconConnectionError('connect failed'), id='connect'),
	pytest.param('authenticate', RconAuthenticationError('authentication failed'), id='authenticate'),
	pytest.param('execute', RconTimeoutError('command failed'), id='execute'),
])
def test_failure_propagates_stops_further_work_and_exits_context(
	client: MagicMock, monkeypatch: pytest.MonkeyPatch, operation: str, error: BaseException,
):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '-i', 'localhost:25575', 'first', 'second'])
	stdin = Mock(wraps=io.StringIO('interactive\n'))
	monkeypatch.setattr(sys, 'stdin', stdin)
	getattr(client, operation).side_effect = error
	with pytest.raises(type(error)) as caught:
		main()
	assert caught.value is error
	stdin.readline.assert_not_called()
	if operation == 'connect':
		client.authenticate.assert_not_called()
		client.execute.assert_not_called()
	elif operation == 'authenticate':
		client.execute.assert_not_called()
	else:
		client.execute.assert_called_once_with('first', timeout=10.0)
	client.__enter__.assert_called_once_with()
	client.__exit__.assert_called_once()
	assert client.__exit__.call_args[0][1] is error


def test_ctrl_c_during_interactive_execution_propagates(client: MagicMock, monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'password', '-i', 'localhost:25575'])
	stdin = Mock(wraps=io.StringIO('first\nsecond\n'))
	monkeypatch.setattr(sys, 'stdin', stdin)
	error = KeyboardInterrupt()
	client.execute.side_effect = error
	with pytest.raises(KeyboardInterrupt) as caught:
		main()
	assert caught.value is error
	stdin.readline.assert_called_once_with()
	client.execute.assert_called_once_with('first', timeout=10.0)
	client.__exit__.assert_called_once()
	assert client.__exit__.call_args[0][1] is error
