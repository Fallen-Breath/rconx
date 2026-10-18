import sys
from typing import Optional

import pytest

from rconx.cli.arguments import SizeLimit, parse_args
from rconx.client.preset import RconPreset


def test_defaults_and_zero_commands(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.delenv('RCONX_PASSWORD', raising=False)
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', 'localhost:25575'])
	options = parse_args()
	assert options.address.host == 'localhost'
	assert options.address.port == 25575
	assert options.password == 'secret'
	assert options.commands == []
	assert options.preset == RconPreset.minecraft
	assert options.timeout == 10.0
	assert not options.interactive
	assert options.idle_window is None
	assert options.encoding is None
	assert not options.raw
	assert options.max_send_packet_size is None
	assert options.max_receive_packet_size is None
	assert options.max_response_size is None


@pytest.mark.parametrize('password_option, preset_option, interactive_option, timeout_option', [
	pytest.param('-p', '-P', '-i', '-t', id='short'),
	pytest.param('--password', '--preset', '--interactive', '--timeout', id='long'),
])
def test_option_aliases(
		monkeypatch: pytest.MonkeyPatch, password_option: str, preset_option: str, interactive_option: str, timeout_option: str,
):
	monkeypatch.setattr(sys, 'argv', [
		'rconx', timeout_option, '0.25', interactive_option, password_option, 'secret', preset_option, 'srcds', 'localhost:25575', 'list',
	])
	options = parse_args()
	assert options.password == 'secret'
	assert options.preset == RconPreset.srcds
	assert options.interactive
	assert options.timeout == 0.25
	assert options.commands == ['list']


def test_address_ends_local_option_parsing_and_preserves_commands(monkeypatch: pytest.MonkeyPatch):
	commands = ['say hello world', '', '  中文  ', '--help', '-i', '--', '--password', 'different', '--timeout=0', '--unknown']
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', 'localhost:25575'] + commands)
	options = parse_args()
	assert options.commands == commands
	assert options.password == 'secret'
	assert not options.interactive
	assert options.timeout == 10.0


def test_password_option_after_address_remains_a_command(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv('RCONX_PASSWORD', 'default secret')
	monkeypatch.setattr(sys, 'argv', ['rconx', 'localhost:25575', '-p', 'secret'])
	options = parse_args()
	assert options.password == 'default secret'
	assert options.commands == ['-p', 'secret']


def test_unknown_local_option_is_rejected(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '--unknown', '-p', 'secret', 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


@pytest.mark.parametrize('password', ['', '  中文 secret  ', '-i'])
def test_password_value_is_preserved(monkeypatch: pytest.MonkeyPatch, password: str):
	monkeypatch.setenv('RCONX_PASSWORD', 'default secret')
	monkeypatch.setattr(sys, 'argv', ['rconx', '--password=' + password, 'localhost:25575'])
	assert parse_args().password == password


@pytest.mark.parametrize('default_password', [None, 'default secret'], ids=['no-default', 'default-set'])
@pytest.mark.parametrize('password', ['environment secret', ''])
def test_password_from_named_environment_variable(monkeypatch: pytest.MonkeyPatch, password: str, default_password: Optional[str]):
	if default_password is None:
		monkeypatch.delenv('RCONX_PASSWORD', raising=False)
	else:
		monkeypatch.setenv('RCONX_PASSWORD', default_password)
	monkeypatch.setenv('RCONX_TEST_PASSWORD', password)
	monkeypatch.setattr(sys, 'argv', ['rconx', '--password-env', 'RCONX_TEST_PASSWORD', 'localhost:25575'])
	assert parse_args().password == password


@pytest.mark.parametrize('password', ['default secret', ''])
def test_password_from_default_environment_variable(monkeypatch: pytest.MonkeyPatch, password: str):
	monkeypatch.setenv('RCONX_PASSWORD', password)
	monkeypatch.setattr(sys, 'argv', ['rconx', 'localhost:25575'])
	assert parse_args().password == password


def test_missing_default_password_environment_variable_fails(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.delenv('RCONX_PASSWORD', raising=False)
	monkeypatch.setattr(sys, 'argv', ['rconx', 'localhost:25575'])
	with pytest.raises(Exception):
		parse_args()


def test_password_sources_are_mutually_exclusive(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv('RCONX_PASSWORD', 'default secret')
	monkeypatch.setenv('RCONX_TEST_PASSWORD', 'environment secret')
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--password-env', 'RCONX_TEST_PASSWORD', 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


def test_undefined_password_environment_variable_fails(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv('RCONX_PASSWORD', 'default secret')
	monkeypatch.delenv('RCONX_TEST_PASSWORD', raising=False)
	monkeypatch.setattr(sys, 'argv', ['rconx', '--password-env', 'RCONX_TEST_PASSWORD', 'localhost:25575'])
	with pytest.raises(Exception):
		parse_args()


def test_password_environment_option_requires_a_name(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setenv('RCONX_PASSWORD', 'default secret')
	monkeypatch.setattr(sys, 'argv', ['rconx', '--password-env'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


def test_address_is_required(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret'])
	with pytest.raises(Exception):
		parse_args()


@pytest.mark.parametrize('address, host, port', [
	pytest.param('example.org:1', 'example.org', 1, id='hostname-minimum-port'),
	pytest.param('127.0.0.1:65535', '127.0.0.1', 65535, id='ipv4-maximum-port'),
	pytest.param('[2001:db8::1]:25575', '2001:db8::1', 25575, id='ipv6'),
])
def test_address_formats(monkeypatch: pytest.MonkeyPatch, address: str, host: str, port: int):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', address])
	options = parse_args()
	assert options.address.host == host
	assert options.address.port == port


@pytest.mark.parametrize('address', [
	pytest.param('localhost', id='missing-port'),
	pytest.param(':25575', id='empty-host'),
	pytest.param('localhost:', id='empty-port'),
	pytest.param('localhost:0', id='port-below-minimum'),
	pytest.param('localhost:65536', id='port-above-maximum'),
	pytest.param('localhost:port', id='noninteger-port'),
	pytest.param('::1:25575', id='unbracketed-ipv6'),
	pytest.param('[::1]25575', id='missing-port-separator'),
	pytest.param('[localhost]:25575', id='bracketed-hostname'),
])
def test_invalid_address_is_rejected(monkeypatch: pytest.MonkeyPatch, address: str):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', address])
	with pytest.raises(Exception):
		parse_args()


@pytest.mark.parametrize('preset', [RconPreset.minecraft, RconPreset.srcds, RconPreset.cuberite, RconPreset.single_packet])
def test_presets_without_idle_window(monkeypatch: pytest.MonkeyPatch, preset: RconPreset):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--preset', preset.name, 'localhost:25575'])
	assert parse_args().preset == preset


def test_unknown_preset_is_rejected(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--preset', 'unknown', 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


@pytest.mark.parametrize('window', [0.0, 0.25])
def test_idle_preset_with_window(monkeypatch: pytest.MonkeyPatch, window: float):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--preset', 'idle_timeout', '--idle-window', str(window), 'localhost:25575'])
	options = parse_args()
	assert options.preset == RconPreset.idle_timeout
	assert options.idle_window == window


def test_idle_preset_requires_window(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--preset', 'idle_timeout', 'localhost:25575'])
	with pytest.raises(Exception):
		parse_args()


def test_non_idle_preset_rejects_window(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--idle-window', '0.25', 'localhost:25575'])
	with pytest.raises(Exception):
		parse_args()


@pytest.mark.parametrize('window', ['-1', 'nan', 'inf', 'none'])
def test_invalid_idle_window_is_rejected(monkeypatch: pytest.MonkeyPatch, window: str):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--preset', 'idle_timeout', '--idle-window=' + window, 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


def test_raw_and_encoding_options(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--raw', '--encoding', 'latin1', 'localhost:25575'])
	options = parse_args()
	assert options.raw
	assert options.encoding == 'latin1'


def test_raw_and_interactive_are_mutually_exclusive(monkeypatch: pytest.MonkeyPatch):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--raw', '--interactive', 'localhost:25575'])
	with pytest.raises(Exception):
		parse_args()


@pytest.mark.parametrize('value, expected', [
	pytest.param('0', 0.0, id='zero'),
	pytest.param('0.25', 0.25, id='positive'),
	pytest.param('none', None, id='unlimited'),
])
def test_timeout_values(monkeypatch: pytest.MonkeyPatch, value: str, expected: Optional[float]):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--timeout', value, 'localhost:25575'])
	assert parse_args().timeout == expected


@pytest.mark.parametrize('value', ['-1', 'nan', 'inf', 'invalid'])
def test_invalid_timeout_is_rejected(monkeypatch: pytest.MonkeyPatch, value: str):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', '--timeout=' + value, 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0


@pytest.mark.parametrize('option, attribute', [
	pytest.param('--max-send-packet-size', 'max_send_packet_size', id='send'),
	pytest.param('--max-receive-packet-size', 'max_receive_packet_size', id='receive'),
	pytest.param('--max-response-size', 'max_response_size', id='response'),
])
@pytest.mark.parametrize('value, expected', [
	pytest.param('0', 0, id='zero'),
	pytest.param('64', 64, id='positive'),
	pytest.param('none', None, id='unlimited'),
])
def test_size_limit_values(monkeypatch: pytest.MonkeyPatch, option: str, attribute: str, value: str, expected: Optional[int]):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', option, value, 'localhost:25575'])
	limit: Optional[SizeLimit] = getattr(parse_args(), attribute)
	assert limit is not None
	assert limit.value == expected


@pytest.mark.parametrize('option', ['--max-send-packet-size', '--max-receive-packet-size', '--max-response-size'])
@pytest.mark.parametrize('value', ['-1', '1.5', 'invalid'])
def test_invalid_size_limit_is_rejected(monkeypatch: pytest.MonkeyPatch, option: str, value: str):
	monkeypatch.setattr(sys, 'argv', ['rconx', '-p', 'secret', option + '=' + value, 'localhost:25575'])
	with pytest.raises(SystemExit) as caught:
		parse_args()
	assert caught.value.code != 0
