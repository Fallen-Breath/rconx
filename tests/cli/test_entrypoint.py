import os
import subprocess
import sys
from pathlib import Path
from typing import List

import pytest

if sys.version_info >= (3, 8):
	from importlib.metadata import version
else:
	from importlib_metadata import version


def run_python(arguments: List[str], directory: Path) -> 'subprocess.CompletedProcess[str]':
	environment = os.environ.copy()
	environment.pop('RCONX_PASSWORD', None)
	return subprocess.run(
		[sys.executable] + arguments,
		cwd=str(directory),
		env=environment,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		universal_newlines=True,
		timeout=10,
	)


def test_module_without_arguments_shows_help(tmp_path: Path):
	result = run_python(['-m', 'rconx'], tmp_path)
	output = result.stdout + result.stderr
	assert 'HOST:PORT' in output
	assert '--password' in output
	assert '--interactive' in output
	assert 'Traceback (most recent call last)' not in output


@pytest.mark.parametrize('arguments', [
	pytest.param(['-p', 'secret'], id='missing-address'),
	pytest.param(['-p', 'secret', 'localhost:0'], id='invalid-address'),
	pytest.param(['--timeout=nan', '-p', 'secret', 'localhost:25575'], id='invalid-timeout'),
	pytest.param(['--max-response-size=-1', '-p', 'secret', 'localhost:25575'], id='invalid-size-limit'),
	pytest.param(['--encoding=unknown-encoding', '-p', 'secret', 'localhost:25575'], id='unknown-encoding'),
	pytest.param(['--encoding=', '-p', 'secret', 'localhost:25575'], id='empty-encoding'),
	pytest.param(['--encoding=base64_codec', '-p', 'secret', 'localhost:25575'], id='non-text-encoding'),
	pytest.param(['localhost:25575'], id='missing-default-password'),
	pytest.param(['--password-env', 'RCONX_CLI_TEST_MISSING_PASSWORD', 'localhost:25575'], id='missing-named-password'),
	pytest.param(['--password-file', 'missing-password.bin', 'localhost:25575'], id='missing-password-file'),
])
def test_module_reports_input_errors_without_traceback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arguments: List[str]):
	monkeypatch.delenv('RCONX_CLI_TEST_MISSING_PASSWORD', raising=False)
	result = run_python(['-m', 'rconx'] + arguments, tmp_path)
	assert result.returncode != 0
	assert result.stdout == ''
	assert result.stderr.strip()
	assert 'Traceback (most recent call last)' not in result.stderr


@pytest.mark.parametrize('option', ['-h', '--help'])
def test_module_help_needs_no_connection_arguments(tmp_path: Path, option: str):
	result = run_python(['-m', 'rconx', option], tmp_path)
	assert result.returncode == 0
	assert 'HOST:PORT' in result.stdout
	assert '--password' in result.stdout
	assert '--interactive' in result.stdout
	assert result.stderr == ''


def test_module_version_needs_no_connection_arguments(tmp_path: Path):
	result = run_python(['-m', 'rconx', '--version'], tmp_path)
	assert result.returncode == 0
	assert result.stdout.strip() == 'rconx {}'.format(version('rconx'))
	assert result.stderr == ''


def test_importing_main_module_does_not_start_cli(tmp_path: Path):
	result = run_python(['-c', 'import rconx.__main__'], tmp_path)
	assert result.returncode == 0
	assert result.stdout == ''
	assert result.stderr == ''
