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
