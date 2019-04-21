#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `leiserbik` package."""

import pytest

from click.testing import CliRunner

from leiserbik import watcher
from leiserbik import cli


#@pytest.fixture
def test_user_by_id():
    """Test of user status by id
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')

    statuses = watcher.user_by_id('jday11on',14992604202)
    assert len(statuses) == 1

def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)

    help_result = runner.invoke(cli.main, ['--help'])

    assert '--help' in help_result.output
    assert 'Show this message and exit.' in help_result.output
