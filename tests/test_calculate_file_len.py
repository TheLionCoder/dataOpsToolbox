# *-* encoding: utf-8 *-*
import sys

sys.path.extend([".", ".."])

from click.testing import CliRunner
from dataopstoolkit.python.calculate_file_len import calculate_file_len, main


def test_calculate_file_len(tmp_path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("line1\nline2\nline3\n")

    assert calculate_file_len(test_file) == 2
    assert calculate_file_len(test_file, has_header=False) == 3


def test_main(tmp_path, caplog, monkeypatch):
    # Create a test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("line1\nline2\nline3\n")

    runner = CliRunner()
    result = runner.invoke(main, ["-p", str(tmp_path), "-e", "txt", "--has-header"])

    assert result.exit_code == 0
    assert f"listing files in {tmp_path} with extension txt." in caplog.text
    assert "3" in caplog.text
