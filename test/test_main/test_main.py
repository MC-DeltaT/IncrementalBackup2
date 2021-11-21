from helpers import run_application


# TODO? check program console output


def test_no_args() -> None:
    process = run_application()
    assert process.returncode == 1


def test_invalid_command() -> None:
    process = run_application('magic_command', 'foo', 'bar')
    assert process.returncode == 1
