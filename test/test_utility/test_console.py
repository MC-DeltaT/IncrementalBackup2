from pytest import CaptureFixture

from incremental_backup._utility.console import print_error, print_warning


def test_print_warning(capfd: CaptureFixture[str]) -> None:
    print_warning("something\xc0to\x12dr4w-attention  to")
    out, err = capfd.readouterr()
    assert out == "WARNING: something\xc0to\x12dr4w-attention  to\n"
    assert err == ""


def test_print_error(capfd: CaptureFixture[str]) -> None:
    print_error("Uh oh something has gone w\u3475r\u58e5o\ufd87n\uafdbg!")
    out, err = capfd.readouterr()
    assert out == ""
    assert err == "ERROR: Uh oh something has gone w\u3475r\u58e5o\ufd87n\uafdbg!\n"
