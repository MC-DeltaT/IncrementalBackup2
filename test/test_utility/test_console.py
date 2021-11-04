from incremental_backup.utility.console import print_error, print_warning


def test_print_warning(capfd) -> None:
    print_warning('something\xC0to\x12dr4w-attention  to')
    out, err = capfd.readouterr()
    assert out == 'WARNING: something\xC0to\x12dr4w-attention  to\n'
    assert err == ''


def test_print_error(capfd) -> None:
    print_error('Uh oh something has gone w\u3475r\u58E5o\uFD87n\uAFDBg!')
    out, err = capfd.readouterr()
    assert out == ''
    assert err == 'ERROR: Uh oh something has gone w\u3475r\u58E5o\uFD87n\uAFDBg!\n'


