from incremental_backup.utility.filesystem import Directory


def test_directory_init() -> None:
    directory = Directory('foo\u6856_BAR~!@#$%^&')
    assert directory.name == 'foo\u6856_BAR~!@#$%^&'
    assert directory.files == []
    assert directory.subdirectories == []
