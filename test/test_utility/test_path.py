import os

from incremental_backup.utility.path import path_name_equal


if os.name == 'nt':
    def test_path_name_equal_windows() -> None:
        assert path_name_equal('foo', 'foo')
        assert path_name_equal('foo', 'Foo')
        assert path_name_equal('a file.jpg', 'a file.jpg')
        assert path_name_equal('abcxyz ~!@#$_-', 'ABCXYZ ~!@#$_-')
        assert path_name_equal('354210', '354210')
        assert path_name_equal('\n\x12\r\xFF\uAB01', '\n\x12\r\xFF\uAB01')
        assert not path_name_equal('foo', 'fo0')
        assert not path_name_equal('file1.png', 'file1.bmp')
        assert not path_name_equal(' \n', '\n ')
        assert not path_name_equal('qwerty', 'qwertyasdf')

else:
    def test_path_name_equal_linux() -> None:
        assert path_name_equal('barqux', 'barqux')
        assert path_name_equal('something_here.txt', 'something_here.txt')
        assert path_name_equal('poertiyuj6720398!~@$%&*^(', 'poertiyuj6720398!~@$%&*^(')
        assert path_name_equal('\uDCBA\uFFA0x\xEE', '\uDCBA\uFFA0x\xEE')
        assert not path_name_equal('barqux', 'BarQux')
        assert not path_name_equal('something_also_here.bin', 'something_also_here.ibm')
        assert not path_name_equal(' \t ', ' \n ')
