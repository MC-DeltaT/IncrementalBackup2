import re
from typing import Iterable


__all__ = [
    'ExcludePattern',
    'is_path_excluded'
]


class ExcludePattern:
    def __init__(self, pattern: str, /) -> None:
        self.pattern = self._compile_pattern(pattern)

    def matches(self, path: str, /) -> bool:
        return self.pattern.fullmatch(path) is not None

    @staticmethod
    def _compile_pattern(pattern: str, /) -> re.Pattern:
        return re.compile(pattern, re.DOTALL)


def is_path_excluded(path: str, exclude_patterns: Iterable[ExcludePattern], /) -> bool:
    """Checks if a path is matched by any path exclude pattern.

        :param path: The path in question. Should be an absolute POSIX-style path, where the root is the backup source
            directory. Paths that are directories should end in a forward slash ('/'). Paths that are files should not
            end in a forward slash. Path components should be normalised with `os.path.normcase()`.
        :param exclude_patterns: Compiled path exclude patterns, from `compile_exclude_pattern()`.
    """

    return any(pattern.matches(path) for pattern in exclude_patterns)