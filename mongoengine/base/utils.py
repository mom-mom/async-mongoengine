import re
from typing import Any, NoReturn


class LazyRegexCompiler:
    """Descriptor to allow lazy compilation of regex"""

    def __init__(self, pattern: str, flags: int = 0) -> None:
        self._pattern = pattern
        self._flags = flags
        self._compiled_regex: re.Pattern[str] | None = None

    @property
    def compiled_regex(self) -> re.Pattern[str]:
        if self._compiled_regex is None:
            self._compiled_regex = re.compile(self._pattern, self._flags)
        return self._compiled_regex

    def __get__(self, instance: Any, owner: type[Any]) -> re.Pattern[str]:
        return self.compiled_regex

    def __set__(self, instance: Any, value: Any) -> NoReturn:
        raise AttributeError("Can not set attribute LazyRegexCompiler")


class NonOrderedList(list[Any]):
    """Simple utility class to compare lists without considering order (useful in context of indexes)"""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, list):
            # Compare sorted versions of the lists
            return sorted(self) == sorted(other)
        return False
