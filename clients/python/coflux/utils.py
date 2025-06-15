import fnmatch
import os


def _expand_braces(pattern: str) -> list[str]:
    parts = []
    i = pattern.find("{")
    if i < 0:
        return [pattern]
    depth = 0
    for j, ch in enumerate(pattern[i:], start=i):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                break
    else:
        return [pattern]
    pre = pattern[:i]
    post = pattern[j + 1 :]
    opts = []
    buf = ""
    d = 0
    for ch in pattern[i + 1 : j]:
        if ch == "{":
            d += 1
            buf += ch
        elif ch == "}":
            d -= 1
            buf += ch
        elif ch == "," and d == 0:
            opts.append(buf)
            buf = ""
        else:
            buf += ch
    opts.append(buf)
    for opt in opts:
        for rest in _expand_braces(pre + opt + post):
            parts.append(rest)
    return parts


def _match_parts(match_parts: list[str], path_parts: list[str]) -> bool:
    if not match_parts:
        return not path_parts
    head, *tail = match_parts
    if head == "**" and not tail:
        return True
    if head == "**":
        if _match_parts(tail, path_parts):
            return True
        return bool(path_parts and _match_parts(match_parts, path_parts[1:]))
    if not path_parts:
        return False
    if fnmatch.fnmatchcase(path_parts[0], head):
        return _match_parts(tail, path_parts[1:])
    return False


class GlobMatcher:
    def __init__(self, pattern: str):
        self._patterns: list[list[str]] = []
        for pat in _expand_braces(pattern):
            norm = os.path.normpath(pat).strip(os.sep)
            parts = norm.split(os.sep) if norm else []
            self._patterns.append(parts)

    def match(self, path: str) -> bool:
        norm_path = os.path.normpath(path).strip(os.sep)
        parts = norm_path.split(os.sep) if norm_path else []
        for pat_parts in self._patterns:
            if _match_parts(pat_parts, parts):
                return True
        return False
