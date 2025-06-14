import fnmatch


def match_path(match_parts: list[str], path_parts: list[str]) -> bool:
    if not match_parts or not path_parts:
        return (not path_parts) == (not match_parts)
    match_head, *match_tail = match_parts
    path_head, *path_tail = path_parts
    if match_head == "**":
        if match_path(match_tail, path_parts):
            return True
        if path_parts and match_path(match_parts, path_tail):
            return True
        return False
    if not path_parts:
        return False
    if fnmatch.fnmatchcase(path_head, match_head):
        return match_path(match_tail, path_tail)
    return False
