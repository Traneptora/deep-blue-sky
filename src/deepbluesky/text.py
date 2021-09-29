# text.py
# varous text processing stuff here
from __future__ import annotations

import re

from typing import Any

def identity(arg: Any) -> Any:
    return arg

def owoify(text: str) -> str:
    text = re.sub(r'r{1,2}|l{1,2}', 'w', text)
    text = re.sub(r'R{1,2}|L{1,2}', 'W', text)
    text = re.sub(r'([Nn])(?=[AEIOUYaeiouy])', r'\1y', text)
    return text

def spongebob(text: str) -> str:
    total = ''
    upper = False
    for char in text.lower():
        # space characters and the like are not
        # lowercase even if the string is lowercase
        if char.islower():
            if upper:
                total += char.upper()
            else:
                total += char
            upper = not upper
        else:
            total += char
    return total

def removeprefix(base: str, prefix: str) -> str:
    try:
        return base.removeprefix(prefix)
    except AttributeError:
        pass
    if base.startswith(prefix):
        return base[len(prefix):]
    return base

def removesuffix(base: str, suffix: str) -> str:
    try:
        return base.removesuffix(suffix)
    except AttributeError:
        pass
    if base.endswith(suffix):
        return base[:len(suffix)]
    return base

def pluralize(count: int, singular: str, plural: Optional[str] = None) -> str:
    return singular if count == 1 else plural if plural is not None else singular + 's'
