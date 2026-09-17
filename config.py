"""
config.py
=========
The one place the Python entry points read their configuration from.

Values come from the process environment: docker-compose.yml injects them into
every container from the repo's .env (`env_file:`), run.sh sources the same file
for host-side runs, and .env.example documents every variable.

There are deliberately no fallbacks here.  A missing or malformed variable is a
configuration error and is reported as one, naming the variable, instead of the
process silently running against a made-up port, host or topic.  A CLI flag
always wins over the environment — `resolve` is how a script expresses that.
"""

import os
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

_HINT = "set it in .env (copy .env.example) or export it"


class ConfigError(SystemExit):
    def __init__(self, message: str) -> None:
        super().__init__(f"config: {message}")


def _raw(name: str) -> str:
    return os.environ.get(name, "").strip()


def _cast(name: str, raw: str, cast: Callable[[str], T]) -> T:
    try:
        return cast(raw)
    except (TypeError, ValueError):
        raise ConfigError(f"{name}={raw!r} is not a valid {getattr(cast, '__name__', 'value')}")


def require(name: str, cast: Callable[[str], T] = str) -> T:
    raw = _raw(name)
    if raw == "":
        raise ConfigError(f"{name} is not set — {_HINT}")
    return _cast(name, raw, cast)


def optional(name: str, cast: Callable[[str], T] = str) -> Optional[T]:
    """None when unset or empty: the variable's own documented default applies."""
    raw = _raw(name)
    return None if raw == "" else _cast(name, raw, cast)


def flag(raw: str) -> bool:
    if raw.lower() in ("1", "true", "yes", "on"):
        return True
    if raw.lower() in ("0", "false", "no", "off"):
        return False
    raise ValueError(raw)


flag.__name__ = "0/1 flag"


def resolve(value: Optional[T], name: str, cast: Callable[[str], T] = str) -> T:
    """The CLI value if one was given, else the required variable."""
    return value if value is not None else require(name, cast)


def resolve_optional(value: Optional[T], name: str, cast: Callable[[str], T] = str) -> Optional[T]:
    return value if value is not None else optional(name, cast)


def helics_broker_address() -> str:
    return f"tcp://{require('HELICS_BROKER_HOST')}:{require('HELICS_BROKER_PORT', int)}"
