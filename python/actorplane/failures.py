"""Bounded, interpreter-independent failure records.

Capture deliberately copies traceback metadata only. Exception text, arguments,
locals, source lines, and traceback/frame objects never enter a Failure record.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Any

def _bounded(value: str, limit: int) -> str:
    return _bounded_pair(value, limit)[0]

def _bounded_pair(value: str, limit: int) -> tuple[str, bool]:
    original = value[:limit].encode("utf-8", "replace")
    raw = original[:limit]
    return raw.decode("utf-8", "ignore"), len(value) > limit or len(original) > limit

@dataclass(frozen=True)
class TraceFrame:
    file: str
    line: int
    function: str

@dataclass(frozen=True)
class Failure:
    sequence: int
    elapsed_ns: int
    actor: Any
    event_id: int | None
    schema: int | None
    operation: tuple[int, int, int] | None
    phase: str
    handler: str
    exception_type: str
    frames: tuple[TraceFrame, ...]
    truncated: bool
    action: str
    coalesced: int = 0

def capture_failure(exc: BaseException, phase: str, handler: str):
    """Return bridge-ready bounded failure metadata without inspecting exception text."""
    frames = deque(maxlen=8)
    tb = BaseException.__traceback__.__get__(exc, type(exc))
    total = 0
    truncated = False
    while tb is not None and total < 256:
        total += 1
        frame = tb.tb_frame
        filename, file_truncated = _bounded_pair(frame.f_code.co_filename, 256)
        function, function_truncated = _bounded_pair(frame.f_code.co_name, 128)
        truncated |= file_truncated or function_truncated
        frames.append((
            filename,
            int(tb.tb_lineno),
            function,
        ))
        tb = tb.tb_next
    truncated |= total > 8 or tb is not None
    typ = type(exc)
    module = getattr(typ, "__module__", "builtins") or "builtins"
    qualname = getattr(typ, "__qualname__", typ.__name__)
    if not isinstance(module, str): module = "unknown"
    if not isinstance(qualname, str): qualname = "Exception"
    module, module_truncated = _bounded_pair(module, 128)
    qualname, name_truncated = _bounded_pair(qualname, 128)
    exception_type, type_truncated = _bounded_pair(f"{module}.{qualname}", 128)
    phase, phase_truncated = _bounded_pair(phase, 128)
    handler, handler_truncated = _bounded_pair(handler, 128)
    truncated |= phase_truncated or handler_truncated or module_truncated or name_truncated or type_truncated
    return phase, handler, exception_type, list(frames), truncated
