import pytest
import gc, weakref
from actorplane.failures import TraceFrame, Failure, capture_failure

def test_capture_copies_bounded_metadata_without_exception_text():
    class Hostile(Exception):
        def __str__(self): raise AssertionError("must not call str")
    try:
        raise Hostile("secret locals")
    except Hostile as exc:
        phase, handler, typ, frames, truncated = capture_failure(exc, "dispatch", "worker")
    assert phase == "dispatch" and handler == "worker"
    assert typ.endswith("Hostile")
    assert frames and all(len(item) == 3 for item in frames)
    assert all("secret" not in str(item) for item in frames)

def test_capture_trims_to_last_eight_frames():
    def recurse(n):
        if n == 0: raise ValueError()
        return recurse(n - 1)
    try: recurse(12)
    except ValueError as exc: result = capture_failure(exc, "handler", "deep")
    assert len(result[3]) == 8 and result[4] is True
    assert all(frame[2] == "recurse" for frame in result[3])

def test_records_are_frozen_and_do_not_retain_traceback_objects():
    frame = TraceFrame("x.py", 4, "f")
    failure = Failure(1, 2, None, None, None, None, "p", "h", "ValueError", (frame,), False, "stop")
    with pytest.raises(Exception): failure.phase = "changed"
    assert failure.frames == (frame,)

def test_unicode_bounds_are_bytes_and_safe():
    phase, handler, typ, frames, truncated = capture_failure(ValueError(), "é" * 200, "函数" * 200)
    assert len(phase.encode()) <= 128
    assert len(handler.encode()) <= 128
    assert len(typ.encode()) <= 128

def test_capture_output_does_not_keep_traceback_locals_alive():
    class Local: pass
    refs, retained_reports = [], []
    def produce():
        local = Local(); refs.append(weakref.ref(local))
        try: raise ValueError()
        except ValueError as exc: retained_reports.append(capture_failure(exc, "p", "h"))
    produce(); gc.collect()
    assert refs[0]() is None
    assert retained_reports[0][3]
