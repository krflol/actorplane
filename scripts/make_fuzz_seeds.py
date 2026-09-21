"""Recreate the small, reviewable seed corpus; generated fuzz inputs live in target/."""
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[1] / "fuzz" / "corpus"


def write(target, name, data):
    directory = ROOT / target
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"seed-{name}.bin").write_bytes(data)


def main():
    wire = {
        "signed-zero": bytes([0]) + struct.pack("<q", 0),
        "unsigned-max": bytes([1]) + struct.pack("<Q", 2**64 - 1),
        "finite-float": bytes([2]) + struct.pack("<d", -0.0),
        "nan": bytes([3]) + struct.pack("<Q", 0x7FF8000000000000),
        "boolean": bytes([4, 1]),
        "utf8": bytes([5]) + struct.pack("<I", 2) + "é".encode(),
        "bad-utf8": bytes([5]) + struct.pack("<I", 1) + b"\xff",
        "bytes": bytes([6]) + struct.pack("<I", 3) + b"abc",
        "huge-length": bytes([6]) + struct.pack("<I", 2**32 - 1),
        "enum": bytes([7]) + struct.pack("<I", 2),
        "optional-some": bytes([8, 1, 1]),
        "optional-none": bytes([8, 0]),
        "sequence": bytes([9]) + struct.pack("<Iqq", 2, -8, 8),
        "nested": bytes([10, 1]) + struct.pack("<II", 1, 2) + b"xy",
        "descriptor-valid": bytes([11, 0, 1, 1]) + struct.pack("<q", 0),
        "descriptor-invalid": bytes([11, 15, 19]),
    }
    for name, data in wire.items():
        write("schema_wire", name, data)
    traces = {
        "delivery": [(0, 0, 0, 0), (2, 0, 0, 0), (2, 0, 0, 0), (3, 1, 0, 0), (4, 0, 0, 0), (3, 2, 1, 0), (4, 0, 1, 0)],
        "expiry": [(1, 0, 0, 5), (2, 0, 0, 0), (9, 0, 0, 5), (3, 1, 0, 0)],
        "reuse": [(0, 0, 0, 0), (2, 0, 0, 0), (8, 0, 0, 0), (5, 0, 1, 0), (0, 0, 0, 1), (2, 0, 0, 0), (3, 1, 0, 0)],
        "stop-with-task": [(15, 0, 0, 0), (8, 0, 0, 0), (17, 0, 0, 0)],
        "retention": [(0, 0, 0, 0), (2, 0, 0, 0), (2, 0, 0, 0), (10, 0, 0, 1), (11, 0, 0, 0), (0, 0, 0, 2), (11, 0, 0, 1), (0, 0, 0, 3)],
        "drain": [(15, 0, 0, 0), (15, 1, 0, 0), (0, 0, 0, 0), (13, 0, 0, 8), (13, 1, 0, 8), (16, 0, 0, 1)] + [(2, 0, 0, 0)] * 4 + [(3, 1, 0, 0)],
        "handles": [(12, 0, 0, i) for i in range(3)] + [(14, 0, 0, 0), (19, 0, 0, 0), (18, 0, 0, 0)],
        "all-actions": [(op, op % 4, (op + 1) % 4, op % 16) for op in range(21)],
    }
    for name, commands in traces.items():
        header = bytes([0, 0, 0 if name == "retention" else 3, 3])
        write("routing_lifecycle", name, header + b"".join(bytes(c) for c in commands))
    print(f"Wrote {len(wire)} schema and {len(traces)} lifecycle seeds")


if __name__ == "__main__":
    main()
