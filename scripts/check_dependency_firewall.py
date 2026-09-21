"""Fail if a core, native, or test-harness crate gains a Python bridge dependency."""
import json
import subprocess


def main() -> None:
    graph = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--locked"], text=True
    ))
    packages = {p["id"]: p["name"] for p in graph["packages"]}
    nodes = {n["id"]: n["dependencies"] for n in graph["resolve"]["nodes"]}
    for root_name in ("actorplane-core", "actorplane-native", "actorplane-test"):
        root = next(key for key, name in packages.items() if name == root_name)
        seen, pending = set(), [root]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            name = packages[current]
            if name.startswith("pyo3") or name == "actorplane-python":
                raise SystemExit(f"Python dependency leaked into {root_name}: {name}")
            pending.extend(nodes[current])
        print(f"{root_name}: {len(seen)} packages, no Python dependency")


if __name__ == "__main__":
    main()
