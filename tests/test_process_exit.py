"""Process-finalization regressions using a clean interpreter subprocess."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))

def _run(code):
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(ROOT, "python") + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                          text=True, timeout=10, check=False)

def test_explicit_close_with_native_pipeline_exits_cleanly():
    result = _run("""
from actorplane import World, Actor, Pulse, CountSnapshot, handles
class A(Actor):
    def on_start(self, ctx): self.counter = ctx.native_counter(.002, .01, target=ctx.actor)
    @handles(CountSnapshot)
    def snapshot(self, event, ctx): pass
w = World(); w.spawn(A); w.run_for(.03); report = w.close()
assert report['native_done'] is True and report['python_done'] is True
state = w.inspect(); assert state['active_tasks'] == 0 and state['retained_payload_bytes'] == 0
print('closed')
""")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("closed")

def test_context_cleanup_preserves_original_exception():
    result = _run("""
from actorplane import World, Actor, Pulse
holder = {}
stops = []
class A(Actor):
    def on_start(self, ctx): self.timer = ctx.after(.01, Pulse(1), target=ctx.actor)
    def on_stop(self, ctx):
        stops.append(True)
        raise RuntimeError('cleanup marker')
holder['world'] = World()
holder['world'].spawn(A)
try:
    with holder['world']:
        holder['world'].step()
        raise ValueError('original marker')
except ValueError as exc:
    print(type(exc).__name__, str(exc))
assert stops == [True]
state = holder['world'].inspect()
assert state['python_registrations'] == 0 and state['active_tasks'] == 0 and state['retained_payload_bytes'] == 0
""")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ValueError original marker"

def test_repeated_close_is_idempotent_and_reports_consistently():
    result = _run("""
from actorplane import World, Actor, Pulse, handles
class A(Actor):
    @handles(Pulse)
    def pulse(self, event, ctx): pass
w = World(mailbox_capacity=8); ref = w.spawn(A); w.step()
for value in (1, 2, 3): ref.send(Pulse(value))
first = w.close(); second = w.close()
assert first == second
assert first['discarded'] == 3
print('idempotent')
""")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("idempotent")
