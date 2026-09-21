import pytest
from actorplane import Actor, event, Event
from actorplane.components import Component, Interface, Output, component, declarations, BoundComponent
from actorplane.native import PulseSource, WindowCounter, SnapshotSink

@event("components.value", 1)
class Value(Event): number: int

def test_component_factories_are_immutable_and_topologically_ordered():
    source = component(PulseSource, interval=.1)
    sink = component(SnapshotSink, depends_on=("first",), mutable=(1,2))
    class A(Actor):
        first = source; second = sink
    assert [n for n,_ in declarations(A)] == ["first", "second"]
    assert source.config == (("interval", .1),)

def test_dependency_and_config_validation():
    missing = component(WindowCounter, depends_on=("missing",), bad=(1,))
    class A(Actor): value = missing
    with pytest.raises(ValueError): declarations(A)
    with pytest.raises(TypeError): component(PulseSource, values=[1])

def test_ports_are_frozen_and_bound_mapping_is_immutable():
    class C(Component): output = Output(Value)
    proxy = BoundComponent(None, (("output", object()),))
    assert proxy.output is not None
    with pytest.raises(TypeError): proxy.ports["x"] = object()
    with pytest.raises(AttributeError): C().output = object()
