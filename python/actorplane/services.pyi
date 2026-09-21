from typing import Any
from .authoring import ActorRef, OperationHandle, World
from .components import Interface, LinkToken, Port

class ServiceRef:
    name: str
    target: ActorRef
    contract: Interface
    def acquire(self, owner: ActorRef) -> ServiceLease: ...

class ServiceLease:
    scope: ActorRef
    owner: ActorRef
    service: ActorRef
    contract: Interface
    def request(self, port: str, event: Any, *, timeout: float = ..., options: Any = ...) -> OperationHandle: ...
    def link(self, output: str, target: Port) -> LinkToken: ...
    def release(self) -> bool: ...

def register(world: World, name: str, target: ActorRef, contract: Interface) -> ServiceRef: ...
def acquire(world: World, owner: ActorRef, name: str, contract: Interface, *, expected_target: ActorRef | None = ...) -> ServiceLease: ...
