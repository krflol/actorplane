"""Python authoring API for actorplane.

The module is intentionally small: Python declares schemas and behaviour while
the native world owns identity, queues, and lifecycle.
"""
from .authoring import (Actor, ActorRef, World, Context, Event, event, handles,
                        Pulse, CountSnapshot, AdmissionError, HandlerFailed,
                        QueueFull, CodecError, AffinityError, LifecycleError,
                        OperationHandle, OperationOutcome, OperationError,
                        FailurePolicy, PublicationTicket, PublicationOutcome,
                        PublicationStatus, DeliveryReport)
from .failures import Failure, TraceFrame
from .envelopes import Envelope, MessageOptions, TraceContext
from .schemas import IntRange, FloatPolicy, Length
from .components import Component, Interface, Output, Port, BoundComponent, LinkToken, component
from .services import ServiceRef, ServiceLease
from . import native

__all__ = ["Actor", "ActorRef", "World", "Context", "Event", "event",
           "handles", "Pulse", "CountSnapshot", "AdmissionError",
           "HandlerFailed", "QueueFull", "CodecError", "AffinityError",
           "LifecycleError", "OperationHandle", "OperationOutcome", "OperationError",
           "FailurePolicy", "Failure", "TraceFrame", "Envelope", "MessageOptions", "TraceContext", "IntRange", "FloatPolicy", "Length",
           "PublicationTicket", "PublicationOutcome", "PublicationStatus", "DeliveryReport",
           "Component", "Interface", "Output", "Port", "BoundComponent", "LinkToken", "component", "native",
           "ServiceRef", "ServiceLease"]

try:  # The package remains importable for documentation and type checking.
    from . import _native  # type: ignore
except ImportError:  # pragma: no cover - exercised when building sdist
    _native = None
