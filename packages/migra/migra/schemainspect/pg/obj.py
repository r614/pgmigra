"""Re-export shim for backward compatibility.

All classes have been moved to the objects/ package and the PostgreSQL
inspector class has been moved to inspector.py.
"""

from .inspector import PostgreSQL  # noqa: F401
from .objects import (  # noqa: F401
    InspectedCollation,
    InspectedComment,
    InspectedConstraint,
    InspectedDomain,
    InspectedEnum,
    InspectedExtension,
    InspectedFunction,
    InspectedIndex,
    InspectedPrivilege,
    InspectedRangeType,
    InspectedRole,
    InspectedRowPolicy,
    InspectedSchema,
    InspectedSelectable,
    InspectedSequence,
    InspectedTrigger,
    InspectedType,
)
