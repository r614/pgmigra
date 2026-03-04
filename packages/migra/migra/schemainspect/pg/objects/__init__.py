from .collation import InspectedCollation
from .comment import InspectedComment
from .constraint import InspectedConstraint
from .domain import InspectedDomain
from .enum import InspectedEnum
from .extension import InspectedExtension
from .fdw import InspectedFDW
from .foreign_server import InspectedForeignServer
from .index import InspectedIndex
from .privilege import InspectedPrivilege
from .publication import InspectedPublication
from .range_type import InspectedRangeType
from .role import InspectedRole
from .row_policy import InspectedRowPolicy
from .rule import InspectedRule
from .schema import InspectedSchema
from .selectable import InspectedFunction, InspectedSelectable
from .sequence import InspectedSequence
from .statistics import InspectedStatistics
from .trigger import InspectedTrigger
from .type import InspectedType
from .user_mapping import InspectedUserMapping

__all__ = [
    "InspectedCollation",
    "InspectedComment",
    "InspectedConstraint",
    "InspectedDomain",
    "InspectedEnum",
    "InspectedExtension",
    "InspectedFDW",
    "InspectedForeignServer",
    "InspectedFunction",
    "InspectedIndex",
    "InspectedPrivilege",
    "InspectedPublication",
    "InspectedRangeType",
    "InspectedRole",
    "InspectedRowPolicy",
    "InspectedRule",
    "InspectedSchema",
    "InspectedSelectable",
    "InspectedSequence",
    "InspectedStatistics",
    "InspectedTrigger",
    "InspectedType",
    "InspectedUserMapping",
]
