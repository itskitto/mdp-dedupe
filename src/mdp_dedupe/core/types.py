"""Type definitions for the deduplication module."""
from typing import Dict, List, Tuple, TypeVar, Union
from typing_extensions import TypedDict

# Type variable for generic record types
T = TypeVar("T")

class DedupeField(TypedDict):
    """Type definition for a dedupe field configuration."""
    name: str
    type: str
    has_missing: bool

class DedupeConfig(TypedDict):
    """Type definition for dedupe configuration section."""
    threshold: float
    fields: List[DedupeField]

# Type aliases for clarity
RecordDict = Dict[str, Union[str, int, float, None]]
RecordID = str
ClusterID = int
Score = float
ClusterResult = Tuple[List[RecordID], Score]
DedupeResults = List[Tuple[ClusterID, ClusterResult]]
