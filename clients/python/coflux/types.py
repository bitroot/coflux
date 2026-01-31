import typing as t

if t.TYPE_CHECKING:
    from . import models

TargetType = t.Literal["workflow", "task"]

Requires = dict[str, list[str]]

Metadata = dict[str, t.Any]

Reference = (
    tuple[t.Literal["execution"], str, "models.ExecutionMetadata"]
    | tuple[t.Literal["asset"], str, "models.AssetMetadata"]
    | tuple[t.Literal["fragment"], str, str, int, dict[str, t.Any]]
)

Value = t.Union[
    tuple[t.Literal["raw"], t.Any, list[Reference]],
    tuple[t.Literal["blob"], str, int, list[Reference]],
]

Result = t.Union[
    tuple[t.Literal["value"], Value],
    tuple[t.Literal["error"], str, str],
    tuple[t.Literal["abandoned"]],
    tuple[t.Literal["cancelled"]],
]
