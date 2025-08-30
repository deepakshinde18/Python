from dataclasses import dataclass
from typing import Optional, List

@dataclass
class ColumnDef:
    name: str
    data_type: str
    is_nullable: bool
    default: Optional[str]
    char_max: Optional[int]
    num_precision: Optional[int]
    num_scale: Optional[int]

@dataclass
class TableDef:
    schema: str
    name: str
    columns: List[ColumnDef]
    pk_cols: List[str]
