from .base import PartitionStrategy

class HashPartitionStrategy(PartitionStrategy):
    def parent_clause(self, column: str) -> str:
        return f"PARTITION BY HASH ({column})"

    def child_clause(self, row: dict, schema: str, table: str):
        modulus, remainder = row.get("partitionhashmodulus"), row.get("partitionhashremainder")
        if modulus and remainder is not None:
            return (
                f"CREATE TABLE {schema}.{row['partitiontablename']} "
                f"PARTITION OF {schema}.{table} "
                f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder});"
            )
