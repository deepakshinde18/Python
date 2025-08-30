from .base import PartitionStrategy

class DefaultPartitionStrategy(PartitionStrategy):
    def parent_clause(self, column: str) -> str:
        return ""

    def child_clause(self, row: dict, schema: str, table: str):
        if row.get("partitionisdefault"):
            return (
                f"CREATE TABLE {schema}.{row['partitiontablename']} "
                f"PARTITION OF {schema}.{table} DEFAULT;"
            )
