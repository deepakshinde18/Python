from .base import PartitionStrategy

class ListPartitionStrategy(PartitionStrategy):
    def parent_clause(self, column: str) -> str:
        return f"PARTITION BY LIST ({column})"

    def child_clause(self, row: dict, schema: str, table: str):
        values = row["partitionlistvalues"]
        if values:
            return (
                f"CREATE TABLE {schema}.{row['partitiontablename']} "
                f"PARTITION OF {schema}.{table} "
                f"FOR VALUES IN ({values});"
            )
