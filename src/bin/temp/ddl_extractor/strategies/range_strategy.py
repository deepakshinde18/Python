from .base import PartitionStrategy

class RangePartitionStrategy(PartitionStrategy):
    def parent_clause(self, column: str) -> str:
        return f"PARTITION BY RANGE ({column})"

    def child_clause(self, row: dict, schema: str, table: str):
        start, end = row["partitionrangestart"], row["partitionrangeend"]
        if start and end:
            return (
                f"CREATE TABLE {schema}.{row['partitiontablename']} "
                f"PARTITION OF {schema}.{table} "
                f"FOR VALUES FROM ('{start}') TO ('{end}');"
            )
