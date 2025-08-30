import psycopg2.extras
from ..models import TableDef, ColumnDef
from ..strategies.registry import PartitionStrategyRegistry

class GreenplumDDLExtractor:
    TYPE_NORMALIZERS = {
        "character varying": "varchar",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
    }

    def __init__(self, pool):
        self.pool = pool

    def load_table_def(self, fq_table: str) -> TableDef:
        # same as before
        ...

    def to_postgres_ddl(self, t: TableDef, include_if_not_exists: bool = True) -> str:
        # same as before
        ...

    def load_partitions(self, fq_table: str):
        schema, table = self._split_qualified(fq_table)
        parts_sqls = []
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT partitiontablename, partitionrangestart, partitionrangeend,
                           partitionlistvalues, partitionisdefault, partitiontype,
                           partitionhashmodulus, partitionhashremainder, partitioncolumn
                    FROM pg_partitions
                    WHERE schemaname = %s AND tablename = %s
                """, (schema, table))
                for row in cur.fetchall():
                    key = "d" if row.get("partitionisdefault") else row["partitiontype"]
                    strategy = PartitionStrategyRegistry.get(key)
                    if strategy:
                        stmt = strategy.child_clause(row, schema, table)
                        if stmt:
                            parts_sqls.append(stmt)
        return parts_sqls

    def _detect_partition_clause(self, fq_table: str):
        schema, table = self._split_qualified(fq_table)
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT partitiontype, partitioncolumn
                    FROM pg_partitions
                    WHERE schemaname = %s AND tablename = %s
                    LIMIT 1
                """, (schema, table))
                row = cur.fetchone()
                if not row:
                    return None
                strategy = PartitionStrategyRegistry.get(row["partitiontype"])
                return strategy.parent_clause(row["partitioncolumn"]) if strategy else None

    @staticmethod
    def _split_qualified(fq: str):
        if '.' in fq:
            return fq.split('.', 1)
        return 'public', fq
