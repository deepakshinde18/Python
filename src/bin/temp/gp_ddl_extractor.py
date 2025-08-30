from dataclasses import dataclass
from typing import List, Optional, Tuple
import psycopg2
import psycopg2.extras

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


GP_TYPE_NORMALIZERS = {
    "character varying": "varchar",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
}


class DDLExtractor:
    def __init__(self, pool):
        self.pool = pool

    # ---------------- Load table definition ----------------
    def load_table_def(self, fq_table: str) -> TableDef:
        schema, table = self._split_qualified(fq_table)
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Columns
                cur.execute("""
                    SELECT
                      c.ordinal_position,
                      c.column_name,
                      c.data_type,
                      c.is_nullable,
                      c.column_default,
                      c.character_maximum_length,
                      c.numeric_precision,
                      c.numeric_scale
                    FROM information_schema.columns c
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (schema, table))
                cols = [
                    ColumnDef(
                        name=row["column_name"],
                        data_type=GP_TYPE_NORMALIZERS.get(row["data_type"], row["data_type"]),
                        is_nullable=(row["is_nullable"] == "YES"),
                        default=row["column_default"],
                        char_max=row["character_maximum_length"],
                        num_precision=row["numeric_precision"],
                        num_scale=row["numeric_scale"],
                    )
                    for row in cur.fetchall()
                ]

                # Primary Key
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class t   ON t.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
                    WHERE i.indisprimary = TRUE AND n.nspname = %s AND t.relname = %s
                    ORDER BY array_position(i.indkey, a.attnum)
                """, (schema, table))
                pk_cols = [r[0] for r in cur.fetchall()]

        return TableDef(schema=schema, name=table, columns=cols, pk_cols=pk_cols)

    # ---------------- Convert to Postgres DDL ----------------
    def to_postgres_ddl(self, t: TableDef, include_if_not_exists: bool = True) -> str:
        col_lines = []
        for c in t.columns:
            dt = c.data_type
            if dt in ("character varying", "varchar", "character") and c.char_max:
                dt = f"varchar({c.char_max})"
            elif dt in ("numeric", "decimal"):
                if c.num_precision and c.num_scale is not None:
                    dt = f"numeric({c.num_precision},{c.num_scale})"
                elif c.num_precision:
                    dt = f"numeric({c.num_precision})"
            null_sql = "NOT NULL" if not c.is_nullable else "NULL"
            default_sql = f" DEFAULT {c.default}" if c.default else ""
            col_lines.append(f"  \"{c.name}\" {dt}{default_sql} {null_sql}")

        pk_sql = (
            f",\n  PRIMARY KEY ({', '.join(f'\"{c}\"' for c in t.pk_cols)})"
            if t.pk_cols else ""
        )
        ine = "IF NOT EXISTS " if include_if_not_exists else ""
        ddl = (
            f"CREATE TABLE {ine}\"{t.schema}\".\"{t.name}\" (\n"
            + ",\n".join(col_lines)
            + pk_sql
            + "\n);"
        )
        return ddl

    # ---------------- Distribution Clause ----------------
    def get_distribution_clause(self, fq_table: str) -> str:
        schema, table = self._split_qualified(fq_table)
        clause = "DISTRIBUTED RANDOMLY"
        with self.pool.get() as conn:
            with conn.cursor() as cur:
                for colname in ("distkey", "attrnums"):  # GP6+ vs GP5
                    try:
                        cur.execute(f"""
                            SELECT a.attname
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            JOIN gp_distribution_policy p ON p.localoid = c.oid
                            LEFT JOIN unnest(p.{colname}) AS u(attnum) ON true
                            LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
                            WHERE n.nspname = %s AND c.relname = %s;
                        """, (schema, table))
                        rows = [r[0] for r in cur.fetchall() if r[0] is not None]
                        if rows:
                            clause = f"DISTRIBUTED BY ({', '.join(rows)})"
                            break
                    except psycopg2.Error:
                        conn.rollback()
                        continue
        return clause

    # ---------------- Partition Extraction ----------------
    def load_partitions(self, fq_table: str) -> List[str]:
        """
        Convert Greenplum partitions into Postgres PARTITION OF statements.
        Currently supports RANGE partitions.
        """
        schema, table = self._split_qualified(fq_table)
        parts_sqls: List[str] = []
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT partitiontablename, partitionrangestart, partitionrangeend
                    FROM pg_partitions
                    WHERE schemaname = %s AND tablename = %s
                    ORDER BY partitionrangestart
                """, (schema, table))
                for row in cur.fetchall():
                    pname = row["partitiontablename"]
                    start = row["partitionrangestart"]
                    end = row["partitionrangeend"]
                    sql_stmt = (
                        f"CREATE TABLE {schema}.{pname} PARTITION OF {schema}.{table} "
                        f"FOR VALUES FROM ('{start}') TO ('{end}');"
                    )
                    parts_sqls.append(sql_stmt)
        return parts_sqls

    # ---------------- Helper ----------------
    @staticmethod
    def _split_qualified(fq: str) -> Tuple[str, str]:
        if '.' in fq:
            s, t = fq.split('.', 1)
            return s, t
        return 'public', fq
