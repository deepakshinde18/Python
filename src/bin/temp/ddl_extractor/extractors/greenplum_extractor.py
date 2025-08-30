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

    def generate_full_script(self, fq_table: str, include_if_not_exists: bool = True) -> str:
        """
        Generate a full DDL script for Postgres:
        - parent table (with PARTITION BY if applicable)
        - all child partitions (RANGE, LIST, HASH, DEFAULT)
        """
        # Load parent definition
        tdef = self.load_table_def(fq_table)
        parent_sql = self.to_postgres_ddl(tdef, include_if_not_exists=include_if_not_exists)

        # Load child partitions
        child_sqls = self.load_partitions(fq_table)

        # Assemble full script
        full_script = "\n\n".join([parent_sql] + child_sqls)
        return full_script
    
    @staticmethod
    def _split_qualified(fq: str):
        if '.' in fq:
            return fq.split('.', 1)
        return 'public', fq


import psycopg2.extras
from typing import List, Optional
from dataclasses import dataclass

from ..models import TableDef, ColumnDef
from ..strategies.registry import PartitionStrategyRegistry


@dataclass
class UniqueConstraint:
    name: str
    cols: List[str]


@dataclass
class ForeignKeyConstraint:
    name: str
    cols: List[str]
    ref_schema: str
    ref_table: str
    ref_cols: List[str]


class GreenplumDDLExtractor:
    TYPE_NORMALIZERS = {
        "character varying": "varchar",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
    }

    def __init__(self, pool):
        self.pool = pool

    # ------------------------
    # Load Table Definition
    # ------------------------
    def load_table_def(self, fq_table: str) -> TableDef:
        schema, table = self._split_qualified(fq_table)

        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Columns
                cur.execute("""
                    SELECT column_name, data_type, is_nullable,
                           column_default, character_maximum_length,
                           numeric_precision, numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """, (schema, table))
                cols = [
                    ColumnDef(
                        name=r["column_name"],
                        data_type=self.TYPE_NORMALIZERS.get(r["data_type"], r["data_type"]),
                        is_nullable=(r["is_nullable"] == "YES"),
                        default=r["column_default"],
                        char_max=r["character_maximum_length"],
                        num_precision=r["numeric_precision"],
                        num_scale=r["numeric_scale"],
                    )
                    for r in cur.fetchall()
                ]

                # Primary Key
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class t   ON t.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
                    WHERE i.indisprimary = TRUE
                      AND n.nspname = %s
                      AND t.relname = %s
                    ORDER BY cols.ord;
                """, (schema, table))
                pk_cols = [r[0] for r in cur.fetchall()]

        return TableDef(schema=schema, name=table, columns=cols, pk_cols=pk_cols)

    # ------------------------
    # Constraints
    # ------------------------
    def load_unique_constraints(self, fq_table: str) -> List[UniqueConstraint]:
        schema, table = self._split_qualified(fq_table)
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT con.conname,
                           array_agg(a.attname ORDER BY cols.ord) AS cols
                    FROM pg_constraint con
                    JOIN pg_class t   ON t.oid = con.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
                    WHERE con.contype = 'u'
                      AND n.nspname = %s
                      AND t.relname = %s
                    GROUP BY con.conname;
                """, (schema, table))
                return [UniqueConstraint(r["conname"], r["cols"]) for r in cur.fetchall()]

    def load_foreign_keys(self, fq_table: str) -> List[ForeignKeyConstraint]:
        schema, table = self._split_qualified(fq_table)
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT con.conname,
                           nsp.nspname AS ref_schema,
                           rt.relname  AS ref_table,
                           array_agg(a.attname ORDER BY cols.ord) AS cols,
                           array_agg(ra.attname ORDER BY refcols.ord) AS ref_cols
                    FROM pg_constraint con
                    JOIN pg_class t   ON t.oid = con.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_class rt  ON rt.oid = con.confrelid
                    JOIN pg_namespace nsp ON nsp.oid = rt.relnamespace
                    JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
                    JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS refcols(attnum, ord) ON TRUE
                    JOIN pg_attribute a  ON a.attrelid = t.oid  AND a.attnum = cols.attnum
                    JOIN pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = refcols.attnum
                    WHERE con.contype = 'f'
                      AND n.nspname = %s
                      AND t.relname = %s
                    GROUP BY con.conname, nsp.nspname, rt.relname;
                """, (schema, table))
                return [
                    ForeignKeyConstraint(
                        r["conname"], r["cols"], r["ref_schema"], r["ref_table"], r["ref_cols"]
                    )
                    for r in cur.fetchall()
                ]

    # ------------------------
    # Partitioning
    # ------------------------
    def load_partitions(self, fq_table: str) -> List[str]:
        schema, table = self._split_qualified(fq_table)
        parts_sqls = []
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT pr.parname,
                           pr.parisdefault,
                           pr.parrangestart,
                           pr.parrangeend,
                           pr.parlistvalues,
                           pr.parhashmodulus,
                           pr.parhashremainder,
                           p.parkind,
                           (SELECT array_agg(attname ORDER BY attnum)
                            FROM pg_attribute
                            WHERE attrelid = p.parrelid
                              AND attnum = ANY (p.paratts)) AS partition_columns
                    FROM pg_partition_rule pr
                    JOIN pg_partition p ON p.oid = pr.paroid
                    JOIN pg_class t ON t.oid = p.parrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = %s
                      AND t.relname = %s
                    ORDER BY pr.parruleord;
                """, (schema, table))
    
                for row in cur.fetchall():
                    key = "d" if row["parisdefault"] else row["parkind"]
                    strategy = PartitionStrategyRegistry.get(key)
                    if strategy:
                        stmt = strategy.child_clause(row, schema, table)
                        if stmt:
                            parts_sqls.append(stmt)
        return parts_sqls


    def _detect_partition_clause(self, fq_table: str) -> Optional[str]:
        schema, table = self._split_qualified(fq_table)
        with self.pool.get() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT p.parkind,
                           (SELECT array_agg(attname ORDER BY attnum)
                            FROM pg_attribute
                            WHERE attrelid = p.parrelid
                              AND attnum = ANY (p.paratts)) AS cols
                    FROM pg_partition p
                    JOIN pg_class t   ON t.oid = p.parrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = %s
                      AND t.relname = %s
                    LIMIT 1;
                """, (schema, table))
                row = cur.fetchone()
                if not row:
                    return None
                strategy = PartitionStrategyRegistry.get(row["parkind"])
                if strategy and row["cols"]:
                    return strategy.parent_clause(", ".join(row["cols"]))
                return None


    # ------------------------
    # Generate DDL
    # ------------------------
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
            null_sql = "NULL" if c.is_nullable else "NOT NULL"
            default_sql = f" DEFAULT {c.default}" if c.default else ""
            col_lines.append(f'  "{c.name}" {dt}{default_sql} {null_sql}')

        pk_sql = (
            f",\n  PRIMARY KEY ({', '.join(f'\"{c}\"' for c in t.pk_cols)})"
            if t.pk_cols else ""
        )
        ine = "IF NOT EXISTS " if include_if_not_exists else ""
        partition_clause = self._detect_partition_clause(f"{t.schema}.{t.name}")

        ddl = (
            f"CREATE TABLE {ine}\"{t.schema}\".\"{t.name}\" (\n"
            + ",\n".join(col_lines)
            + pk_sql
            + "\n)"
            + (f" {partition_clause}" if partition_clause else "")
            + ";"
        )
        return ddl

    def generate_full_script(self, fq_table: str, include_if_not_exists: bool = True) -> str:
        tdef = self.load_table_def(fq_table)
        parent_sql = self.to_postgres_ddl(tdef, include_if_not_exists=include_if_not_exists)
        partition_sqls = self.load_partitions(fq_table)

        # Constraints
        uniques = self.load_unique_constraints(fq_table)
        ucons_sqls = [
            f"ALTER TABLE \"{tdef.schema}\".\"{tdef.name}\" "
            f"ADD CONSTRAINT {u.name} UNIQUE ({', '.join(u.cols)});"
            for u in uniques
        ]

        fks = self.load_foreign_keys(fq_table)
        fk_sqls = [
            f"ALTER TABLE \"{tdef.schema}\".\"{tdef.name}\" "
            f"ADD CONSTRAINT {f.name} FOREIGN KEY ({', '.join(f.cols)}) "
            f"REFERENCES \"{f.ref_schema}\".\"{f.ref_table}\" ({', '.join(f.ref_cols)});"
            for f in fks
        ]

        return "\n\n".join([parent_sql] + partition_sqls + ucons_sqls + fk_sqls)

    # ------------------------
    # Helpers
    # ------------------------
    @staticmethod
    def _split_qualified(fq: str):
        if "." in fq:
            return fq.split(".", 1)
        return "public", fq

