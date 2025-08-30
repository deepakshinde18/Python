#!/usr/bin/env python3
import argparse
import sys
from ddl_extractor.pooled_db import PooledDB, PsycopgConnector
from ddl_extractor.extractors.greenplum_extractor import GreenplumDDLExtractor


def main():
    parser = argparse.ArgumentParser(
        description="Extract DDL from Greenplum and convert it for Postgres."
    )
    parser.add_argument(
        "--host", required=True, help="Greenplum/Postgres host"
    )
    parser.add_argument(
        "--port", type=int, default=5432, help="Greenplum/Postgres port"
    )
    parser.add_argument(
        "--dbname", required=True, help="Database name"
    )
    parser.add_argument(
        "--user", required=True, help="Database user"
    )
    parser.add_argument(
        "--password", required=True, help="Database password"
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Table to extract, in schema.table format (e.g., public.sales)",
    )
    parser.add_argument(
        "--outfile",
        help="File to save DDL. If omitted, prints to stdout",
    )

    args = parser.parse_args()

    # Setup connection pool
    connector = PsycopgConnector(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    pool = PooledDB(connector, minconn=1, maxconn=3)

    try:
        extractor = GreenplumDDLExtractor(pool)
        script = extractor.generate_full_script(args.table)

        if args.outfile:
            with open(args.outfile, "w", encoding="utf-8") as f:
                f.write(script + "\n")
            print(f"✅ DDL saved to {args.outfile}")
        else:
            print(script)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        pool.closeall()


if __name__ == "__main__":
    main()
