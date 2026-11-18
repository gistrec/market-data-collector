#!/usr/bin/env python3
"""Print schema.sql with the Kafka credentials filled in from the environment.

    KAFKA_BOOTSTRAP=... KAFKA_API_KEY=... KAFKA_API_SECRET=... \\
        python3 clickhouse/render.py | clickhouse-client --multiquery
"""
import os
import pathlib
import sys

NAMES = ("KAFKA_BOOTSTRAP", "KAFKA_API_KEY", "KAFKA_API_SECRET")


def main() -> None:
    missing = [n for n in NAMES if not os.environ.get(n)]
    if missing:
        sys.exit(f"missing environment variables: {', '.join(missing)}")
    sql = (pathlib.Path(__file__).parent / "schema.sql").read_text()
    for name in NAMES:
        value = os.environ[name]
        if "'" in value or "\\" in value:
            sys.exit(f"{name} contains a quote or a backslash; refusing to build SQL from it")
        sql = sql.replace("{{" + name + "}}", value)
    sys.stdout.write(sql)


if __name__ == "__main__":
    main()
