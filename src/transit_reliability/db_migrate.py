from __future__ import annotations

from transit_reliability.config import project_root
from transit_reliability.db import get_connection


def main() -> None:
    schema_sql = (project_root() / "sql" / "schema.sql").read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.execute(schema_sql)
        conn.commit()
    print("Applied sql/schema.sql")


if __name__ == "__main__":
    main()

