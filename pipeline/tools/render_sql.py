#!/usr/bin/env python3
"""Write ready-to-paste SQL with your catalog filled in (for the SQL editor / manual mode).

    python tools/render_sql.py --catalog my_sandbox
    -> rendered/<catalog>/silver/10_player.sql, ... (no {{ }} tokens left)

The SQL editor treats {{x}} as a legacy query parameter, which is why the raw files show a warning there.
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from lib.sql_runner import CELL_SEP, render, split_statements  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--analyst-group", default="analysts")
    ap.add_argument("--skip-create-catalog", action="store_true", help="drop the CREATE CATALOG statement from setup (use an existing catalog)")
    a = ap.parse_args()
    params = {"catalog": a.catalog, "analyst_group": a.analyst_group}
    out = ROOT / "rendered" / a.catalog
    for f in sorted((ROOT / "sql").rglob("*.sql")):
        dest = out / f.relative_to(ROOT / "sql")
        dest.parent.mkdir(parents=True, exist_ok=True)
        text = render(f.read_text(), params)
        if a.skip_create_catalog and f.name == "00_objects.sql":
            cells = [c for c in text.split(CELL_SEP) if "CREATE CATALOG" not in c]
            text = CELL_SEP.join(cells)
        dest.write_text(text)
        if f.parent.name == "dq":   # one result grid instead of 14 tabs: UNION ALL of every check
            union = "\nUNION ALL\n".join(s for s in split_statements(text))
            (dest.parent / f"{f.stem}_combined.sql").write_text(
                f"SELECT * FROM (\n{union}\n) ORDER BY CASE severity WHEN 'error' THEN 0 ELSE 1 END, violations DESC;\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
