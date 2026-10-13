"""Dashboard queries (step 05): the versioned SQL behind the AI/BI dashboard.

The `.sql` files in `dashboards/sql/` are the source of truth. The dashboard
`dashboards/sales_overview.lvdash.json` holds the same text as its datasets.
Each query reads only Gold and takes the named parameters `:catalog` and
`:env`, which are bound as values and never pasted into the SQL text.
`run_query` lets tests and notebooks run exactly what the dashboard runs.
"""

from pathlib import Path

from .naming import layer_names

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "dashboards"
SQL_DIR = DASHBOARD_DIR / "sql"
DASHBOARD_FILE = DASHBOARD_DIR / "sales_overview.lvdash.json"

# Query name -> file in SQL_DIR. The dashboard dataset names match the keys.
QUERIES = {
    "kpis": "kpis.sql",
    "revenue_trend": "revenue_trend.sql",
    "top_products": "top_products.sql",
    "unsold_products": "unsold_products.sql",
    "top_customers": "top_customers.sql",
    "inactive_customers": "inactive_customers.sql",
}


def load_query(name):
    """Return the SQL text of one dashboard query."""
    if name not in QUERIES:
        raise ValueError(f"Unknown dashboard query {name!r}; expected one of {sorted(QUERIES)}.")
    return (SQL_DIR / QUERIES[name]).read_text(encoding="utf-8")


def run_query(spark, name, catalog, env):
    """Validate catalog and env, then run one dashboard query and return its DataFrame."""
    names = layer_names(catalog, env)
    text = load_query(name)
    return spark.sql(text, args={"catalog": names.catalog, "env": names.env})
