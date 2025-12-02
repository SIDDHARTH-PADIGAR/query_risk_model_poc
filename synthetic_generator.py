import random
import csv
from metadata_extractor import extract_metadata

# Table definitions for realistic cost scaling
TABLES = {
    "small_table":       {"rows": 10_000,       "columns": 4},
    "mid_table":         {"rows": 200_000,      "columns": 6},
    "big_sales_table":   {"rows": 10_000_000,   "columns": 8},
    "huge_events":       {"rows": 50_000_000,   "columns": 10},
    "user_master":       {"rows": 1_000_000,    "columns": 5},
    "geo_dim":           {"rows": 2_000_000,    "columns": 4},
    "orders":            {"rows": 5_000_000,    "columns": 6},
    "products":          {"rows": 500_000,      "columns": 6},
    "reviews":           {"rows": 3_000_000,    "columns": 6}
}

def random_table():
    return random.choice(list(TABLES.keys()))

def generate_query(num_joins, nested, groupby, orderby, filters, select_star):
    base_table = random_table()
    sql = f"SELECT {'*' if select_star else 't0.id'} FROM {base_table} t0"

    for j in range(num_joins):
        jt = random_table()
        sql += f" JOIN {jt} t{j+1} ON t0.id = t{j+1}.id"

    # Subqueries (nested)
    for _ in range(nested):
        inner = f"SELECT id FROM {random_table()}"
        sql = f"SELECT * FROM ({inner}) sub"

    # Filters
    if filters > 0:
        f = [f"t0.col{i} > {random.randint(10,1000)}" for i in range(filters)]
        sql += " WHERE " + " AND ".join(f)

    if groupby:
        sql += " GROUP BY t0.id"

    if orderby:
        sql += " ORDER BY t0.id"

    return sql, base_table

# ======= Realistic Labeling Logic =======

def label_from_meta(meta):
    score = 0

    # Table size footprint
    rows = meta["estimated_table_size_max"]
    if rows > 20_000_000:
        score += 3
    elif rows > 1_000_000:
        score += 2
    else:
        score += 1

    # Join cost
    score += meta["num_joins"] * 2

    # Subqueries
    score += meta["num_subqueries"] * 2
    score += meta["subquery_depth"]

    # Aggregation & sorting
    if meta["num_aggregates"] > 0:
        score += 1
    if meta["has_orderby"] == 1:
        score += 1
    if meta["has_groupby"] == 1:
        score += 1

    # SELECT *
    if meta["select_star"] == 1 and rows > 1_000_000:
        score += 3

    # Cartesian join
    if meta.get("cartesian_join", 0) == 1:
        return 2  # force high

    # Final risk labels
    if score <= 4:
        return 0
    elif score <= 8:
        return 1
    else:
        return 2

def main(n=2000, out="synthetic.csv"):
    rows = []

    for _ in range(n):
        num_joins = random.choices([0,1,2,3], [0.5,0.25,0.2,0.05])[0]
        nested = random.choices([0,1,2], [0.85,0.13,0.02])[0]
        groupby = random.random() < 0.3
        orderby = random.random() < 0.3
        filters = random.choices([0,1,2], [0.6,0.3,0.1])[0]
        select_star = random.random() < 0.4

        sql, base = generate_query(num_joins, nested, groupby, orderby, filters, select_star)

        meta = extract_metadata(sql)
        meta["estimated_table_size_max"] = TABLES[base]["rows"]
        label = label_from_meta(meta)

        meta["label"] = label
        meta["sql"] = sql
        rows.append(meta)

    keys = ["sql","num_tables","num_joins","num_filters","num_subqueries",
            "subquery_depth","num_aggregates","has_groupby","has_orderby",
            "has_limit","select_star","window_functions","udf_usage","s3_scan",
            "cartesian_join","query_length","estimated_table_size_max",
            "estimated_join_output","estimated_output_rows","estimated_sort_cost",
            "select_star_columns_estimate","label"]

    with open(out, "w", newline="", encoding="utf8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            for k in keys:
                r.setdefault(k, 0)
            w.writerow({k:r[k] for k in keys})

    print(f"Synthetic dataset written to {out}")

if __name__ == "__main__":
    main()
