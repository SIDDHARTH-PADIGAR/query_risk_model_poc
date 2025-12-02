import re
import sys
import os
import json
from infer import predict

def load_queries_strict(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r", encoding="utf8") as f:
        text = f.read()

    # Regex captures "1. query here" including multi-line
    pattern = r"(?:^|\n)(\d+)\.\s*(.*?)\s*(?=\n\d+\.|\Z)"

    matches = re.findall(pattern, text, flags=re.S)

    queries = []
    for _, sql in matches:
        cleaned = " ".join(
            line.strip() for line in sql.strip().splitlines()
        )
        queries.append(cleaned)

    return queries


def run_batch(path):
    queries = load_queries_strict(path)
    print(f"Loaded {len(queries)} queries from:\n{path}\n")

    for i, sql in enumerate(queries):
        print("=" * 90)
        print(f"QUERY {i+1}")
        print(sql)
        print("-" * 90)

        try:
            out = predict(sql)
            print(json.dumps(out, indent=2))
        except Exception as e:
            print(f"ERROR during inference: {e}")

        print("=" * 90 + "\n")


if __name__ == "__main__":
    default_path = os.path.join(os.getcwd(), "test_queries.txt")

    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        print(f"No file provided. Using default:\n{default_path}\n")
        path = default_path

    run_batch(path)
