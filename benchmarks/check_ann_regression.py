import argparse
import json
import sys
from pathlib import Path


def load(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--max-regression-ratio", type=float, default=1.50)
    args = parser.parse_args()

    baseline = load(Path(args.baseline))
    current = load(Path(args.current))
    ratio_limit = args.max_regression_ratio

    regressions = []
    for engine in ("usearch", "faiss_flat"):
        b = baseline["metrics"][engine]["search_sec"]
        c = current["metrics"][engine]["search_sec"]
        ratio = c / b if b > 0 else float("inf")
        print(f"{engine}: baseline={b:.6f}s current={c:.6f}s ratio={ratio:.3f}")
        if ratio > ratio_limit:
            regressions.append((engine, ratio, ratio_limit))

    if regressions:
        for engine, ratio, limit in regressions:
            print(
                f"ERROR: {engine} search regression ratio {ratio:.3f} > {limit:.3f}",
                file=sys.stderr,
            )
        sys.exit(1)

    print("ANN regression check passed.")


if __name__ == "__main__":
    main()
