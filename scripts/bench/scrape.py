#!/usr/bin/env python3
"""scrape.py — Scrape all tempo prometheus metrics to NDJSON.

Usage:
    python3 scripts/bench/scrape.py <output.ndjson> <stop-file> [interval]

Each line is a JSON object with ts_ms and every prometheus metric as a key.
Labeled metrics are flattened: metric{quantile="0.5"} → metric.q0.5
                                metric{reason="foo"}  → metric.foo

Stops when <stop-file> is created. Default interval: 500ms.
"""

import json
import os
import sys
import time
import urllib.request


def parse_prometheus(text: str) -> dict:
    """Parse prometheus text format into a flat dict."""
    metrics = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # Split "metric_name{labels} value" or "metric_name value"
        if "{" in line:
            name_part, rest = line.split("{", 1)
            labels_part, value_part = rest.split("} ", 1)
            # Parse labels
            labels = {}
            for pair in labels_part.split(","):
                if "=" not in pair:
                    continue
                k, v = pair.split("=", 1)
                labels[k.strip()] = v.strip().strip('"')
            # Flatten labels into key
            if "quantile" in labels:
                q = labels["quantile"]
                key = f"{name_part}.q{q}"
            else:
                # Join all label values
                suffix = ".".join(labels.values())
                key = f"{name_part}.{suffix}"
        else:
            parts = line.split()
            if len(parts) < 2:
                continue
            key = parts[0]
            value_part = parts[1]

        try:
            value = float(value_part)
            # Store as int if it's a whole number
            if value == int(value):
                value = int(value)
            metrics[key] = value
        except ValueError:
            continue

    return metrics


def main():
    if len(sys.argv) < 3:
        print("usage: scrape.py <output.ndjson> <stop-file> [interval]", file=sys.stderr)
        sys.exit(1)

    outfile = sys.argv[1]
    stopfile = sys.argv[2]
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 0.5
    metrics_url = os.environ.get("METRICS_URL", "http://127.0.0.1:9001/metrics")

    n = 0
    with open(outfile, "w") as f:
        while not os.path.exists(stopfile):
            ts = int(time.time() * 1000)
            try:
                with urllib.request.urlopen(metrics_url, timeout=2) as resp:
                    text = resp.read().decode()
            except Exception:
                time.sleep(interval)
                continue

            record = parse_prometheus(text)
            record["ts_ms"] = ts
            f.write(json.dumps(record, separators=(",", ":")))
            f.write("\n")
            f.flush()
            n += 1

            time.sleep(interval)

    print(f"Scraped {n} samples to {outfile}", file=sys.stderr)


if __name__ == "__main__":
    main()
