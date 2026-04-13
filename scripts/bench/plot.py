#!/usr/bin/env python3
"""plot.py — Generate benchmark plots from report samples.

Usage:
    uv run --with matplotlib python3 scripts/bench/plot.py [DATADIR]

If DATADIR is omitted, reads from /tmp/txgen-bench-datadir.

Reads $DATADIR/report.json and extracts the unified `samples` array to
produce $DATADIR/bench_plots.png with a multi-panel overview.
"""

import json
import os
import sys
from collections import defaultdict


def load_report(report_path: str) -> dict:
    """Load the full JSON report."""
    with open(report_path) as f:
        return json.load(f)


def load_samples(report: dict) -> list[dict]:
    """Extract samples grouped by scrape offset.

    Returns a list of dicts keyed by (name, labels_tuple) → value,
    plus a "ts_ms" entry for the time axis.
    """
    samples = report.get("samples", [])
    if not samples:
        return []

    by_offset: dict[int, dict] = defaultdict(dict)
    for s in samples:
        offset = s["offset_ms"]
        name = s["name"]
        labels = s.get("labels", {})
        label_key = tuple(sorted(labels.items()))

        by_offset[offset][(name, label_key)] = s["value"]
        by_offset[offset]["ts_ms"] = s.get("unix_ms", offset)

    return [by_offset[k] for k in sorted(by_offset.keys())]


def col(rows: list[dict], name: str, conv=float, default=0, **labels):
    """Extract a metric column by name and optional label filters.

    Labels are matched as subsets — a sample with extra labels (e.g.
    injected metadata) still matches as long as all specified labels
    are present.
    """
    result = []
    match_items = set(labels.items())
    for row in rows:
        val = default
        for key, v in row.items():
            if not isinstance(key, tuple):
                continue
            n, lk = key
            if n != name:
                continue
            if match_items <= set(lk):
                val = v
                break
        result.append(conv(val))
    return result


def avg(xs: list) -> float:
    return sum(xs) / len(xs) if xs else 0


def steady_avg(ts: list, xs: list, warmup: float = 15) -> float:
    vals = [v for t, v in zip(ts, xs) if t > warmup]
    return avg(vals)


def main():
    if len(sys.argv) > 1:
        datadir = sys.argv[1]
    else:
        with open("/tmp/txgen-bench-datadir") as f:
            datadir = f.read().strip()

    report_path = os.path.join(datadir, "report.json")
    if not os.path.exists(report_path):
        print(f"error: {report_path} not found", file=sys.stderr)
        sys.exit(1)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    report = load_report(report_path)
    rows = load_samples(report)
    if not rows:
        print("error: no sample data in report (was --metrics-url used?)", file=sys.stderr)
        sys.exit(1)

    # Deduplicate: keep last sample per block number
    HEIGHT = "reth_blockchain_tree_canonical_chain_height"
    seen: dict[int, dict] = {}
    for r in rows:
        block = int(col([r], HEIGHT, int, 0)[0])
        seen[block] = r
    rows = sorted(seen.values(), key=lambda r: r["ts_ms"])

    # Print available metric names for reference
    all_names = sorted(set(
        key[0] for row in rows for key in row.keys()
        if isinstance(key, tuple)
    ))
    keys_path = os.path.join(datadir, "metric_keys.txt")
    with open(keys_path, "w") as f:
        for k in all_names:
            f.write(k + "\n")
    print(f"Available metrics ({len(all_names)} names) written to {keys_path}")

    # ── Aliases ──────────────────────────────────────────────────────
    P = "reth_tempo_payload_builder"

    t0 = rows[0]["ts_ms"]
    ts = [(r["ts_ms"] - t0) / 1000.0 for r in rows]

    # First sample offset_ms for converting block marker offsets to plot time
    offset0 = sorted(set(s["offset_ms"] for s in report.get("samples", [])))[0] if report.get("samples") else 0

    # ── Extract columns ──────────────────────────────────────────────
    # Throughput
    tx_last = col(rows, f"{P}_total_transactions_last", int)
    gps = col(rows, f"{P}_gas_per_second_last")
    rlp_size = col(rows, f"{P}_rlp_block_size_bytes_last")

    # Gas
    gas_used = col(rows, f"{P}_gas_used_last")
    pay_gas = col(rows, f"{P}_payment_gas_used_last")
    pay_limit = col(rows, f"{P}_payment_gas_limit_last")
    gen_gas_limit = col(rows, f"{P}_general_gas_limit_last")
    shared_gas_limit = col(rows, f"{P}_shared_gas_limit_last")
    total_gas_limit = [p + g + s for p, g, s in zip(pay_limit, gen_gas_limit, shared_gas_limit)]
    gas_pct = [u / t * 100 if t > 0 else 0 for u, t in zip(gas_used, total_gas_limit)]
    pay_fill = [g / l * 100 if l > 0 else 0 for g, l in zip(pay_gas, pay_limit)]

    # Builder timing
    build_p50 = col(rows, f"{P}_payload_build_duration_seconds", quantile="0.5")
    build_p99 = col(rows, f"{P}_payload_build_duration_seconds", quantile="0.99")
    sr_p50 = col(rows, f"{P}_state_root_with_updates_duration_seconds", quantile="0.5")
    sr_p99 = col(rows, f"{P}_state_root_with_updates_duration_seconds", quantile="0.99")
    fin_p50 = col(rows, f"{P}_payload_finalization_duration_seconds", quantile="0.5")
    fin_p99 = col(rows, f"{P}_payload_finalization_duration_seconds", quantile="0.99")

    # Builder anatomy
    total_tx_exec_p50 = col(rows, f"{P}_total_transaction_execution_duration_seconds", quantile="0.5")
    state_setup_p50 = col(rows, f"{P}_state_setup_duration_seconds", quantile="0.5")
    hashed_post_p50 = col(rows, f"{P}_hashed_post_state_duration_seconds", quantile="0.5")
    build_count = col(rows, f"{P}_payload_build_duration_seconds_count", int)

    # Execution
    tx_exec_p50 = col(rows, f"{P}_transaction_execution_duration_seconds", quantile="0.5")
    tx_exec_p99 = col(rows, f"{P}_transaction_execution_duration_seconds", quantile="0.99")

    # Pool
    pool_pending = col(rows, "reth_transaction_pool_pending_pool_transactions", int)
    pool_basefee = col(rows, "reth_transaction_pool_basefee_pool_transactions", int)
    pool_queued = col(rows, "reth_transaction_pool_queued_pool_transactions", int)
    fetch_p50 = col(rows, f"{P}_pool_fetch_duration_seconds", quantile="0.5")
    skip_nonce = col(rows, f"{P}_pool_transactions_skipped_total", int, reason="nonce_too_low")
    skip_invalid = col(rows, f"{P}_pool_transactions_skipped_total", int, reason="invalid_tx")
    skip_nonce_delta = [b - a for a, b in zip([skip_nonce[0]] + skip_nonce, skip_nonce)]
    skip_invalid_delta = [b - a for a, b in zip([skip_invalid[0]] + skip_invalid, skip_invalid)]

    # ── Derived columns ──────────────────────────────────────────────
    ggas_s = [g / 1e9 for g in gps]
    rlp_kb = [v / 1024 for v in rlp_size]
    build_p50_ms = [v * 1000 for v in build_p50]
    build_p99_ms = [v * 1000 for v in build_p99]
    sr_p50_ms = [v * 1000 for v in sr_p50]
    sr_p99_ms = [v * 1000 for v in sr_p99]
    fin_p50_ms = [v * 1000 for v in fin_p50]
    fin_p99_ms = [v * 1000 for v in fin_p99]
    total_tx_exec_p50_ms = [v * 1000 for v in total_tx_exec_p50]
    state_setup_p50_ms = [v * 1000 for v in state_setup_p50]
    hashed_post_p50_ms = [v * 1000 for v in hashed_post_p50]
    tx_p50_us = [v * 1e6 for v in tx_exec_p50]
    tx_p99_us = [v * 1e6 for v in tx_exec_p99]
    fetch_p50_ms = [v * 1000 for v in fetch_p50]

    # Builds per block: delta of cumulative build count between deduped samples
    builds_per_block = [b - a for a, b in zip([build_count[0]] + build_count, build_count)]

    # ── Plot ─────────────────────────────────────────────────────────
    fig, axes = plt.subplots(5, 3, figsize=(20, 25))
    duration = ts[-1] - ts[0]
    n_blocks = len(rows)

    # ── Block markers ──────────────────────────────────────────────────
    block_markers = report.get("block_markers", [])
    marker_ts = [(m["offset_ms"] - offset0) / 1000.0 for m in block_markers]

    fig.suptitle(
        f"Tempo Bench: {n_blocks} blocks over {duration:.0f}s "
        f"(avg {avg(tx_last):.0f} tx/block)",
        fontsize=14,
        fontweight="bold",
    )

    # ── Metadata subtitle ────────────────────────────────────────────
    metadata = report.get("metadata")
    if metadata:
        meta_str = "  |  ".join(f"{k}={v}" for k, v in metadata.items())
        fig.text(0.5, 0.965, meta_str, ha="center", fontsize=10,
                 color="#555555", fontstyle="italic")

    def add_markers(ax):
        """Draw a rug plot of block markers along the bottom edge."""
        if marker_ts:
            ax.eventplot(marker_ts, orientation="horizontal",
                         lineoffsets=0, linelengths=0.06,
                         colors="#E91E63", alpha=0.6, linewidths=0.6,
                         transform=ax.get_xaxis_transform())

    def pl(ax, ys, color, **kw):
        ax.plot(ts, ys, color=color, linewidth=0.8, **kw)

    def style(ax, ylabel="", title=""):
        ax.set_xlabel("Time (s)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        add_markers(ax)

    ax = axes[0][0]
    pl(ax, tx_last, "#2196F3")
    a = avg(tx_last)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.0f}")
    style(ax, "Txs", "Txs per Block"); ax.legend()

    ax = axes[0][1]
    pl(ax, ggas_s, "#FF9800")
    a = steady_avg(ts, ggas_s)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"steady={a:.2f}")
    style(ax, "Ggas/s", "Gas Throughput (Ggas/s)"); ax.legend()

    ax = axes[0][2]
    pl(ax, rlp_kb, "#3F51B5")
    a = avg(rlp_kb)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.0f} KB")
    style(ax, "KB", "RLP Block Size"); ax.legend()

    # ── Row 2: Block Gas ─────────────────────────────────────────────
    ax = axes[1][0]
    pl(ax, gas_pct, "#E91E63")
    a = steady_avg(ts, gas_pct)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"steady={a:.2f}%")
    style(ax, "%", "Block Gas Used %"); ax.legend()

    ax = axes[1][1]
    pl(ax, pay_fill, "#9C27B0")
    ax.set_ylim(0, 105)
    a = steady_avg(ts, pay_fill)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"steady={a:.1f}%")
    style(ax, "%", "Payment Gas Fill %"); ax.legend()

    axes[1][2].axis("off")

    # ── Row 3: Block Builder ─────────────────────────────────────────
    ax = axes[2][0]
    pl(ax, build_p50_ms, "#2196F3", label="p50")
    pl(ax, build_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Payload Build Duration"); ax.legend()

    ax = axes[2][1]
    pl(ax, sr_p50_ms, "#9C27B0", label="p50")
    pl(ax, sr_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "State Root Duration"); ax.legend()

    ax = axes[2][2]
    pl(ax, fin_p50_ms, "#607D8B", label="p50")
    pl(ax, fin_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Payload Finalization Duration"); ax.legend()

    # ── Row 4: Block Anatomy ─────────────────────────────────────────
    ax = axes[3][0]
    pl(ax, total_tx_exec_p50_ms, "#FF5722", label="tx execution")
    pl(ax, state_setup_p50_ms, "#4CAF50", label="state setup")
    pl(ax, hashed_post_p50_ms, "#00BCD4", label="hashed post state")
    style(ax, "ms", "Build Time Breakdown (p50)"); ax.legend()

    ax = axes[3][1]
    pl(ax, builds_per_block, "#795548")
    a = avg(builds_per_block)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.1f}")
    style(ax, "Count", "Builds per Block"); ax.legend()

    ax = axes[3][2]
    pl(ax, tx_p50_us, "#FF5722", label="p50")
    pl(ax, tx_p99_us, "#F44336", alpha=0.7, label="p99")
    style(ax, "µs", "Per-Tx Execution Duration"); ax.legend()

    # ── Row 5: Pool ──────────────────────────────────────────────────
    ax = axes[4][0]
    pl(ax, pool_pending, "#2196F3", label="pending")
    pl(ax, pool_basefee, "#FF9800", label="basefee")
    pl(ax, pool_queued, "#F44336", label="queued")
    style(ax, "Txs", "Txpool by Sub-pool"); ax.legend()

    ax = axes[4][1]
    pl(ax, fetch_p50_ms, "#009688", label="p50")
    style(ax, "ms", "Pool Fetch Duration"); ax.legend()

    ax = axes[4][2]
    pl(ax, skip_nonce_delta, "#F44336", label="nonce_too_low")
    pl(ax, skip_invalid_delta, "#FF9800", label="invalid_tx")
    style(ax, "Count", "Skipped Txs per Block"); ax.legend()

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out_path = os.path.join(datadir, "bench_plots.png")
    plt.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")

    # ── Summary ──────────────────────────────────────────────────────
    if marker_ts:
        print(f"\nBlock markers: {len(marker_ts)}")
    print(f"Blocks (deduped): {n_blocks}")
    print(f"Time range: {ts[0]:.1f}s – {ts[-1]:.1f}s ({duration:.1f}s)")
    print(f"Avg txs/block: {avg(tx_last):.0f}")
    print(f"Steady Ggas/s: {steady_avg(ts, ggas_s):.2f}")
    print(f"Block gas:  {steady_avg(ts, gas_pct):.2f}%")
    print(f"Pay fill:   {steady_avg(ts, pay_fill):.1f}%")
    print(f"Build       p50={avg(build_p50_ms):.2f}ms  p99={avg(build_p99_ms):.2f}ms")
    print(f"State root  p50={avg(sr_p50_ms):.2f}ms  p99={avg(sr_p99_ms):.2f}ms")
    print(f"Finalize    p50={avg(fin_p50_ms):.2f}ms  p99={avg(fin_p99_ms):.2f}ms")
    print(f"Tx exec     p50={avg(tx_p50_us):.1f}µs  p99={avg(tx_p99_us):.1f}µs")
    print(f"Total tx exec p50={avg(total_tx_exec_p50_ms):.2f}ms")
    print(f"State setup   p50={avg(state_setup_p50_ms):.2f}ms")
    print(f"Hashed post   p50={avg(hashed_post_p50_ms):.2f}ms")
    print(f"Pool fetch  p50={avg(fetch_p50_ms):.2f}ms")
    print(f"Builds/block: {avg(builds_per_block):.1f}")
    print(f"Avg RLP size: {avg(rlp_kb):.0f} KB")
    print(f"Skipped nonce_low: {skip_nonce[-1] if skip_nonce else 0}")
    print(f"Skipped invalid: {skip_invalid[-1] if skip_invalid else 0}")


if __name__ == "__main__":
    main()
