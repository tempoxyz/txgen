#!/usr/bin/env python3
"""plot.py — Generate benchmark plots from report samples.

Usage:
    uv run --with matplotlib python3 scripts/bench/plot.py [DATADIR]

If DATADIR is omitted, reads from /tmp/txgen-bench-datadir.

Reads $DATADIR/report.json and extracts the unified `samples` array to
produce $DATADIR/bench_plots.png with a multi-panel overview.

Samples are point-in-time metric snapshots plotted over time.
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


def delta(xs: list) -> list:
    """Compute per-sample deltas from a cumulative series."""
    return [b - a for a, b in zip([xs[0]] + xs, xs)]


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

    metadata = report.get("metadata", {})
    mode = metadata.get("mode", "send")

    if mode == "replay":
        plot_replay(plt, report, rows, metadata, datadir)
    else:
        plot_send(plt, report, rows, metadata, datadir)


def plot_send(plt, report, rows, metadata, datadir):
    """Plot layout for send mode (payload builder metrics)."""
    P = "reth_tempo_payload_builder"

    t0 = rows[0]["ts_ms"]
    ts = [(r["ts_ms"] - t0) / 1000.0 for r in rows]
    n_scrapes = len(rows)

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
    skip_nonce_delta = delta(skip_nonce)
    skip_invalid_delta = delta(skip_invalid)

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
    builds_delta = delta(build_count)

    # ── Plot ─────────────────────────────────────────────────────────
    fig, axes = plt.subplots(5, 3, figsize=(20, 25))
    duration = ts[-1] - ts[0]

    fig.suptitle(
        f"Tempo Bench: {n_scrapes} scrapes over {duration:.0f}s "
        f"(avg {avg(tx_last):.0f} tx/block)",
        fontsize=14,
        fontweight="bold",
    )

    if metadata:
        meta_str = "  |  ".join(f"{k}={v}" for k, v in metadata.items())
        fig.text(0.5, 0.965, meta_str, ha="center", fontsize=10,
                 color="#555555", fontstyle="italic")

    def pl(ax, ys, color, **kw):
        ax.plot(ts, ys, color=color, linewidth=0.8, **kw)

    def style(ax, ylabel="", title=""):
        ax.set_xlabel("Time (s)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, alpha=0.3)

    ax = axes[0][0]
    pl(ax, tx_last, "#2196F3")
    a = avg(tx_last)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.0f}")
    style(ax, "Txs", "Txs per Block (latest)"); ax.legend()

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

    ax = axes[2][0]
    pl(ax, build_p50_ms, "#2196F3", label="p50")
    pl(ax, build_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Payload Build Duration (rolling)"); ax.legend()

    ax = axes[2][1]
    pl(ax, sr_p50_ms, "#9C27B0", label="p50")
    pl(ax, sr_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "State Root Duration (rolling)"); ax.legend()

    ax = axes[2][2]
    pl(ax, fin_p50_ms, "#607D8B", label="p50")
    pl(ax, fin_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Finalization Duration (rolling)"); ax.legend()

    ax = axes[3][0]
    pl(ax, total_tx_exec_p50_ms, "#FF5722", label="tx execution")
    pl(ax, state_setup_p50_ms, "#4CAF50", label="state setup")
    pl(ax, hashed_post_p50_ms, "#00BCD4", label="hashed post state")
    style(ax, "ms", "Build Time Breakdown (rolling p50)"); ax.legend()

    ax = axes[3][1]
    pl(ax, builds_delta, "#795548")
    a = avg(builds_delta)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.1f}")
    style(ax, "Count", "Builds per Scrape"); ax.legend()

    ax = axes[3][2]
    pl(ax, tx_p50_us, "#FF5722", label="p50")
    pl(ax, tx_p99_us, "#F44336", alpha=0.7, label="p99")
    style(ax, "µs", "Per-Tx Execution Duration (rolling)"); ax.legend()

    ax = axes[4][0]
    pl(ax, pool_pending, "#2196F3", label="pending")
    pl(ax, pool_basefee, "#FF9800", label="basefee")
    pl(ax, pool_queued, "#F44336", label="queued")
    style(ax, "Txs", "Txpool by Sub-pool"); ax.legend()

    ax = axes[4][1]
    pl(ax, fetch_p50_ms, "#009688", label="p50")
    style(ax, "ms", "Pool Fetch Duration (rolling)"); ax.legend()

    ax = axes[4][2]
    pl(ax, skip_nonce_delta, "#F44336", label="nonce_too_low")
    pl(ax, skip_invalid_delta, "#FF9800", label="invalid_tx")
    style(ax, "Count", "Skipped Txs per Scrape"); ax.legend()

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out_path = os.path.join(datadir, "bench_plots.png")
    plt.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\nScrapes: {n_scrapes}")
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
    print(f"Builds/scrape: {avg(builds_delta):.1f}")
    print(f"Avg RLP size: {avg(rlp_kb):.0f} KB")
    print(f"Skipped nonce_low: {skip_nonce[-1] if skip_nonce else 0}")
    print(f"Skipped invalid: {skip_invalid[-1] if skip_invalid else 0}")


def plot_replay(plt, report, rows, metadata, datadir):
    """Plot layout for replay mode (engine API / block validation metrics)."""
    E = "reth_consensus_engine_beacon"
    V = "reth_sync_block_validation"
    X = "reth_sync_execution"
    S = "reth_tree_root_sparse_trie"

    t0 = rows[0]["ts_ms"]
    ts = [(r["ts_ms"] - t0) / 1000.0 for r in rows]
    n_scrapes = len(rows)

    # ── Engine throughput ────────────────────────────────────────────
    blocks_sent = col(rows, "txgen_blocks_sent_total", int)
    blocks_ok = col(rows, "txgen_blocks_success_total", int)
    blocks_fail = col(rows, "txgen_blocks_failed_total", int)
    blocks_sent_delta = delta(blocks_sent)
    blocks_ok_delta = delta(blocks_ok)

    gas_per_sec = col(rows, f"{E}_new_payload_gas_per_second_last")
    ggas_s = [g / 1e9 for g in gas_per_sec]

    gas_processed = col(rows, f"{X}_gas_processed_total", int)

    # ── Engine latency ───────────────────────────────────────────────
    np_latency_p50 = col(rows, f"{E}_new_payload_latency", quantile="0.5")
    np_latency_p99 = col(rows, f"{E}_new_payload_latency", quantile="0.99")
    fcu_latency_p50 = col(rows, f"{E}_forkchoice_updated_latency", quantile="0.5")
    fcu_latency_p99 = col(rows, f"{E}_forkchoice_updated_latency", quantile="0.99")

    # ── Block validation ─────────────────────────────────────────────
    validation_p50 = col(rows, f"{V}_payload_validation_histogram", quantile="0.5")
    validation_p99 = col(rows, f"{V}_payload_validation_histogram", quantile="0.99")
    state_root_p50 = col(rows, f"{V}_state_root_histogram", quantile="0.5")
    state_root_p99 = col(rows, f"{V}_state_root_histogram", quantile="0.99")

    # ── Execution ────────────────────────────────────────────────────
    exec_p50 = col(rows, f"{X}_execution_histogram", quantile="0.5")
    exec_p99 = col(rows, f"{X}_execution_histogram", quantile="0.99")
    exec_gps = col(rows, f"{X}_gas_per_second")

    # ── Persistence ──────────────────────────────────────────────────
    persist_p50 = col(rows, f"{E}_persistence_duration", quantile="0.5")
    persist_p99 = col(rows, f"{E}_persistence_duration", quantile="0.99")

    # ── Sparse trie ──────────────────────────────────────────────────
    sparse_p50 = col(rows, f"{S}_total_duration_histogram", quantile="0.5")
    sparse_p99 = col(rows, f"{S}_total_duration_histogram", quantile="0.99")
    sparse_mem = col(rows, f"{S}_retained_memory_bytes", int)

    # ── Engine status ────────────────────────────────────────────────
    np_valid = col(rows, f"{E}_new_payload_valid", int)
    np_invalid = col(rows, f"{E}_new_payload_invalid", int)
    np_error = col(rows, f"{E}_new_payload_error", int)
    fcu_valid = col(rows, f"{E}_forkchoice_updated_valid", int)
    fcu_invalid = col(rows, f"{E}_forkchoice_updated_invalid", int)
    fcu_error = col(rows, f"{E}_forkchoice_updated_error", int)

    # ── Memory ───────────────────────────────────────────────────────
    jemalloc_resident = col(rows, "reth_jemalloc_resident", int)
    jemalloc_allocated = col(rows, "reth_jemalloc_allocated", int)

    # ── Derived columns ──────────────────────────────────────────────
    np_p50_ms = [v * 1000 for v in np_latency_p50]
    np_p99_ms = [v * 1000 for v in np_latency_p99]
    fcu_p50_ms = [v * 1000 for v in fcu_latency_p50]
    fcu_p99_ms = [v * 1000 for v in fcu_latency_p99]
    val_p50_ms = [v * 1000 for v in validation_p50]
    val_p99_ms = [v * 1000 for v in validation_p99]
    sr_p50_ms = [v * 1000 for v in state_root_p50]
    sr_p99_ms = [v * 1000 for v in state_root_p99]
    exec_p50_ms = [v * 1000 for v in exec_p50]
    exec_p99_ms = [v * 1000 for v in exec_p99]
    exec_ggas_s = [g / 1e9 for g in exec_gps]
    persist_p50_ms = [v * 1000 for v in persist_p50]
    persist_p99_ms = [v * 1000 for v in persist_p99]
    sparse_p50_ms = [v * 1000 for v in sparse_p50]
    sparse_p99_ms = [v * 1000 for v in sparse_p99]
    sparse_mem_kb = [v / 1024 for v in sparse_mem]
    resident_mb = [v / (1024 * 1024) for v in jemalloc_resident]
    allocated_mb = [v / (1024 * 1024) for v in jemalloc_allocated]
    gas_processed_rebase = [g - gas_processed[0] for g in gas_processed]
    np_valid_d = delta(np_valid)
    np_invalid_d = delta(np_invalid)
    np_error_d = delta(np_error)

    # ── Plot ─────────────────────────────────────────────────────────
    fig, axes = plt.subplots(5, 3, figsize=(20, 25))
    duration = ts[-1] - ts[0]

    total_blocks = blocks_ok[-1] if blocks_ok else 0
    bps = total_blocks / duration if duration > 0 else 0

    fig.suptitle(
        f"Replay Bench: {total_blocks} blocks over {duration:.0f}s "
        f"({bps:.1f} blocks/s)",
        fontsize=14,
        fontweight="bold",
    )

    if metadata:
        meta_str = "  |  ".join(f"{k}={v}" for k, v in metadata.items())
        fig.text(0.5, 0.965, meta_str, ha="center", fontsize=10,
                 color="#555555", fontstyle="italic")

    def pl(ax, ys, color, **kw):
        ax.plot(ts, ys, color=color, linewidth=0.8, **kw)

    def style(ax, ylabel="", title=""):
        ax.set_xlabel("Time (s)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, alpha=0.3)

    # ── Row 1: Throughput ────────────────────────────────────────────
    ax = axes[0][0]
    pl(ax, blocks_ok_delta, "#2196F3", label="success")
    pl(ax, blocks_sent_delta, "#FF9800", alpha=0.5, label="sent")
    a = avg(blocks_ok_delta)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"avg={a:.1f}")
    style(ax, "Blocks", "Blocks per Scrape"); ax.legend()

    ax = axes[0][1]
    pl(ax, ggas_s, "#FF9800")
    a = steady_avg(ts, ggas_s)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"steady={a:.2f}")
    style(ax, "Ggas/s", "Engine Gas Throughput"); ax.legend()

    ax = axes[0][2]
    pl(ax, gas_processed_rebase, "#4CAF50")
    style(ax, "Gas", "Cumulative Gas Processed")

    # ── Row 2: Engine Latency ────────────────────────────────────────
    ax = axes[1][0]
    pl(ax, np_p50_ms, "#2196F3", label="p50")
    pl(ax, np_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "newPayload Latency (rolling)"); ax.legend()

    ax = axes[1][1]
    pl(ax, fcu_p50_ms, "#9C27B0", label="p50")
    pl(ax, fcu_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "forkchoiceUpdated Latency (rolling)"); ax.legend()

    ax = axes[1][2]
    pl(ax, val_p50_ms, "#607D8B", label="p50")
    pl(ax, val_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Payload Validation Duration (rolling)"); ax.legend()

    # ── Row 3: Execution & State Root ────────────────────────────────
    ax = axes[2][0]
    pl(ax, exec_p50_ms, "#FF5722", label="p50")
    pl(ax, exec_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Block Execution Duration (rolling)"); ax.legend()

    ax = axes[2][1]
    pl(ax, sr_p50_ms, "#9C27B0", label="p50")
    pl(ax, sr_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "State Root Duration (rolling)"); ax.legend()

    ax = axes[2][2]
    pl(ax, exec_ggas_s, "#4CAF50")
    a = steady_avg(ts, exec_ggas_s)
    ax.axhline(y=a, color="red", linestyle="--", alpha=0.5, label=f"steady={a:.2f}")
    style(ax, "Ggas/s", "Execution Gas Throughput"); ax.legend()

    # ── Row 4: Persistence & Sparse Trie ─────────────────────────────
    ax = axes[3][0]
    pl(ax, persist_p50_ms, "#795548", label="p50")
    pl(ax, persist_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Persistence Duration (rolling)"); ax.legend()

    ax = axes[3][1]
    pl(ax, sparse_p50_ms, "#00BCD4", label="p50")
    pl(ax, sparse_p99_ms, "#F44336", alpha=0.7, label="p99")
    style(ax, "ms", "Sparse Trie Duration (rolling)"); ax.legend()

    ax = axes[3][2]
    pl(ax, sparse_mem_kb, "#3F51B5")
    style(ax, "KB", "Sparse Trie Retained Memory")

    # ── Row 5: Engine Status & Memory ────────────────────────────────
    ax = axes[4][0]
    pl(ax, np_valid_d, "#4CAF50", label="valid")
    pl(ax, np_invalid_d, "#F44336", label="invalid")
    pl(ax, np_error_d, "#FF9800", label="error")
    style(ax, "Count", "newPayload Status per Scrape"); ax.legend()

    ax = axes[4][1]
    pl(ax, resident_mb, "#E91E63", label="resident")
    pl(ax, allocated_mb, "#9C27B0", alpha=0.7, label="allocated")
    style(ax, "MB", "Memory (jemalloc)"); ax.legend()

    ax = axes[4][2]
    pl(ax, blocks_ok, "#4CAF50", label="success")
    pl(ax, blocks_fail, "#F44336", label="failed")
    style(ax, "Blocks", "Cumulative Blocks"); ax.legend()

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out_path = os.path.join(datadir, "bench_plots.png")
    plt.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\nScrapes: {n_scrapes}")
    print(f"Time range: {ts[0]:.1f}s – {ts[-1]:.1f}s ({duration:.1f}s)")
    print(f"Total blocks: {total_blocks}")
    print(f"Blocks/s: {bps:.1f}")
    print(f"Steady Ggas/s (engine): {steady_avg(ts, ggas_s):.2f}")
    print(f"Steady Ggas/s (exec):   {steady_avg(ts, exec_ggas_s):.2f}")
    print(f"newPayload  p50={avg(np_p50_ms):.2f}ms  p99={avg(np_p99_ms):.2f}ms")
    print(f"FCU         p50={avg(fcu_p50_ms):.2f}ms  p99={avg(fcu_p99_ms):.2f}ms")
    print(f"Validation  p50={avg(val_p50_ms):.2f}ms  p99={avg(val_p99_ms):.2f}ms")
    print(f"Execution   p50={avg(exec_p50_ms):.2f}ms  p99={avg(exec_p99_ms):.2f}ms")
    print(f"State root  p50={avg(sr_p50_ms):.2f}ms  p99={avg(sr_p99_ms):.2f}ms")
    print(f"Persistence p50={avg(persist_p50_ms):.2f}ms  p99={avg(persist_p99_ms):.2f}ms")
    print(f"Sparse trie p50={avg(sparse_p50_ms):.2f}ms  p99={avg(sparse_p99_ms):.2f}ms")
    print(f"Sparse trie mem: {avg(sparse_mem_kb):.1f} KB")
    print(f"Memory resident: {avg(resident_mb):.0f} MB")
    print(f"Failed blocks: {blocks_fail[-1] if blocks_fail else 0}")
    print(f"Invalid payloads: {np_invalid[-1] if np_invalid else 0}")
    print(f"Payload errors: {np_error[-1] if np_error else 0}")


if __name__ == "__main__":
    main()
