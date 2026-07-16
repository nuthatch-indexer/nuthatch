# Backfill benchmarks (RFC-0004)

**House rule:** every performance number nuthatch publishes traces to a `bench-report.json`
produced by `nuthatch bench backfill` — with date, provider, hardware, and commit. No hand-typed
numbers, including flattering ones. This page documents the harness and the pinned workloads; the
baseline matrix is filled in from real runs (an archive node is needed for the historical ranges).

## The harness

```sh
nuthatch bench backfill --dir <nest> --from <block> --to <block> [--runs 3] [--rpc <url>] [--out report.json]
```

Runs the real **fetch → decode → store** path over a pinned block range and reports the **median**
across runs of:

- **events/sec** — total decoded events ÷ wall-clock (excluding init). The headline.
- **wall-clock (s)**, **peak RSS (MB)**, **RPC requests** (including failover retries).

It writes to a throwaway store per run (never the nest's own DB), so runs are independent and
repeatable. `--rpc` overrides the nest's endpoints — point it at your own node for a T2 run.

Nothing here optimises anything. It exists so the seal-direct / adaptive-chunker / pipeline work
(later RFC-0004 slices) is each gated on a measured before/after, not a wish.

## Workloads (pinned, public, reproducible)

| ID | Nest | Range | Character |
|----|------|-------|-----------|
| W1 | USDC (mainnet) | 100,000 blocks ending 21,400,000 | dense single-contract (~1.3M events) |
| W2 | [horizon-nest](https://github.com/cargopete/horizon-nest) (Arbitrum) | full history from deployment | sparse multi-contract, L2 cadence |
| W3 | USDC + WETH + Uniswap V3 factory (mainnet) | 50,000 blocks | mixed density, multi-table fan-out |

## Sourcing tiers

- **T1** — public RPC defaults (round-robin), as a new user experiences it.
- **T2** — your own node (localhost `eth_getLogs`), via `--rpc`.
- **T3** — a caching proxy (e.g. erpc) in front of T1.

W1/W3 historical ranges and W2's full history require an **archive** node (public sequencer/
non-archive endpoints don't serve old `eth_getLogs`). Public-RPC (T1) numbers are noisy — take the
median of three runs, date them, and read them as "what a new user should expect," not as the
product's capability.

## Baseline (pre-optimization)

_Pending — populated from archive-node runs. A representative smoke run (USDC, 201 recent blocks,
public RPC, single-threaded): ~400 events/sec, ~47 MB peak, latency-bound on sequential `getLogs`.
The pipeline and seal-direct slices target this number directly._
