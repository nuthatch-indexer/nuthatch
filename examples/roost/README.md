# Example roost: two nests, one chain, one runtime

Two nests — the **ARB** token and native **USDC**, both on Arbitrum One — hosted in a single
[roost](../../docs/rfcs/0012-multi-nest-runtime-and-nest-packaging.md): one process, one cursor, one
`getLogs` per block window fanned out to both. Two nests cost roughly one nest's worth of RPC chatter,
and each nest's tables are byte-identical to running it solo with `nuthatch dev`.

```
examples/roost/
  roost.toml            # chain + rpc_urls + the mounted-nest list
  nests/
    arb/                # a nest dir, exactly as `nuthatch init` produces one
      nuthatch.toml
      abis/erc20.json
    usdc/
      nuthatch.toml
      abis/erc20.json
```

## Run it

Point it at an Arbitrum RPC (yours, or a public one) and follow the tip:

```sh
nuthatch roost dev --dir examples/roost \
  --rpc https://arb1.your-node.example \
  --backfill 5000 \
  --listen 127.0.0.1:8288
```

`--backfill 5000` indexes the last ~5000 blocks so there's data in seconds; drop it to backfill each
nest from deployment. On startup you'll see the footprint projection and, if it fits the budget, both
nests come up under one cursor.

## Talk to it

```sh
curl localhost:8288/nests            # the roster: both nests + per-nest & roost RSS
curl localhost:8288/arb/tables       # the ARB nest's schema
curl localhost:8288/arb/entities     # recent ARB transfers
curl 'localhost:8288/usdc/sql?q=SELECT%20count(*)%20FROM%20usdc__transfer'
```

Every nest's full API lives under its `/<name>/…` prefix; `/sql` is per-nest scoped (a query sees one
nest's data). Each nest keeps its own store under `nests/<name>/` — the cursor is shared, the data is
not.

## Distributing a nest as a blob

A nest can be packaged as a content-addressed **blob** (its authored inputs pinned by hash) and
installed elsewhere:

```sh
nuthatch nest pack examples/roost/nests/arb        # prints the blob hash
nuthatch nest mount arb-<hash>.nest --dir ./arb    # verifies + installs a runnable nest
```

`mount` regenerates the decode registry from the blob's inputs and asserts it matches the manifest —
so a mounted nest decodes exactly as its author's did. See the RFC's §4–5 for the blob format and the
local-first (no-phone-home) resolution.

## Notes

- Both nests must be on the roost's chain (`[nest].chain`/`chain_id` == the roost's); a mismatch is
  refused at startup — a different chain needs its own roost.
- Static and factory nests can be co-mounted; nests may mount at different heights and each backfills
  its own history — the cursor only couples them at the tip.
- The footprint budget is per-*runtime*. Set `max_rss_mb` in `roost.toml` to cap it; a mount projected
  over the ceiling is refused before it starts.
