//! The roost (RFC-0012 §1–3): one runtime hosting many nests on the same chain. This slice (§ plan 1)
//! lands the **layout + serving surface** — a `roost.toml` naming the chain and the mounted nests, a
//! `/nests` roster, and every nest's full API under a `/<name>/…` prefix — while each nest still runs
//! its **own** cursor (the naive step). Collapsing to one shared cursor per chain is slice 2; doing
//! routing + per-nest isolation first means slice 2 has a known-good baseline to prove byte-identity
//! against.
//!
//! Isolation is by construction: each nest keeps its own directory (`nests/<name>/` — its own
//! `nuthatch.redb`, `segments/`, views), so one nest's bad view or runaway factory can't touch
//! another's data (the CLAUDE.md non-negotiable). The roost only shares the *chain identity* and, from
//! slice 2, the *cursor* — never the stores.

use crate::config::Config;
use crate::indexer::{self, NestRuntime};
use crate::rpc::{self, RpcClient};
use crate::source::Source;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The roost manifest file, at the roost directory root. Sibling of a nest's `nuthatch.toml`.
pub const ROOST_FILE: &str = "roost.toml";

/// Where mounted nests live under the roost dir: `nests/<name>/` is a nest directory, exactly as a
/// standalone nest is today.
pub const NESTS_DIR: &str = "nests";

/// A roost manifest: the shared chain identity plus the list of mounted nests. Chain identity is
/// hoisted **here**, above the per-nest configs, because it is what the shared cursor is keyed on — a
/// roost is one chain by definition (a second chain is a second cursor is a second process).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Roost {
    pub roost: RoostMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoostMeta {
    /// Human name for the roost (logging/roster only).
    pub name: String,
    /// The shared chain the cursor follows. Every mounted nest's `[nest].chain` must equal this.
    pub chain: String,
    /// The shared chain id. Every mounted nest's `[nest].chain_id` must equal this.
    pub chain_id: u64,
    /// RPC endpoints for the shared chain (a nest's own `rpc_urls` are ignored in a roost — the roost
    /// owns the chain connection). Overridable at runtime with `--rpc`.
    pub rpc_urls: Vec<String>,
    /// The mounted nests, by directory name under `nests/`. (A future slice resolves blob hashes here
    /// via `nest mount`; slice 1 takes plain directory names already present on disk.)
    pub nests: Vec<String>,
}

impl Roost {
    /// Load and validate `roost.toml` from a roost directory.
    pub fn load(dir: &Path) -> Result<Roost> {
        let path = dir.join(ROOST_FILE);
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("no {ROOST_FILE} in {}", dir.display()))?;
        let roost: Roost =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        if roost.roost.nests.is_empty() {
            bail!(
                "roost '{}' mounts no nests (empty `nests` list)",
                roost.roost.name
            );
        }
        // Reject duplicate mounts and any name that would collide with a reserved top-level route
        // (`/nests`, `/health`) — the roster and per-nest prefixes share one path namespace.
        let mut seen = std::collections::HashSet::new();
        for n in &roost.roost.nests {
            if n == "nests" || n == "health" {
                bail!("nest name '{n}' is reserved (collides with a roost route)");
            }
            if !seen.insert(n) {
                bail!("nest '{n}' is mounted more than once");
            }
        }
        Ok(roost)
    }

    /// The on-disk directory of a mounted nest, relative to the roost dir.
    pub fn nest_dir(dir: &Path, name: &str) -> PathBuf {
        dir.join(NESTS_DIR).join(name)
    }
}

/// Load a mounted nest's config and assert it belongs to this roost's chain. A chain (or chain-id)
/// mismatch is a hard error: a different chain needs its own roost (its own cursor), so co-mounting it
/// here would silently break the single-cursor model in slice 2.
fn load_mounted_nest(roost_dir: &Path, roost: &RoostMeta, name: &str) -> Result<(PathBuf, Config)> {
    let dir = Roost::nest_dir(roost_dir, name);
    let config = Config::load(&dir)
        .with_context(|| format!("loading mounted nest '{name}' from {}", dir.display()))?;
    if config.nest.chain != roost.chain || config.nest.chain_id != roost.chain_id {
        bail!(
            "nest '{name}' is on {} (chain_id {}) but this roost is {} (chain_id {}) — a different \
             chain needs its own roost",
            config.nest.chain,
            config.nest.chain_id,
            roost.chain,
            roost.chain_id
        );
    }
    Ok((dir, config))
}

/// `nuthatch roost dev <dir>`: bring up every mounted nest and serve them behind one listener.
///
/// Slice 1 is deliberately naive on ingestion — one `Source` (cursor) per nest — to land the serving
/// surface and per-nest isolation before the shared-cursor collapse (slice 2). The process shares a
/// fate with all of its nests: if any nest's ingestion dies, the whole roost exits non-zero rather than
/// serve one nest's stale data as if healthy (the single-failure-boundary rule, generalised).
#[allow(clippy::too_many_arguments)]
pub async fn dev(
    dir: PathBuf,
    listen: String,
    rpc_override: Vec<String>,
    backfill: Option<u64>,
    seal_direct: bool,
    concurrency: usize,
    window_override: Option<u64>,
    no_admin: bool,
) -> Result<()> {
    let roost = Roost::load(&dir)?;
    let meta = &roost.roost;
    tracing::info!(
        "roost '{}' on {} (chain_id {}): mounting {} nest(s) — one cursor each (slice 1)",
        meta.name,
        meta.chain,
        meta.chain_id,
        meta.nests.len(),
    );

    // The roost owns the chain connection: `--rpc` overrides win, else the roost's configured
    // endpoints. A per-nest source is built from the same URLs (slice 2 collapses these to one cursor).
    let rpc_urls = rpc::merge_rpcs(&rpc_override, meta.rpc_urls.clone());
    if rpc_urls.is_empty() {
        bail!(
            "roost '{}' has no rpc_urls (set them in {ROOST_FILE} or pass --rpc)",
            meta.name
        );
    }
    let endpoint_count = rpc_urls.len();
    let concurrency = indexer::safe_backfill_concurrency(endpoint_count, concurrency);
    let admin_enabled = indexer::admin_enabled(no_admin, &listen);

    // Bring up each nest. A failure to mount any nest fails the whole roost — better a loud refusal at
    // startup than a roost silently serving a subset of what the operator asked for.
    let mut ingests = Vec::new();
    let mut alert_workers = Vec::new();
    let mut states = Vec::new();
    let mut roster = Vec::new();
    for name in &meta.nests {
        let (nest_path, config) = load_mounted_nest(&dir, meta, name)?;
        let source: Arc<dyn Source> = Arc::new(RpcClient::new(rpc_urls.clone())?);
        let rt: NestRuntime = indexer::spawn_nest(
            source,
            nest_path,
            config,
            backfill,
            seal_direct,
            concurrency,
            window_override,
            admin_enabled,
        )
        .await
        .with_context(|| format!("bringing up nest '{name}'"))?;
        roster.push(serde_json::json!({
            "name": name,
            "chain": rt.state.chain,
            "registry_hash": rt.state.nest_info.get("registry_hash").cloned().unwrap_or_default(),
            "table_count": rt.state.tables.len(),
            "base_path": format!("/{name}"),
        }));
        ingests.push(rt.ingest);
        if let Some(w) = rt.alert_worker {
            alert_workers.push(w);
        }
        states.push((name.clone(), rt.state));
    }

    let roster = serde_json::json!({ "roost": meta.name, "chain": meta.chain, "nests": roster });

    // Fate-share the server with every nest's ingestion. `select_all` resolves as soon as the *first*
    // ingest task ends — a clean shutdown returns Ok, an error/panic propagates as the process exit.
    let mut ingest_set = ingests;
    let result = tokio::select! {
        r = crate::serve::run_roost(&listen, roster, states) => r,
        (joined, idx, _rest) = futures::future::select_all(&mut ingest_set) => match joined {
            Ok(inner) => inner,
            Err(e) if e.is_panic() => Err(anyhow::anyhow!("nest ingestion loop #{idx} panicked")),
            Err(e) => Err(anyhow::anyhow!("nest ingestion loop #{idx} failed: {e}")),
        },
    };
    for h in &ingest_set {
        h.abort();
    }
    for w in &alert_workers {
        w.abort();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CONFIG_FILE;

    /// Write a minimal roost.toml + one nest dir on the given chain.
    fn write_roost(dir: &Path, chain: &str, chain_id: u64, nest_chain: &str, nest_chain_id: u64) {
        std::fs::write(
            dir.join(ROOST_FILE),
            format!(
                "[roost]\nname = \"test\"\nchain = \"{chain}\"\nchain_id = {chain_id}\n\
                 rpc_urls = [\"http://localhost:8545\"]\nnests = [\"a\"]\n"
            ),
        )
        .unwrap();
        let nest = Roost::nest_dir(dir, "a");
        std::fs::create_dir_all(&nest).unwrap();
        std::fs::write(
            nest.join(CONFIG_FILE),
            format!(
                "[nest]\nname = \"a\"\nchain = \"{nest_chain}\"\nchain_id = {nest_chain_id}\n\
                 rpc_urls = []\n\n[[contracts]]\nalias = \"t\"\naddress = \"0x0000000000000000000000000000000000000001\"\nabi = \"abi.json\"\n"
            ),
        )
        .unwrap();
        // A trivially-valid ABI so Config::load's downstream users don't choke (load itself doesn't read it).
        std::fs::write(nest.join("abi.json"), "[]").unwrap();
    }

    #[test]
    fn loads_a_valid_roost() {
        let d = tempfile::tempdir().unwrap();
        write_roost(d.path(), "arbitrum-one", 42161, "arbitrum-one", 42161);
        let r = Roost::load(d.path()).unwrap();
        assert_eq!(r.roost.chain, "arbitrum-one");
        assert_eq!(r.roost.nests, vec!["a"]);
    }

    #[test]
    fn rejects_a_nest_on_the_wrong_chain() {
        let d = tempfile::tempdir().unwrap();
        // Roost says arbitrum-one; the nest claims mainnet → hard error.
        write_roost(d.path(), "arbitrum-one", 42161, "mainnet", 1);
        let roost = Roost::load(d.path()).unwrap();
        let err = load_mounted_nest(d.path(), &roost.roost, "a")
            .unwrap_err()
            .to_string();
        assert!(err.contains("needs its own roost"), "got: {err}");
    }

    #[test]
    fn rejects_reserved_and_duplicate_nest_names() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(ROOST_FILE),
            "[roost]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = [\"nests\"]\n",
        )
        .unwrap();
        assert!(Roost::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("reserved"));

        std::fs::write(
            d.path().join(ROOST_FILE),
            "[roost]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = [\"a\", \"a\"]\n",
        )
        .unwrap();
        assert!(Roost::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("more than once"));
    }

    #[test]
    fn rejects_an_empty_nest_list() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(ROOST_FILE),
            "[roost]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = []\n",
        )
        .unwrap();
        assert!(Roost::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("no nests"));
    }
}
