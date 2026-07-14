//! Incremental view maintenance (the IVM core) via DBSP. The first declarative view: per-address
//! token balances, derived from Transfer facts. This is the differentiator vs imperative indexers —
//! we never hand-write "on transfer, load balance, add, save". We state balance = Σ(in) − Σ(out) as
//! a circuit, and DBSP maintains it incrementally: a new transfer is a +1 delta, and a reorg is the
//! *same* transfer re-fed with weight −1 (a retraction). Backfill and tip use the identical circuit.
//!
//! Balances are accumulated in i64 base units (fine for USDC-class tokens; values that don't fit i64
//! are skipped by the caller). A future slice generalises the accumulator.

use anyhow::{anyhow, Context, Result};
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};

/// A balance delta fed to the circuit: (address, signed_value). Insert carries weight +1, a reorg
/// retraction carries weight −1.
type Delta = Tup2<String, i64>;
type WeightedBatch = Vec<Tup2<Delta, i64>>;

/// The two deltas a single transfer contributes: +value to `to`, −value to `from`.
pub fn transfer_deltas(from: &str, to: &str, value: i64, weight: i64) -> WeightedBatch {
    vec![
        Tup2(Tup2(to.to_string(), value), weight),
        Tup2(Tup2(from.to_string(), -value), weight),
    ]
}

/// Owns the DBSP circuit. `step` applies a weighted batch and folds the resulting changes into
/// `balances`. Kept separate from the threading so it can be driven deterministically in tests.
struct BalanceCircuit {
    circuit: dbsp::DBSPHandle,
    input: dbsp::ZSetHandle<Delta>,
    output: OutputHandle<OrdZSet<Tup2<String, i64>>>,
}

impl BalanceCircuit {
    fn new() -> Result<Self> {
        let (circuit, (input, output)) = Runtime::init_circuit(1, build_circuit)
            .map_err(|e| anyhow!("failed to build IVM circuit: {e}"))?;
        Ok(Self { circuit, input, output })
    }

    /// Feed a batch, advance the circuit one transaction, and apply the emitted changes.
    fn step(&mut self, mut batch: WeightedBatch, balances: &mut HashMap<String, i64>) -> Result<()> {
        self.input.append(&mut batch);
        self.circuit.transaction().map_err(|e| anyhow!("IVM transaction: {e}"))?;

        // The aggregate emits a change stream: a key whose balance moves from old→new appears as
        // (key,old,−1) and (key,new,+1); a key falling to zero appears only as (key,old,−1).
        let changes = self.output.consolidate();
        let mut set: HashMap<String, i64> = HashMap::new();
        let mut cleared: Vec<String> = Vec::new();
        changes.iter().for_each(|(rec, (), weight): (Tup2<String, i64>, (), i64)| {
            if weight > 0 {
                set.insert(rec.0.clone(), rec.1);
            } else if weight < 0 {
                cleared.push(rec.0);
            }
        });
        for (k, v) in set.iter() {
            balances.insert(k.clone(), *v);
        }
        for k in cleared {
            if !set.contains_key(&k) {
                balances.remove(&k); // balance returned to zero → drop the key
            }
        }
        Ok(())
    }
}

fn build_circuit(
    circuit: &mut RootCircuit,
) -> Result<(dbsp::ZSetHandle<Delta>, OutputHandle<OrdZSet<Tup2<String, i64>>>), anyhow::Error> {
    let (stream, handle) = circuit.add_input_zset::<Delta>();
    // Sum the signed values per address. aggregate_linear is the incremental Σ; the Z-set weight
    // carries insert/retract, so a retraction subtracts automatically.
    let balances = stream
        .map_index(|d: &Delta| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i64| *v);
    let out = balances.map(|(addr, bal): (&String, &i64)| Tup2(addr.clone(), *bal));
    Ok((handle, out.output()))
}

/// Cheap-to-clone handle to the live balance view. Sends batches to the circuit thread; reads the
/// maintained balances from shared state.
#[derive(Clone)]
pub struct BalanceView {
    tx: Sender<WeightedBatch>,
    balances: Arc<RwLock<HashMap<String, i64>>>,
}

impl BalanceView {
    /// Spawn the circuit on its own thread (DBSP drives worker threads; keep it off the async pool).
    pub fn start() -> Result<Self> {
        let (tx, rx) = channel::<WeightedBatch>();
        let balances = Arc::new(RwLock::new(HashMap::new()));
        let shared = balances.clone();
        std::thread::Builder::new()
            .name("nuthatch-ivm".into())
            .spawn(move || {
                let mut circuit = match BalanceCircuit::new() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("IVM circuit failed to start: {e:#}");
                        return;
                    }
                };
                while let Ok(batch) = rx.recv() {
                    let mut map = shared.write().unwrap();
                    if let Err(e) = circuit.step(batch, &mut map) {
                        tracing::error!("IVM step failed: {e:#}");
                        break;
                    }
                }
            })
            .context("failed to spawn IVM thread")?;
        Ok(Self { tx, balances })
    }

    /// Enqueue a weighted batch (built via `transfer_deltas`). Non-blocking; drops silently if the
    /// circuit thread has died (already logged there).
    pub fn apply(&self, batch: WeightedBatch) {
        if !batch.is_empty() {
            let _ = self.tx.send(batch);
        }
    }

    pub fn balance(&self, address: &str) -> Option<i64> {
        self.balances.read().ok()?.get(address).copied()
    }

    /// Top `n` addresses by balance, descending.
    pub fn top(&self, n: usize) -> Vec<(String, i64)> {
        let map = match self.balances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        let mut v: Vec<(String, i64)> = map.iter().map(|(k, val)| (k.clone(), *val)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(n);
        v
    }

    pub fn holders(&self) -> usize {
        self.balances.read().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic IVM golden test: insert transfers, check derived balances; then retract one
    /// (a reorg) and check the view converges to the state as if that transfer never happened.
    #[test]
    fn balances_are_maintained_incrementally_with_retraction() {
        let mut circuit = BalanceCircuit::new().unwrap();
        let mut bal = HashMap::new();

        // alice→bob 100, then bob→carol 30.
        circuit.step(transfer_deltas("alice", "bob", 100, 1), &mut bal).unwrap();
        circuit.step(transfer_deltas("bob", "carol", 30, 1), &mut bal).unwrap();
        assert_eq!(bal.get("alice"), Some(&-100));
        assert_eq!(bal.get("bob"), Some(&70));
        assert_eq!(bal.get("carol"), Some(&30));

        // Reorg: retract bob→carol 30. carol returns to zero (key dropped); bob back to −100+...
        circuit.step(transfer_deltas("bob", "carol", 30, -1), &mut bal).unwrap();
        assert_eq!(bal.get("alice"), Some(&-100));
        assert_eq!(bal.get("bob"), Some(&100));
        assert_eq!(bal.get("carol"), None, "zero balance must drop the key");
    }
}
