//! `nuthatch init` — the first 30 seconds a user judges us on. Resolve the ABI, write a project.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;

use crate::abi;
use crate::chains;
use crate::cli::InitArgs;
use crate::config::{Config, ABI_FILE};

pub async fn init(args: InitArgs) -> Result<()> {
    let address = normalise_address(&args.address)?;
    let chain = chains::lookup(&args.chain)
        .with_context(|| format!("unknown chain '{}' (try: mainnet)", args.chain))?;
    let dir = PathBuf::from(&args.dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;

    println!("→ resolving ABI for {address} on {}…", chain.name);
    let abi = abi::resolve(chain.chain_id, &address).await?;

    if !abi::has_transfer_event(&abi) {
        // Not necessarily a problem: the skeleton decodes Transfer by topic0, not from the ABI,
        // so proxies (whose resolved ABI is the proxy's, not the token's) still index fine.
        println!(
            "· note: this ABI declares no `Transfer` event (often a proxy). The skeleton decodes \
             Transfer by topic0 regardless, so `nuthatch dev` will still index transfers at this address."
        );
    }

    std::fs::write(
        dir.join(ABI_FILE),
        serde_json::to_string_pretty(&abi).context("failed to serialise ABI")?,
    )
    .context("failed to write abi.json")?;

    let config = Config {
        chain: chain.name.to_string(),
        chain_id: chain.chain_id,
        address: address.clone(),
        rpc_urls: chain.rpc_urls.iter().map(|s| s.to_string()).collect(),
        event: "Transfer".to_string(),
    };
    config.save(&dir)?;
    scaffold_ai_surface(&dir, chain.name, &address)?;

    println!("✓ scaffolded nuthatch project in {}", dir.display());
    println!("    nuthatch.toml              config");
    println!("    abi.json                   resolved ABI");
    println!("    llms.txt                   how an AI agent queries this index");
    println!("    .claude/skills/nuthatch/   Claude Code skill (offline, no phone-home)");
    println!();
    println!("next:  nuthatch dev{}", dir_hint(&args.dir));
    println!("       nuthatch mcp   (expose this index to a coding agent over MCP)");
    Ok(())
}

/// Scaffold the AI-native surface into the project: an `llms.txt` and a Claude Code skill. These are
/// static docs — no network, no keys — so any agent learns the real query surface instead of
/// hallucinating one.
fn scaffold_ai_surface(dir: &std::path::Path, chain: &str, address: &str) -> Result<()> {
    let llms = format!(
        "# nuthatch — {address} on {chain}\n\
         \n\
         A self-hosted blockchain index. Query it locally; there is no third-party API.\n\
         \n\
         ## Live HTTP API (run `nuthatch dev`)\n\
         - `GET /`                    index status\n\
         - `GET /entities?limit=N`    recent transfers\n\
         - `GET /entity/{{id}}`         one transfer by id (`{{block:012}}-{{logindex:06}}`)\n\
         - `GET /sql?q=SELECT...`     read-only SQL over sealed transfers (a `transfers` view)\n\
         - `GET /balances?limit=N`    top holder balances (incrementally maintained)\n\
         - `GET /balance/{{address}}`   one address's derived balance\n\
         \n\
         ## MCP (for coding agents)\n\
         Run `nuthatch mcp` (stdio) to expose tools: status, schema, sql, entity, balance,\n\
         top_balances. Fully offline against the local instance; nothing phones home.\n"
    );
    std::fs::write(dir.join("llms.txt"), llms).context("failed to write llms.txt")?;

    let skill_dir = dir.join(".claude/skills/nuthatch");
    std::fs::create_dir_all(&skill_dir).context("failed to create skill dir")?;
    let skill = format!(
        "---\n\
         name: nuthatch\n\
         description: Query this self-hosted nuthatch blockchain index ({address} on {chain}) — \
         transfers, balances, and read-only SQL. Use when asked about on-chain transfers, holders, \
         or balances for this contract.\n\
         ---\n\
         \n\
         # Querying the nuthatch index\n\
         \n\
         This project indexes contract `{address}` on {chain}. Data is local — never call an\n\
         external API for it.\n\
         \n\
         ## Preferred: MCP\n\
         If a `nuthatch` MCP server is configured, use its tools. Call `schema` first to learn the\n\
         data model, then `sql` / `entity` / `balance` / `top_balances`.\n\
         \n\
         ## Fallback: HTTP (a `nuthatch dev` must be running)\n\
         - Recent transfers:   `curl localhost:8288/entities?limit=20`\n\
         - Read-only SQL:      `curl -G localhost:8288/sql --data-urlencode 'q=SELECT count(*) FROM transfers'`\n\
         - Address balance:    `curl localhost:8288/balance/0x...`\n\
         - Top holders:        `curl localhost:8288/balances?limit=10`\n\
         \n\
         The `transfers` SQL view has columns: block_number, log_index, from, to, value, value_hex,\n\
         tx_hash. `sql` sees finalized data only; balances/entity cover the live tip.\n"
    );
    std::fs::write(skill_dir.join("SKILL.md"), skill).context("failed to write SKILL.md")?;
    Ok(())
}

fn dir_hint(dir: &str) -> String {
    if dir == "." {
        String::new()
    } else {
        format!(" --dir {dir}")
    }
}

/// Minimal sanity check + lowercasing. Full checksum validation is a later concern.
fn normalise_address(addr: &str) -> Result<String> {
    let a = addr.trim();
    let hex = a.strip_prefix("0x").unwrap_or(a);
    if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("'{addr}' is not a 20-byte hex address");
    }
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}
