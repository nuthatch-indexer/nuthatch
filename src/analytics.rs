//! Read-only analytical SQL over the sealed Parquet segments, via an embedded DuckDB. DuckDB is
//! single-writer/OLAP: we only ever ATTACH the segments read-only here; the ingestion path never
//! writes DuckDB. Queries see *finalized* data (what's been sealed); the hot tip lives in redb.
//!
//! The binary stays single-file: DuckDB is statically bundled. Memory is capped so an analytical
//! query can't blow the embedded-mode RAM budget.

use anyhow::{bail, Context, Result};
use duckdb::types::ValueRef;
use duckdb::Connection;
use serde_json::{Map, Value};
use std::path::Path;

/// Cap DuckDB's working memory so `/sql` can't breach the embedded footprint budget.
const MEM_LIMIT: &str = "512MB";
const MAX_THREADS: u32 = 2;

/// Run a read-only query. A `transfers` view over all sealed segments is in scope. Only
/// SELECT/WITH statements are accepted — this is a query surface, not a mutation surface.
pub fn query(dir: &Path, sql: &str) -> Result<Vec<Value>> {
    let trimmed = sql.trim_start().to_ascii_lowercase();
    if !(trimmed.starts_with("select") || trimmed.starts_with("with")) {
        bail!("only SELECT/WITH queries are allowed on the read-only SQL surface");
    }

    let conn = Connection::open_in_memory().context("failed to open DuckDB")?;
    conn.execute_batch(&format!(
        "SET memory_limit='{MEM_LIMIT}'; SET threads={MAX_THREADS};"
    ))
    .context("failed to configure DuckDB")?;
    define_transfers_view(&conn, dir)?;

    let mut stmt = conn.prepare(sql).context("failed to prepare query")?;
    let mut rows = stmt.query([]).context("query failed")?;
    // Column metadata is only materialised once the statement has executed — read it off the
    // executed result, not the prepared statement.
    let column_names: Vec<String> = rows
        .as_ref()
        .map(|s| s.column_names().iter().map(|c| c.to_string()).collect())
        .unwrap_or_default();

    let mut out = Vec::new();
    while let Some(row) = rows.next().context("row read failed")? {
        let mut obj = Map::new();
        for (i, name) in column_names.iter().enumerate() {
            obj.insert(name.clone(), value_to_json(row.get_ref(i)?));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Point-read fallback: fetch a single sealed transfer by (block, log_index). Used when the hot
/// store has already pruned it. Integers are interpolated (not user text), so no injection surface.
pub fn get_transfer(dir: &Path, block: u64, log_index: u64) -> Result<Option<Value>> {
    let sql = format!(
        "SELECT * FROM transfers WHERE block_number = {block} AND log_index = {log_index} LIMIT 1"
    );
    Ok(query(dir, &sql)?.into_iter().next())
}

/// Expose the sealed segments as a `transfers` view. When nothing is sealed yet, define an empty
/// but correctly-typed view so queries still parse and return zero rows.
fn define_transfers_view(conn: &Connection, dir: &Path) -> Result<()> {
    let seg_dir = dir.join(crate::seal::SEGMENTS_DIR);
    let has_segments = std::fs::read_dir(&seg_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        })
        .unwrap_or(false);

    let ddl = if has_segments {
        let glob = seg_dir.join("*.parquet");
        format!(
            "CREATE VIEW transfers AS SELECT * FROM read_parquet('{}')",
            glob.display()
        )
    } else {
        // Empty, correctly-typed view (schema mirrors seal::schema()).
        "CREATE VIEW transfers AS SELECT \
            CAST(NULL AS UBIGINT) AS block_number, \
            CAST(NULL AS UBIGINT) AS log_index, \
            CAST(NULL AS VARCHAR) AS \"from\", \
            CAST(NULL AS VARCHAR) AS \"to\", \
            CAST(NULL AS VARCHAR) AS value, \
            CAST(NULL AS VARCHAR) AS value_hex, \
            CAST(NULL AS VARCHAR) AS tx_hash \
         WHERE 1=0"
            .to_string()
    };
    conn.execute_batch(&ddl).context("failed to define transfers view")?;
    Ok(())
}

fn value_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => Value::from(i),
        ValueRef::SmallInt(i) => Value::from(i),
        ValueRef::Int(i) => Value::from(i),
        ValueRef::BigInt(i) => Value::from(i),
        ValueRef::UTinyInt(i) => Value::from(i),
        ValueRef::USmallInt(i) => Value::from(i),
        ValueRef::UInt(i) => Value::from(i),
        ValueRef::UBigInt(i) => Value::from(i),
        ValueRef::Float(f) => Value::from(f),
        ValueRef::Double(f) => Value::from(f),
        ValueRef::HugeInt(i) => Value::String(i.to_string()),
        ValueRef::Text(bytes) => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        // Timestamps, decimals, nested types etc. — stringify for the skeleton surface.
        other => Value::String(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_project_returns_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let rows = query(dir.path(), "SELECT count(*) AS n FROM transfers").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["n"], Value::from(0u64));
    }

    #[test]
    fn rejects_non_select() {
        let dir = tempfile::tempdir().unwrap();
        assert!(query(dir.path(), "DROP VIEW transfers").is_err());
    }

    #[test]
    fn queries_a_sealed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"from":"0xa","to":"0xb","value":"5","value_hex":"0x5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"from":"0xa","to":"0xc","value":"7","value_hex":"0x7","block_number":10,"tx_hash":"0xt","log_index":1}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();

        let rows = query(dir.path(), "SELECT count(*) AS n FROM transfers").unwrap();
        assert_eq!(rows[0]["n"], Value::from(2u64));

        let one = get_transfer(dir.path(), 10, 1).unwrap().unwrap();
        assert_eq!(one["to"], Value::from("0xc"));
        assert_eq!(one["value"], Value::from("7"));
    }
}
