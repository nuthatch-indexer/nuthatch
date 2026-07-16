//! `recurrence` — a toy EFFECTFUL stage (RFC-0008 C4) that demonstrates a granted host capability.
//!
//! It reads a batch of addresses (Arrow IPC), and for each keeps a running "how many times have I
//! seen this address across all batches?" count in the host `kv` store — state a *pure* stage could
//! not hold. It emits one annotation per input row: `(address, seen)`. The `kv` import is visible in
//! the component's type, so the host refuses to instantiate it unless `kv` was granted. It still
//! cannot write canonical entities: its only output is the annotation batch the host records.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    world: "effectful-kv",
    path: "../../wit",
});

use nuthatch::transform::kv;

struct Component;

impl exports::nuthatch::transform::effectful::Guest for Component {
    fn run(batch: Vec<u8>) -> Result<Vec<u8>, String> {
        let input = read_batch(&batch)?;
        let addrs = input
            .column_by_name("address")
            .ok_or("input batch has no `address` column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("`address` column is not utf8")?;

        let mut out_addr = Vec::new();
        let mut out_seen = Vec::new();
        for i in 0..input.num_rows() {
            let addr = addrs.value(i);
            // Read the prior count from the granted kv store, increment, write it back.
            let prev: u64 = kv::get(addr)
                .and_then(|b| String::from_utf8(b).ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let seen = prev + 1;
            kv::set(addr, seen.to_string().as_bytes());
            out_addr.push(addr.to_string());
            out_seen.push(seen);
        }

        let out = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("address", DataType::Utf8, false),
                Field::new("seen", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(StringArray::from(out_addr)),
                Arc::new(UInt64Array::from(out_seen)),
            ],
        )
        .map_err(|e| e.to_string())?;
        write_batch(&out)
    }
}

fn read_batch(bytes: &[u8]) -> Result<RecordBatch, String> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| e.to_string())?;
    match reader.next() {
        Some(b) => b.map_err(|e| e.to_string()),
        None => Err("empty Arrow IPC stream".to_string()),
    }
}

fn write_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut out, &batch.schema()).map_err(|e| e.to_string())?;
        writer.write(batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }
    Ok(out)
}

export!(Component);
