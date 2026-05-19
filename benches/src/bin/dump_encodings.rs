//! Dump per-column encoding distribution for the cached `small`
//! purchases fixture. Useful for deciding which lazy decoders are
//! worth writing — if a column is mostly Dictionary, a fast dict-coded
//! aggregate path beats a lazy FSST decoder, etc.
//!
//! Run:
//!     cargo run -p bqlite-benches --release --bin dump_encodings

use std::collections::BTreeMap;
use std::path::Path;

use bqlite_benches::common::*;
use bqlite_storage::segment::reader::SegmentFileReader;

fn encoding_name(d: u8) -> &'static str {
    match d {
        0 => "Plain",
        1 => "Dictionary",
        2 => "Delta",
        3 => "DoubleDelta",
        4 => "BitPacking",
        5 => "Rle",
        6 => "Constant",
        7 => "Fsst",
        8 => "For",
        9 => "PFor",
        10 => "Alp",
        _ => "Unknown",
    }
}

fn walk_segments(root: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            walk_segments(&p, out);
        } else if p.extension().and_then(|s| s.to_str()) == Some("seg") {
            out.push(p);
        }
    }
}

fn main() {
    let scale = BenchScale::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    eprintln!(
        "fixture: scale={} rows={} at {}\n",
        scale.label(),
        fixture.manifest.rows,
        fixture.db_path().display(),
    );

    let mut seg_paths = Vec::new();
    walk_segments(fixture.db_path(), &mut seg_paths);
    seg_paths.sort();
    eprintln!("found {} segment files", seg_paths.len());

    let schema = bqlite_benches::common::purchases_schema();
    let cols: Vec<String> = schema
        .columns()
        .iter()
        .map(|c| c.name.to_string())
        .collect();

    // (column_name, encoding_name) -> (chunk_count, total_bytes, total_rows)
    let mut tallies: BTreeMap<(String, &'static str), (u64, u64, u64)> = BTreeMap::new();
    let mut total_rg = 0u64;

    for path in &seg_paths {
        let reader = match SegmentFileReader::open(path, schema.clone()) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("skip {}: {e}", path.display());
                continue;
            }
        };
        let footer = reader.footer();
        let rgs = footer.row_groups();
        total_rg += rgs.len() as u64;
        for rg in rgs {
            for chunk in &rg.columns {
                let ord = chunk.column_ordinal as usize;
                let col_name = cols.get(ord).cloned().unwrap_or_else(|| format!("#{ord}"));
                let enc = encoding_name(chunk.encoding);
                let entry = tallies.entry((col_name, enc)).or_insert((0, 0, 0));
                entry.0 += 1;
                entry.1 += chunk.byte_length;
                entry.2 += rg.row_count;
            }
        }
    }

    eprintln!("total row groups: {total_rg}\n");

    // Group tallies by column for readability.
    let mut by_col: BTreeMap<String, Vec<(&'static str, u64, u64, u64)>> = BTreeMap::new();
    for ((col, enc), (chunks, bytes, rows)) in tallies {
        by_col.entry(col).or_default().push((enc, chunks, bytes, rows));
    }

    println!(
        "{:<14} {:<12} {:>8} {:>12} {:>14} {:>10}",
        "column", "encoding", "chunks", "bytes", "rows_in_rg", "bytes/row",
    );
    println!("{}", "-".repeat(76));
    for col_name in &cols {
        let Some(rows) = by_col.get(col_name) else {
            continue;
        };
        let mut rows = rows.clone();
        rows.sort_by_key(|r| std::cmp::Reverse(r.2));
        for (enc, chunks, bytes, rg_rows) in rows {
            let bpr = if rg_rows > 0 {
                bytes as f64 / rg_rows as f64
            } else {
                0.0
            };
            println!(
                "{:<14} {:<12} {:>8} {:>12} {:>14} {:>10.3}",
                col_name, enc, chunks, bytes, rg_rows, bpr,
            );
        }
    }
}
