//! Multi-source Comparison: bandingkan hasil query dari banyak koneksi
//! (MySQL, PostgreSQL, SQLite, SQL Server, MongoDB) per kunci baris.
//!
//! Tiap entri adalah satu query di satu koneksi. Variabel `${nama}` diganti
//! teks apa adanya di semua query sebelum dijalankan. Baris dicocokkan lewat
//! kolom kunci yang dipasangkan per posisi (kunci ke-1 entri A dengan kunci
//! ke-1 entri B), sehingga nama kunci boleh berbeda antar engine. Kolom nilai
//! dicocokkan lewat nama (tanpa beda huruf besar) setelah alias per entri.
//!
//! Pembandingnya ([`diff_sources`]) murni; hanya [`run`] yang menyentuh
//! database.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use super::catalog::{self, Endpoint};
use super::compare::{CompareOptions, CompareProgressHandle, normalize, with_progress};
use super::mongo_source;
use super::{NULL_MARKER, TableData};
use crate::models::enums::{DatabasePool, DatabaseType};
use crate::models::structs::ConnectionConfig;
use crate::schema_objects::sql::quote_qualified;

/// Satu variabel yang bisa dipakai di semua query (`${nama}`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompareVariable {
    pub name: String,
    pub value: String,
}

/// Satu sumber data: query di satu koneksi.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompareEntry {
    /// Nama tampilan entri; kosong = "Query N".
    pub label: String,
    pub connection_id: i64,
    pub database: Option<String>,
    /// SQL, atau JSON query MongoDB (lihat [`mongo_source::parse_mongo_query`]).
    pub query: String,
    /// Kolom kunci, dipasangkan per posisi antar entri. Kosong = `_id`, `id`,
    /// atau kolom pertama.
    pub key_columns: Vec<String>,
    /// Alias kolom: `(nama asli, nama umum)`.
    pub aliases: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiCompareOptions {
    /// Baris maksimum per entri. `None` = tanpa batas.
    pub row_limit: Option<u64>,
    /// Kolom (nama umum) yang tidak dibandingkan.
    pub ignore_columns: Vec<String>,
    pub trim_trailing_space: bool,
    pub tolerant_values: bool,
    pub case_insensitive: bool,
}

impl Default for MultiCompareOptions {
    fn default() -> Self {
        Self {
            row_limit: Some(100_000),
            ignore_columns: Vec::new(),
            trim_trailing_space: true,
            tolerant_values: true,
            case_insensitive: false,
        }
    }
}

impl MultiCompareOptions {
    /// Opsi normalisasi nilai yang sama dengan Data Compare dua tabel.
    fn value_options(&self) -> CompareOptions {
        CompareOptions {
            trim_trailing_space: self.trim_trailing_space,
            tolerant_values: self.tolerant_values,
            case_insensitive: self.case_insensitive,
            ..Default::default()
        }
    }
}

/// Definisi lengkap satu comparison yang disimpan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiCompareSpec {
    pub name: String,
    pub variables: Vec<CompareVariable>,
    pub entries: Vec<CompareEntry>,
    pub options: MultiCompareOptions,
}

impl MultiCompareSpec {
    /// Jumlah koneksi berbeda yang dipakai entri.
    pub fn connection_count(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.connection_id)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Periksa definisi sebelum disimpan atau dijalankan.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("Give the comparison a name".to_string());
        }
        let mut names = HashSet::new();
        for var in &self.variables {
            let name = var.name.trim();
            if !is_valid_variable_name(name) {
                return Err(format!(
                    "Variable name \"{name}\" may only use letters, digits and _"
                ));
            }
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Variable \"{name}\" is defined twice"));
            }
        }
        if self.entries.len() < 2 {
            return Err("Add at least two queries to compare".to_string());
        }
        for (i, entry) in self.entries.iter().enumerate() {
            let label = entry_label(entry, i);
            if entry.connection_id <= 0 {
                return Err(format!("{label}: choose a connection"));
            }
            if entry.query.trim().is_empty() {
                return Err(format!("{label}: the query is empty"));
            }
            substitute_variables(&entry.query, &self.variables)
                .map_err(|e| format!("{label}: {e}"))?;
        }
        Ok(())
    }
}

/// Nama tampilan entri: label, atau "Query N".
pub fn entry_label(entry: &CompareEntry, index: usize) -> String {
    let label = entry.label.trim();
    if label.is_empty() {
        format!("Query {}", index + 1)
    } else {
        label.to_string()
    }
}

pub fn is_valid_variable_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Teks placeholder untuk variabel `name`.
pub fn variable_placeholder(name: &str) -> String {
    format!("${{{name}}}")
}

/// Ganti setiap `${nama}` dengan nilai variabelnya, apa adanya (tanpa kutip).
/// `${...}` dengan nama tidak sah dibiarkan; nama sah yang tidak dikenal
/// adalah error.
pub fn substitute_variables(query: &str, vars: &[CompareVariable]) -> Result<String, String> {
    let mut out = String::with_capacity(query.len());
    let mut rest = query;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let name = &after[..end];
        if !is_valid_variable_name(name) {
            out.push_str("${");
            rest = after;
            continue;
        }
        let var = vars
            .iter()
            .find(|v| v.name.trim().eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("unknown variable ${{{name}}}"))?;
        out.push_str(&var.value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Engine yang bisa menjadi entri comparison.
pub fn supports_engine(db: &DatabaseType) -> bool {
    catalog::supports_sql(db) || matches!(db, DatabaseType::MongoDB)
}

/// Query awal saat user memilih tabel/koleksi.
pub fn default_query(db: &DatabaseType, table: &str) -> String {
    match db {
        DatabaseType::MongoDB => format!(
            "{{\"collection\": {}, \"filter\": {{}}}}",
            serde_json::Value::String(table.to_string())
        ),
        _ => format!("SELECT * FROM {}", quote_qualified(db, table)),
    }
}

/// Alias dari teks `asli=umum`, dipisah baris baru atau koma.
pub fn parse_aliases(text: &str) -> Vec<(String, String)> {
    text.split(['\n', ','])
        .filter_map(|line| {
            let (from, to) = line.split_once('=')?;
            let (from, to) = (from.trim(), to.trim());
            (!from.is_empty() && !to.is_empty()).then(|| (from.to_string(), to.to_string()))
        })
        .collect()
}

pub fn format_aliases(aliases: &[(String, String)]) -> String {
    aliases
        .iter()
        .map(|(from, to)| format!("{from}={to}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Daftar nama dipisah koma.
pub fn parse_list(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

// ─── Pembanding ─────────────────────────────────────────────────────────────

/// Data yang sudah dibaca dari satu entri.
#[derive(Clone, Debug, Default)]
pub struct SourceData {
    pub label: String,
    pub data: TableData,
    pub key_columns: Vec<String>,
    pub aliases: Vec<(String, String)>,
    pub truncated: bool,
}

/// Satu kunci yang berbeda: hilang di sebagian sumber atau nilainya berubah.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MultiRowDiff {
    /// Nilai kunci untuk ditampilkan (dari sumber pertama yang memilikinya).
    pub key: Vec<String>,
    /// Per sumber: baris dengan kunci ini ada?
    pub present: Vec<bool>,
    /// `[sumber][kolom]`; `None` = NULL atau baris tidak ada.
    pub values: Vec<Vec<Option<String>>>,
    /// Indeks kolom (di [`MultiCompareResult::columns`]) yang nilainya tidak
    /// sama di antara sumber yang memiliki baris ini.
    pub changed: Vec<usize>,
}

impl MultiRowDiff {
    pub fn is_missing(&self) -> bool {
        self.present.iter().any(|p| !p)
    }

    /// Ringkasan satu baris: "Missing in: A; Changed: amount".
    pub fn status(&self, labels: &[String], columns: &[String]) -> String {
        let mut parts = Vec::new();
        let missing: Vec<&str> = self
            .present
            .iter()
            .zip(labels)
            .filter(|(p, _)| !**p)
            .map(|(_, l)| l.as_str())
            .collect();
        if !missing.is_empty() {
            parts.push(format!("Missing in: {}", missing.join(", ")));
        }
        if !self.changed.is_empty() {
            let cols: Vec<&str> = self
                .changed
                .iter()
                .filter_map(|c| columns.get(*c).map(String::as_str))
                .collect();
            parts.push(format!("Changed: {}", cols.join(", ")));
        }
        parts.join("; ")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceSummary {
    pub label: String,
    pub rows: usize,
    /// Baris dengan kunci kembar (hanya yang pertama dibandingkan).
    pub duplicate_keys: usize,
    /// Kunci yang ada di sumber lain tetapi tidak di sini.
    pub missing: usize,
    pub truncated: bool,
    /// Indeks baris (di [`MultiCompareResult::source_data`]) yang ikut
    /// berbeda: nilainya berubah atau kuncinya hilang di sumber lain.
    pub differing_rows: Vec<usize>,
}

/// Satu kunci gabungan dengan baris sumbernya, untuk tampilan berdampingan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRow {
    /// Per sumber: indeks baris di [`MultiCompareResult::source_data`];
    /// `None` = kunci tidak ada di sumber itu.
    pub rows: Vec<Option<usize>>,
    /// Indeks di [`MultiCompareResult::diffs`] bila kunci ini berbeda.
    pub diff: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MultiCompareResult {
    pub labels: Vec<String>,
    /// Nama kunci dari sumber pertama (setelah alias).
    pub key_columns: Vec<String>,
    /// Kolom nilai yang ada di semua sumber dan dibandingkan.
    pub columns: Vec<String>,
    /// Kolom yang hanya ada di sebagian sumber (tidak dibandingkan).
    pub partial_columns: Vec<String>,
    pub sources: Vec<SourceSummary>,
    pub diffs: Vec<MultiRowDiff>,
    pub identical: usize,
    /// Kunci berbeda dari gabungan semua sumber.
    pub total_keys: usize,
    /// Data mentah tiap sumber (urutan sama dengan `labels`), untuk ditinjau
    /// di tab hasil. Diisi oleh [`run`]; [`diff_sources`] membiarkannya kosong.
    pub source_data: Vec<TableData>,
    /// Semua kunci gabungan (termasuk yang identik) urut kemunculan, dengan
    /// baris tiap sumber.
    pub key_rows: Vec<KeyRow>,
    /// Per sumber: indeks kolom kunci di header `source_data`.
    pub key_index: Vec<Vec<usize>>,
    /// Per sumber: indeks kolom di header `source_data` untuk tiap `columns`.
    pub column_index: Vec<Vec<usize>>,
}

impl MultiCompareResult {
    /// `(baris hilang di sebagian sumber, baris berubah)`.
    pub fn counts(&self) -> (usize, usize) {
        let missing = self.diffs.iter().filter(|d| d.is_missing()).count();
        (missing, self.diffs.len() - missing)
    }

    pub fn truncated(&self) -> bool {
        self.sources.iter().any(|s| s.truncated)
    }

    /// Ringkasan satu baris untuk sidebar dan toast.
    pub fn summary(&self) -> String {
        let (missing, changed) = self.counts();
        format!(
            "{} keys compared, {} identical, {} differ ({} missing, {} changed)",
            self.total_keys,
            self.identical,
            self.diffs.len(),
            missing,
            changed
        )
    }

    /// Header kolom tabel hasil (urutan sama dengan [`Self::to_table_data`]).
    pub fn table_headers(&self) -> Vec<String> {
        let mut headers = self.key_columns.clone();
        headers.push("Status".to_string());
        for col in &self.columns {
            for label in &self.labels {
                headers.push(format!("{col} @ {label}"));
            }
        }
        headers
    }

    /// Hasil sebagai tabel datar untuk diekspor: kunci, status, lalu satu
    /// kolom per (kolom nilai, sumber). Sel dari baris yang tidak ada = NULL.
    pub fn to_table_data(&self) -> TableData {
        let rows = self
            .diffs
            .iter()
            .map(|d| {
                let mut cells: Vec<Option<String>> = d
                    .key
                    .iter()
                    .map(|k| (k != NULL_MARKER).then(|| k.clone()))
                    .collect();
                cells.push(Some(d.status(&self.labels, &self.columns)));
                for c in 0..self.columns.len() {
                    for s in 0..self.labels.len() {
                        cells.push(d.values.get(s).and_then(|v| v.get(c)).cloned().flatten());
                    }
                }
                cells
            })
            .collect();
        TableData::from_cells(self.table_headers(), rows)
    }
}

fn find_column(headers: &[String], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.eq_ignore_ascii_case(name))
}

/// Header setelah alias diterapkan.
fn canonical_headers(source: &SourceData) -> Vec<String> {
    source
        .data
        .headers
        .iter()
        .map(|h| {
            source
                .aliases
                .iter()
                .find(|(from, _)| from.eq_ignore_ascii_case(h))
                .map(|(_, to)| to.clone())
                .unwrap_or_else(|| h.clone())
        })
        .collect()
}

/// Indeks kolom kunci satu sumber. Nama kunci boleh nama asli atau alias.
fn key_indices(source: &SourceData, canonical: &[String]) -> Result<Vec<usize>, String> {
    if source.key_columns.is_empty() {
        let idx = find_column(canonical, "_id")
            .or_else(|| find_column(canonical, "id"))
            .or((!canonical.is_empty()).then_some(0))
            .ok_or_else(|| format!("{}: the query returned no columns", source.label))?;
        return Ok(vec![idx]);
    }
    source
        .key_columns
        .iter()
        .map(|k| {
            find_column(canonical, k)
                .or_else(|| find_column(&source.data.headers, k))
                .ok_or_else(|| format!("{}: key column \"{k}\" is not in the result", source.label))
        })
        .collect()
}

type Key = Vec<Option<String>>;

/// Bandingkan hasil dari dua sumber atau lebih. Hanya baris yang berbeda
/// yang masuk ke [`MultiCompareResult::diffs`].
pub fn diff_sources(
    sources: &[SourceData],
    opts: &MultiCompareOptions,
) -> Result<MultiCompareResult, String> {
    if sources.len() < 2 {
        return Err("At least two sources are needed".to_string());
    }
    let value_opts = opts.value_options();
    let canon: Vec<Vec<String>> = sources.iter().map(canonical_headers).collect();
    let keys: Vec<Vec<usize>> = sources
        .iter()
        .zip(&canon)
        .map(|(s, c)| key_indices(s, c))
        .collect::<Result<_, _>>()?;
    let key_count = keys[0].len();
    if let Some((i, k)) = keys.iter().enumerate().find(|(_, k)| k.len() != key_count) {
        return Err(format!(
            "Every query needs the same number of key columns ({} has {}, {} has {})",
            sources[0].label,
            key_count,
            sources[i].label,
            k.len()
        ));
    }
    let ignored = |name: &str| {
        opts.ignore_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(name))
    };
    let is_value_col = |s: usize, idx: usize| !keys[s].contains(&idx) && !ignored(&canon[s][idx]);

    // Kolom nilai yang ada di semua sumber, urut seperti sumber pertama.
    let mut columns = Vec::new();
    let mut col_idx: Vec<Vec<usize>> = vec![Vec::new(); sources.len()];
    for (i0, name) in canon[0].iter().enumerate() {
        if !is_value_col(0, i0) || find_column(&columns, name).is_some() {
            continue;
        }
        let found: Option<Vec<usize>> = (0..sources.len())
            .map(|s| find_column(&canon[s], name).filter(|i| is_value_col(s, *i)))
            .collect();
        if let Some(found) = found {
            columns.push(name.clone());
            for (s, i) in found.into_iter().enumerate() {
                col_idx[s].push(i);
            }
        }
    }
    let mut partial_columns: Vec<String> = Vec::new();
    for (s, headers) in canon.iter().enumerate() {
        for (i, name) in headers.iter().enumerate() {
            if is_value_col(s, i)
                && find_column(&columns, name).is_none()
                && find_column(&partial_columns, name).is_none()
            {
                partial_columns.push(name.clone());
            }
        }
    }

    let key_of = |s: usize, row: usize| -> Key {
        keys[s]
            .iter()
            .map(|c| normalize(sources[s].data.value(row, *c), &value_opts))
            .collect()
    };

    // Indeks per sumber (baris pertama per kunci) dan urutan kunci gabungan.
    let mut index: Vec<HashMap<Key, usize>> = Vec::with_capacity(sources.len());
    let mut summaries = Vec::with_capacity(sources.len());
    let mut order: Vec<Key> = Vec::new();
    let mut seen: HashSet<Key> = HashSet::new();
    for (s, source) in sources.iter().enumerate() {
        let rows = source.data.rows.len();
        let mut map = HashMap::with_capacity(rows);
        let mut duplicates = 0usize;
        for row in 0..rows {
            let key = key_of(s, row);
            if map.contains_key(&key) {
                duplicates += 1;
                continue;
            }
            if !seen.contains(&key) {
                seen.insert(key.clone());
                order.push(key.clone());
            }
            map.insert(key, row);
        }
        index.push(map);
        summaries.push(SourceSummary {
            label: source.label.clone(),
            rows,
            duplicate_keys: duplicates,
            missing: 0,
            truncated: source.truncated,
            differing_rows: Vec::new(),
        });
    }
    drop(seen);

    let mut diffs = Vec::new();
    let mut key_rows = Vec::with_capacity(order.len());
    let mut identical = 0usize;
    for key in &order {
        let rows: Vec<Option<usize>> = index.iter().map(|m| m.get(key).copied()).collect();
        let changed: Vec<usize> = (0..columns.len())
            .filter(|c| {
                let mut values = rows.iter().enumerate().filter_map(|(s, row)| {
                    row.map(|r| normalize(sources[s].data.value(r, col_idx[s][*c]), &value_opts))
                });
                match values.next() {
                    Some(first) => values.any(|v| v != first),
                    None => false,
                }
            })
            .collect();
        let all_present = rows.iter().all(Option::is_some);
        if all_present && changed.is_empty() {
            identical += 1;
            key_rows.push(KeyRow { rows, diff: None });
            continue;
        }
        key_rows.push(KeyRow {
            rows: rows.clone(),
            diff: Some(diffs.len()),
        });
        for (s, row) in rows.iter().enumerate() {
            match row {
                None => summaries[s].missing += 1,
                Some(r) => summaries[s].differing_rows.push(*r),
            }
        }
        let (first_s, first_row) = rows
            .iter()
            .enumerate()
            .find_map(|(s, r)| r.map(|r| (s, r)))
            .unwrap_or((0, 0));
        let key_display = keys[first_s]
            .iter()
            .map(|c| {
                sources[first_s]
                    .data
                    .value(first_row, *c)
                    .map_or_else(|| NULL_MARKER.to_string(), str::to_string)
            })
            .collect();
        let values = rows
            .iter()
            .enumerate()
            .map(|(s, row)| {
                (0..columns.len())
                    .map(|c| {
                        row.and_then(|r| sources[s].data.value(r, col_idx[s][c]))
                            .map(str::to_string)
                    })
                    .collect()
            })
            .collect();
        diffs.push(MultiRowDiff {
            key: key_display,
            present: rows.iter().map(Option::is_some).collect(),
            values,
            changed,
        });
    }

    let key_columns = keys[0].iter().map(|i| canon[0][*i].clone()).collect();
    Ok(MultiCompareResult {
        labels: sources.iter().map(|s| s.label.clone()).collect(),
        key_columns,
        columns,
        partial_columns,
        sources: summaries,
        diffs,
        identical,
        total_keys: order.len(),
        source_data: Vec::new(),
        key_rows,
        key_index: keys,
        column_index: col_idx,
    })
}

// ─── Eksekusi ───────────────────────────────────────────────────────────────

/// Tempat sebuah entri dijalankan.
#[derive(Clone)]
pub enum EntryTarget {
    Sql(Box<Endpoint>),
    Mongo {
        conn: Box<ConnectionConfig>,
        pool: Option<DatabasePool>,
        database: String,
    },
}

/// Entri yang koneksinya sudah ditemukan; siap dijalankan.
#[derive(Clone)]
pub struct ResolvedEntry {
    pub label: String,
    pub target: EntryTarget,
    pub query: String,
    pub key_columns: Vec<String>,
    pub aliases: Vec<(String, String)>,
}

/// Potong baris ke batas; `true` bila ada yang terbuang.
fn apply_limit<T>(rows: &mut Vec<T>, limit: Option<u64>) -> bool {
    match limit {
        Some(limit) => {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let over = rows.len() > limit;
            rows.truncate(limit);
            over
        }
        None => false,
    }
}

async fn fetch_entry(
    target: EntryTarget,
    query: &str,
    opts: &MultiCompareOptions,
    cancel: &AtomicBool,
) -> Result<(TableData, bool), String> {
    match target {
        EntryTarget::Sql(ep) => {
            let ep = (*ep).connect().await?;
            let set = ep.query(query).await?;
            let mut rows = set.rows;
            let truncated = apply_limit(&mut rows, opts.row_limit);
            Ok((TableData::new(set.headers, rows), truncated))
        }
        EntryTarget::Mongo {
            conn,
            pool,
            database,
        } => {
            let parsed = mongo_source::parse_mongo_query(query)?;
            let pool = match pool {
                Some(pool) => pool,
                None => crate::connection::pool::create_connection_pool_for_config(&conn).await?,
            };
            let DatabasePool::MongoDB(client) = pool else {
                return Err("The connection is not a MongoDB connection".to_string());
            };
            mongo_source::fetch(&client, &database, &parsed, opts.row_limit, cancel).await
        }
    }
}

/// Jalankan semua entri berurutan lalu bandingkan hasilnya.
pub async fn run(
    entries: Vec<ResolvedEntry>,
    variables: Vec<CompareVariable>,
    opts: MultiCompareOptions,
    progress: CompareProgressHandle,
    cancel: Arc<AtomicBool>,
) -> Result<MultiCompareResult, String> {
    with_progress(&progress, |p| {
        p.tables_total = entries.len() + 1;
        p.tables_done = 0;
    });
    let mut sources = Vec::with_capacity(entries.len());
    for entry in entries {
        if cancel.load(Ordering::Relaxed) {
            return Err("Cancelled".to_string());
        }
        with_progress(&progress, |p| {
            p.current_table = format!("Reading {}", entry.label)
        });
        let query = substitute_variables(&entry.query, &variables)
            .map_err(|e| format!("{}: {e}", entry.label))?;
        let (data, truncated) = fetch_entry(entry.target, &query, &opts, &cancel)
            .await
            .map_err(|e| format!("{}: {e}", entry.label))?;
        log::info!(
            "[COMPARE] {} returned {} rows{}",
            entry.label,
            data.rows.len(),
            if truncated { " (truncated)" } else { "" }
        );
        sources.push(SourceData {
            label: entry.label,
            data,
            key_columns: entry.key_columns,
            aliases: entry.aliases,
            truncated,
        });
        with_progress(&progress, |p| p.tables_done += 1);
    }
    if cancel.load(Ordering::Relaxed) {
        return Err("Cancelled".to_string());
    }
    with_progress(&progress, |p| {
        p.current_table = "Comparing rows".to_string()
    });
    // Data mentah ikut dikembalikan supaya tab hasil bisa menampilkan baris
    // tiap sumber tanpa membaca ulang.
    let (mut result, sources) =
        tokio::task::spawn_blocking(move || diff_sources(&sources, &opts).map(|r| (r, sources)))
            .await
            .map_err(|e| format!("Comparison stopped: {e}"))??;
    result.source_data = sources.into_iter().map(|s| s.data).collect();
    with_progress(&progress, |p| p.tables_done += 1);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn var(name: &str, value: &str) -> CompareVariable {
        CompareVariable {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn source(label: &str, headers: &[&str], rows: &[&[&str]], keys: &[&str]) -> SourceData {
        SourceData {
            label: label.to_string(),
            data: TableData::new(
                headers.iter().map(|h| h.to_string()).collect(),
                rows.iter()
                    .map(|r| r.iter().map(|c| c.to_string()).collect())
                    .collect(),
            ),
            key_columns: keys.iter().map(|k| k.to_string()).collect(),
            aliases: Vec::new(),
            truncated: false,
        }
    }

    #[test]
    fn variables_are_substituted_verbatim() {
        let vars = [var("from", "2026-01-01"), var("shop", "7")];
        let sql = "SELECT * FROM t WHERE d >= '${from}' AND shop = ${SHOP}";
        assert_eq!(
            substitute_variables(sql, &vars).unwrap(),
            "SELECT * FROM t WHERE d >= '2026-01-01' AND shop = 7"
        );
    }

    #[test]
    fn unknown_variable_is_an_error_and_odd_braces_are_kept() {
        assert!(substitute_variables("x = ${missing}", &[]).is_err());
        assert_eq!(
            substitute_variables("a ${not valid} ${", &[]).unwrap(),
            "a ${not valid} ${"
        );
    }

    #[test]
    fn default_query_matches_engine() {
        assert_eq!(
            default_query(&DatabaseType::MySQL, "orders"),
            "SELECT * FROM `orders`"
        );
        assert_eq!(
            default_query(&DatabaseType::MsSQL, "dbo.orders"),
            "SELECT * FROM [dbo].[orders]"
        );
        assert_eq!(
            default_query(&DatabaseType::MongoDB, "orders"),
            "{\"collection\": \"orders\", \"filter\": {}}"
        );
    }

    #[test]
    fn aliases_and_lists_parse() {
        assert_eq!(
            parse_aliases("customerId=customer_id\n bad , total = amount"),
            vec![
                ("customerId".to_string(), "customer_id".to_string()),
                ("total".to_string(), "amount".to_string())
            ]
        );
        assert_eq!(parse_list(" a, ,b "), vec!["a", "b"]);
    }

    #[test]
    fn three_sources_report_only_differences() {
        let mysql = source(
            "MySQL",
            &["id", "amount", "status"],
            &[
                &["1", "10.00", "paid"],
                &["2", "20", "open"],
                &["3", "30", "paid"],
            ],
            &["id"],
        );
        let mssql = source(
            "MSSQL",
            &["order_id", "amount", "status"],
            &[
                &["1", "10", "paid"],
                &["2", "25", "open"],
                &["3", "30", "paid"],
            ],
            &["order_id"],
        );
        let mut mongo = source(
            "Mongo",
            &["_id", "total", "status", "note"],
            &[
                &["1", "10", "paid", "x"],
                &["2", "20", "open", "y"],
                &["4", "40", "new", "z"],
            ],
            &["_id"],
        );
        mongo.aliases = vec![("total".to_string(), "amount".to_string())];

        let r = diff_sources(&[mysql, mssql, mongo], &MultiCompareOptions::default()).unwrap();
        assert_eq!(r.columns, vec!["amount", "status"]);
        assert_eq!(r.partial_columns, vec!["note"]);
        assert_eq!(r.total_keys, 4);
        assert_eq!(r.identical, 1);
        assert_eq!(r.diffs.len(), 3);
        // Kunci 2: amount berbeda di MSSQL.
        let k2 = r.diffs.iter().find(|d| d.key == ["2"]).unwrap();
        assert_eq!(k2.changed, vec![0]);
        assert!(!k2.is_missing());
        // Kunci 3 tidak ada di Mongo, kunci 4 hanya di Mongo.
        let k3 = r.diffs.iter().find(|d| d.key == ["3"]).unwrap();
        assert_eq!(k3.present, vec![true, true, false]);
        assert!(k3.changed.is_empty());
        let k4 = r.diffs.iter().find(|d| d.key == ["4"]).unwrap();
        assert_eq!(k4.present, vec![false, false, true]);
        assert_eq!(r.sources[2].missing, 1);
        assert_eq!(r.sources[0].missing, 1);
        assert_eq!(r.counts(), (2, 1));
        // Baris sumber yang ikut berbeda (untuk tampilan data per sumber):
        // MySQL baris 2 dan 3 (kunci 2 berubah, kunci 3 hilang di Mongo),
        // Mongo baris 2 dan 3 (kunci 2 berubah, kunci 4 hanya di sana).
        assert_eq!(r.sources[0].differing_rows, vec![1, 2]);
        assert_eq!(r.sources[1].differing_rows, vec![1, 2]);
        assert_eq!(r.sources[2].differing_rows, vec![1, 2]);
        assert!(
            r.source_data.is_empty(),
            "diff_sources leaves raw data to run()"
        );
        // Pemetaan berdampingan: semua kunci urut kemunculan, termasuk yang
        // identik (kunci 1), dengan indeks baris tiap sumber.
        assert_eq!(r.key_rows.len(), 4);
        assert_eq!(r.key_rows[0].rows, vec![Some(0), Some(0), Some(0)]);
        assert_eq!(r.key_rows[0].diff, None);
        assert_eq!(r.key_rows[2].rows, vec![Some(2), Some(2), None]);
        assert_eq!(r.key_rows[2].diff, Some(1));
        assert_eq!(r.key_rows[3].rows, vec![None, None, Some(2)]);
        assert_eq!(r.key_index[2], vec![0]);
        // Kolom "amount" di Mongo adalah "total" (indeks 1) lewat alias.
        assert_eq!(r.column_index[2], vec![1, 2]);

        let table = r.to_table_data();
        assert_eq!(
            table.headers,
            vec![
                "id",
                "Status",
                "amount @ MySQL",
                "amount @ MSSQL",
                "amount @ Mongo",
                "status @ MySQL",
                "status @ MSSQL",
                "status @ Mongo"
            ]
        );
        assert_eq!(table.rows.len(), 3);
        assert_eq!(table.value(0, 1), Some("Changed: amount"));
        assert_eq!(table.value(2, 1), Some("Missing in: MySQL, MSSQL"));
        assert_eq!(table.value(2, 2), None);
    }

    #[test]
    fn default_key_prefers_id_columns() {
        let a = source("A", &["name", "id"], &[&["x", "1"]], &[]);
        let b = source("B", &["_id", "name"], &[&["1", "x"]], &[]);
        let r = diff_sources(&[a, b], &MultiCompareOptions::default()).unwrap();
        assert_eq!(r.identical, 1);
        assert!(r.diffs.is_empty());
    }

    #[test]
    fn key_count_mismatch_and_missing_key_are_errors() {
        let a = source("A", &["a", "b"], &[], &["a", "b"]);
        let b = source("B", &["a", "b"], &[], &["a"]);
        assert!(diff_sources(&[a.clone(), b], &MultiCompareOptions::default()).is_err());
        let c = source("C", &["a", "b"], &[], &["zzz", "b"]);
        assert!(diff_sources(&[a, c], &MultiCompareOptions::default()).is_err());
    }

    #[test]
    fn duplicate_keys_and_ignored_columns() {
        let a = source(
            "A",
            &["id", "v", "ts"],
            &[&["1", "a", "1"], &["1", "b", "2"]],
            &[],
        );
        let b = source("B", &["id", "v", "ts"], &[&["1", "a", "9"]], &[]);
        let opts = MultiCompareOptions {
            ignore_columns: vec!["TS".to_string()],
            ..Default::default()
        };
        let r = diff_sources(&[a, b], &opts).unwrap();
        assert_eq!(r.sources[0].duplicate_keys, 1);
        assert_eq!(r.identical, 1);
        assert!(r.partial_columns.is_empty());
    }

    async fn sqlite_endpoint(name: &str, setup: &[&'static str]) -> Endpoint {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for sql in setup {
            sqlx::query(*sql).execute(&pool).await.unwrap();
        }
        Endpoint::new(
            ConnectionConfig {
                name: name.to_string(),
                connection_type: DatabaseType::SQLite,
                ..Default::default()
            },
            Some(DatabasePool::SQLite(Arc::new(pool))),
            None,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_compares_a_join_with_a_flat_table() {
        // Sisi A butuh JOIN; sisi B satu tabel datar dengan nama kolom lain.
        let a = sqlite_endpoint(
            "A",
            &[
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, shop INTEGER)",
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO customers VALUES (1, 'Ann'), (2, 'Bob')",
                "INSERT INTO orders VALUES (10, 1, 5.0, 7), (11, 2, 8.5, 7), (12, 2, 1.0, 7), (13, 1, 9.0, 8)",
            ],
        )
        .await;
        let b = sqlite_endpoint(
            "B",
            &[
                "CREATE TABLE flat (order_no INTEGER, customer TEXT, amount REAL, shop INTEGER)",
                "INSERT INTO flat VALUES (10, 'Ann', 5, 7), (11, 'Bob', 9, 7), (14, NULL, 2, 7)",
            ],
        )
        .await;
        let entries = vec![
            ResolvedEntry {
                label: "A".to_string(),
                target: EntryTarget::Sql(Box::new(a)),
                query: "SELECT o.id, c.name AS customer, o.total FROM orders o \
                        JOIN customers c ON c.id = o.customer_id WHERE o.shop = ${shop}"
                    .to_string(),
                key_columns: vec!["id".to_string()],
                aliases: vec![("total".to_string(), "amount".to_string())],
            },
            ResolvedEntry {
                label: "B".to_string(),
                target: EntryTarget::Sql(Box::new(b)),
                query: "SELECT order_no, customer, amount FROM flat WHERE shop = ${shop}"
                    .to_string(),
                key_columns: vec!["order_no".to_string()],
                aliases: Vec::new(),
            },
        ];
        let progress = CompareProgressHandle::default();
        let r = run(
            entries,
            vec![var("shop", "7")],
            MultiCompareOptions::default(),
            progress.clone(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        assert_eq!(r.columns, vec!["customer", "amount"]);
        assert_eq!(r.total_keys, 4);
        assert_eq!(r.identical, 1);
        let keys: Vec<&str> = r.diffs.iter().map(|d| d.key[0].as_str()).collect();
        assert_eq!(keys, vec!["11", "12", "14"]);
        assert_eq!(r.diffs[0].changed, vec![1]);
        assert_eq!(r.diffs[1].present, vec![true, false]);
        assert_eq!(r.diffs[2].values[1][0], None);
        // Data mentah tiap sumber ikut kembali, urut seperti `labels`.
        assert_eq!(r.source_data.len(), 2);
        assert_eq!(r.source_data[0].headers.len(), 3);
        assert_eq!(
            r.source_data[0].rows.len(),
            r.sources[0].rows,
            "rows read must match the summary"
        );
        let p = progress.lock().unwrap();
        assert_eq!(p.tables_done, p.tables_total);
    }

    #[tokio::test]
    async fn run_stops_when_cancelled_and_honours_row_limit() {
        let a = sqlite_endpoint(
            "A",
            &[
                "CREATE TABLE t (id INTEGER)",
                "INSERT INTO t VALUES (1),(2),(3)",
            ],
        )
        .await;
        let make = |ep: Endpoint| ResolvedEntry {
            label: ep.conn.name.clone(),
            target: EntryTarget::Sql(Box::new(ep)),
            query: "SELECT id FROM t ORDER BY id".to_string(),
            key_columns: Vec::new(),
            aliases: Vec::new(),
        };
        let cancel = Arc::new(AtomicBool::new(true));
        let err = run(
            vec![make(a.clone()), make(a.clone())],
            Vec::new(),
            MultiCompareOptions::default(),
            CompareProgressHandle::default(),
            cancel,
        )
        .await
        .unwrap_err();
        assert_eq!(err, "Cancelled");

        let opts = MultiCompareOptions {
            row_limit: Some(2),
            ..Default::default()
        };
        let r = run(
            vec![make(a.clone()), make(a)],
            Vec::new(),
            opts,
            CompareProgressHandle::default(),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        assert!(r.truncated());
        assert_eq!(r.sources[0].rows, 2);
        assert_eq!(r.identical, 2);
    }

    #[test]
    fn spec_validation() {
        let entry = |q: &str| CompareEntry {
            connection_id: 1,
            query: q.to_string(),
            ..Default::default()
        };
        let mut spec = MultiCompareSpec {
            name: "Orders".to_string(),
            variables: vec![var("d", "1")],
            entries: vec![entry("SELECT ${d}"), entry("SELECT 1")],
            ..Default::default()
        };
        assert!(spec.validate().is_ok());
        assert_eq!(spec.connection_count(), 1);
        spec.entries[1].query = "SELECT ${nope}".to_string();
        assert!(spec.validate().unwrap_err().contains("Query 2"));
        spec.entries.pop();
        assert!(spec.validate().is_err());
    }
}
