//! Comparison multi-sumber yang disimpan di `connections.db`, beserta
//! statistik run terakhir untuk sidebar. Tabel dibuat lazily; fungsi di sini
//! hanya butuh `&SqlitePool` sehingga bisa diuji dengan pool in-memory.

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use super::multi_compare::{MultiCompareResult, MultiCompareSpec};

/// Statistik run terakhir yang ditampilkan di sidebar.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LastRun {
    /// Waktu lokal selesai, `YYYY-MM-DD HH:MM`.
    pub ran_at: String,
    pub duration_ms: u64,
    pub total_keys: usize,
    pub identical: usize,
    pub missing: usize,
    pub changed: usize,
    /// Jumlah baris per entri, urut seperti entri.
    pub rows: Vec<usize>,
    pub truncated: bool,
    /// Pesan bila run gagal; angka lain lalu tidak bermakna.
    pub error: Option<String>,
}

impl LastRun {
    fn now() -> String {
        chrono::Local::now().format("%Y-%m-%d %H:%M").to_string()
    }

    pub fn from_result(result: &MultiCompareResult, duration_ms: u64) -> Self {
        let (missing, changed) = result.counts();
        Self {
            ran_at: Self::now(),
            duration_ms,
            total_keys: result.total_keys,
            identical: result.identical,
            missing,
            changed,
            rows: result.sources.iter().map(|s| s.rows).collect(),
            truncated: result.truncated(),
            error: None,
        }
    }

    pub fn failed(error: &str, duration_ms: u64) -> Self {
        Self {
            ran_at: Self::now(),
            duration_ms,
            error: Some(error.to_string()),
            ..Default::default()
        }
    }

    pub fn differ(&self) -> usize {
        self.missing + self.changed
    }

    /// Total baris yang dibaca dari semua entri.
    pub fn rows_read(&self) -> usize {
        self.rows.iter().sum()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SavedMultiCompare {
    pub id: i64,
    pub spec: MultiCompareSpec,
    pub last_run: Option<LastRun>,
    pub updated_at: String,
}

pub async fn ensure_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS multi_compare_saved (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE COLLATE NOCASE,
            spec_json TEXT NOT NULL,
            last_run_json TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

type Row = (i64, String, Option<String>, Option<String>);

fn from_row((id, spec, last_run, updated): Row) -> SavedMultiCompare {
    let spec = serde_json::from_str(&spec).unwrap_or_else(|e| {
        log::warn!("[COMPARE] saved comparison {id} has an unreadable definition: {e}");
        MultiCompareSpec::default()
    });
    SavedMultiCompare {
        id,
        spec,
        last_run: last_run.and_then(|j| serde_json::from_str(&j).ok()),
        updated_at: updated.unwrap_or_default(),
    }
}

/// Semua comparison tersimpan, urut nama.
pub async fn list(pool: &SqlitePool) -> Result<Vec<SavedMultiCompare>, sqlx::Error> {
    ensure_table(pool).await?;
    let rows = sqlx::query_as::<_, Row>(
        "SELECT id, spec_json, last_run_json, CAST(updated_at AS TEXT)
         FROM multi_compare_saved ORDER BY name COLLATE NOCASE",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(from_row).collect())
}

pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<SavedMultiCompare>, sqlx::Error> {
    ensure_table(pool).await?;
    let row = sqlx::query_as::<_, Row>(
        "SELECT id, spec_json, last_run_json, CAST(updated_at AS TEXT)
         FROM multi_compare_saved WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(from_row))
}

/// Simpan definisi baru (`id = None`) atau timpa yang ada. Mengembalikan id.
/// Nama harus unik (tanpa beda huruf besar).
pub async fn save(
    pool: &SqlitePool,
    id: Option<i64>,
    spec: &MultiCompareSpec,
) -> Result<i64, String> {
    ensure_table(pool).await.map_err(|e| e.to_string())?;
    let name = spec.name.trim();
    let clash: Option<i64> =
        sqlx::query_scalar("SELECT id FROM multi_compare_saved WHERE name = ? COLLATE NOCASE")
            .bind(name)
            .fetch_optional(pool)
            .await
            .map_err(|e| e.to_string())?;
    if clash.is_some_and(|other| Some(other) != id) {
        return Err(format!("A comparison named \"{name}\" already exists"));
    }
    let mut spec = spec.clone();
    spec.name = name.to_string();
    let json = serde_json::to_string(&spec).map_err(|e| e.to_string())?;
    match id {
        Some(id) => {
            let done = sqlx::query(
                "UPDATE multi_compare_saved
                 SET name = ?, spec_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
            .bind(name)
            .bind(&json)
            .bind(id)
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
            if done.rows_affected() == 0 {
                return Err("The comparison no longer exists".to_string());
            }
            Ok(id)
        }
        None => {
            let done = sqlx::query(
                "INSERT INTO multi_compare_saved (name, spec_json, updated_at)
                 VALUES (?, ?, CURRENT_TIMESTAMP)",
            )
            .bind(name)
            .bind(&json)
            .execute(pool)
            .await
            .map_err(|e| e.to_string())?;
            Ok(done.last_insert_rowid())
        }
    }
}

pub async fn set_last_run(pool: &SqlitePool, id: i64, run: &LastRun) -> Result<(), sqlx::Error> {
    ensure_table(pool).await?;
    let json = serde_json::to_string(run).unwrap_or_default();
    sqlx::query("UPDATE multi_compare_saved SET last_run_json = ? WHERE id = ?")
        .bind(json)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), sqlx::Error> {
    ensure_table(pool).await?;
    sqlx::query("DELETE FROM multi_compare_saved WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_transfer::multi_compare::{CompareEntry, CompareVariable};

    async fn pool() -> SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    fn spec(name: &str) -> MultiCompareSpec {
        MultiCompareSpec {
            name: name.to_string(),
            variables: vec![CompareVariable {
                name: "d".to_string(),
                value: "2026-01-01".to_string(),
            }],
            entries: vec![
                CompareEntry {
                    connection_id: 1,
                    query: "SELECT 1".to_string(),
                    key_columns: vec!["id".to_string()],
                    aliases: vec![("a".to_string(), "b".to_string())],
                    ..Default::default()
                },
                CompareEntry {
                    connection_id: 2,
                    database: Some("shop".to_string()),
                    query: "{\"collection\":\"o\"}".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn save_list_update_and_delete() {
        let pool = pool().await;
        let id = save(&pool, None, &spec(" Orders ")).await.unwrap();
        let items = list(&pool).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].spec.name, "Orders");
        assert_eq!(items[0].spec.entries, spec("Orders").entries);
        assert!(items[0].last_run.is_none());

        // Nama kembar ditolak, tetapi menyimpan ulang dirinya sendiri boleh.
        assert!(save(&pool, None, &spec("orders")).await.is_err());
        assert_eq!(save(&pool, Some(id), &spec("Orders v2")).await.unwrap(), id);

        let run = LastRun {
            total_keys: 10,
            identical: 7,
            missing: 1,
            changed: 2,
            rows: vec![5, 6],
            ..Default::default()
        };
        set_last_run(&pool, id, &run).await.unwrap();
        let item = get(&pool, id).await.unwrap().unwrap();
        assert_eq!(item.spec.name, "Orders v2");
        assert_eq!(item.last_run.as_ref().unwrap().differ(), 3);
        assert_eq!(item.last_run.unwrap().rows_read(), 11);

        delete(&pool, id).await.unwrap();
        assert!(list(&pool).await.unwrap().is_empty());
        assert!(save(&pool, Some(id), &spec("x")).await.is_err());
    }
}
