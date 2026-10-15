//! Catatan repository yang sudah di-setup otomatis, disimpan personal di
//! `{data_dir}/repo_setups.json` (tidak ikut sync; path lokal dan id koneksi
//! hanya berlaku di komputer ini). File rusak dikarantina seperti
//! `git_repos.json`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const FILE_NAME: &str = "repo_setups.json";

/// Koneksi yang dibuat/diadopsi dari repository.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackedConnection {
    /// `environment|file#KEY`, stabil walau password berubah.
    pub identity: String,
    #[serde(default)]
    pub connection_id: Option<i64>,
    pub name: String,
    pub environment: String,
    /// Hash DSN lengkap; berubah bila host/port/user/password berubah.
    pub digest: String,
    #[serde(default)]
    pub needs_credentials: bool,
    /// Koneksi milik user yang sudah ada (cocok server/database) dan hanya
    /// dipakai ulang: tidak pernah diubah oleh sinkronisasi.
    #[serde(default)]
    pub adopted: bool,
    /// Tidak ditemukan lagi di repository (tidak dihapus otomatis).
    #[serde(default)]
    pub stale: bool,
}

/// Service API dan folder HTTP-nya.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackedService {
    pub name: String,
    pub rel_dir: String,
    #[serde(default)]
    pub folder_id: Option<String>,
    #[serde(default)]
    pub endpoints: usize,
    #[serde(default)]
    pub stale: bool,
}

/// Diagram database yang dikelola (group per service).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackedDiagram {
    pub connection_id: i64,
    pub database: String,
    #[serde(default)]
    pub groups: usize,
    #[serde(default)]
    pub tables: usize,
    /// `service|id tabel` yang pernah ditempatkan; tidak ditambahkan lagi
    /// bila user mengeluarkannya dari group.
    #[serde(default)]
    pub placed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepoSetup {
    /// Kunci repository yang sama dengan tab Git (URL ternormalisasi atau
    /// `path:{folder}`).
    pub key: String,
    pub path: String,
    pub name: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default = "yes")]
    pub auto_sync: bool,
    #[serde(default)]
    pub fingerprint: String,
    /// Detik Unix sinkronisasi terakhir.
    #[serde(default)]
    pub last_sync_at: i64,
    #[serde(default)]
    pub connections: Vec<TrackedConnection>,
    #[serde(default)]
    pub services: Vec<TrackedService>,
    #[serde(default)]
    pub diagrams: Vec<TrackedDiagram>,
    /// File env/konfigurasi yang dipakai (relatif), untuk sidik jari.
    #[serde(default)]
    pub watched_files: Vec<String>,
    /// Hash nilai `.env` terakhir yang diimpor per `environment|KEY`.
    /// Variabel project hanya ditimpa bila nilai di repository berubah,
    /// sehingga editan user tetap bertahan.
    #[serde(default)]
    pub imported_vars: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub last_summary: String,
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl RepoSetup {
    pub fn new(key: &str, path: &str, name: &str) -> Self {
        Self {
            key: key.to_string(),
            path: path.to_string(),
            name: name.to_string(),
            project_id: None,
            auto_sync: true,
            fingerprint: String::new(),
            last_sync_at: 0,
            connections: Vec::new(),
            services: Vec::new(),
            diagrams: Vec::new(),
            watched_files: Vec::new(),
            imported_vars: Default::default(),
            last_summary: String::new(),
            warnings: Vec::new(),
        }
    }

    pub fn stale_count(&self) -> usize {
        self.connections.iter().filter(|c| c.stale).count()
            + self.services.iter().filter(|s| s.stale).count()
    }
}

fn yes() -> bool {
    true
}

fn default_interval() -> u32 {
    5
}

/// Pengaturan repository setup (Preferences > Git).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetupSettings {
    /// Interval pemeriksaan perubahan (menit); 0 = hanya saat jendela fokus
    /// dan manual.
    #[serde(default = "default_interval")]
    pub interval_min: u32,
    /// Tampilkan ringkasan dulu sebelum perubahan diterapkan.
    #[serde(default)]
    pub review_before_apply: bool,
    /// Dokumentasikan endpoint baru dengan AI (memakai backend AI aktif).
    #[serde(default)]
    pub ai_enrich: bool,
    /// Buka tab diagram database saat setup pertama.
    #[serde(default = "yes")]
    pub open_diagrams: bool,
}

impl Default for SetupSettings {
    fn default() -> Self {
        Self {
            interval_min: default_interval(),
            review_before_apply: false,
            ai_enrich: false,
            open_diagrams: true,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FileBody {
    #[serde(default)]
    setups: Vec<RepoSetup>,
    #[serde(default)]
    settings: SetupSettings,
}

#[derive(Debug)]
pub struct RepoSetupStore {
    file: PathBuf,
    pub setups: Vec<RepoSetup>,
    pub settings: SetupSettings,
}

impl Default for RepoSetupStore {
    fn default() -> Self {
        Self::load(Self::default_file())
    }
}

impl RepoSetupStore {
    pub fn load(file: PathBuf) -> Self {
        let body = match std::fs::read(&file) {
            Ok(bytes) => serde_json::from_slice::<FileBody>(&bytes).unwrap_or_else(|e| {
                log::warn!("[REPO_SETUP] ignoring unreadable {}: {e}", file.display());
                let _ = crate::directory::quarantine_corrupt_file(&file);
                FileBody::default()
            }),
            Err(_) => FileBody::default(),
        };
        Self {
            file,
            setups: body.setups,
            settings: body.settings,
        }
    }

    pub fn default_file() -> PathBuf {
        crate::config::get_data_dir().join(FILE_NAME)
    }

    pub fn save(&self) -> std::io::Result<()> {
        if let Some(parent) = self.file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = FileBody {
            setups: self.setups.clone(),
            settings: self.settings.clone(),
        };
        let json = serde_json::to_vec_pretty(&body).map_err(std::io::Error::other)?;
        crate::directory::write_file_atomically(&self.file, &json)
    }

    pub fn get(&self, key: &str) -> Option<&RepoSetup> {
        self.setups.iter().find(|s| s.key == key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut RepoSetup> {
        self.setups.iter_mut().find(|s| s.key == key)
    }

    /// Setup untuk `key`, dibuat bila belum ada.
    pub fn entry(&mut self, key: &str, path: &str, name: &str) -> &mut RepoSetup {
        if let Some(i) = self.setups.iter().position(|s| s.key == key) {
            let s = &mut self.setups[i];
            s.path = path.to_string();
            return s;
        }
        self.setups.push(RepoSetup::new(key, path, name));
        let last = self.setups.len() - 1;
        &mut self.setups[last]
    }

    pub fn remove(&mut self, key: &str) {
        self.setups.retain(|s| s.key != key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_and_quarantines_corrupt_file() {
        let dir = std::env::temp_dir().join(format!("tabular-setup-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let file = dir.join(FILE_NAME);
        let mut s = RepoSetupStore::load(file.clone());
        assert!(s.setups.is_empty());
        assert_eq!(s.settings.interval_min, 5);
        s.entry("github.com/o/a", "/tmp/a", "a")
            .connections
            .push(TrackedConnection {
                identity: "development|.env#DATABASE_URL".into(),
                connection_id: Some(3),
                ..Default::default()
            });
        s.entry("github.com/o/a", "/tmp/a2", "a");
        assert_eq!(s.setups.len(), 1);
        assert_eq!(s.setups[0].path, "/tmp/a2");
        s.settings.ai_enrich = true;
        s.save().expect("save");
        let s2 = RepoSetupStore::load(file.clone());
        assert_eq!(s2.setups, s.setups);
        assert!(s2.settings.ai_enrich);
        assert!(s2.get("github.com/o/a").is_some_and(|x| x.auto_sync));
        std::fs::write(&file, "{oops").expect("write");
        assert!(RepoSetupStore::load(file.clone()).setups.is_empty());
        assert!(!file.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
