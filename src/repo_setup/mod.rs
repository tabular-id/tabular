//! Repository setup otomatis: dari satu folder git, temukan koneksi database
//! (`.env`, docker-compose, konfigurasi framework), service API beserta
//! endpoint-nya (monolith atau microservices), dan variabel environment;
//! lalu UI (`window_egui::repo_setup_jobs`) membentuk project, koneksi,
//! folder HTTP API, dan group diagram sekaligus. Watcher latar memakai
//! [`fingerprint`] untuk menjalankan ulang discovery hanya bila repository
//! berubah.
//!
//! Modul ini headless: tidak bergantung pada `Tabular`, sehingga bisa diuji
//! dan dijalankan di thread latar. Nilai rahasia dari `.env` tidak pernah
//! dicatat ke log atau dikirim ke AI.

pub mod compose;
pub mod config_files;
pub mod diagram_groups;
pub mod diff;
pub mod discover;
pub mod dsn_detect;
pub mod env_files;
pub mod fingerprint;
pub mod services;
pub mod store;
pub mod yaml;

pub use discover::{PlannedConnection, SetupPlan, discover};

/// Error domain repository setup.
#[derive(Debug, thiserror::Error)]
pub enum RepoSetupError {
    #[error("{0} is not inside a git repository")]
    NotARepo(String),
    #[error("Scanning the repository failed: {0}")]
    Scan(String),
    #[error("Cancelled")]
    Cancelled,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
