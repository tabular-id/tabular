//! Job database yang dijalankan worker latar (`init.rs::start_background_worker`):
//! refresh koneksi, ambil daftar database, buka pool koneksi, struktur tabel,
//! kunci Redis, tes koneksi.
//!
//! Worker mencatat mulai/selesai tiap job di [`DbJobs`] (dipakai bersama antar
//! thread); UI mencerminkannya ke panel Background Processes lewat
//! [`sync_db_jobs`] sekali per frame. Job sukses hilang sendiri setelah
//! [`KEEP_DONE_FOR`]; job gagal tetap tampil sampai user membuangnya.

use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use eframe::egui;

use super::Tabular;
use super::background_tasks::{Snapshot, TaskId, TaskOwner, format_duration};
use crate::models::enums::BackgroundResult;

/// Lama entri sukses tetap terlihat di panel sebelum dibuang otomatis.
const KEEP_DONE_FOR: Duration = Duration::from_secs(4);

/// Sasaran job: koneksi tersimpan (namanya dicari saat ditampilkan) atau
/// nama langsung (mis. koneksi baru yang sedang dites).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbJobTarget {
    Connection(i64),
    Named(String),
}

#[derive(Debug)]
struct DbJob {
    id: u64,
    label: &'static str,
    target: DbJobTarget,
    detail: String,
    started_at: Instant,
    /// Waktu selesai dan hasilnya; `None` = masih berjalan.
    outcome: Option<(Instant, Result<(), String>)>,
    /// Entri panel untuk job ini.
    task: Option<TaskId>,
}

#[derive(Debug, Default)]
struct Inner {
    jobs: Vec<DbJob>,
    next_id: u64,
    ctx: Option<egui::Context>,
}

/// Daftar job database bersama antara worker latar dan UI.
#[derive(Clone, Debug, Default)]
pub struct DbJobs {
    inner: Arc<Mutex<Inner>>,
}

impl DbJobs {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Catat job baru yang mulai berjalan.
    pub fn start(
        &self,
        label: &'static str,
        target: DbJobTarget,
        detail: impl Into<String>,
    ) -> DbJobHandle {
        let mut inner = self.lock();
        inner.next_id += 1;
        let id = inner.next_id;
        inner.jobs.push(DbJob {
            id,
            label,
            target,
            detail: detail.into(),
            started_at: Instant::now(),
            outcome: None,
            task: None,
        });
        // UI mungkin sedang diam; bangunkan supaya job langsung terlihat.
        if let Some(ctx) = &inner.ctx {
            ctx.request_repaint();
        }
        DbJobHandle {
            jobs: self.clone(),
            id,
            error: Arc::new(Mutex::new(None)),
        }
    }

    fn finish(&self, id: u64, outcome: Result<(), String>) {
        let mut inner = self.lock();
        if let Some(job) = inner.jobs.iter_mut().find(|j| j.id == id) {
            job.outcome.get_or_insert((Instant::now(), outcome));
        }
        if let Some(ctx) = &inner.ctx {
            ctx.request_repaint();
        }
    }

    /// Jumlah job yang masih berjalan.
    pub fn running_count(&self) -> usize {
        self.lock()
            .jobs
            .iter()
            .filter(|j| j.outcome.is_none())
            .count()
    }
}

/// Pegangan satu job di thread task-nya. Job dianggap selesai saat pegangan
/// ini dibuang: gagal bila ada hasil gagal yang terkirim (lewat
/// [`TrackedSender`]) atau [`DbJobHandle::fail`] dipanggil, sukses bila tidak.
#[derive(Debug)]
pub struct DbJobHandle {
    jobs: DbJobs,
    id: u64,
    error: Arc<Mutex<Option<String>>>,
}

impl DbJobHandle {
    /// Pengirim hasil yang juga mencatat kegagalan job ini.
    pub fn sender(&self, inner: &Sender<BackgroundResult>) -> TrackedSender {
        TrackedSender {
            inner: inner.clone(),
            error: self.error.clone(),
        }
    }

    /// Tandai job gagal (mis. task panik).
    pub fn fail(&self, message: impl Into<String>) {
        *self.error.lock().unwrap_or_else(PoisonError::into_inner) = Some(message.into());
    }
}

impl Drop for DbJobHandle {
    fn drop(&mut self) {
        let error = self
            .error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.jobs.finish(self.id, error.map_or(Ok(()), Err));
    }
}

/// `Sender<BackgroundResult>` yang mencatat hasil gagal ke job-nya.
#[derive(Debug)]
pub struct TrackedSender {
    inner: Sender<BackgroundResult>,
    error: Arc<Mutex<Option<String>>>,
}

impl TrackedSender {
    /// Kirim hasil ke UI; `false` bila UI sudah tidak menerima.
    pub fn send(&self, result: BackgroundResult) -> bool {
        if let Some(message) = failure_message(&result) {
            *self.error.lock().unwrap_or_else(PoisonError::into_inner) = Some(message);
        }
        self.inner.send(result).is_ok()
    }
}

/// Pesan gagal yang terkandung dalam hasil worker, bila ada.
pub fn failure_message(result: &BackgroundResult) -> Option<String> {
    match result {
        BackgroundResult::ConnectionFailed { error_message, .. } => Some(error_message.clone()),
        BackgroundResult::RefreshComplete { success: false, .. } => {
            Some("Could not refresh the connection".to_string())
        }
        BackgroundResult::TestConnectionComplete {
            success: false,
            message,
        } => Some(message.clone()),
        BackgroundResult::TableStructureFetched {
            columns: None,
            columns_detail: None,
            ..
        } => Some("Could not load the table structure".to_string()),
        BackgroundResult::RedisBrowserStateFetched { state, .. } => state.last_error.clone(),
        _ => None,
    }
}

/// Cerminkan job database ke panel Background Processes. Dipanggil sekali
/// per frame dari UI thread.
pub fn sync_db_jobs(t: &mut Tabular) {
    let jobs = t.db_jobs.clone();
    let mut inner = jobs.lock();
    if inner.ctx.is_none() {
        inner.ctx = t.egui_ctx.clone();
    }
    if inner.jobs.is_empty() {
        return;
    }
    // Nama koneksi dicari dulu: `get_connection_name` meminjam `t` utuh.
    let titles: Vec<String> = inner
        .jobs
        .iter()
        .map(|job| {
            let target = match &job.target {
                DbJobTarget::Connection(id) => t
                    .get_connection_name(*id)
                    .unwrap_or_else(|| format!("Connection #{id}")),
                DbJobTarget::Named(name) => name.clone(),
            };
            format!("{}: {}", job.label, target)
        })
        .collect();

    let bg = &mut t.background_tasks;
    let now = Instant::now();
    for (job, title) in inner.jobs.iter_mut().zip(&titles) {
        let result = job.outcome.as_ref().map(|(at, r)| match r {
            Ok(()) => Ok(format!(
                "Finished in {}",
                format_duration(at.saturating_duration_since(job.started_at))
            )),
            Err(e) => Err(e.clone()),
        });
        let snap = |result| Snapshot {
            owner: TaskOwner::Database,
            title,
            subtitle: &job.detail,
            steps: &[],
            started_at: Some(job.started_at),
            last_activity_at: None,
            hidden: true,
            result,
        };
        // Job yang gagal sebelum sempat tercatat tetap harus terlihat; job
        // sukses yang secepat itu tidak perlu ditampilkan.
        if job.task.is_none() && matches!(result, Some(Err(_))) {
            bg.mirror(&mut job.task, snap(None));
        }
        bg.mirror(&mut job.task, snap(result));
    }
    inner.jobs.retain(|job| match &job.outcome {
        None => true,
        // Entri gagal disimpan registry sampai user membuangnya.
        Some((_, Err(_))) => false,
        Some((at, Ok(()))) => {
            if now.saturating_duration_since(*at) < KEEP_DONE_FOR {
                return true;
            }
            if let Some(id) = job.task {
                bg.dismiss(id);
            }
            false
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_the_handle_finishes_the_job() {
        let jobs = DbJobs::default();
        let handle = jobs.start("Refreshing", DbJobTarget::Connection(1), "");
        assert_eq!(jobs.running_count(), 1);
        drop(handle);
        assert_eq!(jobs.running_count(), 0);
        let inner = jobs.lock();
        assert!(matches!(inner.jobs[0].outcome, Some((_, Ok(())))));
    }

    #[test]
    fn a_failed_result_marks_the_job_failed() {
        let jobs = DbJobs::default();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = jobs.start("Loading databases", DbJobTarget::Connection(7), "");
        let sender = handle.sender(&tx);
        assert!(sender.send(BackgroundResult::ConnectionFailed {
            connection_id: 7,
            error_message: "Login failed".into(),
        }));
        drop(handle);
        assert!(rx.try_recv().is_ok());
        let inner = jobs.lock();
        assert_eq!(
            inner.jobs[0].outcome.as_ref().map(|(_, r)| r.clone()),
            Some(Err("Login failed".to_string()))
        );
    }

    #[test]
    fn fail_overrides_success() {
        let jobs = DbJobs::default();
        let handle = jobs.start("Connecting", DbJobTarget::Named("local".into()), "");
        handle.fail("panic");
        drop(handle);
        let inner = jobs.lock();
        assert!(matches!(&inner.jobs[0].outcome, Some((_, Err(m))) if m == "panic"));
    }

    #[test]
    fn successful_results_are_not_failures() {
        assert_eq!(
            failure_message(&BackgroundResult::DatabasesFetched {
                connection_id: 1,
                databases: vec!["a".into()],
            }),
            None
        );
        assert!(
            failure_message(&BackgroundResult::RefreshComplete {
                connection_id: 1,
                success: false,
                databases: Vec::new(),
            })
            .is_some()
        );
    }
}
