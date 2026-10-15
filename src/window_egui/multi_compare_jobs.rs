//! Job background Multi-source Comparison. Job berjalan di runtime aplikasi
//! dan dimiliki `Tabular`, bukan tab, sehingga tetap jalan saat tab ditutup
//! atau user pindah ke pekerjaan lain. Keadaannya dicerminkan ke panel
//! Background Tasks; hasilnya dikirim ke tab pemilik dan statistiknya
//! disimpan ke `connections.db` untuk sidebar.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use eframe::egui;

use super::Tabular;
use super::background_tasks::{Snapshot, TaskId, TaskOwner};
use super::transfer_ui::Slot;
use crate::agent::harness::{ProgressStatus, ProgressStep};
use crate::data_transfer::compare::CompareProgressHandle;
use crate::data_transfer::multi_compare::{
    self, EntryTarget, MultiCompareResult, MultiCompareSpec, ResolvedEntry, entry_label,
};
use crate::data_transfer::multi_compare_saved::{self as saved, LastRun, SavedMultiCompare};
use crate::models::enums::DatabaseType;

pub struct CompareJob {
    pub id: u64,
    /// Tab yang memulai job (id stabil `QueryTab::id`); `None` = dari sidebar.
    pub tab_id: Option<usize>,
    pub saved_id: Option<i64>,
    pub name: String,
    started: Instant,
    progress: CompareProgressHandle,
    cancel: Arc<AtomicBool>,
    slot: Slot<Result<MultiCompareResult, String>>,
    task: Option<TaskId>,
    steps: Vec<ProgressStep>,
    /// Ringkasan (atau pesan gagal) setelah selesai.
    result: Option<Result<String, String>>,
}

impl CompareJob {
    pub fn is_running(&self) -> bool {
        self.result.is_none()
    }

    /// Teks progress untuk tab: "Reading MySQL (1/4)".
    pub fn progress_text(&self) -> String {
        let p = self.progress.lock().map(|p| p.clone()).unwrap_or_default();
        let mut text = if p.current_table.is_empty() {
            "Starting".to_string()
        } else {
            p.current_table
        };
        if p.tables_total > 0 {
            text.push_str(&format!(" ({}/{})", p.tables_done, p.tables_total));
        }
        if self.cancel.load(Ordering::Relaxed) {
            text.push_str(" · Cancelling…");
        }
        text
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

/// State Comparison milik `Tabular`: daftar tersimpan untuk sidebar, job
/// yang berjalan, dan hasil terakhir per comparison tersimpan.
#[derive(Default)]
pub struct MultiCompareUi {
    pub saved: Vec<SavedMultiCompare>,
    pub saved_loaded: bool,
    pub jobs: Vec<CompareJob>,
    next_job: u64,
    /// Hasil terakhir per id comparison, untuk tab yang dibuka lagi.
    pub results: HashMap<i64, Arc<MultiCompareResult>>,
    /// Comparison yang menunggu konfirmasi hapus: `(id, nama)`.
    pub confirm_delete: Option<(i64, String)>,
}

impl MultiCompareUi {
    pub fn job_for_tab(&self, tab_id: usize, saved_id: Option<i64>) -> Option<&CompareJob> {
        self.jobs
            .iter()
            .filter(|j| j.is_running())
            .find(|j| j.tab_id == Some(tab_id) || (saved_id.is_some() && j.saved_id == saved_id))
    }

    pub fn running_for_saved(&self, saved_id: i64) -> Option<&CompareJob> {
        self.jobs
            .iter()
            .find(|j| j.is_running() && j.saved_id == Some(saved_id))
    }
}

/// Muat ulang daftar comparison tersimpan dari `connections.db`.
pub fn reload_saved(t: &mut Tabular) {
    t.multi_compare.saved_loaded = true;
    let Some(pool) = t.db_pool.clone() else {
        return;
    };
    match t.get_runtime().block_on(saved::list(pool.as_ref())) {
        Ok(items) => t.multi_compare.saved = items,
        Err(e) => log::warn!("[COMPARE] loading saved comparisons failed: {e}"),
    }
}

/// Cari koneksi tiap entri dan siapkan target eksekusinya.
fn resolve_entries(t: &Tabular, spec: &MultiCompareSpec) -> Result<Vec<ResolvedEntry>, String> {
    spec.entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let label = entry_label(entry, i);
            let conn = t
                .connections
                .iter()
                .find(|c| c.id == Some(entry.connection_id))
                .ok_or_else(|| format!("{label}: the connection no longer exists"))?;
            let database = entry.database.clone().unwrap_or_default();
            let target = match conn.connection_type {
                DatabaseType::MongoDB => {
                    let pool = t
                        .connection_pools
                        .get(&entry.connection_id)
                        .cloned()
                        .or_else(|| {
                            crate::connection::pool::lock_or_recover(&t.shared_connection_pools)
                                .get(&entry.connection_id)
                                .cloned()
                        });
                    let database = if database.trim().is_empty() {
                        conn.database.clone()
                    } else {
                        database
                    };
                    EntryTarget::Mongo {
                        conn: Box::new(conn.clone()),
                        pool,
                        database,
                    }
                }
                ref other if multi_compare::supports_engine(other) => EntryTarget::Sql(Box::new(
                    t.transfer_endpoint(entry.connection_id, &database)
                        .ok_or_else(|| format!("{label}: the connection no longer exists"))?,
                )),
                ref other => {
                    return Err(format!(
                        "{label}: {} connections cannot be compared",
                        other.as_db_str()
                    ));
                }
            };
            Ok(ResolvedEntry {
                label,
                target,
                query: entry.query.clone(),
                key_columns: entry.key_columns.clone(),
                aliases: entry.aliases.clone(),
            })
        })
        .collect()
}

/// Mulai comparison di background. Gagal bila definisinya tidak sah atau
/// comparison yang sama masih berjalan.
pub fn start(
    t: &mut Tabular,
    ctx: &egui::Context,
    tab_id: Option<usize>,
    saved_id: Option<i64>,
    spec: &MultiCompareSpec,
) -> Result<(), String> {
    spec.validate()?;
    let busy = t.multi_compare.jobs.iter().any(|j| {
        j.is_running()
            && ((tab_id.is_some() && j.tab_id == tab_id)
                || (saved_id.is_some() && j.saved_id == saved_id))
    });
    if busy {
        return Err("This comparison is already running".to_string());
    }
    let entries = resolve_entries(t, spec)?;
    let progress = CompareProgressHandle::default();
    let cancel = Arc::new(AtomicBool::new(false));
    let variables = spec.variables.clone();
    let options = spec.options.clone();
    let slot = {
        let progress = progress.clone();
        let cancel = cancel.clone();
        t.spawn_slot(ctx, async move {
            multi_compare::run(entries, variables, options, progress, cancel).await
        })
    };
    t.multi_compare.next_job += 1;
    let name = spec.name.trim().to_string();
    log::info!(
        "[COMPARE] started \"{}\" with {} queries",
        name,
        spec.entries.len()
    );
    t.multi_compare.jobs.push(CompareJob {
        id: t.multi_compare.next_job,
        tab_id,
        saved_id,
        name,
        started: Instant::now(),
        progress,
        cancel,
        slot,
        task: None,
        steps: Vec::new(),
        result: None,
    });
    Ok(())
}

/// Hentikan job milik tab atau comparison tersimpan ini.
pub fn cancel(t: &mut Tabular, tab_id: usize, saved_id: Option<i64>) {
    if let Some(job) = t.multi_compare.job_for_tab(tab_id, saved_id) {
        job.cancel();
    }
}

/// Dipanggil setiap frame dari `app_impl`.
pub fn tick(t: &mut Tabular, ctx: &egui::Context) {
    if t.multi_compare.jobs.is_empty() {
        return;
    }
    poll(t);
    mirror_tasks(t);
    if t.multi_compare.jobs.iter().any(CompareJob::is_running) {
        // Progress dibaca dari state bersama; cukup beberapa kali per detik.
        ctx.request_repaint_after(Duration::from_millis(250));
    }
}

fn poll(t: &mut Tabular) {
    let mut finished = Vec::new();
    for job in t.multi_compare.jobs.iter_mut().filter(|j| j.is_running()) {
        let text = job.progress_text();
        job.steps = vec![ProgressStep {
            step_index: None,
            description: text,
            detail: None,
            status: ProgressStatus::Active,
            tool_name: None,
        }];
        if let Some(result) = job.slot.take() {
            let duration = job.started.elapsed();
            job.result = Some(match &result {
                Ok(r) => Ok(r.summary()),
                Err(e) => Err(e.clone()),
            });
            if let Some(step) = job.steps.first_mut() {
                step.status = if result.is_ok() {
                    ProgressStatus::Done
                } else {
                    ProgressStatus::Error
                };
            }
            finished.push((job.tab_id, job.saved_id, job.name.clone(), duration, result));
        }
    }
    for (tab_id, saved_id, name, duration, result) in finished {
        deliver(t, tab_id, saved_id, &name, duration, result);
    }
}

/// Kirim hasil ke tab pemilik, simpan statistik, dan beri tahu user.
fn deliver(
    t: &mut Tabular,
    tab_id: Option<usize>,
    saved_id: Option<i64>,
    name: &str,
    duration: Duration,
    result: Result<MultiCompareResult, String>,
) {
    let ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    let run = match &result {
        Ok(r) => LastRun::from_result(r, ms),
        Err(e) => LastRun::failed(e, ms),
    };
    match &result {
        Ok(r) => log::info!("[COMPARE] \"{name}\" finished: {}", r.summary()),
        Err(e) => log::warn!("[COMPARE] \"{name}\" failed: {e}"),
    }
    let shared = result.map(Arc::new);
    if let (Some(id), Ok(r)) = (saved_id, &shared) {
        t.multi_compare.results.insert(id, r.clone());
    }
    if let Some(id) = saved_id {
        if let Some(item) = t.multi_compare.saved.iter_mut().find(|s| s.id == id) {
            item.last_run = Some(run.clone());
        }
        if let Some(pool) = t.db_pool.clone()
            && let Err(e) = t
                .get_runtime()
                .block_on(saved::set_last_run(pool.as_ref(), id, &run))
        {
            log::warn!("[COMPARE] saving run statistics failed: {e}");
        }
    }

    let mut visible = false;
    let active_id = t.query_tabs.get(t.active_tab_index).map(|tab| tab.id);
    for tab in t.query_tabs.iter_mut() {
        let Some(state) = tab.compare_state.as_mut() else {
            continue;
        };
        let owns = tab_id == Some(tab.id) || (saved_id.is_some() && state.saved_id == saved_id);
        if !owns {
            continue;
        }
        state.set_result(shared.clone());
        visible |= active_id == Some(tab.id);
    }
    if !visible {
        match &shared {
            Ok(r) => t
                .toasts
                .success(format!("Comparison \"{name}\" finished: {}", r.summary())),
            Err(e) if e == "Cancelled" => t.toasts.info(format!("Comparison \"{name}\" cancelled")),
            Err(e) => t.toasts.error(format!("Comparison \"{name}\" failed: {e}")),
        }
    }
}

fn mirror_tasks(t: &mut Tabular) {
    let bg = &mut t.background_tasks;
    let mut show: Option<(Option<usize>, Option<i64>)> = None;
    for job in t.multi_compare.jobs.iter_mut() {
        let out = bg.mirror(
            &mut job.task,
            Snapshot {
                owner: TaskOwner::Compare,
                title: &job.name,
                subtitle: "Multi-source comparison",
                steps: &job.steps,
                started_at: Some(job.started),
                last_activity_at: None,
                hidden: true,
                result: job.result.clone(),
            },
        );
        if out.show {
            show = Some((job.tab_id, job.saved_id));
        }
        if out.cancel {
            job.cancel();
        }
    }
    // Job selesai dibuang setelah entrinya hilang dari panel.
    t.multi_compare
        .jobs
        .retain(|j| j.is_running() || j.task.is_some_and(|id| bg.contains(id)));
    if let Some((tab_id, saved_id)) = show {
        super::multi_compare_ui::show_job_owner(t, tab_id, saved_id);
    }
}
