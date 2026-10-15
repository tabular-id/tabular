//! Job background Integration Testing: run suite berurutan (dengan fase
//! cleanup), verifikasi sisa data di database, dan generate langkah dengan AI.
//!
//! Job dimiliki `Tabular`, bukan tab, sehingga tetap berjalan saat tab ditutup.
//! Keadaannya dicerminkan ke panel Background Tasks. Tab membaca hasil lewat
//! `suite_id`.

use std::collections::HashMap;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use eframe::egui;

use super::Tabular;
use super::background_tasks::{Snapshot, TaskId, TaskOwner};
use crate::agent::harness::{ProgressStatus, ProgressStep};
use crate::http_tests::{
    self, DbCheckTarget, DbResidueCheck, HttpTestStep, HttpTestSuite, RunScope, StepGenHandle,
    StepGenMode, StepResult, TestEvent, TestGenHandle, TestRun, TestRunHandle,
};
use crate::repo_scan::RepoJobEvent;

/// Hasil generate langkah: langkah baru + variabel tambahan.
pub type GenResult = Result<(Vec<HttpTestStep>, Vec<(String, String)>), String>;

/// Run suite yang sedang berjalan.
pub struct RunJob {
    pub suite: HttpTestSuite,
    pub scope: RunScope,
    pub run_id: String,
    started_at: chrono::DateTime<chrono::Utc>,
    started: Instant,
    handle: TestRunHandle,
    /// Hasil per id langkah.
    pub results: HashMap<String, StepResult>,
    /// Indeks langkah (di `suite.steps`) yang sedang dikirim.
    pub current: Option<usize>,
    pub in_cleanup: bool,
    project_name: Option<String>,
    environment: Option<String>,
    db_targets: Vec<DbCheckTarget>,
    task: Option<TaskId>,
    steps: Vec<ProgressStep>,
    /// Selesai: ringkasan (Ok) atau pesan gagal (Err).
    finished: Option<Result<String, String>>,
}

impl RunJob {
    pub fn is_running(&self) -> bool {
        self.finished.is_none()
    }

    pub fn is_cancelling(&self) -> bool {
        self.handle.is_cancelling()
    }

    /// "Running 3/12: Create PO" atau "Cleaning up test data".
    pub fn progress_text(&self) -> String {
        let total = (0..self.suite.steps.len())
            .filter(|i| self.scope.includes(*i))
            .count();
        let done = self.results.len();
        let mut text = if self.in_cleanup {
            format!("Cleaning up test data ({done}/{total})")
        } else {
            format!("Running {done}/{total}")
        };
        if let Some(name) = self
            .current
            .and_then(|i| self.suite.steps.get(i))
            .map(|s| s.name.as_str())
        {
            text.push_str(&format!(": {name}"));
        }
        if self.is_cancelling() && !self.in_cleanup {
            text.push_str(" (stopping, cleanup will still run)");
        }
        text
    }
}

/// Verifikasi database setelah run.
struct DbCheckJob {
    suite_id: String,
    run: TestRun,
    rx: mpsc::Receiver<Vec<DbResidueCheck>>,
}

/// Generate langkah untuk suite yang ada.
pub struct GenJob {
    pub mode: StepGenMode,
    handle: StepGenHandle,
    pub started: Instant,
    pub status: String,
}

/// Generate suite baru untuk project.
pub struct SuiteGenJob {
    project_id: Option<String>,
    handle: TestGenHandle,
    pub started: Instant,
    pub status: String,
}

/// State Integration Testing milik `Tabular`.
#[derive(Default)]
pub struct IntegrationTestUi {
    pub runs: HashMap<String, RunJob>,
    db_checks: Vec<DbCheckJob>,
    pub gens: HashMap<String, GenJob>,
    pub suite_gen: Option<SuiteGenJob>,
    /// Hasil generate langkah yang belum diambil tab (per suite).
    pub gen_results: HashMap<String, GenResult>,
    /// Run terakhir yang selesai per suite.
    pub finished: HashMap<String, TestRun>,
    /// Naik setiap ada run tersimpan; tab memuat ulang riwayatnya.
    pub runs_rev: u64,
}

// ─── Konteks project ────────────────────────────────────────────────────────

/// Project pemilik suite, atau project aktif bila suite belum terikat.
pub fn suite_project<'a>(
    t: &'a Tabular,
    suite: &HttpTestSuite,
) -> Option<&'a crate::project::Project> {
    match &suite.project_id {
        Some(id) => t.projects.list.iter().find(|p| &p.id == id),
        None => super::project_ui::active(t),
    }
}

/// Variabel environment project untuk suite (dengan secret dari keychain).
pub fn project_vars(t: &mut Tabular, suite: &HttpTestSuite) -> HashMap<String, String> {
    let ws = suite_project(t, suite).and_then(|p| p.http_workspace_id.clone());
    super::project_ui::http_vars_for_workspace(t, ws.as_deref())
}

/// Nama variabel environment project (tanpa nilai) untuk lint.
pub fn project_var_names(t: &Tabular, suite: &HttpTestSuite) -> std::collections::HashSet<String> {
    suite_project(t, suite)
        .and_then(|p| p.active_environment())
        .map(|e| {
            e.variables
                .iter()
                .map(|v| v.key.trim().to_string())
                .filter(|k| !k.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Folder request workspace HTTP project (untuk AI dan cakupan). Tanpa
/// workspace project: semua workspace.
pub fn project_catalogs(t: &Tabular, suite: &HttpTestSuite) -> Vec<http_tests::FolderCatalog> {
    let ws_id = suite_project(t, suite).and_then(|p| p.http_workspace_id.clone());
    let mut out = Vec::new();
    for ws in &t.yaak_workspaces {
        if ws_id.as_deref().is_some_and(|id| id != ws.id) {
            continue;
        }
        for f in &ws.folders {
            let c = http_tests::FolderCatalog::from_folder(f);
            if !c.requests.is_empty() {
                out.push(c);
            }
        }
    }
    out
}

/// Semua request katalog (datar).
pub fn project_requests(
    t: &Tabular,
    suite: &HttpTestSuite,
) -> Vec<crate::http_collection::SavedRequest> {
    project_catalogs(t, suite)
        .into_iter()
        .flat_map(|c| c.requests)
        .collect()
}

fn db_targets(t: &Tabular, suite: &HttpTestSuite) -> Vec<DbCheckTarget> {
    let Some(p) = suite_project(t, suite) else {
        return Vec::new();
    };
    p.member_connections(&t.connections)
        .into_iter()
        .filter_map(|c| {
            Some(DbCheckTarget {
                connection_id: c.id?,
                connection_name: c.name.clone(),
            })
        })
        .collect()
}

// ─── Run ────────────────────────────────────────────────────────────────────

/// Mulai run suite (atau sebagian group-nya). Gagal bila suite ini sedang berjalan.
pub fn start_run(t: &mut Tabular, suite: HttpTestSuite, scope: RunScope) -> Result<(), String> {
    if t.integration_tests
        .runs
        .get(&suite.id)
        .is_some_and(RunJob::is_running)
    {
        return Err("This suite is already running".into());
    }
    let env_vars = project_vars(t, &suite);
    let vars = http_tests::initial_vars_with(&suite, &env_vars);
    let run_id = vars.get("run_id").cloned().unwrap_or_default();
    let project = suite_project(t, &suite);
    let project_name = project.map(|p| p.name.clone());
    let environment = project
        .and_then(|p| p.active_environment())
        .map(|e| e.name.clone());
    let targets = db_targets(t, &suite);
    log::info!(
        "[INTEGRATION_TEST] start '{}' run {run_id} groups={:?} env={:?}",
        suite.name,
        scope.groups,
        environment
    );
    let handle = http_tests::spawn_run(suite.clone(), scope.clone(), vars);
    t.integration_tests.runs.insert(
        suite.id.clone(),
        RunJob {
            suite,
            scope,
            run_id,
            started_at: chrono::Utc::now(),
            started: Instant::now(),
            handle,
            results: HashMap::new(),
            current: None,
            in_cleanup: false,
            project_name,
            environment,
            db_targets: targets,
            task: None,
            steps: Vec::new(),
            finished: None,
        },
    );
    Ok(())
}

/// Hentikan langkah Test; cleanup tetap dijalankan.
pub fn stop_run(t: &mut Tabular, suite_id: &str) {
    if let Some(job) = t.integration_tests.runs.get(suite_id) {
        job.handle.cancel();
    }
}

/// Hentikan seketika termasuk cleanup.
pub fn abort_run(t: &mut Tabular, suite_id: &str) {
    if let Some(job) = t.integration_tests.runs.get(suite_id) {
        job.handle.abort();
    }
}

// ─── Generate ───────────────────────────────────────────────────────────────

/// Backend AI aktif, atau pesan kenapa belum siap.
fn backend(t: &Tabular) -> Result<(crate::ai_assistant::ChatBackend, String), String> {
    let target = t.effective_chat_target();
    crate::ai_assistant::backend_ready_for(t, target)?;
    Ok((
        crate::ai_assistant::chat_backend_for(t, target),
        crate::ai_assistant::backend_label_for(t, target),
    ))
}

const NO_API_MESSAGE: &str = "The project has no HTTP API requests yet. Generate endpoints from the repository or add requests to the project's API workspace first.";

/// Generate langkah tambahan (atau cleanup) untuk suite.
pub fn start_step_gen(
    t: &mut Tabular,
    suite: &HttpTestSuite,
    instructions: String,
    mode: StepGenMode,
) -> Result<(), String> {
    if t.integration_tests.gens.contains_key(&suite.id) {
        return Err("The AI is already working on this suite".into());
    }
    let (backend, label) = backend(t)?;
    let catalogs = project_catalogs(t, suite);
    if catalogs.is_empty() && mode == StepGenMode::Append {
        return Err(NO_API_MESSAGE.into());
    }
    let handle = http_tests::spawn_step_generate(
        backend,
        label.clone(),
        catalogs,
        suite.clone(),
        instructions,
        mode,
    );
    t.integration_tests.gens.insert(
        suite.id.clone(),
        GenJob {
            mode,
            handle,
            started: Instant::now(),
            status: format!("Asking {label}"),
        },
    );
    Ok(())
}

/// Generate suite baru dari seluruh API project aktif.
pub fn start_suite_gen(t: &mut Tabular, instructions: String) -> Result<(), String> {
    if t.integration_tests.suite_gen.is_some() {
        return Err("The AI is already designing a suite".into());
    }
    let (backend, label) = backend(t)?;
    let project_id = super::project_ui::active(t).map(|p| p.id.clone());
    let probe = HttpTestSuite {
        project_id: project_id.clone(),
        ..Default::default()
    };
    let catalogs = project_catalogs(t, &probe);
    if catalogs.is_empty() {
        return Err(NO_API_MESSAGE.into());
    }
    let handle = http_tests::spawn_generate(backend, label.clone(), catalogs, instructions);
    t.integration_tests.suite_gen = Some(SuiteGenJob {
        project_id,
        handle,
        started: Instant::now(),
        status: format!("Asking {label}"),
    });
    Ok(())
}

pub fn cancel_gen(t: &mut Tabular, suite_id: &str) {
    if let Some(job) = t.integration_tests.gens.remove(suite_id) {
        job.handle.cancel();
    }
}

pub fn cancel_suite_gen(t: &mut Tabular) {
    if let Some(job) = t.integration_tests.suite_gen.take() {
        job.handle.cancel();
    }
}

// ─── Tick ───────────────────────────────────────────────────────────────────

/// Dipanggil setiap frame dari `app_impl`.
pub fn tick(t: &mut Tabular, ctx: &egui::Context) {
    let ui = &t.integration_tests;
    if ui.runs.is_empty() && ui.db_checks.is_empty() && ui.gens.is_empty() && ui.suite_gen.is_none()
    {
        return;
    }
    poll_runs(t);
    poll_db_checks(t);
    poll_gens(t);
    mirror_tasks(t);
    let ui = &t.integration_tests;
    if ui.runs.values().any(RunJob::is_running)
        || !ui.db_checks.is_empty()
        || !ui.gens.is_empty()
        || ui.suite_gen.is_some()
    {
        ctx.request_repaint_after(Duration::from_millis(150));
    }
}

fn active_step(text: String) -> Vec<ProgressStep> {
    vec![ProgressStep {
        step_index: None,
        description: text,
        detail: None,
        status: ProgressStatus::Active,
        tool_name: None,
    }]
}

fn poll_runs(t: &mut Tabular) {
    let mut done: Vec<(String, TestRun, Vec<DbCheckTarget>, Vec<String>)> = Vec::new();
    for (suite_id, job) in t.integration_tests.runs.iter_mut() {
        if !job.is_running() {
            continue;
        }
        let mut finished_run = None;
        loop {
            match job.handle.rx.try_recv() {
                Ok(TestEvent::StepStarted(i)) => job.current = Some(i),
                Ok(TestEvent::StepFinished(r)) => {
                    job.current = None;
                    job.results.insert(r.step_id.clone(), *r);
                }
                Ok(TestEvent::CleanupStarted) => job.in_cleanup = true,
                Ok(TestEvent::Finished(summary)) => {
                    let mut run = TestRun::from_summary(
                        &job.suite,
                        &job.scope,
                        job.run_id.clone(),
                        job.started_at,
                        *summary,
                    );
                    run.project_name = job.project_name.clone();
                    run.environment = job.environment.clone();
                    finished_run = Some(run);
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    job.finished = Some(Err("The test run stopped unexpectedly".into()));
                    break;
                }
            }
        }
        if let Some(run) = finished_run {
            job.in_cleanup = false;
            job.current = None;
            let tables = http_tests::touched_tables(&job.suite, &run.results);
            job.steps = active_step("Verifying the database is clean".into());
            done.push((suite_id.clone(), run, job.db_targets.clone(), tables));
        } else if job.is_running() {
            job.steps = active_step(job.progress_text());
        }
    }
    for (suite_id, run, targets, tables) in done {
        if !tables.is_empty() && !targets.is_empty() && !run.aborted {
            let rx = spawn_db_check(targets, tables, run.run_id.clone());
            t.integration_tests
                .db_checks
                .push(DbCheckJob { suite_id, run, rx });
        } else {
            finalize(t, &suite_id, run);
        }
    }
}

/// Jalankan pemeriksaan sisa data di thread terpisah dengan runtime sendiri.
fn spawn_db_check(
    targets: Vec<DbCheckTarget>,
    tables: Vec<String>,
    run_id: String,
) -> mpsc::Receiver<Vec<DbResidueCheck>> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let checks = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt.block_on(async {
                match crate::agent::core::open_cache_pool().await {
                    Ok(pool) => {
                        let session = crate::agent::core::HeadlessSession::new(pool);
                        http_tests::check_db_residue(&session, &targets, &tables, &run_id).await
                    }
                    Err(e) => {
                        log::warn!("[INTEGRATION_TEST] db check unavailable: {e}");
                        Vec::new()
                    }
                }
            }),
            Err(e) => {
                log::warn!("[INTEGRATION_TEST] db check runtime failed: {e}");
                Vec::new()
            }
        };
        let _ = tx.send(checks);
    });
    rx
}

fn poll_db_checks(t: &mut Tabular) {
    let mut ready = Vec::new();
    t.integration_tests
        .db_checks
        .retain_mut(|job| match job.rx.try_recv() {
            Ok(checks) => {
                job.run.db_checks = checks;
                ready.push((job.suite_id.clone(), std::mem::take(&mut job.run)));
                false
            }
            Err(mpsc::TryRecvError::Empty) => true,
            Err(mpsc::TryRecvError::Disconnected) => {
                ready.push((job.suite_id.clone(), std::mem::take(&mut job.run)));
                false
            }
        });
    for (suite_id, run) in ready {
        finalize(t, &suite_id, run);
    }
}

/// Simpan run, kabarkan hasilnya, dan tandai job selesai.
fn finalize(t: &mut Tabular, suite_id: &str, run: TestRun) {
    use crate::http_test_report::Verdict;
    if let Err(e) = http_tests::save_run(&run) {
        t.toasts.error(e);
    }
    let stats = crate::http_test_report::compute(&run);
    let mut msg = format!(
        "{}: {}. {} passed, {} failed, {} blocked.",
        run.suite_name,
        stats.verdict().label(),
        stats.tests.passed,
        stats.tests.failed + stats.tests.error,
        stats.tests.blocked
    );
    if stats.data_clean() {
        msg.push_str(" Test data cleaned up.");
    } else {
        msg.push_str(&format!(
            " {} item(s) of test data may be left behind.",
            stats.residue_items + stats.db_residue_rows as usize
        ));
    }
    log::info!("[INTEGRATION_TEST] {msg}");
    match stats.verdict() {
        Verdict::Pass => t.toasts.success(msg.clone()),
        Verdict::PassWithDeviations => t.toasts.warning(msg.clone()),
        _ => t.toasts.error(msg.clone()),
    }
    if let Some(job) = t.integration_tests.runs.get_mut(suite_id) {
        job.finished = Some(match stats.verdict() {
            Verdict::Pass | Verdict::PassWithDeviations => Ok(msg),
            _ => Err(msg),
        });
    }
    t.integration_tests
        .finished
        .insert(suite_id.to_string(), run);
    t.integration_tests.runs_rev += 1;
}

fn poll_gens(t: &mut Tabular) {
    let mut done: Vec<(String, GenResult)> = Vec::new();
    for (suite_id, job) in t.integration_tests.gens.iter_mut() {
        loop {
            match job.handle.rx.try_recv() {
                Ok(RepoJobEvent::Progress(step)) => {
                    job.status = step.detail.unwrap_or(step.description);
                }
                Ok(RepoJobEvent::Activity) => {}
                Ok(RepoJobEvent::Finished(result)) => {
                    done.push((suite_id.clone(), result));
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    done.push((
                        suite_id.clone(),
                        Err("The AI stopped without a reply".into()),
                    ));
                    break;
                }
            }
        }
    }
    for (suite_id, result) in done {
        t.integration_tests.gens.remove(&suite_id);
        let tab_open = t.query_tabs.iter().any(|tab| {
            tab.integration_test_state
                .as_ref()
                .is_some_and(|s| s.suite_id() == suite_id)
        });
        if tab_open {
            t.integration_tests.gen_results.insert(suite_id, result);
            continue;
        }
        // Tidak ada tab terbuka: gabungkan langsung ke suite di disk.
        match result {
            Ok((steps, vars)) => match http_tests::load_suite(&suite_id) {
                Some(mut suite) => {
                    let n = http_tests::merge_generated_steps(&mut suite, steps, vars);
                    match http_tests::save_suite(&suite) {
                        Ok(()) => t
                            .toasts
                            .success(format!("Added {n} AI step(s) to '{}'", suite.name)),
                        Err(e) => t.toasts.error(e),
                    }
                }
                None => t.toasts.warning("The suite no longer exists"),
            },
            Err(e) => t.toasts.error(format!("AI step generation failed: {e}")),
        }
    }

    let mut suites_done = None;
    if let Some(job) = t.integration_tests.suite_gen.as_mut() {
        loop {
            match job.handle.rx.try_recv() {
                Ok(RepoJobEvent::Progress(step)) => {
                    job.status = step.detail.unwrap_or(step.description);
                }
                Ok(RepoJobEvent::Activity) => {}
                Ok(RepoJobEvent::Finished(result)) => {
                    suites_done = Some((job.project_id.clone(), result));
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    suites_done = Some((
                        job.project_id.clone(),
                        Err("The AI stopped without a reply".into()),
                    ));
                    break;
                }
            }
        }
    }
    if let Some((project_id, result)) = suites_done {
        t.integration_tests.suite_gen = None;
        match result {
            Ok(mut suites) => {
                let mut first = None;
                for s in &mut suites {
                    s.project_id = project_id.clone();
                    match http_tests::save_suite(s) {
                        Ok(()) => {
                            first.get_or_insert_with(|| s.id.clone());
                        }
                        Err(e) => t.toasts.error(e),
                    }
                }
                let steps: usize = suites.iter().map(|s| s.steps.len()).sum();
                t.toasts.success(format!(
                    "AI designed {} suite(s) with {steps} step(s)",
                    suites.len()
                ));
                if let Some(id) = first {
                    super::integration_test_ui::open_suite(t, &id);
                }
            }
            Err(e) => t.toasts.error(format!("AI suite generation failed: {e}")),
        }
    }
}

fn mirror_tasks(t: &mut Tabular) {
    let bg = &mut t.background_tasks;
    let mut show: Option<String> = None;
    for (suite_id, job) in t.integration_tests.runs.iter_mut() {
        let out = bg.mirror(
            &mut job.task,
            Snapshot {
                owner: TaskOwner::IntegrationTest,
                title: &job.suite.name,
                subtitle: "Integration test run",
                steps: &job.steps,
                started_at: Some(job.started),
                last_activity_at: None,
                hidden: true,
                result: job.finished.clone(),
            },
        );
        if out.show {
            show = Some(suite_id.clone());
        }
        if out.cancel {
            job.handle.cancel();
        }
    }
    // Run selesai dibuang setelah entrinya hilang dari panel.
    t.integration_tests
        .runs
        .retain(|_, j| j.is_running() || j.task.is_some_and(|id| bg.contains(id)));
    if let Some(id) = show {
        super::integration_test_ui::open_suite(t, &id);
    }
}
