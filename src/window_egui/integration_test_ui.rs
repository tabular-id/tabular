//! Tab Integration Testing: editor suite langkah HTTP berurutan milik project.
//!
//! - Kolom kiri: group (mis. Purchasing, Plant), variabel.
//! - Tengah: tabel langkah dalam urutan eksekusi (Setup → Test per group →
//!   Cleanup) beserta aliran data antar langkah, hasil run, dan lint.
//! - Kanan: detail langkah (extract, assertion, hasil terakhir).
//!
//! Klik dua kali (atau tombol buka) membuka langkah di tab HTTP client; simpan
//! di sana menulis balik ke suite. Run berjalan di
//! [`super::integration_test_jobs`]; cleanup selalu dijalankan setelahnya.

use std::collections::HashMap;

use eframe::egui;
use egui_icons::icons as ic;

use super::integration_test_jobs as jobs;
use super::{Tabular, style};
use crate::http_collection::SavedRequest;
use crate::http_test_report::{self as report, ReportFormat, ReportInput, Verdict};
use crate::http_tests::{
    self, AssertOp, Assertion, Extract, HttpTestStep, HttpTestSuite, LintLevel, RunScope,
    StepGenMode, StepOutcome, StepPhase, StepResult, TestRun,
};
use crate::models::structs::HttpMethod;

/// Jumlah run riwayat yang dimuat di tab.
const HISTORY_LIMIT: usize = 20;
const LEFT_WIDTH: f32 = 210.0;
const RIGHT_WIDTH: f32 = 380.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum View {
    #[default]
    Steps,
    Report,
    History,
}

/// Filter group di kolom kiri.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum GroupFilter {
    #[default]
    All,
    /// `""` = langkah tanpa group.
    Group(String),
}

/// State tab (disimpan di `QueryTab::integration_test_state`).
#[derive(Debug, Clone, Default)]
pub struct IntegrationTestTabState {
    suite: HttpTestSuite,
    /// `http_tests::suites_revision()` saat suite terakhir dimuat/disimpan.
    rev: u64,
    dirty: bool,
    selected: Option<String>,
    filter: GroupFilter,
    view: View,
    last_run: Option<TestRun>,
    history: Vec<TestRun>,
    runs_rev: Option<u64>,
    ai_open: bool,
    ai_prompt: String,
    new_group: String,
    renaming: Option<(String, String)>,
    /// Group yang akan dijalankan, menunggu konfirmasi (kosong = semua).
    confirm_run: Option<Vec<String>>,
    allow_production: bool,
    confirm_delete_suite: bool,
    picker: Option<String>,
}

impl IntegrationTestTabState {
    fn new(suite: HttpTestSuite) -> Self {
        Self {
            rev: http_tests::suites_revision(),
            suite,
            ..Default::default()
        }
    }

    pub fn suite_id(&self) -> &str {
        &self.suite.id
    }

    fn title(&self) -> String {
        format!("Tests: {}", self.suite.name.trim())
    }

    fn mark(&mut self) {
        self.dirty = true;
        self.suite.touch();
    }

    fn selected_index(&self) -> Option<usize> {
        self.selected
            .as_deref()
            .and_then(|id| self.suite.step_index(id))
    }
}

// ─── Membuka tab ────────────────────────────────────────────────────────────

fn open_tab(t: &mut Tabular, suite: HttpTestSuite) {
    let st = IntegrationTestTabState::new(suite);
    crate::editor::create_new_tab(t, st.title(), String::new());
    if let Some(tab) = t.query_tabs.get_mut(t.active_tab_index) {
        tab.integration_test_state = Some(st);
    }
}

/// Buka suite; tab yang sudah membukanya diaktifkan saja.
pub fn open_suite(t: &mut Tabular, suite_id: &str) {
    if let Some(idx) = t.query_tabs.iter().position(|tab| {
        tab.integration_test_state
            .as_ref()
            .is_some_and(|s| s.suite.id == suite_id)
    }) {
        crate::editor::switch_to_tab(t, idx);
        return;
    }
    match http_tests::load_suite(suite_id) {
        Some(s) => open_tab(t, s),
        None => t
            .toasts
            .warning("The integration test suite no longer exists"),
    }
}

/// Tombol header: buka suite project aktif, atau buat suite kosong bila belum ada.
pub fn open_for_active_project(t: &mut Tabular) {
    super::project_ui::ensure_loaded(t);
    let project_id = super::project_ui::active(t).map(|p| p.id.clone());
    if let Some(idx) = t.query_tabs.iter().position(|tab| {
        tab.integration_test_state
            .as_ref()
            .is_some_and(|s| s.suite.project_id == project_id)
    }) {
        crate::editor::switch_to_tab(t, idx);
        return;
    }
    let mut suites = match &project_id {
        Some(id) => http_tests::load_project_suites(id),
        None => http_tests::load_suites()
            .into_iter()
            .filter(|s| s.project_id.is_none())
            .collect(),
    };
    suites.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    if let Some(first) = suites.into_iter().next() {
        open_tab(t, first);
        return;
    }
    let suite = HttpTestSuite::new("Integration tests", project_id);
    if let Err(e) = http_tests::save_suite(&suite) {
        t.toasts.error(e);
    }
    open_tab(t, suite);
}

// ─── Snapshot keadaan aplikasi untuk satu frame ─────────────────────────────

/// Data dari `Tabular` yang dibutuhkan render (tanpa memegang pinjaman).
struct Live {
    running: bool,
    cancelling: bool,
    current: Option<usize>,
    progress: String,
    /// Hasil run berjalan (bila ada) atau run terakhir, per id langkah.
    results: HashMap<String, StepResult>,
    run_scope: Option<Vec<bool>>,
    gen_status: Option<String>,
    suite_gen_status: Option<String>,
    ai_ready: Result<String, String>,
    project_name: Option<String>,
    env_name: Option<String>,
    env_production: bool,
    known_vars: std::collections::HashSet<String>,
    suites: Vec<(String, String)>,
    catalogs: Vec<http_tests::FolderCatalog>,
}

impl Default for Live {
    fn default() -> Self {
        Self {
            running: false,
            cancelling: false,
            current: None,
            progress: String::new(),
            results: HashMap::new(),
            run_scope: None,
            gen_status: None,
            suite_gen_status: None,
            ai_ready: Err(String::new()),
            project_name: None,
            env_name: None,
            env_production: false,
            known_vars: Default::default(),
            suites: Vec::new(),
            catalogs: Vec::new(),
        }
    }
}

fn snapshot(t: &mut Tabular, st: &IntegrationTestTabState, need_catalog: bool) -> Live {
    let mut live = Live::default();
    if let Some(job) = t.integration_tests.runs.get(&st.suite.id)
        && job.is_running()
    {
        live.running = true;
        live.cancelling = job.is_cancelling();
        live.current = job.current;
        live.progress = job.progress_text();
        live.results = job.results.clone();
        live.run_scope = job.scope.include.clone();
    } else if let Some(run) = &st.last_run {
        live.results = run
            .results
            .iter()
            .map(|r| (r.step_id.clone(), r.clone()))
            .collect();
    }
    live.gen_status = t
        .integration_tests
        .gens
        .get(&st.suite.id)
        .map(|g| format!("{} ({}s)", g.status, g.started.elapsed().as_secs()));
    live.suite_gen_status = t
        .integration_tests
        .suite_gen
        .as_ref()
        .map(|g| format!("{} ({}s)", g.status, g.started.elapsed().as_secs()));
    let target = t.effective_chat_target();
    live.ai_ready = crate::ai_assistant::backend_ready_for(t, target)
        .map(|()| crate::ai_assistant::backend_label_for(t, target));
    if let Some(p) = jobs::suite_project(t, &st.suite) {
        live.project_name = Some(p.name.clone());
        if let Some(env) = p.active_environment() {
            live.env_name = Some(env.name.clone());
            live.env_production =
                env.environment() == Some(crate::connection_env::Environment::Production);
        }
    }
    live.known_vars = jobs::project_var_names(t, &st.suite);
    let mut suites: Vec<HttpTestSuite> = match &st.suite.project_id {
        Some(id) => http_tests::load_project_suites(id),
        None => http_tests::load_suites()
            .into_iter()
            .filter(|s| s.project_id.is_none())
            .collect(),
    };
    suites.sort_by_key(|s| s.name.to_lowercase());
    live.suites = suites.into_iter().map(|s| (s.id, s.name)).collect();
    if need_catalog {
        live.catalogs = jobs::project_catalogs(t, &st.suite);
    }
    live
}

/// Aksi yang butuh `&mut Tabular`, dijalankan setelah render.
enum Act {
    Run(Vec<String>),
    Stop,
    Abort,
    OpenStep(String),
    GenSteps(StepGenMode),
    GenSuite,
    CancelGen,
    NewSuite,
    SwitchSuite(String),
    DeleteSuite,
    DuplicateSuite,
    OpenHtmlReport,
    Export(ReportFormat),
    CopyMarkdown,
}

// ─── Render ─────────────────────────────────────────────────────────────────

pub fn render_active_tab(t: &mut Tabular, ui: &mut egui::Ui) {
    let idx = t.active_tab_index;
    let Some(mut st) = t
        .query_tabs
        .get_mut(idx)
        .and_then(|tab| tab.integration_test_state.take())
    else {
        return;
    };
    sync_from_app(t, &mut st);
    // Katalog dibaca setiap frame hanya bila dibutuhkan (picker / laporan).
    let need_catalog = st.picker.is_some() || st.view == View::Report;
    let live = snapshot(t, &st, need_catalog);
    let mut acts: Vec<Act> = Vec::new();

    egui::Frame::NONE
        .inner_margin(egui::Margin {
            left: 12,
            right: 12,
            top: 8,
            bottom: 6,
        })
        .show(ui, |ui| {
            toolbar(ui, &mut st, &live, &mut acts);
            if st.ai_open {
                ui.add_space(6.0);
                ai_bar(ui, &mut st, &live, &mut acts);
            }
        });
    let y = ui.cursor().top();
    ui.painter().hline(
        ui.max_rect().x_range(),
        y,
        egui::Stroke::new(1.0, style::nav_border(ui.ctx())),
    );
    ui.add_space(4.0);

    match st.view {
        View::Steps => body(ui, &mut st, &live, &mut acts),
        View::Report => report_view(ui, &mut st, &live, &mut acts),
        View::History => history_view(ui, &mut st),
    }

    confirm_run_modal(ui.ctx(), &mut st, &live, &mut acts);
    confirm_delete_modal(ui.ctx(), &mut st, &mut acts);
    picker_window(ui.ctx(), &mut st, &live);

    // Simpan otomatis saat user tidak sedang mengetik.
    if st.dirty && !ui.ctx().egui_wants_keyboard_input() {
        save(t, &mut st);
    }

    let title = st.title();
    let suite_snapshot = st.suite.clone();
    let last_run = st.last_run.clone();
    if let Some(tab) = t.query_tabs.get_mut(idx) {
        tab.title = title;
        tab.integration_test_state = Some(st);
    }
    let ctx = ui.ctx().clone();
    for act in acts {
        apply(t, &ctx, idx, &suite_snapshot, last_run.as_ref(), act);
    }
}

/// Muat ulang suite/riwayat dan ambil hasil AI yang tertunda.
fn sync_from_app(t: &mut Tabular, st: &mut IntegrationTestTabState) {
    let rev = http_tests::suites_revision();
    if rev != st.rev && !st.dirty {
        if let Some(s) = http_tests::load_suite(&st.suite.id) {
            st.suite = s;
        }
        st.rev = rev;
    }
    if let Some(result) = t.integration_tests.gen_results.remove(&st.suite.id) {
        match result {
            Ok((steps, vars)) => {
                let n = http_tests::merge_generated_steps(&mut st.suite, steps, vars);
                st.dirty = true;
                t.toasts.success(format!("Added {n} step(s) from AI"));
            }
            Err(e) => t.toasts.error(format!("AI step generation failed: {e}")),
        }
    }
    if st.runs_rev != Some(t.integration_tests.runs_rev) {
        st.runs_rev = Some(t.integration_tests.runs_rev);
        st.history = http_tests::load_runs(&st.suite.id, HISTORY_LIMIT);
        st.last_run = st.history.first().cloned();
    }
}

fn save(t: &mut Tabular, st: &mut IntegrationTestTabState) {
    st.suite.ensure_ids();
    st.suite.normalize_order();
    if let Err(e) = http_tests::save_suite(&st.suite) {
        t.toasts.error(e);
    }
    st.rev = http_tests::suites_revision();
    st.dirty = false;
}

fn weak(ui: &mut egui::Ui, text: impl Into<String>) {
    ui.label(
        egui::RichText::new(text.into())
            .small()
            .color(ui.visuals().weak_text_color()),
    );
}

fn icon_button(ui: &mut egui::Ui, glyph: &str, tip: &str) -> egui::Response {
    ui.add(egui::Button::new(egui::RichText::new(glyph).size(14.0)).frame(false))
        .on_hover_text(tip)
}

fn outcome_color(ctx: &egui::Context, o: StepOutcome) -> egui::Color32 {
    match o {
        StepOutcome::Passed => style::theme_success(ctx),
        StepOutcome::Failed | StepOutcome::Error => style::theme_danger(ctx),
        StepOutcome::Blocked => style::theme_warning(ctx),
        StepOutcome::Skipped => style::theme_muted_text(ctx),
    }
}

fn outcome_glyph(o: StepOutcome) -> &'static str {
    match o {
        StepOutcome::Passed => ic::ICON_CHECK_CIRCLE.codepoint,
        StepOutcome::Failed => ic::ICON_CANCEL.codepoint,
        StepOutcome::Error => ic::ICON_ERROR.codepoint,
        StepOutcome::Blocked => ic::ICON_BLOCK.codepoint,
        StepOutcome::Skipped => ic::ICON_SKIP_NEXT.codepoint,
    }
}

fn verdict_color(ctx: &egui::Context, v: Verdict) -> egui::Color32 {
    match v {
        Verdict::Pass => style::theme_success(ctx),
        Verdict::Fail => style::theme_danger(ctx),
        _ => style::theme_warning(ctx),
    }
}

fn group_label(g: &str) -> &str {
    if g.trim().is_empty() { "No group" } else { g }
}

// ─── Toolbar ────────────────────────────────────────────────────────────────

fn toolbar(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live, acts: &mut Vec<Act>) {
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(ic::ICON_FACT_CHECK.codepoint)
                .size(18.0)
                .color(style::theme_accent(ui.ctx())),
        );
        egui::ComboBox::from_id_salt(("it_suite_pick", st.suite.id.as_str()))
            .selected_text(egui::RichText::new(st.suite.name.clone()).strong())
            .width(220.0)
            .show_ui(ui, |ui| {
                for (id, name) in &live.suites {
                    if ui
                        .selectable_label(*id == st.suite.id, name.as_str())
                        .clicked()
                        && *id != st.suite.id
                    {
                        acts.push(Act::SwitchSuite(id.clone()));
                    }
                }
                ui.separator();
                if ui
                    .button(format!("{} New empty suite", ic::ICON_ADD.codepoint))
                    .clicked()
                {
                    acts.push(Act::NewSuite);
                }
                if ui
                    .button(format!(
                        "{} Duplicate this suite",
                        ic::ICON_CONTENT_COPY.codepoint
                    ))
                    .clicked()
                {
                    acts.push(Act::DuplicateSuite);
                }
            });
        if ui
            .add(
                egui::TextEdit::singleline(&mut st.suite.name)
                    .desired_width(180.0)
                    .hint_text("Suite name"),
            )
            .changed()
        {
            st.mark();
        }
        if let Some(p) = &live.project_name {
            let env = live
                .env_name
                .clone()
                .unwrap_or_else(|| "no environment".into());
            let color = if live.env_production {
                style::theme_danger(ui.ctx())
            } else {
                style::theme_muted_text(ui.ctx())
            };
            ui.label(
                egui::RichText::new(format!("{p} · {env}"))
                    .small()
                    .color(color),
            );
        }

        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            more_menu(ui, st, acts);
            if ui
                .selectable_label(
                    st.ai_open,
                    format!("{} AI", ic::ICON_AUTO_AWESOME.codepoint),
                )
                .on_hover_text("Generate steps, cleanup and whole suites with AI")
                .clicked()
            {
                st.ai_open = !st.ai_open;
            }
            for (v, label) in [
                (View::History, "History"),
                (View::Report, "Report"),
                (View::Steps, "Steps"),
            ] {
                if ui.selectable_label(st.view == v, label).clicked() {
                    st.view = v;
                }
            }
            ui.separator();
            run_buttons(ui, st, live, acts);
        });
    });
}

fn run_buttons(
    ui: &mut egui::Ui,
    st: &mut IntegrationTestTabState,
    live: &Live,
    acts: &mut Vec<Act>,
) {
    if live.running {
        if ui
            .button(format!("{} Abort", ic::ICON_CLOSE.codepoint))
            .on_hover_text("Stop immediately, including cleanup. Test data may be left behind.")
            .clicked()
        {
            acts.push(Act::Abort);
        }
        if !live.cancelling
            && ui
                .button(format!("{} Stop", ic::ICON_STOP.codepoint))
                .on_hover_text("Stop the remaining tests. Cleanup still runs.")
                .clicked()
        {
            acts.push(Act::Stop);
        }
        ui.add(egui::Spinner::new().size(14.0));
        ui.label(egui::RichText::new(&live.progress).small());
        return;
    }
    let enabled = st.suite.steps.iter().any(|s| s.enabled);
    if let GroupFilter::Group(g) = &st.filter {
        let g = g.clone();
        if ui
            .add_enabled(enabled, egui::Button::new("Run all"))
            .clicked()
        {
            st.confirm_run = Some(Vec::new());
        }
        let label = format!("{} Run {}", ic::ICON_PLAY_ARROW.codepoint, group_label(&g));
        if ui
            .add_enabled(enabled, crate::diagram_repo::accent_button(ui, label))
            .on_hover_text("Run this group plus the steps it depends on, then clean up")
            .clicked()
        {
            st.confirm_run = Some(vec![g]);
        }
    } else if ui
        .add_enabled(
            enabled,
            crate::diagram_repo::accent_button(
                ui,
                format!("{} Run all", ic::ICON_PLAY_ARROW.codepoint),
            ),
        )
        .on_hover_text("Run every step in order, then clean up the test data")
        .clicked()
    {
        st.confirm_run = Some(Vec::new());
    }
}

fn more_menu(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, acts: &mut Vec<Act>) {
    ui.menu_button(ic::ICON_EXPAND_MORE.codepoint, |ui| {
        if ui
            .button(format!(
                "{} Auto-order cleanup steps",
                ic::ICON_SORT.codepoint
            ))
            .on_hover_text("Remove data in reverse order of creation")
            .clicked()
        {
            http_tests::auto_order_cleanup(&mut st.suite);
            st.mark();
            ui.close();
        }
        if ui
            .checkbox(&mut st.suite.stop_on_failure, "Stop after first failure")
            .on_hover_text("Remaining tests are blocked after a failure. Cleanup still runs.")
            .changed()
        {
            st.mark();
        }
        ui.horizontal(|ui| {
            ui.label("Pass criterion");
            let mut v = st.suite.pass_threshold();
            if ui
                .add(
                    egui::DragValue::new(&mut v)
                        .range(0.0..=100.0)
                        .speed(1.0)
                        .suffix(" %"),
                )
                .changed()
            {
                st.suite.min_pass_rate = (v < 100.0).then_some(v);
                st.mark();
            }
        });
        ui.separator();
        if ui
            .button(format!(
                "{} Copy report as Markdown",
                ic::ICON_CONTENT_COPY.codepoint
            ))
            .clicked()
        {
            acts.push(Act::CopyMarkdown);
            ui.close();
        }
        if ui
            .button(format!("{} Delete suite…", ic::ICON_DELETE.codepoint))
            .clicked()
        {
            st.confirm_delete_suite = true;
            ui.close();
        }
    });
}

fn ai_bar(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live, acts: &mut Vec<Act>) {
    style::theme_card_frame(ui.ctx()).show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new(ic::ICON_AUTO_AWESOME.codepoint)
                    .color(style::theme_accent(ui.ctx())),
            );
            match &live.ai_ready {
                Ok(label) => weak(ui, format!("AI: {label}")),
                Err(e) => {
                    ui.label(
                        egui::RichText::new(format!("AI not ready: {e}"))
                            .small()
                            .color(style::theme_warning(ui.ctx())),
                    );
                }
            }
        });
        ui.add(
            egui::TextEdit::multiline(&mut st.ai_prompt)
                .desired_rows(2)
                .desired_width(f32::INFINITY)
                .hint_text(
                    "Describe the flow or a group, e.g. \"Purchasing: create vendor, create PO, approve, receive goods\". \
                     The AI uses the project's API endpoints, chains ids between steps and adds cleanup.",
                ),
        );
        ui.horizontal(|ui| {
            let ready = live.ai_ready.is_ok();
            if let Some(status) = live.gen_status.as_ref().or(live.suite_gen_status.as_ref()) {
                ui.add(egui::Spinner::new().size(14.0));
                ui.label(egui::RichText::new(status).small());
                if ui.small_button("Cancel").clicked() {
                    acts.push(Act::CancelGen);
                }
                return;
            }
            if ui
                .add_enabled(ready, crate::diagram_repo::accent_button(ui, "Add steps"))
                .on_hover_text(
                    "Append AI-designed steps to this suite (into the selected group when one is selected)",
                )
                .clicked()
            {
                acts.push(Act::GenSteps(StepGenMode::Append));
            }
            if ui
                .add_enabled(
                    ready && !st.suite.steps.is_empty(),
                    egui::Button::new("Add missing cleanup"),
                )
                .on_hover_text("Ask the AI for cleanup steps for data that is not removed yet")
                .clicked()
            {
                acts.push(Act::GenSteps(StepGenMode::Cleanup));
            }
            if ui
                .add_enabled(ready, egui::Button::new("Design new suites"))
                .on_hover_text("Let the AI design complete suites from all of the project's API endpoints")
                .clicked()
            {
                acts.push(Act::GenSuite);
            }
        });
    });
}

// ─── Body: group, tabel, detail ─────────────────────────────────────────────

fn body(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live, acts: &mut Vec<Act>) {
    let height = ui.available_height();
    ui.horizontal_top(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(LEFT_WIDTH, height),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.set_min_size(egui::vec2(LEFT_WIDTH, height));
                egui::ScrollArea::vertical()
                    .id_salt("it_left")
                    .show(ui, |ui| left_panel(ui, st, live));
            },
        );
        ui.separator();
        let center_w = (ui.available_width() - RIGHT_WIDTH - 12.0).max(360.0);
        ui.allocate_ui_with_layout(
            egui::vec2(center_w, height),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.set_min_size(egui::vec2(center_w, height));
                center_panel(ui, st, live, acts);
            },
        );
        ui.separator();
        let right_w = ui.available_width();
        ui.allocate_ui_with_layout(
            egui::vec2(right_w, height),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.set_min_size(egui::vec2(right_w, height));
                egui::ScrollArea::vertical()
                    .id_salt("it_right")
                    .show(ui, |ui| right_panel(ui, st, live, acts));
            },
        );
    });
}

fn left_panel(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live) {
    ui.label(egui::RichText::new("Groups").strong());
    weak(
        ui,
        "Run one group at a time; steps it needs from other groups run too.",
    );
    ui.add_space(4.0);
    let total = st.suite.steps.len();
    if ui
        .selectable_label(
            st.filter == GroupFilter::All,
            format!("All steps ({total})"),
        )
        .clicked()
    {
        st.filter = GroupFilter::All;
    }
    let mut groups = st.suite.group_names();
    if st.suite.steps.iter().any(|s| s.group.trim().is_empty()) {
        groups.insert(0, String::new());
    }
    let mut rename_done: Option<(String, String)> = None;
    let mut remove: Option<String> = None;
    let mut move_group: Option<(String, isize)> = None;
    for g in &groups {
        let steps: Vec<&HttpTestStep> = st.suite.steps.iter().filter(|s| s.group == *g).collect();
        let passed = steps
            .iter()
            .filter(|s| live.results.get(&s.id).is_some_and(|r| r.passed()))
            .count();
        let failed = steps
            .iter()
            .filter(|s| {
                live.results.get(&s.id).is_some_and(|r| {
                    matches!(
                        r.outcome(),
                        StepOutcome::Failed | StepOutcome::Error | StepOutcome::Blocked
                    )
                })
            })
            .count();
        if let Some((from, to)) = st.renaming.as_mut()
            && from == g
        {
            let resp = ui.add(egui::TextEdit::singleline(to).desired_width(LEFT_WIDTH - 20.0));
            if resp.lost_focus() {
                rename_done = Some((from.clone(), to.clone()));
            } else {
                resp.request_focus();
            }
            continue;
        }
        let selected = st.filter == GroupFilter::Group(g.clone());
        let resp = ui
            .horizontal(|ui| {
                let r = ui.selectable_label(
                    selected,
                    format!(
                        "{} {} ({})",
                        ic::ICON_FOLDER.codepoint,
                        group_label(g),
                        steps.len()
                    ),
                );
                if passed > 0 {
                    ui.label(
                        egui::RichText::new(format!("{passed}"))
                            .small()
                            .color(style::theme_success(ui.ctx())),
                    )
                    .on_hover_text("Passed in the last run");
                }
                if failed > 0 {
                    ui.label(
                        egui::RichText::new(format!("{failed}"))
                            .small()
                            .color(style::theme_danger(ui.ctx())),
                    )
                    .on_hover_text("Failed or blocked in the last run");
                }
                r
            })
            .inner;
        if resp.clicked() {
            st.filter = GroupFilter::Group(g.clone());
        }
        if !g.is_empty() {
            resp.context_menu(|ui| {
                if ui.button("Rename").clicked() {
                    st.renaming = Some((g.clone(), g.clone()));
                    ui.close();
                }
                if ui.button("Move up").clicked() {
                    move_group = Some((g.clone(), -1));
                    ui.close();
                }
                if ui.button("Move down").clicked() {
                    move_group = Some((g.clone(), 1));
                    ui.close();
                }
                if ui.button("Remove group (keep steps)").clicked() {
                    remove = Some(g.clone());
                    ui.close();
                }
            });
        }
    }
    if let Some((from, to)) = rename_done {
        st.renaming = None;
        if !to.trim().is_empty() && to.trim() != from {
            st.suite.rename_group(&from, &to);
            if st.filter == GroupFilter::Group(from) {
                st.filter = GroupFilter::Group(to.trim().to_string());
            }
            st.mark();
        }
    }
    if let Some(g) = remove {
        st.suite.remove_group(&g);
        st.filter = GroupFilter::All;
        st.mark();
    }
    if let Some((g, d)) = move_group {
        st.suite.move_group(&g, d);
        st.mark();
    }
    ui.horizontal(|ui| {
        let resp = ui.add(
            egui::TextEdit::singleline(&mut st.new_group)
                .desired_width(LEFT_WIDTH - 46.0)
                .hint_text("New group, e.g. Purchasing"),
        );
        let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
        if (icon_button(ui, ic::ICON_ADD.codepoint, "Add group").clicked() || enter)
            && !st.new_group.trim().is_empty()
        {
            let name = st.new_group.trim().to_string();
            st.suite.groups = st.suite.group_names();
            if !st.suite.groups.contains(&name) {
                st.suite.groups.push(name.clone());
            }
            st.filter = GroupFilter::Group(name);
            st.new_group.clear();
            st.mark();
        }
    });

    ui.add_space(10.0);
    ui.separator();
    variables_section(ui, st, live);
}

fn variables_section(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live) {
    ui.label(egui::RichText::new("Variables").strong());
    weak(
        ui,
        "Suite values override the project environment; empty values use the environment. {{run_id}} is unique per run.",
    );
    let mut remove: Option<usize> = None;
    let mut changed = false;
    for (i, (k, v)) in st.suite.variables.iter_mut().enumerate() {
        ui.horizontal(|ui| {
            changed |= ui
                .add(
                    egui::TextEdit::singleline(k)
                        .desired_width(80.0)
                        .hint_text("name"),
                )
                .changed();
            let lower = k.to_ascii_lowercase();
            let secret = crate::http_client_widgets::is_sensitive_key(k)
                || lower.contains("token")
                || lower.contains("password");
            let hint = if live.known_vars.contains(k.trim()) {
                "from environment"
            } else {
                "value"
            };
            changed |= ui
                .add(
                    egui::TextEdit::singleline(v)
                        .desired_width(LEFT_WIDTH - 120.0)
                        .password(secret)
                        .hint_text(hint),
                )
                .changed();
            if icon_button(ui, ic::ICON_CLOSE.codepoint, "Remove variable").clicked() {
                remove = Some(i);
            }
        });
    }
    if let Some(i) = remove {
        st.suite.variables.remove(i);
        changed = true;
    }
    if ui
        .small_button(format!("{} Variable", ic::ICON_ADD.codepoint))
        .clicked()
    {
        st.suite.variables.push((String::new(), String::new()));
        changed = true;
    }
    if changed {
        st.mark();
    }
    if !live.known_vars.is_empty() {
        let mut names: Vec<&str> = live.known_vars.iter().map(String::as_str).collect();
        names.sort_unstable();
        ui.add_space(4.0);
        weak(
            ui,
            format!(
                "Environment {}: {}",
                live.env_name.clone().unwrap_or_default(),
                names.join(", ")
            ),
        );
    }
}

/// Langkah yang tampil di tabel untuk filter saat ini. `true` menandai langkah
/// group lain yang ikut dijalankan karena dibutuhkan group terpilih.
fn visible_rows(st: &IntegrationTestTabState) -> Vec<(usize, bool)> {
    let order = st.suite.execution_order();
    match &st.filter {
        GroupFilter::All => order.into_iter().map(|i| (i, false)).collect(),
        GroupFilter::Group(g) => {
            let include = http_tests::select_for_groups(&st.suite, std::slice::from_ref(g));
            order
                .into_iter()
                .filter(|&i| include[i] || st.suite.steps[i].group == *g)
                .map(|i| (i, st.suite.steps[i].group != *g))
                .collect()
        }
    }
}

#[derive(Default)]
struct RowActions {
    select: Option<String>,
    open: Option<String>,
    swap: Option<(usize, usize)>,
    delete: Option<usize>,
    toggle: Option<usize>,
}

fn center_panel(
    ui: &mut egui::Ui,
    st: &mut IntegrationTestTabState,
    live: &Live,
    acts: &mut Vec<Act>,
) {
    let deps = http_tests::dependencies(&st.suite);
    let order = st.suite.execution_order();
    let pos: HashMap<usize, usize> = order.iter().enumerate().map(|(p, &i)| (i, p)).collect();
    let rows = visible_rows(st);
    let issues = http_tests::lint(&st.suite, &live.known_vars);
    let errors = issues
        .iter()
        .filter(|i| i.level == LintLevel::Error)
        .count();
    let warnings = issues
        .iter()
        .filter(|i| i.level == LintLevel::Warning)
        .count();

    ui.horizontal(|ui| {
        let title = match &st.filter {
            GroupFilter::All => "All steps".to_string(),
            GroupFilter::Group(g) => format!("Group: {}", group_label(g)),
        };
        ui.label(egui::RichText::new(title).strong());
        weak(ui, "Execution order: setup, tests per group, cleanup.");
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.menu_button(format!("{} Add step", ic::ICON_ADD.codepoint), |ui| {
                if ui.button("From the project's API collection…").clicked() {
                    st.picker = Some(String::new());
                    ui.close();
                }
                if ui.button("Empty request").clicked() {
                    add_step(st, empty_request(), None);
                    ui.close();
                }
                if ui
                    .add_enabled(
                        st.selected_index().is_some(),
                        egui::Button::new("Cleanup step for the selected step"),
                    )
                    .clicked()
                {
                    add_cleanup_for_selected(st);
                    ui.close();
                }
            });
        });
    });
    ui.add_space(4.0);

    let mut ra = RowActions::default();
    let lint_height = if issues.is_empty() { 0.0 } else { 130.0 };
    let table_h = (ui.available_height() - lint_height - 8.0).max(120.0);
    egui::ScrollArea::both()
        .id_salt("it_steps")
        .max_height(table_h)
        .auto_shrink([false, false])
        .show(ui, |ui| {
            if rows.is_empty() {
                ui.add_space(20.0);
                weak(
                    ui,
                    "No steps yet. Add a step from the API collection, or open AI and describe the flow to test.",
                );
                return;
            }
            egui::Grid::new(("it_grid", st.suite.id.as_str()))
                .striped(true)
                .num_columns(10)
                .spacing(egui::vec2(10.0, 6.0))
                .show(ui, |ui| {
                    for h in [
                        "#", "", "Phase", "Group", "Method", "Step", "Data flow", "Result", "Time",
                        "",
                    ] {
                        ui.label(egui::RichText::new(h).small().strong());
                    }
                    ui.end_row();
                    for (row_no, &(i, required)) in rows.iter().enumerate() {
                        step_row(ui, st, live, &deps, &order, &pos, &rows, row_no, i, required, &mut ra);
                        ui.end_row();
                    }
                });
        });

    if !issues.is_empty() {
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Checks").strong());
            if errors > 0 {
                ui.label(
                    egui::RichText::new(format!("{} {errors} error(s)", ic::ICON_ERROR.codepoint))
                        .small()
                        .color(style::theme_danger(ui.ctx())),
                );
            }
            if warnings > 0 {
                ui.label(
                    egui::RichText::new(format!(
                        "{} {warnings} warning(s)",
                        ic::ICON_WARNING.codepoint
                    ))
                    .small()
                    .color(style::theme_warning(ui.ctx())),
                );
            }
        });
        egui::ScrollArea::vertical()
            .id_salt("it_lint")
            .max_height(lint_height - 24.0)
            .auto_shrink([false, true])
            .show(ui, |ui| {
                for issue in &issues {
                    let (glyph, color) = match issue.level {
                        LintLevel::Error => {
                            (ic::ICON_ERROR.codepoint, style::theme_danger(ui.ctx()))
                        }
                        LintLevel::Warning => {
                            (ic::ICON_WARNING.codepoint, style::theme_warning(ui.ctx()))
                        }
                        LintLevel::Info => (ic::ICON_INFO.codepoint, style::theme_info(ui.ctx())),
                    };
                    let prefix = issue
                        .step
                        .map(|s| {
                            format!(
                                "#{} {}: ",
                                pos.get(&s).map(|p| p + 1).unwrap_or(0),
                                st.suite.steps[s].name
                            )
                        })
                        .unwrap_or_default();
                    let resp = ui.add(
                        egui::Label::new(
                            egui::RichText::new(format!("{glyph} {prefix}{}", issue.message))
                                .small()
                                .color(color),
                        )
                        .sense(egui::Sense::click()),
                    );
                    if resp.clicked()
                        && let Some(s) = issue.step
                    {
                        ra.select = Some(st.suite.steps[s].id.clone());
                    }
                }
            });
    }

    if let Some(id) = ra.select {
        st.selected = Some(id);
    }
    if let Some(id) = ra.open {
        st.selected = Some(id.clone());
        acts.push(Act::OpenStep(id));
    }
    if let Some(i) = ra.toggle {
        st.suite.steps[i].enabled = !st.suite.steps[i].enabled;
        st.mark();
    }
    if let Some((a, b)) = ra.swap {
        st.suite.steps.swap(a, b);
        st.mark();
    }
    if let Some(i) = ra.delete {
        let removed = st.suite.steps.remove(i);
        for s in &mut st.suite.steps {
            if s.cleanup_for.as_deref() == Some(removed.id.as_str()) {
                s.cleanup_for = None;
            }
        }
        if st.selected.as_deref() == Some(removed.id.as_str()) {
            st.selected = None;
        }
        st.mark();
    }
}

#[allow(clippy::too_many_arguments)]
fn step_row(
    ui: &mut egui::Ui,
    st: &IntegrationTestTabState,
    live: &Live,
    deps: &[http_tests::StepDeps],
    order: &[usize],
    pos: &HashMap<usize, usize>,
    rows: &[(usize, bool)],
    row_no: usize,
    i: usize,
    required: bool,
    ra: &mut RowActions,
) {
    let step = &st.suite.steps[i];
    let same_block = |j: usize| {
        let n = &st.suite.steps[j];
        n.phase == step.phase && n.group == step.group
    };
    let order_pos = order.iter().position(|&x| x == i).unwrap_or(0);
    let can_up = order_pos > 0 && same_block(order[order_pos - 1]);
    let can_down = order_pos + 1 < order.len() && same_block(order[order_pos + 1]);
    let _ = (rows, row_no);
    let selected = st.selected.as_deref() == Some(step.id.as_str());
    let dim = required || !step.enabled;
    let text_color = if dim {
        ui.visuals().weak_text_color()
    } else {
        ui.visuals().text_color()
    };
    ui.label(
        egui::RichText::new(format!("{}", pos[&i] + 1))
            .small()
            .color(text_color),
    );
    let mut on = step.enabled;
    if ui.checkbox(&mut on, "").on_hover_text("Enabled").changed() {
        ra.toggle = Some(i);
    }
    let phase_color = match step.phase {
        StepPhase::Setup => style::theme_info(ui.ctx()),
        StepPhase::Test => text_color,
        StepPhase::Cleanup => style::theme_warning(ui.ctx()),
    };
    ui.label(
        egui::RichText::new(step.phase.label())
            .small()
            .color(phase_color),
    );
    ui.label(
        egui::RichText::new(group_label(&step.group))
            .small()
            .color(text_color),
    );
    crate::http_repo::method_chip(ui, step.request.method.label());
    let mut name = egui::RichText::new(&step.name).color(text_color);
    if required {
        name = name.italics();
    }
    let resp = ui.selectable_label(selected, name);
    let resp = if required {
        resp.on_hover_text("Runs with this group because the group needs its data")
    } else {
        resp.on_hover_text(&step.request.url)
    };
    if resp.clicked() {
        ra.select = Some(step.id.clone());
    }
    if resp.double_clicked() {
        ra.open = Some(step.id.clone());
    }
    // Aliran data: variabel masuk dari langkah sebelumnya, keluar ke langkah berikutnya.
    let d = &deps[i];
    let mut parts = Vec::new();
    if !d.depends_on.is_empty() {
        let from: Vec<String> = d
            .depends_on
            .iter()
            .map(|p| format!("#{}", pos[p] + 1))
            .collect();
        parts.push(format!("from {}", from.join(",")));
    }
    if !d.provides.is_empty() {
        parts.push(format!("gives {}", d.provides.join(",")));
    }
    if let Some(target) = http_tests::cleanup_target(&st.suite, deps, i) {
        parts.push(format!("cleans #{}", pos[&target] + 1));
    }
    let flow = parts.join(" · ");
    ui.label(
        egui::RichText::new(crate::repo_scan::truncate_chars(&flow, 40))
            .small()
            .color(ui.visuals().weak_text_color()),
    )
    .on_hover_text(if flow.is_empty() {
        "No data flow".to_string()
    } else {
        flow.clone()
    });
    let running_here = live.running && live.current == Some(i);
    let result = live.results.get(&step.id);
    if running_here {
        ui.add(egui::Spinner::new().size(12.0));
    } else if let Some(r) = result {
        let o = r.outcome();
        let label = ui.label(
            egui::RichText::new(format!("{} {}", outcome_glyph(o), o.label()))
                .small()
                .color(outcome_color(ui.ctx(), o)),
        );
        if let Some(n) = &r.note {
            label.on_hover_text(n);
        }
    } else if live.running
        && live
            .run_scope
            .as_ref()
            .is_none_or(|m| m.get(i).copied().unwrap_or(false))
    {
        weak(ui, "queued");
    } else {
        weak(ui, "-");
    }
    match result.filter(|r| r.executed() || r.error.is_some()) {
        Some(r) => weak(ui, report::fmt_ms(r.time_ms)),
        None => weak(ui, ""),
    }
    ui.horizontal(|ui| {
        if icon_button(ui, ic::ICON_OPEN_IN_NEW.codepoint, "Open in HTTP client").clicked() {
            ra.open = Some(step.id.clone());
        }
        if ui
            .add_enabled(
                can_up,
                egui::Button::new(ic::ICON_ARROW_UPWARD.codepoint).frame(false),
            )
            .on_hover_text("Move up (within its phase and group)")
            .clicked()
        {
            ra.swap = Some((i, order[order_pos - 1]));
        }
        if ui
            .add_enabled(
                can_down,
                egui::Button::new(ic::ICON_ARROW_DOWNWARD.codepoint).frame(false),
            )
            .on_hover_text("Move down (within its phase and group)")
            .clicked()
        {
            ra.swap = Some((i, order[order_pos + 1]));
        }
        if icon_button(ui, ic::ICON_DELETE.codepoint, "Delete step").clicked() {
            ra.delete = Some(i);
        }
    });
}

fn empty_request() -> SavedRequest {
    SavedRequest {
        name: "New step".into(),
        url: "{{base_url}}/".into(),
        method: HttpMethod::GET,
        form_data: vec![(String::new(), String::new(), true)],
        ..Default::default()
    }
}

/// Tambah langkah ke group yang sedang dipilih.
fn add_step(st: &mut IntegrationTestTabState, request: SavedRequest, request_id: Option<String>) {
    let group = match &st.filter {
        GroupFilter::Group(g) => g.clone(),
        GroupFilter::All => String::new(),
    };
    let mutating = http_tests::is_mutating(&request.method);
    let step = HttpTestStep {
        id: st.suite.next_step_id(),
        name: request.display_name(),
        phase: StepPhase::Test,
        group,
        assertions: vec![Assertion {
            target: "status".into(),
            op: AssertOp::LessThan,
            value: if mutating { "300".into() } else { "400".into() },
        }],
        enabled: true,
        request_id,
        request,
        ..Default::default()
    };
    st.selected = Some(step.id.clone());
    st.suite.steps.push(step);
    st.suite.normalize_order();
    st.mark();
}

/// Buat langkah DELETE untuk langkah terpilih yang membuat data.
fn add_cleanup_for_selected(st: &mut IntegrationTestTabState) {
    let Some(i) = st.selected_index() else {
        return;
    };
    let src = st.suite.steps[i].clone();
    let id_var = src
        .extract
        .iter()
        .map(|e| e.var.clone())
        .find(|v| v.to_ascii_lowercase().contains("id"))
        .or_else(|| src.extract.first().map(|e| e.var.clone()));
    let base = src
        .request
        .url
        .split('?')
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    let url = match &id_var {
        Some(v) => format!("{base}/{{{{{v}}}}}"),
        None => base.to_string(),
    };
    let request = SavedRequest {
        name: format!("Delete: {}", src.name),
        url,
        method: HttpMethod::DELETE,
        headers: src.request.headers.clone(),
        auth_type: src.request.auth_type.clone(),
        bearer_token: src.request.bearer_token.clone(),
        basic_user: src.request.basic_user.clone(),
        basic_pass: src.request.basic_pass.clone(),
        api_key_name: src.request.api_key_name.clone(),
        api_key_value: src.request.api_key_value.clone(),
        api_key_in_header: src.request.api_key_in_header,
        tables: src.request.tables.clone(),
        form_data: vec![(String::new(), String::new(), true)],
        ..Default::default()
    };
    let step = HttpTestStep {
        id: st.suite.next_step_id(),
        name: format!("Delete: {}", src.name),
        phase: StepPhase::Cleanup,
        group: src.group.clone(),
        cleanup_for: Some(src.id.clone()),
        assertions: vec![Assertion {
            target: "status".into(),
            op: AssertOp::LessThan,
            value: "300".into(),
        }],
        enabled: true,
        request,
        ..Default::default()
    };
    st.selected = Some(step.id.clone());
    st.suite.steps.push(step);
    http_tests::auto_order_cleanup(&mut st.suite);
    st.mark();
}

fn right_panel(
    ui: &mut egui::Ui,
    st: &mut IntegrationTestTabState,
    live: &Live,
    acts: &mut Vec<Act>,
) {
    let Some(i) = st.selected_index() else {
        suite_overview(ui, st, live);
        return;
    };
    let deps = http_tests::dependencies(&st.suite);
    let order = st.suite.execution_order();
    let pos: HashMap<usize, usize> = order.iter().enumerate().map(|(p, &x)| (x, p)).collect();
    let groups = st.suite.group_names();
    let creators: Vec<(String, String)> = st
        .suite
        .steps
        .iter()
        .filter(|s| s.phase != StepPhase::Cleanup && http_tests::is_mutating(&s.request.method))
        .map(|s| (s.id.clone(), s.name.clone()))
        .collect();
    let result = live.results.get(&st.suite.steps[i].id).cloned();
    let mut changed = false;
    let mut close = false;

    {
        let step = &mut st.suite.steps[i];
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new(format!("Step #{}", pos[&i] + 1)).strong());
            weak(ui, format!("id {}", step.id));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if icon_button(ui, ic::ICON_CLOSE.codepoint, "Close details").clicked() {
                    close = true;
                }
            });
        });
        changed |= ui
            .add(egui::TextEdit::singleline(&mut step.name).desired_width(f32::INFINITY))
            .changed();
        egui::Grid::new("it_step_meta")
            .num_columns(2)
            .spacing([8.0, 6.0])
            .show(ui, |ui| {
                ui.label("Phase");
                egui::ComboBox::from_id_salt("it_phase")
                    .selected_text(step.phase.label())
                    .show_ui(ui, |ui| {
                        for p in StepPhase::ALL {
                            changed |= ui.selectable_value(&mut step.phase, p, p.label()).changed();
                        }
                    });
                ui.end_row();
                ui.label("Group");
                egui::ComboBox::from_id_salt("it_group")
                    .selected_text(group_label(&step.group).to_string())
                    .show_ui(ui, |ui| {
                        changed |= ui
                            .selectable_value(&mut step.group, String::new(), "No group")
                            .changed();
                        for g in &groups {
                            changed |= ui
                                .selectable_value(&mut step.group, g.clone(), g.as_str())
                                .changed();
                        }
                    });
                ui.end_row();
                if step.phase == StepPhase::Cleanup {
                    ui.label("Cleans");
                    let current = step
                        .cleanup_for
                        .as_deref()
                        .and_then(|id| creators.iter().find(|(c, _)| c == id))
                        .map(|(_, n)| n.clone())
                        .unwrap_or_else(|| "Auto (from variables)".into());
                    egui::ComboBox::from_id_salt("it_cleans")
                        .selected_text(current)
                        .width(220.0)
                        .show_ui(ui, |ui| {
                            changed |= ui
                                .selectable_value(
                                    &mut step.cleanup_for,
                                    None,
                                    "Auto (from variables)",
                                )
                                .changed();
                            for (id, name) in &creators {
                                changed |= ui
                                    .selectable_value(
                                        &mut step.cleanup_for,
                                        Some(id.clone()),
                                        name.as_str(),
                                    )
                                    .changed();
                            }
                        });
                    ui.end_row();
                }
                ui.label("Enabled");
                changed |= ui.checkbox(&mut step.enabled, "").changed();
                ui.end_row();
            });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            crate::http_repo::method_chip(ui, step.request.method.label());
            ui.label(
                egui::RichText::new(crate::repo_scan::truncate_chars(&step.request.url, 60))
                    .family(egui::FontFamily::Monospace)
                    .size(11.5),
            )
            .on_hover_text(&step.request.url);
        });
        if ui
            .button(format!(
                "{} Edit request in HTTP client",
                ic::ICON_OPEN_IN_NEW.codepoint
            ))
            .on_hover_text("Opens a new tab. Save there writes the request back to this step.")
            .clicked()
        {
            acts.push(Act::OpenStep(step.id.clone()));
        }
        ui.add_space(6.0);
        ui.label(egui::RichText::new("Requirement / note").small().strong());
        changed |= ui
            .add(
                egui::TextEdit::multiline(&mut step.notes)
                    .desired_rows(2)
                    .desired_width(f32::INFINITY)
                    .hint_text("What this step verifies, e.g. REQ-PUR-012"),
            )
            .changed();
    }

    data_flow_section(ui, st, live, &deps, &pos, i);
    changed |= extract_section(ui, &mut st.suite.steps[i]);
    changed |= assertions_section(ui, &mut st.suite.steps[i], result.as_ref());
    if let Some(r) = &result {
        last_result_section(ui, r);
    }

    if changed {
        st.mark();
    }
    if close {
        st.selected = None;
    }
}

fn data_flow_section(
    ui: &mut egui::Ui,
    st: &IntegrationTestTabState,
    live: &Live,
    deps: &[http_tests::StepDeps],
    pos: &HashMap<usize, usize>,
    i: usize,
) {
    ui.add_space(6.0);
    ui.label(egui::RichText::new("Data flow").small().strong());
    let d = &deps[i];
    if d.uses.is_empty() && d.provides.is_empty() {
        weak(ui, "This step neither uses nor provides variables.");
    }
    for v in &d.uses {
        let from = d
            .depends_on
            .iter()
            .find(|&&p| deps[p].provides.contains(v))
            .map(|&p| format!("step #{} '{}'", pos[&p] + 1, st.suite.steps[p].name));
        let (text, missing) = match from {
            Some(f) => (f, false),
            None if v == "run_id" => ("the run id".to_string(), false),
            None if st.suite.variables.iter().any(|(k, _)| k == v) => {
                ("a suite variable".to_string(), false)
            }
            None if live.known_vars.contains(v) => ("the environment".to_string(), false),
            None => ("nowhere (missing)".to_string(), true),
        };
        let color = if missing {
            style::theme_danger(ui.ctx())
        } else {
            ui.visuals().text_color()
        };
        ui.label(
            egui::RichText::new(format!("uses {{{{{v}}}}} from {text}"))
                .small()
                .color(color),
        );
    }
    for v in &d.provides {
        let users: Vec<String> = d
            .used_by
            .iter()
            .filter(|&&u| deps[u].uses.contains(v))
            .map(|&u| format!("#{}", pos[&u] + 1))
            .collect();
        let tail = if users.is_empty() {
            " (not used yet)".to_string()
        } else {
            format!(" to {}", users.join(", "))
        };
        ui.label(egui::RichText::new(format!("gives {{{{{v}}}}}{tail}")).small());
    }
}

fn extract_section(ui: &mut egui::Ui, step: &mut HttpTestStep) -> bool {
    let mut changed = false;
    ui.add_space(6.0);
    ui.label(egui::RichText::new("Extract to variables").small().strong());
    weak(ui, "From: status, header:Name, json:$.data.id, body");
    let mut remove: Option<usize> = None;
    for (n, ex) in step.extract.iter_mut().enumerate() {
        ui.horizontal(|ui| {
            changed |= ui
                .add(
                    egui::TextEdit::singleline(&mut ex.var)
                        .desired_width(100.0)
                        .hint_text("variable"),
                )
                .changed();
            ui.label("=");
            changed |= ui
                .add(
                    egui::TextEdit::singleline(&mut ex.from)
                        .desired_width(170.0)
                        .hint_text("json:$.id"),
                )
                .changed();
            if icon_button(ui, ic::ICON_CLOSE.codepoint, "Remove").clicked() {
                remove = Some(n);
            }
        });
    }
    if let Some(n) = remove {
        step.extract.remove(n);
        changed = true;
    }
    if ui
        .small_button(format!("{} Extract", ic::ICON_ADD.codepoint))
        .clicked()
    {
        step.extract.push(Extract {
            var: String::new(),
            from: "json:$.id".into(),
        });
        changed = true;
    }
    changed
}

fn assertions_section(
    ui: &mut egui::Ui,
    step: &mut HttpTestStep,
    result: Option<&StepResult>,
) -> bool {
    let mut changed = false;
    ui.add_space(6.0);
    ui.label(
        egui::RichText::new("Assertions (expected result)")
            .small()
            .strong(),
    );
    let mut remove: Option<usize> = None;
    for (n, a) in step.assertions.iter_mut().enumerate() {
        let res = result.and_then(|r| r.assertions.get(n));
        ui.horizontal(|ui| {
            if let Some(r) = res {
                let (g, c) = if r.passed {
                    (
                        ic::ICON_CHECK_CIRCLE.codepoint,
                        style::theme_success(ui.ctx()),
                    )
                } else {
                    (ic::ICON_CANCEL.codepoint, style::theme_danger(ui.ctx()))
                };
                ui.label(egui::RichText::new(g).color(c))
                    .on_hover_text(format!(
                        "actual: {}",
                        r.actual.clone().unwrap_or_else(|| "(missing)".into())
                    ));
            }
            changed |= ui
                .add(
                    egui::TextEdit::singleline(&mut a.target)
                        .desired_width(110.0)
                        .hint_text("status"),
                )
                .changed();
            egui::ComboBox::from_id_salt(("it_op", n))
                .selected_text(a.op.label())
                .width(80.0)
                .show_ui(ui, |ui| {
                    for op in [
                        AssertOp::Equals,
                        AssertOp::NotEquals,
                        AssertOp::Contains,
                        AssertOp::NotContains,
                        AssertOp::Exists,
                        AssertOp::NotExists,
                        AssertOp::LessThan,
                        AssertOp::GreaterThan,
                        AssertOp::Matches,
                    ] {
                        changed |= ui.selectable_value(&mut a.op, op, op.label()).changed();
                    }
                });
            if !matches!(a.op, AssertOp::Exists | AssertOp::NotExists) {
                changed |= ui
                    .add(egui::TextEdit::singleline(&mut a.value).desired_width(70.0))
                    .changed();
            }
            if icon_button(ui, ic::ICON_CLOSE.codepoint, "Remove").clicked() {
                remove = Some(n);
            }
        });
    }
    if let Some(n) = remove {
        step.assertions.remove(n);
        changed = true;
    }
    ui.horizontal(|ui| {
        if ui
            .small_button(format!("{} Assertion", ic::ICON_ADD.codepoint))
            .clicked()
        {
            step.assertions.push(Assertion {
                target: "status".into(),
                op: AssertOp::Equals,
                value: "200".into(),
            });
            changed = true;
        }
        if let Some(r) = result.filter(|r| r.executed())
            && ui
                .small_button("From last response")
                .on_hover_text("Assert the last status and the top-level JSON fields")
                .clicked()
        {
            step.assertions = assertions_from_response(r);
            changed = true;
        }
    });
    changed
}

fn last_result_section(ui: &mut egui::Ui, r: &StepResult) {
    ui.add_space(8.0);
    ui.separator();
    let o = r.outcome();
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(format!("{} {}", outcome_glyph(o), o.label()))
                .strong()
                .color(outcome_color(ui.ctx(), o)),
        );
        if r.status > 0 {
            weak(
                ui,
                format!("HTTP {} · {}", r.status, report::fmt_ms(r.time_ms)),
            );
        }
    });
    if let Some(n) = &r.note {
        weak(ui, n.clone());
    }
    if let Some(e) = &r.error {
        ui.colored_label(style::theme_danger(ui.ctx()), e);
    }
    if !r.unresolved.is_empty() {
        ui.label(
            egui::RichText::new(format!("Missing variables: {}", r.unresolved.join(", ")))
                .small()
                .color(style::theme_warning(ui.ctx())),
        );
    }
    for (k, v) in &r.extracted {
        weak(
            ui,
            format!("{k} = {}", crate::repo_scan::truncate_chars(v, 60)),
        );
    }
    if !r.url.is_empty() {
        weak(ui, format!("{} {}", r.method, r.url));
    }
    if !r.body_preview.is_empty() {
        egui::ScrollArea::vertical()
            .id_salt("it_body")
            .max_height(220.0)
            .show(ui, |ui| {
                let mut body = r.body_preview.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut body)
                        .font(egui::TextStyle::Monospace)
                        .desired_width(f32::INFINITY)
                        .interactive(false),
                );
            });
    }
}

/// Assertion dari response terakhir: status + field JSON tingkat atas.
fn assertions_from_response(r: &StepResult) -> Vec<Assertion> {
    let mut out = vec![Assertion {
        target: "status".into(),
        op: AssertOp::Equals,
        value: r.status.to_string(),
    }];
    if let Ok(serde_json::Value::Object(map)) =
        serde_json::from_str::<serde_json::Value>(&r.body_preview)
    {
        for k in map.keys().take(5) {
            out.push(Assertion {
                target: format!("json:$.{k}"),
                op: AssertOp::Exists,
                value: String::new(),
            });
        }
    }
    out
}

fn suite_overview(ui: &mut egui::Ui, st: &mut IntegrationTestTabState, live: &Live) {
    ui.label(egui::RichText::new("Suite").strong());
    if ui
        .add(
            egui::TextEdit::multiline(&mut st.suite.description)
                .desired_rows(3)
                .desired_width(f32::INFINITY)
                .hint_text("Purpose and scope of this suite"),
        )
        .changed()
    {
        st.mark();
    }
    let steps = &st.suite.steps;
    let count = |p: StepPhase| steps.iter().filter(|s| s.phase == p).count();
    weak(
        ui,
        format!(
            "{} setup, {} test, {} cleanup step(s) in {} group(s). {} step(s) change data.",
            count(StepPhase::Setup),
            count(StepPhase::Test),
            count(StepPhase::Cleanup),
            st.suite.group_names().len(),
            st.suite.mutating_steps()
        ),
    );
    ui.add_space(8.0);
    match &st.last_run {
        Some(run) => run_summary_card(ui, run),
        None => weak(ui, "Not run yet."),
    }
    ui.add_space(8.0);
    weak(
        ui,
        "Select a step to edit its extract variables and assertions. Double-click a step to edit the request in the HTTP client.",
    );
    if live.running {
        ui.add_space(6.0);
        ui.label(egui::RichText::new(&live.progress).small());
    }
}

fn run_summary_card(ui: &mut egui::Ui, run: &TestRun) {
    let s = report::compute(run);
    style::theme_card_frame(ui.ctx()).show(ui, |ui| {
        ui.label(
            egui::RichText::new(s.verdict().label())
                .size(18.0)
                .strong()
                .color(verdict_color(ui.ctx(), s.verdict())),
        );
        weak(
            ui,
            format!(
                "{} · {}",
                run.started_at,
                run.environment.clone().unwrap_or_default()
            ),
        );
        egui::Grid::new(("it_card", run.run_id.as_str()))
            .num_columns(2)
            .show(ui, |ui| {
                for (k, v) in [
                    ("Passed", s.tests.passed.to_string()),
                    ("Failed", (s.tests.failed + s.tests.error).to_string()),
                    ("Blocked", s.tests.blocked.to_string()),
                    ("Pass rate", format!("{:.1}%", s.tests.pass_rate())),
                    ("p95 response", report::fmt_ms(s.latency.p95)),
                    (
                        "Test data",
                        if s.data_clean() {
                            "cleaned".to_string()
                        } else {
                            format!(
                                "{} item(s) left",
                                s.residue_items + s.db_residue_rows as usize
                            )
                        },
                    ),
                ] {
                    ui.label(egui::RichText::new(k).small());
                    ui.label(egui::RichText::new(v).small().strong());
                    ui.end_row();
                }
            });
    });
}

// ─── Report & riwayat ───────────────────────────────────────────────────────

fn metric_card(ui: &mut egui::Ui, label: &str, value: String, color: Option<egui::Color32>) {
    style::theme_card_frame(ui.ctx()).show(ui, |ui| {
        ui.set_min_width(96.0);
        ui.vertical(|ui| {
            weak(ui, label);
            let mut rt = egui::RichText::new(value).size(17.0).strong();
            if let Some(c) = color {
                rt = rt.color(c);
            }
            ui.label(rt);
        });
    });
}

fn report_view(
    ui: &mut egui::Ui,
    st: &mut IntegrationTestTabState,
    live: &Live,
    acts: &mut Vec<Act>,
) {
    let Some(run) = st.last_run.clone() else {
        ui.add_space(20.0);
        weak(ui, "Run the suite to see its report.");
        return;
    };
    let s = report::compute(&run);
    let catalog: Vec<SavedRequest> = live
        .catalogs
        .iter()
        .flat_map(|c| c.requests.clone())
        .collect();
    egui::ScrollArea::vertical()
        .id_salt("it_report")
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(s.verdict().label())
                        .size(22.0)
                        .strong()
                        .color(verdict_color(ui.ctx(), s.verdict())),
                );
                let scope = if run.groups.is_empty() {
                    "all groups".to_string()
                } else {
                    format!("groups: {}", run.groups.join(", "))
                };
                weak(
                    ui,
                    format!(
                        "Run {} · {} · {} · by {}",
                        run.run_id, run.started_at, scope, run.operator
                    ),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.menu_button(
                        format!("{} Export", ic::ICON_FILE_DOWNLOAD.codepoint),
                        |ui| {
                            for f in ReportFormat::ALL {
                                if ui.button(f.label()).clicked() {
                                    acts.push(Act::Export(f));
                                    ui.close();
                                }
                            }
                        },
                    );
                    if ui
                        .button(format!(
                            "{} Open full report",
                            ic::ICON_DESCRIPTION.codepoint
                        ))
                        .on_hover_text("HTML report in your browser; print it to PDF for sign-off")
                        .clicked()
                    {
                        acts.push(Act::OpenHtmlReport);
                    }
                });
            });
            weak(
                ui,
                "Statistics follow ISTQB terms and the ISO/IEC/IEEE 29119-3 completion report; evidence follows GAMP 5 practice.",
            );
            ui.add_space(8.0);
            let ctx = ui.ctx().clone();
            ui.horizontal_wrapped(|ui| {
                let t = &s.tests;
                metric_card(ui, "Planned", t.planned.to_string(), None);
                metric_card(ui, "Passed", t.passed.to_string(), Some(style::theme_success(&ctx)));
                metric_card(ui, "Failed", t.failed.to_string(), Some(style::theme_danger(&ctx)));
                metric_card(ui, "Error", t.error.to_string(), Some(style::theme_danger(&ctx)));
                metric_card(ui, "Blocked", t.blocked.to_string(), Some(style::theme_warning(&ctx)));
                metric_card(ui, "Skipped", t.skipped.to_string(), None);
                metric_card(ui, "Pass rate", format!("{:.1}%", t.pass_rate()), None);
                metric_card(ui, "Execution rate", format!("{:.1}%", t.execution_rate()), None);
                metric_card(ui, "Criterion", format!("{:.0}%", s.threshold), None);
                metric_card(
                    ui,
                    "Assertions",
                    format!("{}/{}", s.assertions_passed, s.assertions_total),
                    None,
                );
                metric_card(
                    ui,
                    "p50 / p95",
                    format!(
                        "{} / {}",
                        report::fmt_ms(s.latency.p50),
                        report::fmt_ms(s.latency.p95)
                    ),
                    None,
                );
                metric_card(ui, "Duration", report::fmt_ms(run.duration_ms), None);
            });

            ui.add_space(10.0);
            ui.label(egui::RichText::new("Test data cleanup").strong());
            if s.data_clean() {
                let db = if run.db_checks.is_empty() {
                    String::new()
                } else {
                    format!(" Database verified in {} table(s).", run.db_checks.len())
                };
                ui.label(
                    egui::RichText::new(format!(
                        "{} Clean. Cleanup steps passed {}/{}.{db}",
                        ic::ICON_CLEANING_SERVICES.codepoint,
                        s.cleanup.passed,
                        s.cleanup.planned - s.cleanup.skipped,
                    ))
                    .color(style::theme_success(ui.ctx())),
                );
            } else {
                for r in &run.residue {
                    let ids: Vec<String> = r.ids.iter().map(|(k, v)| format!("{k}={v}")).collect();
                    ui.label(
                        egui::RichText::new(format!(
                            "{} {} ({} {}) {}: {}",
                            ic::ICON_WARNING.codepoint,
                            r.step_name,
                            r.method,
                            r.url,
                            ids.join(", "),
                            r.reason
                        ))
                        .small()
                        .color(style::theme_warning(ui.ctx())),
                    );
                }
                for c in run.db_checks.iter().filter(|c| c.rows.unwrap_or(0) > 0) {
                    ui.label(
                        egui::RichText::new(format!(
                            "{} {}.{}: {} row(s) still contain run id {}",
                            ic::ICON_STORAGE.codepoint,
                            c.connection,
                            c.table,
                            c.rows.unwrap_or(0),
                            run.run_id
                        ))
                        .small()
                        .color(style::theme_warning(ui.ctx())),
                    );
                }
            }

            if !s.groups.is_empty() {
                ui.add_space(10.0);
                ui.label(egui::RichText::new("Per group").strong());
                egui::Grid::new("it_rep_groups")
                    .striped(true)
                    .num_columns(7)
                    .show(ui, |ui| {
                        for h in ["Group", "Planned", "Passed", "Failed", "Blocked", "Pass rate", "Time"] {
                            ui.label(egui::RichText::new(h).small().strong());
                        }
                        ui.end_row();
                        for g in &s.groups {
                            let c = &g.counts;
                            ui.label(&g.group);
                            ui.label(c.planned.to_string());
                            ui.label(c.passed.to_string());
                            ui.label((c.failed + c.error).to_string());
                            ui.label(c.blocked.to_string());
                            ui.label(format!("{:.1}%", c.pass_rate()));
                            ui.label(report::fmt_ms(g.time_ms));
                            ui.end_row();
                        }
                    });
            }

            let failures: Vec<&StepResult> = run
                .results
                .iter()
                .filter(|r| {
                    matches!(
                        r.outcome(),
                        StepOutcome::Failed | StepOutcome::Error | StepOutcome::Blocked
                    )
                })
                .collect();
            if !failures.is_empty() {
                ui.add_space(10.0);
                ui.label(egui::RichText::new("What did not pass").strong());
                for r in failures {
                    let o = r.outcome();
                    ui.horizontal_wrapped(|ui| {
                        ui.label(
                            egui::RichText::new(format!("{} {}", outcome_glyph(o), o.label()))
                                .small()
                                .color(outcome_color(ui.ctx(), o)),
                        );
                        if ui.link(&r.name).clicked() {
                            st.selected = Some(r.step_id.clone());
                            st.view = View::Steps;
                        }
                        weak(ui, report::step_details_text(r));
                    });
                }
            }

            if !catalog.is_empty() {
                let cov = report::coverage(&st.suite, &catalog);
                ui.add_space(10.0);
                ui.label(egui::RichText::new("Endpoint coverage").strong());
                ui.label(format!(
                    "{} of {} API endpoints are tested ({:.1}%).",
                    cov.covered,
                    cov.catalog,
                    cov.percent()
                ));
                if !cov.untested.is_empty() {
                    ui.collapsing(
                        format!("{} endpoint(s) without a test", cov.untested.len()),
                        |ui| {
                            for u in cov.untested.iter().take(300) {
                                ui.label(
                                    egui::RichText::new(u)
                                        .family(egui::FontFamily::Monospace)
                                        .size(11.0),
                                );
                            }
                        },
                    );
                }
            }

            let flaky = report::flakiness(&st.history);
            if !flaky.is_empty() {
                ui.add_space(10.0);
                ui.label(egui::RichText::new("Flaky steps").strong());
                for f in flaky {
                    weak(
                        ui,
                        format!("{}: passed {} of {} runs", f.name, f.passes, f.runs),
                    );
                }
            }
        });
}

fn history_view(ui: &mut egui::Ui, st: &mut IntegrationTestTabState) {
    if st.history.is_empty() {
        ui.add_space(20.0);
        weak(ui, "No runs yet.");
        return;
    }
    let mut pick: Option<usize> = None;
    egui::ScrollArea::vertical()
        .id_salt("it_history")
        .auto_shrink([false, false])
        .show(ui, |ui| {
            egui::Grid::new("it_hist")
                .striped(true)
                .num_columns(8)
                .show(ui, |ui| {
                    for h in [
                        "Started",
                        "Scope",
                        "Environment",
                        "Verdict",
                        "Pass rate",
                        "Failed",
                        "Test data",
                        "Duration",
                    ] {
                        ui.label(egui::RichText::new(h).small().strong());
                    }
                    ui.end_row();
                    for (n, run) in st.history.iter().enumerate() {
                        let s = report::compute(run);
                        if ui.link(&run.started_at).clicked() {
                            pick = Some(n);
                        }
                        ui.label(if run.groups.is_empty() {
                            "All".to_string()
                        } else {
                            run.groups.join(", ")
                        });
                        ui.label(run.environment.clone().unwrap_or_default());
                        ui.label(
                            egui::RichText::new(s.verdict().label())
                                .small()
                                .strong()
                                .color(verdict_color(ui.ctx(), s.verdict())),
                        );
                        ui.label(format!("{:.1}%", s.tests.pass_rate()));
                        ui.label((s.tests.failed + s.tests.error).to_string());
                        ui.label(if s.data_clean() {
                            "clean".to_string()
                        } else {
                            format!("{} left", s.residue_items)
                        });
                        ui.label(report::fmt_ms(run.duration_ms));
                        ui.end_row();
                    }
                });
        });
    if let Some(n) = pick {
        st.last_run = st.history.get(n).cloned();
        st.view = View::Report;
    }
}

// ─── Dialog ─────────────────────────────────────────────────────────────────

fn confirm_run_modal(
    ctx: &egui::Context,
    st: &mut IntegrationTestTabState,
    live: &Live,
    acts: &mut Vec<Act>,
) {
    let Some(groups) = st.confirm_run.clone() else {
        return;
    };
    let scope = RunScope::groups(&st.suite, groups.clone());
    let included: Vec<usize> = (0..st.suite.steps.len())
        .filter(|&i| scope.includes(i) && st.suite.steps[i].enabled)
        .collect();
    let mutating = included
        .iter()
        .filter(|&&i| {
            st.suite.steps[i].phase != StepPhase::Cleanup
                && http_tests::is_mutating(&st.suite.steps[i].request.method)
        })
        .count();
    let cleanups = included
        .iter()
        .filter(|&&i| st.suite.steps[i].phase == StepPhase::Cleanup)
        .count();
    let outside = if groups.is_empty() {
        0
    } else {
        included
            .iter()
            .filter(|&&i| !groups.contains(&st.suite.steps[i].group))
            .count()
    };
    let issues = http_tests::lint(&st.suite, &live.known_vars);
    let in_scope = |s: Option<usize>| s.is_none_or(|s| scope.includes(s));
    let errors: Vec<_> = issues
        .iter()
        .filter(|i| i.level == LintLevel::Error && in_scope(i.step))
        .collect();
    let leaks = issues
        .iter()
        .filter(|i| i.level == LintLevel::Warning && i.message.starts_with("Creates data"))
        .filter(|i| in_scope(i.step))
        .count();
    let targets: Vec<String> = st
        .suite
        .variables
        .iter()
        .filter(|(k, v)| k.starts_with("base_url") && !v.is_empty())
        .map(|(k, v)| format!("{k} = {v}"))
        .collect();
    let title = if groups.is_empty() {
        "Run integration tests".to_string()
    } else {
        format!(
            "Run group {}",
            groups
                .iter()
                .map(|g| group_label(g))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let mut decided: Option<bool> = None;
    style::render_modal_backdrop(ctx, "it_confirm_run", true);
    egui::Window::new(title)
        .id(egui::Id::new("it_confirm_run_window"))
        .collapsible(false)
        .resizable(false)
        .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
        .show(ctx, |ui| {
            ui.set_max_width(460.0);
            let extra = if outside > 0 {
                format!(", including {outside} required step(s) from other groups")
            } else {
                String::new()
            };
            ui.label(format!("{} step(s) will run in order{extra}.", included.len()));
            if mutating > 0 {
                ui.label(format!(
                    "{mutating} step(s) create, change or remove data. {cleanups} cleanup step(s) remove the test data afterwards, even when a test fails."
                ));
            }
            if leaks > 0 {
                ui.label(
                    egui::RichText::new(format!(
                        "{leaks} step(s) create data without a cleanup step. Add cleanup first to avoid leaving test data behind."
                    ))
                    .color(style::theme_warning(ui.ctx())),
                );
            }
            ui.label(format!(
                "Environment: {}",
                live.env_name.clone().unwrap_or_else(|| "none".into())
            ));
            for t in &targets {
                ui.label(
                    egui::RichText::new(t)
                        .family(egui::FontFamily::Monospace)
                        .size(11.5),
                );
            }
            if !errors.is_empty() {
                ui.add_space(4.0);
                ui.label(
                    egui::RichText::new(format!(
                        "{} problem(s) will block steps:",
                        errors.len()
                    ))
                    .color(style::theme_danger(ui.ctx())),
                );
                for e in errors.iter().take(5) {
                    weak(ui, e.message.clone());
                }
            }
            if live.env_production {
                ui.add_space(4.0);
                ui.label(
                    egui::RichText::new(
                        "The active environment is Production. Integration tests create and delete real data.",
                    )
                    .strong()
                    .color(style::theme_danger(ui.ctx())),
                );
                ui.checkbox(&mut st.allow_production, "I understand, run against Production");
            }
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui.button("Cancel").clicked() {
                    decided = Some(false);
                }
                let allowed = !live.env_production || st.allow_production;
                if ui
                    .add_enabled(allowed, crate::diagram_repo::accent_button(ui, "Run"))
                    .clicked()
                {
                    decided = Some(true);
                }
            });
        });
    if let Some(yes) = decided {
        st.confirm_run = None;
        st.allow_production = false;
        if yes {
            acts.push(Act::Run(groups));
        }
    }
}

fn confirm_delete_modal(
    ctx: &egui::Context,
    st: &mut IntegrationTestTabState,
    acts: &mut Vec<Act>,
) {
    if !st.confirm_delete_suite {
        return;
    }
    style::render_modal_backdrop(ctx, "it_confirm_delete", true);
    egui::Window::new("Delete suite?")
        .id(egui::Id::new("it_confirm_delete_window"))
        .collapsible(false)
        .resizable(false)
        .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
        .show(ctx, |ui| {
            ui.label(format!(
                "Delete '{}' with its {} step(s) and run history? This cannot be undone.",
                st.suite.name,
                st.suite.steps.len()
            ));
            ui.horizontal(|ui| {
                if ui.button("Cancel").clicked() {
                    st.confirm_delete_suite = false;
                }
                if ui.add(style::btn_danger_ctx(ui.ctx(), "Delete")).clicked() {
                    st.confirm_delete_suite = false;
                    acts.push(Act::DeleteSuite);
                }
            });
        });
}

/// Pemilih request dari collection API project.
fn picker_window(ctx: &egui::Context, st: &mut IntegrationTestTabState, live: &Live) {
    let Some(mut query) = st.picker.take() else {
        return;
    };
    let mut open = true;
    let mut chosen: Option<(SavedRequest, String, String)> = None;
    egui::Window::new("Add step from API collection")
        .id(egui::Id::new(("it_picker", st.suite.id.as_str())))
        .open(&mut open)
        .collapsible(false)
        .default_size(egui::vec2(560.0, 460.0))
        .show(ctx, |ui| {
            ui.add(
                egui::TextEdit::singleline(&mut query)
                    .hint_text("Search method, path or name")
                    .desired_width(f32::INFINITY),
            );
            if live.catalogs.is_empty() {
                weak(ui, "The project's API workspace has no requests yet.");
                return;
            }
            let q = query.to_ascii_lowercase();
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for c in &live.catalogs {
                        ui.label(egui::RichText::new(&c.folder_name).strong());
                        for r in &c.requests {
                            let path =
                                r.route
                                    .clone()
                                    .filter(|p| !p.is_empty())
                                    .unwrap_or_else(|| {
                                        crate::http_collection::extract_endpoint_url(&r.url)
                                    });
                            let hay = format!("{} {} {}", r.method.label(), path, r.display_name())
                                .to_ascii_lowercase();
                            if !q.is_empty() && !hay.contains(&q) {
                                continue;
                            }
                            ui.horizontal(|ui| {
                                crate::http_repo::method_chip(ui, r.method.label());
                                if ui
                                    .selectable_label(
                                        false,
                                        format!("{path}  {}", r.display_name()),
                                    )
                                    .clicked()
                                {
                                    chosen =
                                        Some((r.clone(), c.base_var.clone(), c.base_url.clone()));
                                }
                            });
                        }
                    }
                });
        });
    if let Some((mut req, base_var, base_url)) = chosen {
        let req_id = req.id.clone();
        // Host diganti variabel base URL folder, seperti suite dari AI.
        if !base_url.is_empty() && req.url.starts_with(&base_url) {
            req.url = format!("{{{{{base_var}}}}}{}", &req.url[base_url.len()..]);
            if !st.suite.variables.iter().any(|(k, _)| *k == base_var) {
                st.suite.variables.insert(0, (base_var, base_url));
            }
        }
        // Nilai auth tidak ikut disalin ke suite; pakai variabel.
        let mut needed: Vec<&str> = Vec::new();
        if !req.bearer_token.is_empty() {
            req.bearer_token = "{{token}}".into();
            needed.push("token");
        }
        if !req.basic_pass.is_empty() {
            req.basic_user = "{{username}}".into();
            req.basic_pass = "{{password}}".into();
            needed.extend(["username", "password"]);
        }
        if !req.api_key_value.is_empty() {
            req.api_key_value = "{{api_key}}".into();
            needed.push("api_key");
        }
        for k in needed {
            if !st.suite.variables.iter().any(|(x, _)| x == k) {
                st.suite.variables.push((k.to_string(), String::new()));
            }
        }
        add_step(st, req, Some(req_id));
        return;
    }
    if open {
        st.picker = Some(query);
    }
}

// ─── Aksi ───────────────────────────────────────────────────────────────────

fn apply(
    t: &mut Tabular,
    ctx: &egui::Context,
    idx: usize,
    suite: &HttpTestSuite,
    last_run: Option<&TestRun>,
    act: Act,
) {
    match act {
        Act::Run(groups) => {
            // Simpan dulu agar run memakai definisi terbaru.
            if let Err(e) = http_tests::save_suite(suite) {
                t.toasts.error(e);
            }
            let scope = RunScope::groups(suite, groups);
            if let Err(e) = jobs::start_run(t, suite.clone(), scope) {
                t.toasts.error(e);
            }
        }
        Act::Stop => jobs::stop_run(t, &suite.id),
        Act::Abort => jobs::abort_run(t, &suite.id),
        Act::OpenStep(step_id) => open_step_in_http(t, suite, &step_id, last_run),
        Act::GenSteps(mode) => {
            let instructions = with_tab_state(t, idx, |st| {
                let mut text = st.ai_prompt.trim().to_string();
                if let GroupFilter::Group(g) = &st.filter
                    && !g.is_empty()
                {
                    text = format!("Group \"{g}\": set \"group\":\"{g}\" on new steps. {text}");
                }
                text
            })
            .unwrap_or_default();
            if let Err(e) = jobs::start_step_gen(t, suite, instructions, mode) {
                t.toasts.error(e);
            }
        }
        Act::GenSuite => {
            let instructions =
                with_tab_state(t, idx, |st| st.ai_prompt.trim().to_string()).unwrap_or_default();
            if let Err(e) = jobs::start_suite_gen(t, instructions) {
                t.toasts.error(e);
            }
        }
        Act::CancelGen => {
            jobs::cancel_gen(t, &suite.id);
            jobs::cancel_suite_gen(t);
        }
        Act::NewSuite => {
            let s = HttpTestSuite::new("New suite", suite.project_id.clone());
            match http_tests::save_suite(&s) {
                Ok(()) => replace_suite(t, idx, s),
                Err(e) => t.toasts.error(e),
            }
        }
        Act::DuplicateSuite => {
            let mut s = suite.clone();
            s.id = crate::http_collection::unique_id("suite");
            s.name = format!("{} (copy)", suite.name);
            s.created_at = chrono::Utc::now().to_rfc3339();
            s.touch();
            match http_tests::save_suite(&s) {
                Ok(()) => replace_suite(t, idx, s),
                Err(e) => t.toasts.error(e),
            }
        }
        Act::SwitchSuite(id) => match http_tests::load_suite(&id) {
            Some(s) => replace_suite(t, idx, s),
            None => t.toasts.warning("The suite no longer exists"),
        },
        Act::DeleteSuite => {
            if let Err(e) = http_tests::remove_suite(&suite.id) {
                t.toasts.error(e);
                return;
            }
            t.toasts.success(format!("Deleted suite '{}'", suite.name));
            let next = suite
                .project_id
                .as_deref()
                .and_then(|pid| http_tests::load_project_suites(pid).into_iter().next());
            let next = next.unwrap_or_else(|| {
                let s = HttpTestSuite::new("Integration tests", suite.project_id.clone());
                if let Err(e) = http_tests::save_suite(&s) {
                    log::warn!("[INTEGRATION_TEST] cannot create replacement suite: {e}");
                }
                s
            });
            replace_suite(t, idx, next);
        }
        Act::OpenHtmlReport => {
            let Some(run) = last_run else { return };
            let html = render_report(t, suite, run, ReportFormat::Html);
            let path = std::env::temp_dir().join(report::report_file_name(run, ReportFormat::Html));
            match std::fs::write(&path, html) {
                Ok(()) => crate::self_update::open_url(&format!("file://{}", path.display())),
                Err(e) => t.toasts.error(format!("Could not write the report: {e}")),
            }
        }
        Act::Export(format) => {
            let Some(run) = last_run else { return };
            let text = render_report(t, suite, run, format);
            export_file(t, run, format, text);
        }
        Act::CopyMarkdown => {
            let Some(run) = last_run else {
                t.toasts.info("Run the suite first");
                return;
            };
            let text = render_report(t, suite, run, ReportFormat::Markdown);
            ctx.copy_text(text);
            t.toasts.success("Report copied as Markdown");
        }
    }
}

fn render_report(
    t: &Tabular,
    suite: &HttpTestSuite,
    run: &TestRun,
    format: ReportFormat,
) -> String {
    let history = http_tests::load_runs(&suite.id, HISTORY_LIMIT);
    let catalog = jobs::project_requests(t, suite);
    format.render(&ReportInput {
        suite,
        run,
        history: &history,
        catalog: &catalog,
    })
}

#[cfg(not(target_os = "ios"))]
fn export_file(t: &mut Tabular, run: &TestRun, format: ReportFormat, text: String) {
    let Some(path) = rfd::FileDialog::new()
        .add_filter(format.label(), &[format.extension()])
        .set_file_name(report::report_file_name(run, format))
        .save_file()
    else {
        return;
    };
    match std::fs::write(&path, text) {
        Ok(()) => t
            .toasts
            .success(format!("Report saved to {}", path.display())),
        Err(e) => t.toasts.error(format!("Could not save the report: {e}")),
    }
}

#[cfg(target_os = "ios")]
fn export_file(t: &mut Tabular, _run: &TestRun, _format: ReportFormat, _text: String) {
    t.toasts
        .info("Exporting reports is not available on this device");
}

fn with_tab_state<R>(
    t: &mut Tabular,
    idx: usize,
    f: impl FnOnce(&mut IntegrationTestTabState) -> R,
) -> Option<R> {
    t.query_tabs
        .get_mut(idx)
        .and_then(|tab| tab.integration_test_state.as_mut())
        .map(f)
}

/// Ganti suite yang dibuka tab `idx`.
fn replace_suite(t: &mut Tabular, idx: usize, suite: HttpTestSuite) {
    if let Some(tab) = t.query_tabs.get_mut(idx) {
        let st = IntegrationTestTabState::new(suite);
        tab.title = st.title();
        tab.integration_test_state = Some(st);
    }
}

/// Buka langkah di tab HTTP client baru (atau tab yang sudah mengeditnya).
fn open_step_in_http(
    t: &mut Tabular,
    suite: &HttpTestSuite,
    step_id: &str,
    last_run: Option<&TestRun>,
) {
    if let Some(idx) = t.query_tabs.iter().position(|tab| {
        tab.http_client_state
            .as_ref()
            .and_then(|s| s.test_step.as_ref())
            .is_some_and(|r| r.suite_id == suite.id && r.step_id == step_id)
    }) {
        crate::editor::switch_to_tab(t, idx);
        return;
    }
    let Some(step) = suite.steps.iter().find(|s| s.id == step_id).cloned() else {
        return;
    };
    let env = jobs::project_vars(t, suite);
    let mut vars = http_tests::initial_vars_with(suite, &env);
    // Nilai hasil run terakhir (id, dsb.) agar request bisa langsung dikirim.
    if let Some(run) = last_run {
        for r in &run.results {
            for (k, v) in &r.extracted {
                if v != crate::http_ai::REDACTED {
                    vars.insert(k.clone(), v.clone());
                }
            }
        }
    }
    let mut state = crate::models::structs::HttpClientState::default();
    crate::http_collection::apply_saved_request(&step.request, &mut state);
    // Bukan request collection: simpan menulis ke langkah suite.
    state.saved_request_id = None;
    state.saved_workspace_id = None;
    state.saved_folder_id = None;
    state.save_dialog_name = step.name.clone();
    state.test_step = Some(http_tests::TestStepRef {
        suite_id: suite.id.clone(),
        step_id: step.id.clone(),
        label: format!("{} / {}", suite.name, step.name),
        vars,
    });
    crate::editor::create_new_tab(t, format!("Step: {}", step.name), String::new());
    if let Some(tab) = t.query_tabs.get_mut(t.active_tab_index) {
        tab.http_client_state = Some(state);
    }
}
