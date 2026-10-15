//! Tab editor dan daftar sidebar "Comp" untuk Multi-source Comparison.
//! Logika pembanding ada di `crate::data_transfer::multi_compare`; job
//! background di `multi_compare_jobs`.

use std::sync::Arc;

use eframe::egui;
use egui_icons::icons as i;

use super::multi_compare_jobs as jobs;
use super::transfer_ui::{
    ComboPick, Slot, error_label, filter_combo, fmt_count, modal, muted_label, needs_database,
    warning_label,
};
use super::{Tabular, style};
use crate::data_transfer::catalog;
use crate::data_transfer::multi_compare::{
    self, CompareEntry, CompareVariable, MultiCompareOptions, MultiCompareResult, MultiCompareSpec,
    variable_placeholder,
};
use crate::data_transfer::multi_compare_saved::{self as saved, LastRun, SavedMultiCompare};
use crate::models::enums::{DatabasePool, DatabaseType};

/// Baris selisih yang digambar; ekspor tetap berisi semuanya.
const MAX_ROWS_SHOWN: usize = 500;
const MAX_CELL_CHARS: usize = 60;

// ─── State tab ──────────────────────────────────────────────────────────────

/// Form satu query beserta pemilih koneksi/database/tabelnya.
#[derive(Clone, Default)]
struct EntryForm {
    label: String,
    conn_id: Option<i64>,
    database: String,
    table: String,
    query: String,
    key_columns: String,
    aliases: String,
    databases: Vec<String>,
    databases_for: Option<i64>,
    tables: Vec<String>,
    tables_key: Option<(i64, String)>,
    tables_slot: Option<Slot<Result<Vec<String>, String>>>,
    tables_error: Option<String>,
}

impl EntryForm {
    fn from_entry(entry: &CompareEntry) -> Self {
        Self {
            label: entry.label.clone(),
            conn_id: (entry.connection_id > 0).then_some(entry.connection_id),
            database: entry.database.clone().unwrap_or_default(),
            query: entry.query.clone(),
            key_columns: entry.key_columns.join(", "),
            aliases: multi_compare::format_aliases(&entry.aliases),
            ..Default::default()
        }
    }

    fn to_entry(&self) -> CompareEntry {
        let database = self.database.trim();
        CompareEntry {
            label: self.label.trim().to_string(),
            connection_id: self.conn_id.unwrap_or(0),
            database: (!database.is_empty()).then(|| database.to_string()),
            query: self.query.clone(),
            key_columns: multi_compare::parse_list(&self.key_columns),
            aliases: multi_compare::parse_aliases(&self.aliases),
        }
    }
}

/// State tab Comparison (disimpan di `QueryTab::compare_state`).
#[derive(Clone)]
pub struct MultiCompareTabState {
    /// Id di `multi_compare_saved`; `None` = belum disimpan.
    pub saved_id: Option<i64>,
    name: String,
    variables: Vec<CompareVariable>,
    entries: Vec<EntryForm>,
    row_limit: String,
    ignore_columns: String,
    tolerant: bool,
    trim: bool,
    case_insensitive: bool,
    /// Definisi saat terakhir disimpan/dibuka, untuk penanda "unsaved".
    saved_spec: Option<MultiCompareSpec>,
    error: Option<String>,
    result: Option<Arc<MultiCompareResult>>,
    result_error: Option<String>,
    show_missing: bool,
    show_changed: bool,
    result_filter: String,
    /// Form definisi terlihat; diciutkan setelah hasil keluar.
    setup_open: bool,
}

impl std::fmt::Debug for MultiCompareTabState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiCompareTabState")
            .field("saved_id", &self.saved_id)
            .field("name", &self.name)
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl Default for MultiCompareTabState {
    fn default() -> Self {
        Self::from_spec(None, &MultiCompareSpec::default(), None)
    }
}

impl MultiCompareTabState {
    fn from_spec(
        saved_id: Option<i64>,
        spec: &MultiCompareSpec,
        result: Option<Arc<MultiCompareResult>>,
    ) -> Self {
        let mut entries: Vec<EntryForm> = spec.entries.iter().map(EntryForm::from_entry).collect();
        while entries.len() < 2 {
            entries.push(EntryForm::default());
        }
        let opts = &spec.options;
        Self {
            saved_id,
            name: spec.name.clone(),
            variables: spec.variables.clone(),
            entries,
            row_limit: opts.row_limit.map(|n| n.to_string()).unwrap_or_default(),
            ignore_columns: opts.ignore_columns.join(", "),
            tolerant: opts.tolerant_values,
            trim: opts.trim_trailing_space,
            case_insensitive: opts.case_insensitive,
            saved_spec: saved_id.map(|_| spec.clone()),
            error: None,
            setup_open: result.is_none(),
            result,
            result_error: None,
            show_missing: true,
            show_changed: true,
            result_filter: String::new(),
        }
    }

    fn from_saved(item: &SavedMultiCompare, result: Option<Arc<MultiCompareResult>>) -> Self {
        Self::from_spec(Some(item.id), &item.spec, result)
    }

    /// Definisi dari isi form.
    fn spec(&self) -> Result<MultiCompareSpec, String> {
        let limit = self.row_limit.trim();
        let row_limit =
            if limit.is_empty() {
                None
            } else {
                Some(limit.replace(['_', ','], "").parse::<u64>().map_err(|_| {
                    "Row limit must be a whole number (empty = no limit)".to_string()
                })?)
            };
        Ok(MultiCompareSpec {
            name: self.name.trim().to_string(),
            variables: self
                .variables
                .iter()
                .map(|v| CompareVariable {
                    name: v.name.trim().to_string(),
                    value: v.value.clone(),
                })
                .collect(),
            entries: self.entries.iter().map(EntryForm::to_entry).collect(),
            options: MultiCompareOptions {
                row_limit,
                ignore_columns: multi_compare::parse_list(&self.ignore_columns),
                trim_trailing_space: self.trim,
                tolerant_values: self.tolerant,
                case_insensitive: self.case_insensitive,
            },
        })
    }

    fn unsaved(&self) -> bool {
        match (&self.saved_spec, self.spec()) {
            (Some(saved), Ok(now)) => *saved != now,
            _ => true,
        }
    }

    /// Terima hasil job (dipanggil dari `multi_compare_jobs`).
    pub fn set_result(&mut self, result: Result<Arc<MultiCompareResult>, String>) {
        match result {
            Ok(r) => {
                self.result = Some(r);
                self.result_error = None;
                self.setup_open = false;
            }
            Err(e) => self.result_error = Some(e),
        }
    }
}

fn tab_title(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        "New Comparison".to_string()
    } else {
        format!("Compare: {name}")
    }
}

// ─── Membuka tab ────────────────────────────────────────────────────────────

fn open_tab(t: &mut Tabular, state: MultiCompareTabState) {
    crate::editor::create_new_tab(t, tab_title(&state.name), String::new());
    if let Some(tab) = t.query_tabs.get_mut(t.active_tab_index) {
        tab.compare_state = Some(state);
    }
}

/// Tombol + di sidebar Comp: tab Comparison kosong.
pub fn open_new(t: &mut Tabular) {
    open_tab(t, MultiCompareTabState::default());
}

/// Buka comparison tersimpan; tab yang sudah terbuka diaktifkan saja.
pub fn open_saved(t: &mut Tabular, id: i64) {
    if let Some(idx) = t.query_tabs.iter().position(|tab| {
        tab.compare_state
            .as_ref()
            .is_some_and(|s| s.saved_id == Some(id))
    }) {
        crate::editor::switch_to_tab(t, idx);
        return;
    }
    if !t.multi_compare.saved_loaded {
        jobs::reload_saved(t);
    }
    let Some(item) = t.multi_compare.saved.iter().find(|s| s.id == id).cloned() else {
        t.toasts.warning("The comparison no longer exists");
        return;
    };
    let result = t.multi_compare.results.get(&id).cloned();
    open_tab(t, MultiCompareTabState::from_saved(&item, result));
}

/// "Show" di panel Background Tasks: tampilkan tab pemilik job.
pub fn show_job_owner(t: &mut Tabular, tab_id: Option<usize>, saved_id: Option<i64>) {
    if let Some(idx) = tab_id.and_then(|id| t.query_tabs.iter().position(|tab| tab.id == id))
        && t.query_tabs[idx].compare_state.is_some()
    {
        crate::editor::switch_to_tab(t, idx);
        return;
    }
    if let Some(id) = saved_id {
        open_saved(t, id);
    }
}

// ─── Pemilih koneksi/database/tabel ─────────────────────────────────────────

fn supported_connections(t: &Tabular) -> Vec<(i64, String, DatabaseType)> {
    t.connections
        .iter()
        .filter(|c| multi_compare::supports_engine(&c.connection_type))
        .filter_map(|c| Some((c.id?, c.name.clone(), c.connection_type.clone())))
        .collect()
}

fn connection_type(t: &Tabular, id: Option<i64>) -> Option<DatabaseType> {
    let id = id?;
    t.connections
        .iter()
        .find(|c| c.id == Some(id))
        .map(|c| c.connection_type.clone())
}

fn wants_database(db: &DatabaseType) -> bool {
    needs_database(db) || matches!(db, DatabaseType::MongoDB)
}

fn mongo_pool(t: &Tabular, id: i64) -> Option<DatabasePool> {
    t.connection_pools.get(&id).cloned().or_else(|| {
        crate::connection::pool::lock_or_recover(&t.shared_connection_pools)
            .get(&id)
            .cloned()
    })
}

/// Ambil hasil pemuatan daftar tabel, lalu mulai pemuatan baru bila koneksi
/// atau database berubah. Cache sidebar dipakai dulu; server ditanya hanya
/// bila cache kosong.
fn sync_tables(t: &mut Tabular, ctx: &egui::Context, e: &mut EntryForm) {
    if let Some(slot) = &e.tables_slot
        && let Some(result) = slot.take()
    {
        e.tables_slot = None;
        match result {
            Ok(tables) => {
                e.tables = tables;
                e.tables_error = None;
            }
            Err(err) => {
                e.tables.clear();
                e.tables_error = Some(err);
            }
        }
    }
    let Some(conn_id) = e.conn_id else {
        return;
    };
    let key = (conn_id, e.database.trim().to_string());
    if e.tables_slot.is_some() || e.tables_key.as_ref() == Some(&key) {
        return;
    }
    e.tables_key = Some(key.clone());
    e.tables.clear();
    e.tables_error = None;
    let Some(conn) = t
        .connections
        .iter()
        .find(|c| c.id == Some(conn_id))
        .cloned()
    else {
        return;
    };
    let database = if key.1.is_empty() {
        conn.database.trim().to_string()
    } else {
        key.1.clone()
    };
    for kind in ["table", "view", "collection"] {
        if let Some(names) = crate::cache_data::get_tables_from_cache(t, conn_id, &database, kind) {
            e.tables.extend(names);
        }
    }
    if !e.tables.is_empty() {
        return;
    }
    match conn.connection_type {
        DatabaseType::MongoDB => {
            if database.is_empty() {
                return;
            }
            let pool = mongo_pool(t, conn_id);
            e.tables_slot = Some(t.spawn_slot(ctx, async move {
                let pool = match pool {
                    Some(pool) => pool,
                    None => {
                        crate::connection::pool::create_connection_pool_for_config(&conn).await?
                    }
                };
                let DatabasePool::MongoDB(client) = pool else {
                    return Err("Not a MongoDB connection".to_string());
                };
                let mut names = client
                    .database(&database)
                    .list_collection_names()
                    .await
                    .map_err(|e| e.to_string())?;
                names.sort();
                Ok(names)
            }));
        }
        ref db => {
            if needs_database(db) && database.is_empty() {
                return;
            }
            let Some(endpoint) = t.transfer_endpoint(conn_id, &key.1) else {
                return;
            };
            e.tables_slot = Some(t.spawn_slot(ctx, async move {
                let endpoint = endpoint.connect().await?;
                catalog::list_tables(&endpoint).await
            }));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EntryAction {
    Remove,
    MoveUp,
    MoveDown,
}

/// Sisipkan `text` di posisi kursor TextEdit `id` (akhir teks bila belum
/// pernah difokus).
fn insert_at_cursor(ctx: &egui::Context, id: egui::Id, target: &mut String, text: &str) {
    let char_pos = egui::text_edit::TextEditState::load(ctx, id)
        .and_then(|s| s.cursor.char_range())
        .map(|r| r.primary.index.0);
    let byte = char_pos
        .and_then(|pos| target.char_indices().nth(pos).map(|(b, _)| b))
        .unwrap_or(target.len());
    target.insert_str(byte, text);
}

#[allow(clippy::too_many_lines)]
fn render_entry(
    t: &mut Tabular,
    ui: &mut egui::Ui,
    e: &mut EntryForm,
    index: usize,
    count: usize,
    variables: &[CompareVariable],
    connections: &[(i64, String, DatabaseType)],
    salt: usize,
) -> Option<EntryAction> {
    let ctx = ui.ctx().clone();
    sync_tables(t, &ctx, e);
    let mut action = None;
    let title = format!("Query {}", index + 1);
    style::render_modal_card(ui, None, None, |ui| {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new(&title).strong());
            ui.add(
                egui::TextEdit::singleline(&mut e.label)
                    .hint_text("Label, e.g. MySQL prod")
                    .desired_width(200.0),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(count > 2, egui::Button::new(i::ICON_DELETE.codepoint))
                    .on_hover_text("Remove this query")
                    .clicked()
                {
                    action = Some(EntryAction::Remove);
                }
                if ui
                    .add_enabled(
                        index + 1 < count,
                        egui::Button::new(i::ICON_ARROW_DOWNWARD.codepoint),
                    )
                    .on_hover_text("Move down")
                    .clicked()
                {
                    action = Some(EntryAction::MoveDown);
                }
                if ui
                    .add_enabled(index > 0, egui::Button::new(i::ICON_ARROW_UPWARD.codepoint))
                    .on_hover_text("Move up")
                    .clicked()
                {
                    action = Some(EntryAction::MoveUp);
                }
            });
        });
        ui.add_space(6.0);

        let before_conn = e.conn_id;
        let db_type = connection_type(t, e.conn_id);
        let combo_width = (ui.available_width() - 120.0).clamp(160.0, 360.0);
        let mut picked_table: Option<String> = None;
        egui::Grid::new(("cmp_entry_grid", salt, index))
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label(egui::RichText::new("Connection").weak().small());
                let current = e
                    .conn_id
                    .and_then(|id| connections.iter().find(|c| c.0 == id))
                    .map(|c| format!("{}  ({})", c.1, c.2.as_db_str()))
                    .unwrap_or_else(|| "Select a connection".to_string());
                egui::ComboBox::from_id_salt(("cmp_conn", salt, index))
                    .selected_text(current)
                    .width(combo_width)
                    .show_ui(ui, |ui| {
                        for (id, name, kind) in connections {
                            let label = format!("{}  ({})", name, kind.as_db_str());
                            if ui.selectable_label(e.conn_id == Some(*id), label).clicked() {
                                e.conn_id = Some(*id);
                            }
                        }
                    });
                ui.end_row();

                ui.label(egui::RichText::new("Database").weak().small());
                let has_db = db_type.as_ref().is_some_and(wants_database);
                ui.add_enabled_ui(has_db, |ui| {
                    if !has_db {
                        ui.label(egui::RichText::new("Single database").weak());
                    } else if e.databases.is_empty() {
                        ui.add(
                            egui::TextEdit::singleline(&mut e.database)
                                .hint_text("Database name")
                                .desired_width(combo_width),
                        );
                    } else {
                        let text = if e.database.trim().is_empty() {
                            "Select a database".to_string()
                        } else {
                            e.database.clone()
                        };
                        if let Some(ComboPick::Item(k)) = filter_combo(
                            ui,
                            ("cmp_db", salt, index),
                            &text,
                            combo_width,
                            None,
                            &e.databases,
                        ) {
                            e.database = e.databases[k].clone();
                        }
                    }
                });
                ui.end_row();

                let is_mongo = matches!(db_type, Some(DatabaseType::MongoDB));
                ui.label(
                    egui::RichText::new(if is_mongo { "Collection" } else { "Table" })
                        .weak()
                        .small(),
                );
                ui.horizontal(|ui| {
                    let text = if e.table.is_empty() {
                        "Pick to fill the query".to_string()
                    } else {
                        e.table.clone()
                    };
                    if let Some(ComboPick::Item(k)) = filter_combo(
                        ui,
                        ("cmp_table", salt, index),
                        &text,
                        combo_width,
                        None,
                        &e.tables,
                    ) {
                        picked_table = Some(e.tables[k].clone());
                    }
                    if e.tables_slot.is_some() {
                        ui.add(egui::Spinner::new());
                    }
                });
                ui.end_row();

                ui.label(egui::RichText::new("Key columns").weak().small());
                ui.add(
                    egui::TextEdit::singleline(&mut e.key_columns)
                        .hint_text("Comma separated; default _id, id or the first column")
                        .desired_width(combo_width),
                );
                ui.end_row();

                ui.label(egui::RichText::new("Column aliases").weak().small());
                ui.add(
                    egui::TextEdit::singleline(&mut e.aliases)
                        .hint_text("source=common, e.g. customerId=customer_id")
                        .desired_width(combo_width),
                );
                ui.end_row();
            });
        if let Some(err) = &e.tables_error {
            error_label(ui, err);
        }

        let query_id = egui::Id::new(("cmp_query", salt, index));
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Query").weak().small());
            ui.add_enabled_ui(!variables.is_empty(), |ui| {
                ui.menu_button(
                    format!("{} Insert variable", i::ICON_DATA_OBJECT.codepoint),
                    |ui| {
                        for var in variables {
                            let name = var.name.trim();
                            if !multi_compare::is_valid_variable_name(name) {
                                continue;
                            }
                            let placeholder = variable_placeholder(name);
                            if ui
                                .button(&placeholder)
                                .on_hover_text(format!("Value: {}", var.value))
                                .clicked()
                            {
                                insert_at_cursor(ui.ctx(), query_id, &mut e.query, &placeholder);
                                ui.close();
                            }
                        }
                    },
                )
                .response
                .on_disabled_hover_text("Add a variable above first");
            });
        });
        ui.add(
            egui::TextEdit::multiline(&mut e.query)
                .id(query_id)
                .code_editor()
                .desired_rows(4)
                .desired_width(f32::INFINITY)
                .hint_text(if is_mongo_type(db_type.as_ref()) {
                    "{\"collection\": \"orders\", \"filter\": {\"status\": \"${status}\"}}"
                } else {
                    "SELECT o.id, o.total FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.created_at >= '${from}'"
                }),
        );
        if is_mongo_type(db_type.as_ref()) {
            muted_label(
                ui,
                "MongoDB: JSON with collection plus filter, projection, sort and limit, or a pipeline array. Nested fields become columns like address.city.",
            );
        }

        if let Some(table) = picked_table
            && let Some(kind) = db_type.clone()
        {
            e.query = multi_compare::default_query(&kind, &table);
            if e.key_columns.trim().is_empty() {
                e.key_columns = default_key(t, e, &kind, &table);
            }
            e.table = table;
        }
        if e.conn_id != before_conn {
            on_connection_changed(t, e);
        }
    });
    action
}

fn is_mongo_type(db: Option<&DatabaseType>) -> bool {
    matches!(db, Some(DatabaseType::MongoDB))
}

/// Kunci awal saat tabel dipilih: `_id` untuk MongoDB, primary key dari
/// cache untuk SQL.
fn default_key(t: &mut Tabular, e: &EntryForm, kind: &DatabaseType, table: &str) -> String {
    if matches!(kind, DatabaseType::MongoDB) {
        return "_id".to_string();
    }
    let Some(conn_id) = e.conn_id else {
        return String::new();
    };
    // Nama tabel di cache tanpa skema (mis. `dbo.orders` → `orders`).
    let bare = table.rsplit('.').next().unwrap_or(table);
    crate::cache_data::get_primary_keys_from_cache(t, conn_id, e.database.trim(), bare)
        .filter(|keys| !keys.is_empty())
        .map(|keys| keys.join(", "))
        .unwrap_or_default()
}

fn on_connection_changed(t: &mut Tabular, e: &mut EntryForm) {
    let Some(conn) = e
        .conn_id
        .and_then(|id| t.connections.iter().find(|c| c.id == Some(id)))
        .cloned()
    else {
        return;
    };
    e.database = if wants_database(&conn.connection_type) {
        conn.database.clone()
    } else {
        String::new()
    };
    e.table.clear();
    e.tables.clear();
    e.tables_key = None;
    e.tables_slot = None;
    if e.label.trim().is_empty() {
        e.label = conn.name.clone();
    }
    if e.databases_for != e.conn_id {
        e.databases_for = e.conn_id;
        e.databases = conn
            .id
            .and_then(|id| crate::cache_data::get_databases_from_cache(t, id))
            .unwrap_or_default();
    }
}

// ─── Render tab ─────────────────────────────────────────────────────────────

/// Gambar tab Comparison yang aktif.
pub fn render_active_tab(t: &mut Tabular, ui: &mut egui::Ui) {
    let idx = t.active_tab_index;
    let Some(tab) = t.query_tabs.get_mut(idx) else {
        return;
    };
    let tab_id = tab.id;
    let Some(mut st) = tab.compare_state.take() else {
        return;
    };
    let mut new_title = None;
    egui::Frame::new()
        .inner_margin(egui::Margin::symmetric(12, 10))
        .show(ui, |ui| {
            egui::ScrollArea::vertical()
                .id_salt(("compare_tab_scroll", tab_id))
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    new_title = render_tab(t, ui, tab_id, &mut st);
                });
        });
    // Tab bisa saja berganti selama render (mis. tab lain dibuka).
    if let Some(tab) = t.query_tabs.iter_mut().find(|q| q.id == tab_id) {
        if tab.compare_state.is_none() {
            tab.compare_state = Some(st);
        }
        if let Some(title) = new_title {
            tab.title = title;
        }
    }
}

fn save(t: &mut Tabular, st: &mut MultiCompareTabState) -> Result<MultiCompareSpec, String> {
    let spec = st.spec()?;
    spec.validate()?;
    let pool = t
        .db_pool
        .clone()
        .ok_or_else(|| "The local store is not available".to_string())?;
    let id = t
        .get_runtime()
        .block_on(saved::save(pool.as_ref(), st.saved_id, &spec))?;
    st.saved_id = Some(id);
    st.saved_spec = Some(spec.clone());
    jobs::reload_saved(t);
    // Comparison baru langsung terlihat di sidebar Comp.
    t.selected_database_sub_menu = "Comparisons".to_string();
    Ok(spec)
}

fn export_caption(name: &str) -> String {
    let clean: String = name
        .trim()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    format!("compare_{clean}")
}

/// Mengembalikan judul tab baru bila nama berubah lewat Save.
fn render_tab(
    t: &mut Tabular,
    ui: &mut egui::Ui,
    tab_id: usize,
    st: &mut MultiCompareTabState,
) -> Option<String> {
    let ctx = ui.ctx().clone();
    let mut new_title = None;
    let job = t
        .multi_compare
        .job_for_tab(tab_id, st.saved_id)
        .map(|j| (j.progress_text(), j.elapsed()));
    let running = job.is_some();

    // ── Kepala: nama dan tombol utama ──
    let mut save_clicked = false;
    let mut process_clicked = false;
    let mut cancel_clicked = false;
    let mut export_clicked = false;
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(i::ICON_DIFFERENCE.codepoint)
                .size(20.0)
                .color(style::theme_accent(&ctx)),
        );
        ui.add(
            egui::TextEdit::singleline(&mut st.name)
                .hint_text("Comparison name")
                .font(egui::TextStyle::Heading)
                .desired_width(320.0),
        );
        if st.unsaved() {
            muted_label(
                ui,
                if st.saved_id.is_some() {
                    "Unsaved changes"
                } else {
                    "Not saved yet"
                },
            );
        }
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            let has_rows = st.result.as_ref().is_some_and(|r| !r.diffs.is_empty());
            if ui
                .add_enabled(
                    has_rows,
                    egui::Button::new(format!("{} Export", i::ICON_FILE_DOWNLOAD.codepoint)),
                )
                .on_hover_text("Export the differing rows (CSV, Excel, JSON, ...)")
                .clicked()
            {
                export_clicked = true;
            }
            if running {
                if ui
                    .button(format!("{} Cancel", i::ICON_STOP.codepoint))
                    .clicked()
                {
                    cancel_clicked = true;
                }
            } else if ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(format!("{} PROCESS", i::ICON_PLAY_ARROW.codepoint))
                            .strong()
                            .color(egui::Color32::WHITE),
                    )
                    .fill(style::theme_accent(&ctx)),
                )
                .on_hover_text("Run every query in the background and compare the results")
                .clicked()
            {
                process_clicked = true;
            }
            if ui
                .button(format!("{} Save", i::ICON_SAVE.codepoint))
                .clicked()
            {
                save_clicked = true;
            }
        });
    });
    if let Some((text, elapsed)) = &job {
        ui.horizontal(|ui| {
            ui.add(egui::Spinner::new());
            ui.label(format!(
                "{text} · {}",
                super::background_tasks::format_duration(*elapsed)
            ));
            muted_label(
                ui,
                "Runs in the background; you can keep working in other tabs.",
            );
        });
    }
    if let Some(e) = &st.error {
        error_label(ui, e);
    }
    ui.add_space(6.0);

    if save_clicked {
        match save(t, st) {
            Ok(spec) => {
                st.error = None;
                new_title = Some(tab_title(&spec.name));
                t.toasts
                    .success(format!("Saved comparison \"{}\"", spec.name));
            }
            Err(e) => st.error = Some(e),
        }
    }
    if process_clicked {
        // Comparison tersimpan disimpan ulang dulu supaya statistik sidebar
        // sesuai dengan definisi yang dijalankan.
        let spec = if st.saved_id.is_some() && st.unsaved() {
            save(t, st).inspect(|s| new_title = Some(tab_title(&s.name)))
        } else {
            st.spec()
        };
        match spec.and_then(|spec| jobs::start(t, &ctx, Some(tab_id), st.saved_id, &spec)) {
            Ok(()) => {
                st.error = None;
                st.result_error = None;
            }
            Err(e) => st.error = Some(e),
        }
    }
    if cancel_clicked {
        jobs::cancel(t, tab_id, st.saved_id);
    }
    if export_clicked && let Some(result) = &st.result {
        t.open_export_dialog_for(result.to_table_data(), export_caption(&st.name), None);
    }

    // ── Definisi ──
    let toggle = ui.add(
        egui::Button::new(
            egui::RichText::new(format!(
                "{} Definition  ·  {} queries, {} variables",
                if st.setup_open {
                    i::ICON_EXPAND_MORE.codepoint
                } else {
                    i::ICON_CHEVRON_RIGHT.codepoint
                },
                st.entries.len(),
                st.variables.len()
            ))
            .strong(),
        )
        .frame(false),
    );
    if toggle.clicked() {
        st.setup_open = !st.setup_open;
    }
    if st.setup_open {
        ui.add_space(4.0);
        render_definition(t, ui, tab_id, st);
    }

    ui.add_space(10.0);
    ui.separator();
    render_results(ui, st);
    new_title
}

fn render_definition(
    t: &mut Tabular,
    ui: &mut egui::Ui,
    tab_id: usize,
    st: &mut MultiCompareTabState,
) {
    // Variabel
    style::render_modal_card(
        ui,
        Some("Variables"),
        Some(
            "Use ${name} in any query below. Values are inserted as-is, so add quotes in SQL where needed.",
        ),
        |ui| {
            let mut remove = None;
            egui::Grid::new(("cmp_vars", tab_id))
                .num_columns(3)
                .spacing([8.0, 6.0])
                .show(ui, |ui| {
                    for (k, var) in st.variables.iter_mut().enumerate() {
                        // Grid menyusutkan TextEdit ke isi teksnya; lebar dipaksa.
                        let h = ui.spacing().interact_size.y;
                        ui.add_sized(
                            [160.0, h],
                            egui::TextEdit::singleline(&mut var.name).hint_text("name"),
                        );
                        ui.add_sized(
                            [300.0, h],
                            egui::TextEdit::singleline(&mut var.value).hint_text("value"),
                        );
                        if ui
                            .button(i::ICON_DELETE.codepoint)
                            .on_hover_text("Remove variable")
                            .clicked()
                        {
                            remove = Some(k);
                        }
                        ui.end_row();
                    }
                });
            if let Some(k) = remove {
                st.variables.remove(k);
            }
            if ui
                .button(format!("{} Add variable", i::ICON_ADD.codepoint))
                .clicked()
            {
                st.variables.push(CompareVariable {
                    name: format!("var{}", st.variables.len() + 1),
                    value: String::new(),
                });
            }
        },
    );
    ui.add_space(8.0);

    // Query per koneksi
    ui.label(egui::RichText::new("Queries to compare").strong());
    muted_label(
        ui,
        "Rows are matched on the key columns, by position across queries. Value columns are matched by name; use aliases when names differ.",
    );
    ui.add_space(4.0);
    let connections = supported_connections(t);
    let count = st.entries.len();
    let variables = st.variables.clone();
    let mut action = None;
    for k in 0..count {
        if let Some(a) = render_entry(
            t,
            ui,
            &mut st.entries[k],
            k,
            count,
            &variables,
            &connections,
            tab_id,
        ) {
            action = Some((k, a));
        }
        ui.add_space(6.0);
    }
    match action {
        Some((k, EntryAction::Remove)) => {
            st.entries.remove(k);
        }
        Some((k, EntryAction::MoveUp)) if k > 0 => st.entries.swap(k, k - 1),
        Some((k, EntryAction::MoveDown)) if k + 1 < st.entries.len() => st.entries.swap(k, k + 1),
        _ => {}
    }
    if ui
        .button(format!("{} Add query", i::ICON_ADD.codepoint))
        .clicked()
    {
        st.entries.push(EntryForm::default());
    }
    ui.add_space(8.0);

    // Opsi
    style::render_modal_card(ui, Some("Options"), None, |ui| {
        egui::Grid::new(("cmp_opts", tab_id))
            .num_columns(2)
            .spacing([12.0, 6.0])
            .show(ui, |ui| {
                ui.label(egui::RichText::new("Row limit per query").weak().small());
                ui.add(
                    egui::TextEdit::singleline(&mut st.row_limit)
                        .hint_text("Empty = no limit")
                        .desired_width(160.0),
                );
                ui.end_row();
                ui.label(egui::RichText::new("Ignore columns").weak().small());
                ui.add(
                    egui::TextEdit::singleline(&mut st.ignore_columns)
                        .hint_text("updated_at, version")
                        .desired_width(320.0),
                );
                ui.end_row();
            });
        ui.checkbox(
            &mut st.tolerant,
            "Tolerant values (1.0 = 1, true = 1, timestamp formats)",
        );
        ui.checkbox(&mut st.trim, "Ignore trailing spaces");
        ui.checkbox(&mut st.case_insensitive, "Ignore letter case in values");
    });
}

fn clip(value: &str) -> String {
    if value.chars().count() <= MAX_CELL_CHARS {
        value.to_string()
    } else {
        let mut out: String = value.chars().take(MAX_CELL_CHARS).collect();
        out.push('…');
        out
    }
}

fn stat(ui: &mut egui::Ui, label: &str, value: usize, color: Option<egui::Color32>) {
    ui.vertical(|ui| {
        let mut text = egui::RichText::new(fmt_count(value as u64))
            .strong()
            .size(18.0);
        if let Some(c) = color {
            text = text.color(c);
        }
        ui.label(text);
        ui.label(egui::RichText::new(label).weak().small());
    });
    ui.add_space(18.0);
}

fn row_matches(diff: &multi_compare::MultiRowDiff, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    diff.key.iter().any(|k| k.to_lowercase().contains(needle))
        || diff
            .values
            .iter()
            .flatten()
            .flatten()
            .any(|v| v.to_lowercase().contains(needle))
}

fn render_results(ui: &mut egui::Ui, st: &mut MultiCompareTabState) {
    let ctx = ui.ctx().clone();
    ui.label(egui::RichText::new("Differences").strong().size(15.0));
    if let Some(e) = &st.result_error {
        error_label(ui, e);
    }
    let Some(result) = st.result.clone() else {
        if st.result_error.is_none() {
            muted_label(
                ui,
                "Press PROCESS to compare. Only keys that are missing somewhere or whose values differ are listed.",
            );
        }
        return;
    };
    let (missing, changed) = result.counts();
    ui.add_space(4.0);
    ui.horizontal(|ui| {
        stat(ui, "keys compared", result.total_keys, None);
        stat(
            ui,
            "identical",
            result.identical,
            Some(style::theme_success(&ctx)),
        );
        stat(
            ui,
            "missing somewhere",
            missing,
            Some(style::theme_danger(&ctx)),
        );
        stat(
            ui,
            "values differ",
            changed,
            Some(style::theme_warning(&ctx)),
        );
    });

    egui::Grid::new("cmp_sources")
        .num_columns(4)
        .striped(true)
        .spacing([18.0, 4.0])
        .show(ui, |ui| {
            for h in ["Query", "Rows read", "Missing here", "Duplicate keys"] {
                ui.label(egui::RichText::new(h).weak().small());
            }
            ui.end_row();
            for s in &result.sources {
                ui.label(&s.label);
                let rows = fmt_count(s.rows as u64);
                if s.truncated {
                    ui.label(
                        egui::RichText::new(format!("{rows} (limit reached)"))
                            .color(style::theme_warning(&ctx)),
                    );
                } else {
                    ui.label(rows);
                }
                ui.label(fmt_count(s.missing as u64));
                ui.label(fmt_count(s.duplicate_keys as u64));
                ui.end_row();
            }
        });
    if result.truncated() {
        warning_label(
            ui,
            "A row limit was reached; rows past the limit were not compared and may show up as missing.",
        );
    }
    if result.columns.is_empty() {
        warning_label(
            ui,
            "No value column exists in every query, so only the presence of keys was compared. Use column aliases to match names.",
        );
    }
    if !result.partial_columns.is_empty() {
        warning_label(
            ui,
            &format!(
                "Not compared (missing from some queries): {}",
                result.partial_columns.join(", ")
            ),
        );
    }
    if result.diffs.is_empty() {
        ui.add_space(6.0);
        ui.label(
            egui::RichText::new(format!("{} All rows match.", i::ICON_CHECK.codepoint))
                .color(style::theme_success(&ctx)),
        );
        return;
    }

    ui.add_space(6.0);
    ui.horizontal(|ui| {
        ui.checkbox(
            &mut st.show_missing,
            format!("Missing ({})", fmt_count(missing as u64)),
        );
        ui.checkbox(
            &mut st.show_changed,
            format!("Changed ({})", fmt_count(changed as u64)),
        );
        style::render_search_field(ui, &mut st.result_filter, "Filter rows…", 220.0);
    });
    let needle = st.result_filter.trim().to_lowercase();
    let visible: Vec<&multi_compare::MultiRowDiff> = result
        .diffs
        .iter()
        .filter(|d| {
            if d.is_missing() {
                st.show_missing
            } else {
                st.show_changed
            }
        })
        .filter(|d| row_matches(d, &needle))
        .collect();
    let headers = result.table_headers();
    let key_count = result.key_columns.len();
    let sources = result.labels.len();
    let danger = style::theme_danger(&ctx);
    let warn = style::theme_warning(&ctx);
    let muted = style::theme_muted_text(&ctx);

    ui.add_space(4.0);
    egui::ScrollArea::horizontal()
        .id_salt("cmp_result_scroll")
        .show(ui, |ui| {
            egui::Grid::new("cmp_result_grid")
                .striped(true)
                .spacing([14.0, 4.0])
                .show(ui, |ui| {
                    for h in &headers {
                        ui.label(egui::RichText::new(h).strong().small());
                    }
                    ui.end_row();
                    for diff in visible.iter().take(MAX_ROWS_SHOWN) {
                        for k in &diff.key {
                            let text = if k == crate::data_transfer::NULL_MARKER {
                                "NULL".to_string()
                            } else {
                                clip(k)
                            };
                            ui.label(egui::RichText::new(text).family(egui::FontFamily::Monospace));
                        }
                        let status = diff.status(&result.labels, &result.columns);
                        ui.label(
                            egui::RichText::new(status)
                                .small()
                                .color(if diff.is_missing() { danger } else { warn }),
                        );
                        for c in 0..result.columns.len() {
                            let differs = diff.changed.contains(&c);
                            for s in 0..sources {
                                if !diff.present.get(s).copied().unwrap_or(false) {
                                    ui.label(
                                        egui::RichText::new("missing")
                                            .italics()
                                            .small()
                                            .color(danger),
                                    );
                                    continue;
                                }
                                let value =
                                    diff.values.get(s).and_then(|v| v.get(c)).cloned().flatten();
                                let mut text = match &value {
                                    Some(v) => egui::RichText::new(clip(v))
                                        .family(egui::FontFamily::Monospace),
                                    None => egui::RichText::new("NULL").italics().color(muted),
                                };
                                if differs {
                                    text = text.color(warn).strong();
                                }
                                let response = ui.label(text);
                                if let Some(v) = value
                                    && v.chars().count() > MAX_CELL_CHARS
                                {
                                    response.on_hover_text(v);
                                }
                            }
                        }
                        ui.end_row();
                    }
                });
        });
    let _ = key_count;
    if visible.len() > MAX_ROWS_SHOWN {
        muted_label(
            ui,
            format!(
                "Showing the first {} of {} rows. Export to get all of them.",
                fmt_count(MAX_ROWS_SHOWN as u64),
                fmt_count(visible.len() as u64)
            ),
        );
    }
}

// ─── Sidebar ────────────────────────────────────────────────────────────────

fn last_run_line(run: &LastRun) -> String {
    match &run.error {
        Some(e) => format!("Last run {} failed: {}", run.ran_at, clip(e)),
        None => format!(
            "Last run {}: {} rows read, {} keys, {} differ",
            run.ran_at,
            fmt_count(run.rows_read() as u64),
            fmt_count(run.total_keys as u64),
            fmt_count(run.differ() as u64)
        ),
    }
}

/// Salin comparison dengan nama baru yang belum dipakai.
fn duplicate(t: &mut Tabular, id: i64) -> Result<i64, String> {
    let item = t
        .multi_compare
        .saved
        .iter()
        .find(|s| s.id == id)
        .cloned()
        .ok_or_else(|| "The comparison no longer exists".to_string())?;
    let taken = |name: &str| {
        t.multi_compare
            .saved
            .iter()
            .any(|s| s.spec.name.eq_ignore_ascii_case(name))
    };
    let mut spec = item.spec.clone();
    let base = format!("{} copy", item.spec.name);
    spec.name = base.clone();
    let mut n = 2;
    while taken(&spec.name) {
        spec.name = format!("{base} {n}");
        n += 1;
    }
    let pool = t
        .db_pool
        .clone()
        .ok_or_else(|| "The local store is not available".to_string())?;
    let new_id = t
        .get_runtime()
        .block_on(saved::save(pool.as_ref(), None, &spec))?;
    jobs::reload_saved(t);
    Ok(new_id)
}

fn delete(t: &mut Tabular, id: i64) {
    let Some(pool) = t.db_pool.clone() else {
        return;
    };
    if let Err(e) = t.get_runtime().block_on(saved::delete(pool.as_ref(), id)) {
        t.toasts
            .error(format!("Deleting the comparison failed: {e}"));
        return;
    }
    t.multi_compare.results.remove(&id);
    // Tab yang masih terbuka menjadi comparison yang belum disimpan.
    for tab in t.query_tabs.iter_mut() {
        if let Some(state) = tab.compare_state.as_mut()
            && state.saved_id == Some(id)
        {
            state.saved_id = None;
            state.saved_spec = None;
        }
    }
    jobs::reload_saved(t);
}

enum SidebarAction {
    Open(i64),
    Run(i64),
    Duplicate(i64),
    Delete(i64, String),
}

/// Daftar comparison tersimpan di sidebar DBs > Comp. `filter` = teks
/// pencarian sidebar.
pub fn render_sidebar(t: &mut Tabular, ui: &mut egui::Ui, filter: &str) {
    if !t.multi_compare.saved_loaded {
        jobs::reload_saved(t);
    }
    let ctx = ui.ctx().clone();
    let needle = filter.trim().to_lowercase();
    let items: Vec<SavedMultiCompare> = t
        .multi_compare
        .saved
        .iter()
        .filter(|s| needle.is_empty() || s.spec.name.to_lowercase().contains(&needle))
        .cloned()
        .collect();
    if items.is_empty() {
        ui.add_space(8.0);
        if t.multi_compare.saved.is_empty() {
            ui.label("No comparisons yet");
            muted_label(
                ui,
                "Click + to compare query results across MySQL, PostgreSQL, SQL Server, SQLite and MongoDB connections.",
            );
        } else {
            muted_label(ui, "No comparison matches the search");
        }
    }
    let mut action = None;
    for item in &items {
        let running = t
            .multi_compare
            .running_for_saved(item.id)
            .map(|j| j.progress_text());
        let frame = egui::Frame::new()
            .inner_margin(egui::Margin::symmetric(8, 6))
            .corner_radius(6.0)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(i::ICON_DIFFERENCE.codepoint)
                            .color(style::theme_accent(&ctx)),
                    );
                    ui.label(egui::RichText::new(&item.spec.name).strong());
                });
                muted_label(
                    ui,
                    format!(
                        "{} queries · {} connections · {} variables",
                        item.spec.entries.len(),
                        item.spec.connection_count(),
                        item.spec.variables.len()
                    ),
                );
                if let Some(text) = &running {
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new().size(12.0));
                        ui.label(egui::RichText::new(text).small());
                    });
                } else {
                    match &item.last_run {
                        Some(run) if run.error.is_some() => {
                            error_label(ui, &last_run_line(run));
                        }
                        Some(run) => muted_label(ui, last_run_line(run)),
                        None => muted_label(ui, "Never run"),
                    }
                }
            });
        let response = ui.interact(
            frame.response.rect,
            ui.id().with(("cmp_sidebar_item", item.id)),
            egui::Sense::click(),
        );
        if response.hovered() {
            ui.painter().rect_stroke(
                frame.response.rect,
                6.0,
                egui::Stroke::new(1.0, style::theme_accent(&ctx)),
                egui::StrokeKind::Inside,
            );
        }
        if response.clicked() {
            action = Some(SidebarAction::Open(item.id));
        }
        response.context_menu(|ui| {
            if ui.button("Open").clicked() {
                action = Some(SidebarAction::Open(item.id));
                ui.close();
            }
            if ui
                .add_enabled(running.is_none(), egui::Button::new("Run in background"))
                .clicked()
            {
                action = Some(SidebarAction::Run(item.id));
                ui.close();
            }
            if ui.button("Duplicate").clicked() {
                action = Some(SidebarAction::Duplicate(item.id));
                ui.close();
            }
            ui.separator();
            if ui.button("Delete…").clicked() {
                action = Some(SidebarAction::Delete(item.id, item.spec.name.clone()));
                ui.close();
            }
        });
        ui.add_space(2.0);
    }

    match action {
        Some(SidebarAction::Open(id)) => open_saved(t, id),
        Some(SidebarAction::Run(id)) => {
            let spec = items.iter().find(|s| s.id == id).map(|s| s.spec.clone());
            if let Some(spec) = spec {
                match jobs::start(t, &ctx, None, Some(id), &spec) {
                    Ok(()) => t
                        .toasts
                        .info(format!("Comparing \"{}\" in the background", spec.name)),
                    Err(e) => t.toasts.error(e),
                }
            }
        }
        Some(SidebarAction::Duplicate(id)) => match duplicate(t, id) {
            Ok(new_id) => open_saved(t, new_id),
            Err(e) => t.toasts.error(e),
        },
        Some(SidebarAction::Delete(id, name)) => t.multi_compare.confirm_delete = Some((id, name)),
        None => {}
    }

    if let Some((id, name)) = t.multi_compare.confirm_delete.clone() {
        let mut confirmed = false;
        let mut cancelled = false;
        let close = modal(
            &ctx,
            "cmp_delete_confirm",
            "Delete comparison",
            egui::vec2(420.0, 0.0),
            |ui| {
                ui.label(format!(
                    "Delete \"{name}\"? Its queries and run statistics are removed."
                ));
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("Delete").color(egui::Color32::WHITE),
                            )
                            .fill(style::theme_danger(ui.ctx())),
                        )
                        .clicked()
                    {
                        confirmed = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancelled = true;
                    }
                });
            },
        );
        if confirmed {
            delete(t, id);
        }
        if confirmed || cancelled || close {
            t.multi_compare.confirm_delete = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_roundtrips_through_spec() {
        let spec = MultiCompareSpec {
            name: "Orders".to_string(),
            variables: vec![CompareVariable {
                name: "from".to_string(),
                value: "2026-01-01".to_string(),
            }],
            entries: vec![
                CompareEntry {
                    label: "MySQL".to_string(),
                    connection_id: 3,
                    database: Some("shop".to_string()),
                    query: "SELECT 1".to_string(),
                    key_columns: vec!["id".to_string(), "line".to_string()],
                    aliases: vec![("a".to_string(), "b".to_string())],
                },
                CompareEntry {
                    connection_id: 4,
                    query: "{\"collection\":\"o\"}".to_string(),
                    ..Default::default()
                },
            ],
            options: MultiCompareOptions {
                row_limit: None,
                ignore_columns: vec!["ts".to_string()],
                ..Default::default()
            },
        };
        let state = MultiCompareTabState::from_spec(Some(1), &spec, None);
        assert_eq!(state.spec().unwrap(), spec);
        assert!(!state.unsaved());
        assert!(state.setup_open);
    }

    #[test]
    fn new_tab_has_two_empty_queries_and_rejects_bad_limit() {
        let mut state = MultiCompareTabState::default();
        assert_eq!(state.entries.len(), 2);
        assert!(state.unsaved());
        state.row_limit = "abc".to_string();
        assert!(state.spec().is_err());
        state.row_limit = "1,000".to_string();
        assert_eq!(state.spec().unwrap().options.row_limit, Some(1000));
    }

    #[test]
    fn export_caption_is_file_safe() {
        assert_eq!(export_caption("Orders / Q1"), "compare_Orders___Q1");
    }
}
