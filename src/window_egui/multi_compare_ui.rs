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

/// Koneksi yang bisa dipilih: milik project aktif saja (semua bila tidak ada
/// project aktif). Koneksi di `keep` (sudah dipilih entri) selalu ikut supaya
/// comparison lama tidak kehilangan koneksinya di combobox.
fn supported_connections(t: &Tabular, keep: &[i64]) -> Vec<(i64, String, DatabaseType)> {
    let project = super::project_ui::active(t);
    t.connections
        .iter()
        .filter(|c| multi_compare::supports_engine(&c.connection_type))
        .filter(|c| {
            project.is_none_or(|p| p.owns_connection(c))
                || c.id.is_some_and(|id| keep.contains(&id))
        })
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

// ─── Komponen tampilan ──────────────────────────────────────────────────────

/// Warna pembeda sumber A, B, C, …; sama di kartu sumber dan header hasil.
fn source_color(index: usize, dark: bool) -> egui::Color32 {
    const DARK: [(u8, u8, u8); 6] = [
        (96, 165, 250),
        (167, 139, 250),
        (45, 212, 191),
        (251, 146, 60),
        (244, 114, 182),
        (163, 230, 53),
    ];
    const LIGHT: [(u8, u8, u8); 6] = [
        (37, 99, 235),
        (124, 58, 237),
        (13, 148, 136),
        (234, 88, 12),
        (219, 39, 119),
        (77, 124, 15),
    ];
    let (r, g, b) = if dark { DARK } else { LIGHT }[index % DARK.len()];
    egui::Color32::from_rgb(r, g, b)
}

fn source_letter(index: usize) -> String {
    char::from(b'A' + (index % 26) as u8).to_string()
}

/// Lencana huruf sumber berwarna (A, B, C, …).
fn source_badge(ui: &mut egui::Ui, index: usize, size: f32) {
    let color = source_color(index, ui.visuals().dark_mode);
    let (rect, _) = ui.allocate_exact_size(egui::vec2(size, size), egui::Sense::hover());
    ui.painter().rect_filled(rect, 5.0, color);
    ui.painter().text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        source_letter(index),
        egui::FontId::proportional(size * 0.6),
        egui::Color32::WHITE,
    );
}

/// Judul langkah bernomor. `add_right` mengisi sisi kanan baris.
fn step_header(
    ui: &mut egui::Ui,
    number: usize,
    title: &str,
    hint: &str,
    add_right: impl FnOnce(&mut egui::Ui),
) {
    let ctx = ui.ctx().clone();
    ui.horizontal(|ui| {
        ui.set_min_height(28.0);
        let (rect, _) = ui.allocate_exact_size(egui::vec2(22.0, 22.0), egui::Sense::hover());
        ui.painter().circle(
            rect.center(),
            11.0,
            style::nav_raised(&ctx),
            egui::Stroke::new(1.0, style::nav_border(&ctx)),
        );
        ui.painter().text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            number.to_string(),
            egui::FontId::proportional(12.0),
            style::nav_text_strong(&ctx),
        );
        ui.label(egui::RichText::new(title).strong().size(14.5));
        if !hint.is_empty() {
            ui.label(
                egui::RichText::new(hint)
                    .size(11.5)
                    .color(style::nav_text_muted(&ctx)),
            );
        }
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), add_right);
    });
    ui.add_space(4.0);
}

/// Label kecil di atas field.
fn caption(ui: &mut egui::Ui, text: &str) {
    ui.label(
        egui::RichText::new(text)
            .size(10.5)
            .strong()
            .color(style::nav_text_muted(ui.ctx())),
    );
}

/// Kotak non-aktif selebar field, untuk pilihan yang tidak berlaku.
fn placeholder_box(ui: &mut egui::Ui, width: f32, text: &str) {
    let ctx = ui.ctx().clone();
    egui::Frame::new()
        .fill(style::nav_track(&ctx))
        .corner_radius(4.0)
        .inner_margin(egui::Margin::symmetric(8, 3))
        .show(ui, |ui| {
            ui.set_width(width - 16.0);
            ui.set_min_height(ui.spacing().interact_size.y + 2.0);
            ui.label(
                egui::RichText::new(text)
                    .size(12.0)
                    .color(style::nav_text_muted(&ctx)),
            );
        });
}

/// Tombol ikon kecil tanpa bingkai.
fn icon_button(ui: &mut egui::Ui, icon: &str, tip: &str, enabled: bool) -> bool {
    ui.add_enabled(
        enabled,
        egui::Button::new(egui::RichText::new(icon).size(15.0)).frame(false),
    )
    .on_hover_text(tip)
    .clicked()
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
    let dark = ctx.global_style().visuals.dark_mode;
    let color = source_color(index, dark);
    let db_type = connection_type(t, e.conn_id);
    let is_mongo = is_mongo_type(db_type.as_ref());
    let before_conn = e.conn_id;
    let mut picked_table: Option<String> = None;

    let card = style::modal_card_frame(&ctx)
        .inner_margin(egui::Margin {
            left: 12,
            right: 12,
            top: 12,
            bottom: 10,
        })
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            let full = ui.available_width();

            // Kepala kartu: huruf sumber, label, aksi.
            ui.horizontal(|ui| {
                source_badge(ui, index, 22.0);
                ui.add(
                    egui::TextEdit::singleline(&mut e.label)
                        .hint_text(format!("Source {} name", source_letter(index)))
                        .font(egui::TextStyle::Body)
                        .desired_width((full - 110.0).max(80.0)),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if icon_button(ui, i::ICON_DELETE.codepoint, "Remove source", count > 2) {
                        action = Some(EntryAction::Remove);
                    }
                    if icon_button(
                        ui,
                        i::ICON_ARROW_FORWARD.codepoint,
                        "Move right",
                        index + 1 < count,
                    ) {
                        action = Some(EntryAction::MoveDown);
                    }
                    if icon_button(ui, i::ICON_ARROW_BACK.codepoint, "Move left", index > 0) {
                        action = Some(EntryAction::MoveUp);
                    }
                });
            });
            ui.add_space(8.0);

            // Koneksi.
            caption(ui, "CONNECTION");
            let current = e
                .conn_id
                .and_then(|id| connections.iter().find(|c| c.0 == id))
                .map(|c| format!("{}  ·  {}", c.1, c.2.as_db_str()))
                .unwrap_or_else(|| "Select a connection".to_string());
            egui::ComboBox::from_id_salt(("cmp_conn", salt, index))
                .selected_text(current)
                .width(full)
                .truncate()
                .show_ui(ui, |ui| {
                    if connections.is_empty() {
                        ui.label(
                            egui::RichText::new("No SQL or MongoDB connection in this project")
                                .weak()
                                .small(),
                        );
                    }
                    for (id, name, kind) in connections {
                        let label = format!("{}  ·  {}", name, kind.as_db_str());
                        if ui.selectable_label(e.conn_id == Some(*id), label).clicked() {
                            e.conn_id = Some(*id);
                        }
                    }
                });
            ui.add_space(6.0);

            // Database dan tabel berdampingan.
            let gap = 8.0;
            let half = ((full - gap) / 2.0).floor();
            ui.horizontal_top(|ui| {
                ui.spacing_mut().item_spacing.x = gap;
                ui.vertical(|ui| {
                    ui.set_width(half);
                    ui.horizontal(|ui| caption(ui, "DATABASE"));
                    let has_db = db_type.as_ref().is_some_and(wants_database);
                    if e.conn_id.is_none() {
                        placeholder_box(ui, half, "Pick a connection");
                    } else if !has_db {
                        placeholder_box(ui, half, "Single database");
                    } else if e.databases.is_empty() {
                        ui.add(
                            egui::TextEdit::singleline(&mut e.database)
                                .hint_text("Database name")
                                .desired_width(half),
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
                            half,
                            None,
                            &e.databases,
                        ) {
                            e.database = e.databases[k].clone();
                        }
                    }
                });
                ui.vertical(|ui| {
                    ui.set_width(half);
                    ui.horizontal(|ui| {
                        caption(ui, if is_mongo { "COLLECTION" } else { "TABLE" });
                        if e.tables_slot.is_some() {
                            ui.add(egui::Spinner::new().size(10.0));
                        }
                    });
                    let text = if e.table.is_empty() {
                        "Fill query from…".to_string()
                    } else {
                        e.table.clone()
                    };
                    ui.add_enabled_ui(e.conn_id.is_some(), |ui| {
                        if let Some(ComboPick::Item(k)) = filter_combo(
                            ui,
                            ("cmp_table", salt, index),
                            &text,
                            half,
                            None,
                            &e.tables,
                        ) {
                            picked_table = Some(e.tables[k].clone());
                        }
                    });
                });
            });
            if let Some(err) = &e.tables_error {
                error_label(ui, err);
            }
            ui.add_space(6.0);

            // Query.
            let query_id = egui::Id::new(("cmp_query", salt, index));
            ui.horizontal(|ui| {
                caption(ui, if is_mongo { "QUERY (JSON)" } else { "QUERY" });
                if is_mongo {
                    ui.label(
                        egui::RichText::new(i::ICON_HELP_OUTLINE.codepoint)
                            .size(13.0)
                            .color(style::nav_text_muted(&ctx)),
                    )
                    .on_hover_text(
                        "JSON with \"collection\" plus filter, projection, sort and limit, \
                         or a \"pipeline\" array. Nested fields become columns like address.city.",
                    );
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.add_enabled_ui(!variables.is_empty(), |ui| {
                        ui.menu_button(
                            egui::RichText::new(format!(
                                "{} Variable",
                                i::ICON_DATA_OBJECT.codepoint
                            ))
                            .size(11.5),
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
                                        insert_at_cursor(
                                            ui.ctx(),
                                            query_id,
                                            &mut e.query,
                                            &placeholder,
                                        );
                                        ui.close();
                                    }
                                }
                            },
                        )
                        .response
                        .on_hover_text("Insert a variable at the cursor")
                        .on_disabled_hover_text("Add a variable in step 1 first");
                    });
                });
            });
            ui.add(
                egui::TextEdit::multiline(&mut e.query)
                    .id(query_id)
                    .code_editor()
                    .desired_rows(5)
                    .desired_width(f32::INFINITY)
                    .hint_text(if is_mongo {
                        "{\"collection\": \"orders\", \"filter\": {\"status\": \"${status}\"}}"
                    } else {
                        "SELECT o.id, c.name, o.total\nFROM orders o JOIN customers c ON c.id = o.customer_id\nWHERE o.created_at >= '${from}'"
                    }),
            );
            ui.add_space(6.0);

            // Kunci dan alias berdampingan.
            ui.horizontal_top(|ui| {
                ui.spacing_mut().item_spacing.x = gap;
                ui.vertical(|ui| {
                    ui.set_width(half);
                    ui.horizontal(|ui| {
                        caption(ui, "KEY COLUMNS");
                        ui.label(
                            egui::RichText::new(i::ICON_HELP_OUTLINE.codepoint)
                                .size(14.0)
                                .color(style::nav_text_muted(&ctx)),
                        )
                        .on_hover_text(
                            "Columns that identify a row, comma separated. The 1st key of every \
                             source is matched with the 1st key of the others, and so on. \
                             Empty = _id, id, or the first column.",
                        );
                    });
                    ui.add(
                        egui::TextEdit::singleline(&mut e.key_columns)
                            .hint_text("id")
                            .desired_width(half),
                    );
                });
                ui.vertical(|ui| {
                    ui.set_width(half);
                    ui.horizontal(|ui| {
                        caption(ui, "RENAME COLUMNS");
                        ui.label(
                            egui::RichText::new(i::ICON_HELP_OUTLINE.codepoint)
                                .size(14.0)
                                .color(style::nav_text_muted(&ctx)),
                        )
                        .on_hover_text(
                            "Value columns are compared by name. When this source uses another \
                             name, map it: source=common, e.g. amount=total.",
                        );
                    });
                    ui.add(
                        egui::TextEdit::singleline(&mut e.aliases)
                            .hint_text("amount=total")
                            .desired_width(half),
                    );
                });
            });
        });

    // Garis warna sumber di tepi atas kartu.
    let rect = card.response.rect;
    ui.painter().rect_filled(
        egui::Rect::from_min_size(rect.min, egui::vec2(rect.width(), 3.0)),
        egui::CornerRadius {
            nw: 8,
            ne: 8,
            sw: 0,
            se: 0,
        },
        color,
    );

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

/// Gambar tab Comparison yang aktif: toolbar tetap di atas, isi bergulir.
pub fn render_active_tab(t: &mut Tabular, ui: &mut egui::Ui) {
    let idx = t.active_tab_index;
    let Some(tab) = t.query_tabs.get_mut(idx) else {
        return;
    };
    let tab_id = tab.id;
    let Some(mut st) = tab.compare_state.take() else {
        return;
    };
    let ctx = ui.ctx().clone();
    let job = t
        .multi_compare
        .job_for_tab(tab_id, st.saved_id)
        .map(|j| (j.progress_text(), j.elapsed()));
    let actions = egui::Frame::new()
        .inner_margin(egui::Margin {
            left: 14,
            right: 14,
            top: 10,
            bottom: 8,
        })
        .show(ui, |ui| render_toolbar(ui, &mut st, job.as_ref()))
        .inner;
    let y = ui.cursor().top();
    ui.painter().hline(
        ui.max_rect().x_range(),
        y,
        egui::Stroke::new(1.0, style::nav_border(&ctx)),
    );
    let new_title = apply_toolbar(t, &ctx, tab_id, &mut st, actions);
    egui::ScrollArea::vertical()
        .id_salt(("compare_tab_scroll", tab_id))
        .auto_shrink([false, false])
        .show(ui, |ui| {
            egui::Frame::new()
                .inner_margin(egui::Margin {
                    left: 14,
                    right: 14,
                    top: 12,
                    bottom: 16,
                })
                .show(ui, |ui| render_body(t, ui, tab_id, &mut st));
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

#[derive(Default)]
struct ToolbarActions {
    save: bool,
    process: bool,
    cancel: bool,
    export: bool,
}

fn status_badge(ui: &mut egui::Ui, text: &str, color: egui::Color32) {
    style::render_badge(ui, text, color.gamma_multiply(0.18), color);
}

fn render_toolbar(
    ui: &mut egui::Ui,
    st: &mut MultiCompareTabState,
    job: Option<&(String, std::time::Duration)>,
) -> ToolbarActions {
    let ctx = ui.ctx().clone();
    let mut a = ToolbarActions::default();
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(i::ICON_DIFFERENCE.codepoint)
                .size(20.0)
                .color(style::theme_accent(&ctx)),
        );
        style::render_text_field(
            ui,
            egui::TextEdit::singleline(&mut st.name).hint_text("Name this comparison"),
            300.0,
            None,
        );
        let (text, color) = if st.saved_id.is_none() {
            ("NEW", style::nav_text_muted(&ctx))
        } else if st.unsaved() {
            ("UNSAVED", style::theme_warning(&ctx))
        } else {
            ("SAVED", style::theme_success(&ctx))
        };
        status_badge(ui, text, color);
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            if job.is_some() {
                let btn = style::btn_field_action(ui, format!("{} Cancel", i::ICON_STOP.codepoint));
                if ui.add(btn).clicked() {
                    a.cancel = true;
                }
            } else {
                let btn = style::btn_field_action_primary(
                    ui,
                    format!("{}  PROCESS", i::ICON_PLAY_ARROW.codepoint),
                );
                if ui
                    .add(btn)
                    .on_hover_text("Run every source in the background and list the differences")
                    .clicked()
                {
                    a.process = true;
                }
            }
            let has_rows = st.result.as_ref().is_some_and(|r| !r.diffs.is_empty());
            let btn =
                style::btn_field_action(ui, format!("{} Export", i::ICON_FILE_DOWNLOAD.codepoint));
            if ui
                .add_enabled(has_rows, btn)
                .on_hover_text("Export every differing row (CSV, Excel, JSON, …)")
                .on_disabled_hover_text("Nothing to export yet")
                .clicked()
            {
                a.export = true;
            }
            let btn = style::btn_field_action(ui, format!("{} Save", i::ICON_SAVE.codepoint));
            if ui
                .add(btn)
                .on_hover_text("Save to the Comp list in the sidebar")
                .clicked()
            {
                a.save = true;
            }
            let label = if st.setup_open {
                "Hide setup"
            } else {
                "Edit setup"
            };
            let btn = style::btn_field_action(ui, format!("{} {label}", i::ICON_TUNE.codepoint));
            if ui.add(btn).clicked() {
                st.setup_open = !st.setup_open;
            }
        });
    });
    if let Some((text, elapsed)) = job {
        ui.add_space(6.0);
        egui::Frame::new()
            .fill(style::nav_track(&ctx))
            .corner_radius(6.0)
            .inner_margin(egui::Margin::symmetric(10, 6))
            .show(ui, |ui| {
                ui.set_min_width(ui.available_width());
                ui.horizontal(|ui| {
                    ui.add(egui::Spinner::new().size(14.0));
                    ui.label(egui::RichText::new(text).strong());
                    ui.label(
                        egui::RichText::new(super::background_tasks::format_duration(*elapsed))
                            .color(style::nav_text_muted(&ctx)),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(
                            egui::RichText::new(
                                "Running in the background. You can keep working in other tabs.",
                            )
                            .size(11.5)
                            .color(style::nav_text_muted(&ctx)),
                        );
                    });
                });
            });
    }
    if let Some(e) = &st.error {
        ui.add_space(6.0);
        style::theme_alert_frame(&ctx, true).show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.label(
                egui::RichText::new(format!("{}  {e}", i::ICON_ERROR.codepoint))
                    .color(style::theme_danger(&ctx)),
            );
        });
    }
    a
}

/// Jalankan aksi toolbar. Mengembalikan judul tab baru bila nama berubah.
fn apply_toolbar(
    t: &mut Tabular,
    ctx: &egui::Context,
    tab_id: usize,
    st: &mut MultiCompareTabState,
    a: ToolbarActions,
) -> Option<String> {
    let mut new_title = None;
    if a.save {
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
    if a.process {
        // Comparison tersimpan disimpan ulang dulu supaya statistik sidebar
        // sesuai dengan definisi yang dijalankan.
        let spec = if st.saved_id.is_some() && st.unsaved() {
            save(t, st).inspect(|s| new_title = Some(tab_title(&s.name)))
        } else {
            st.spec()
        };
        match spec.and_then(|spec| jobs::start(t, ctx, Some(tab_id), st.saved_id, &spec)) {
            Ok(()) => {
                st.error = None;
                st.result_error = None;
            }
            Err(e) => st.error = Some(e),
        }
    }
    if a.cancel {
        jobs::cancel(t, tab_id, st.saved_id);
    }
    if a.export
        && let Some(result) = &st.result
    {
        t.open_export_dialog_for(result.to_table_data(), export_caption(&st.name), None);
    }
    new_title
}

fn render_body(t: &mut Tabular, ui: &mut egui::Ui, tab_id: usize, st: &mut MultiCompareTabState) {
    if st.setup_open {
        render_variables(ui, st);
        ui.add_space(14.0);
        render_sources(t, ui, tab_id, st);
        ui.add_space(4.0);
        render_options(ui, st);
        ui.add_space(16.0);
    } else {
        render_setup_summary(ui, st);
        ui.add_space(12.0);
    }
    render_results(ui, st);
}

/// Ringkasan satu baris saat form disembunyikan.
fn render_setup_summary(ui: &mut egui::Ui, st: &mut MultiCompareTabState) {
    let ctx = ui.ctx().clone();
    egui::Frame::new()
        .fill(style::nav_track(&ctx))
        .corner_radius(6.0)
        .inner_margin(egui::Margin::symmetric(10, 6))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal_wrapped(|ui| {
                for (k, e) in st.entries.iter().enumerate() {
                    source_badge(ui, k, 16.0);
                    let label = multi_compare::entry_label(&e.to_entry(), k);
                    ui.label(egui::RichText::new(label).strong());
                    ui.add_space(8.0);
                }
                if !st.variables.is_empty() {
                    let vars: Vec<String> = st
                        .variables
                        .iter()
                        .map(|v| format!("{}={}", v.name.trim(), clip(&v.value)))
                        .collect();
                    ui.label(
                        egui::RichText::new(vars.join("  "))
                            .family(egui::FontFamily::Monospace)
                            .size(11.5)
                            .color(style::nav_text_muted(&ctx)),
                    );
                }
            });
        });
}

fn render_variables(ui: &mut egui::Ui, st: &mut MultiCompareTabState) {
    let ctx = ui.ctx().clone();
    step_header(
        ui,
        1,
        "Variables",
        "Optional. Define a value once and use it in every query as ${name}.",
        |_| {},
    );
    let mut remove = None;
    let mut add = false;
    let muted = style::nav_text_muted(&ctx);
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = egui::vec2(8.0, 8.0);
        for (k, var) in st.variables.iter_mut().enumerate() {
            egui::Frame::new()
                .fill(style::nav_track(&ctx))
                .stroke(egui::Stroke::new(1.0, style::nav_border(&ctx)))
                .corner_radius(6.0)
                .inner_margin(egui::Margin {
                    left: 8,
                    right: 4,
                    top: 3,
                    bottom: 3,
                })
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 4.0;
                        let h = ui.spacing().interact_size.y;
                        let mono = |s: &str| {
                            egui::RichText::new(s)
                                .family(egui::FontFamily::Monospace)
                                .color(muted)
                        };
                        ui.label(mono("${"));
                        ui.add_sized(
                            [96.0, h],
                            egui::TextEdit::singleline(&mut var.name)
                                .hint_text("name")
                                .font(egui::TextStyle::Monospace),
                        );
                        ui.label(mono("} ="));
                        ui.add_sized(
                            [160.0, h],
                            egui::TextEdit::singleline(&mut var.value).hint_text("value"),
                        );
                        if icon_button(ui, i::ICON_CLOSE.codepoint, "Remove variable", true) {
                            remove = Some(k);
                        }
                    });
                });
        }
        let btn = style::btn_field_action(ui, format!("{} Add variable", i::ICON_ADD.codepoint));
        if ui.add(btn).clicked() {
            add = true;
        }
    });
    if let Some(k) = remove {
        st.variables.remove(k);
    }
    if add {
        st.variables.push(CompareVariable {
            name: format!("var{}", st.variables.len() + 1),
            value: String::new(),
        });
    }
}

fn render_sources(
    t: &mut Tabular,
    ui: &mut egui::Ui,
    tab_id: usize,
    st: &mut MultiCompareTabState,
) {
    super::project_ui::ensure_loaded(t);
    let selected: Vec<i64> = st.entries.iter().filter_map(|e| e.conn_id).collect();
    let connections = supported_connections(t, &selected);
    let mut add = false;
    step_header(
        ui,
        2,
        "Sources",
        "One query per connection. Rows are matched on the key columns.",
        |ui| {
            let btn = style::btn_field_action(ui, format!("{} Add source", i::ICON_ADD.codepoint));
            if ui.add(btn).clicked() {
                add = true;
            }
        },
    );
    let count = st.entries.len();
    let avail = ui.available_width();
    // Kartu berdampingan: 1 kolom di layar sempit, sampai 3 kolom.
    let cols = (((avail + 12.0) / 370.0).floor() as usize)
        .clamp(1, 3)
        .min(count.max(1));
    let variables = st.variables.clone();
    let mut action = None;
    let indices: Vec<usize> = (0..count).collect();
    for chunk in indices.chunks(cols) {
        ui.columns(cols, |columns| {
            for (ci, &k) in chunk.iter().enumerate() {
                if let Some(a) = render_entry(
                    t,
                    &mut columns[ci],
                    &mut st.entries[k],
                    k,
                    count,
                    &variables,
                    &connections,
                    tab_id,
                ) {
                    action = Some((k, a));
                }
            }
        });
        ui.add_space(10.0);
    }
    match action {
        Some((k, EntryAction::Remove)) => {
            st.entries.remove(k);
        }
        Some((k, EntryAction::MoveUp)) if k > 0 => st.entries.swap(k, k - 1),
        Some((k, EntryAction::MoveDown)) if k + 1 < st.entries.len() => st.entries.swap(k, k + 1),
        _ => {}
    }
    if add {
        st.entries.push(EntryForm::default());
    }
}

fn render_options(ui: &mut egui::Ui, st: &mut MultiCompareTabState) {
    let ctx = ui.ctx().clone();
    step_header(ui, 3, "Options", "", |_| {});
    style::modal_card_frame(&ctx)
        .inner_margin(egui::Margin::symmetric(12, 8))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing.x = 10.0;
                let h = ui.spacing().interact_size.y;
                caption(ui, "ROW LIMIT");
                ui.add_sized(
                    [90.0, h],
                    egui::TextEdit::singleline(&mut st.row_limit).hint_text("no limit"),
                )
                .on_hover_text("Maximum rows read per source. Empty = no limit.");
                ui.separator();
                caption(ui, "IGNORE COLUMNS");
                ui.add_sized(
                    [220.0, h],
                    egui::TextEdit::singleline(&mut st.ignore_columns)
                        .hint_text("updated_at, version"),
                );
                ui.separator();
                ui.checkbox(&mut st.tolerant, "Tolerant formats")
                    .on_hover_text("1.0 = 1, true = 1, and timestamp formats are treated as equal");
                ui.checkbox(&mut st.trim, "Trim trailing spaces");
                ui.checkbox(&mut st.case_insensitive, "Ignore letter case");
            });
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

/// Kartu angka ringkasan hasil.
fn stat_tile(ui: &mut egui::Ui, label: &str, value: usize, color: egui::Color32) {
    style::modal_card_frame(ui.ctx())
        .inner_margin(egui::Margin::symmetric(12, 8))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.label(
                egui::RichText::new(fmt_count(value as u64))
                    .strong()
                    .size(20.0)
                    .color(color),
            );
            caption(ui, label);
        });
}

#[allow(clippy::too_many_lines)]
fn render_results(ui: &mut egui::Ui, st: &mut MultiCompareTabState) {
    let ctx = ui.ctx().clone();
    let dark = ctx.global_style().visuals.dark_mode;
    let result = st.result.clone();
    let counts = result.as_ref().map(|r| r.counts()).unwrap_or_default();
    let show_filters = result.as_ref().is_some_and(|r| !r.diffs.is_empty());
    step_header(
        ui,
        4,
        "Differences",
        "Only keys that are missing somewhere or have different values.",
        |ui| {
            if show_filters {
                style::render_search_field(ui, &mut st.result_filter, "Filter rows…", 200.0);
                ui.checkbox(
                    &mut st.show_changed,
                    format!("Changed ({})", fmt_count(counts.1 as u64)),
                );
                ui.checkbox(
                    &mut st.show_missing,
                    format!("Missing ({})", fmt_count(counts.0 as u64)),
                );
            }
        },
    );
    if let Some(e) = &st.result_error {
        style::theme_alert_frame(&ctx, true).show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.label(
                egui::RichText::new(format!("{}  {e}", i::ICON_ERROR.codepoint))
                    .color(style::theme_danger(&ctx)),
            );
        });
        ui.add_space(6.0);
    }
    let Some(result) = result else {
        if st.result_error.is_none() {
            egui::Frame::new()
                .fill(style::nav_track(&ctx))
                .corner_radius(8.0)
                .inner_margin(egui::Margin::symmetric(14, 12))
                .show(ui, |ui| {
                    ui.set_min_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(i::ICON_PLAY_ARROW.codepoint)
                                .size(18.0)
                                .color(style::nav_text_muted(&ctx)),
                        );
                        ui.label(
                            egui::RichText::new(
                                "Press PROCESS to read every source and list the rows that differ here.",
                            )
                            .color(style::nav_text_muted(&ctx)),
                        );
                    });
                });
        }
        return;
    };
    let (missing, changed) = counts;

    ui.columns(4, |cols| {
        stat_tile(
            &mut cols[0],
            "KEYS COMPARED",
            result.total_keys,
            style::nav_text_strong(&ctx),
        );
        stat_tile(
            &mut cols[1],
            "IDENTICAL",
            result.identical,
            style::theme_success(&ctx),
        );
        stat_tile(
            &mut cols[2],
            "MISSING SOMEWHERE",
            missing,
            style::theme_danger(&ctx),
        );
        stat_tile(
            &mut cols[3],
            "VALUES DIFFER",
            changed,
            style::theme_warning(&ctx),
        );
    });
    ui.add_space(8.0);

    // Ringkasan per sumber.
    ui.horizontal_wrapped(|ui| {
        ui.spacing_mut().item_spacing = egui::vec2(8.0, 6.0);
        for (k, s) in result.sources.iter().enumerate() {
            egui::Frame::new()
                .fill(style::nav_track(&ctx))
                .corner_radius(6.0)
                .inner_margin(egui::Margin::symmetric(8, 4))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        source_badge(ui, k, 16.0);
                        ui.label(egui::RichText::new(&s.label).strong());
                        let mut text = format!(
                            "{} rows · {} missing",
                            fmt_count(s.rows as u64),
                            fmt_count(s.missing as u64)
                        );
                        if s.duplicate_keys > 0 {
                            text.push_str(&format!(
                                " · {} duplicate keys",
                                fmt_count(s.duplicate_keys as u64)
                            ));
                        }
                        ui.label(
                            egui::RichText::new(text)
                                .size(11.5)
                                .color(style::nav_text_muted(&ctx)),
                        );
                        if s.truncated {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{} limit reached",
                                    i::ICON_WARNING.codepoint
                                ))
                                .size(11.5)
                                .color(style::theme_warning(&ctx)),
                            );
                        }
                    });
                });
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
            "No value column exists in every source, so only the presence of keys was compared. Use Rename columns to match names.",
        );
    }
    if !result.partial_columns.is_empty() {
        warning_label(
            ui,
            &format!(
                "Not compared (missing from some sources): {}",
                result.partial_columns.join(", ")
            ),
        );
    }
    ui.add_space(8.0);
    if result.diffs.is_empty() {
        style::theme_alert_frame(&ctx, false).show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.label(
                egui::RichText::new(format!(
                    "{}  All rows match across {} sources.",
                    i::ICON_CHECK_CIRCLE.codepoint,
                    result.labels.len()
                ))
                .color(style::theme_success(&ctx)),
            );
        });
        return;
    }

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
    let sources = result.labels.len();
    let danger = style::theme_danger(&ctx);
    let warn = style::theme_warning(&ctx);
    let muted = style::nav_text_muted(&ctx);
    let mono = egui::FontFamily::Monospace;

    style::modal_card_frame(&ctx)
        .inner_margin(egui::Margin::symmetric(10, 8))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            egui::ScrollArea::horizontal()
                .id_salt("cmp_result_scroll")
                .show(ui, |ui| {
                    egui::Grid::new("cmp_result_grid")
                        .striped(true)
                        .spacing([16.0, 5.0])
                        .show(ui, |ui| {
                            for k in &result.key_columns {
                                ui.label(egui::RichText::new(k).strong().size(12.0));
                            }
                            ui.label(egui::RichText::new("Status").strong().size(12.0));
                            for col in &result.columns {
                                for s in 0..sources {
                                    ui.horizontal(|ui| {
                                        ui.spacing_mut().item_spacing.x = 4.0;
                                        source_badge(ui, s, 14.0);
                                        ui.label(
                                            egui::RichText::new(col)
                                                .strong()
                                                .size(12.0)
                                                .color(source_color(s, dark)),
                                        );
                                    })
                                    .response
                                    .on_hover_text(format!("{col} in {}", result.labels[s]));
                                }
                            }
                            ui.end_row();
                            for diff in visible.iter().take(MAX_ROWS_SHOWN) {
                                for k in &diff.key {
                                    let text = if k == crate::data_transfer::NULL_MARKER {
                                        "NULL".to_string()
                                    } else {
                                        clip(k)
                                    };
                                    ui.label(
                                        egui::RichText::new(text).family(mono.clone()).strong(),
                                    );
                                }
                                let status = diff.status(&result.labels, &result.columns);
                                ui.label(
                                    egui::RichText::new(status)
                                        .size(12.0)
                                        .color(if diff.is_missing() { danger } else { warn }),
                                );
                                for c in 0..result.columns.len() {
                                    let differs = diff.changed.contains(&c);
                                    for s in 0..sources {
                                        if !diff.present.get(s).copied().unwrap_or(false) {
                                            ui.label(
                                                egui::RichText::new("missing")
                                                    .italics()
                                                    .size(12.0)
                                                    .color(danger),
                                            );
                                            continue;
                                        }
                                        let value = diff
                                            .values
                                            .get(s)
                                            .and_then(|v| v.get(c))
                                            .cloned()
                                            .flatten();
                                        let mut text = match &value {
                                            Some(v) => {
                                                egui::RichText::new(clip(v)).family(mono.clone())
                                            }
                                            None => {
                                                egui::RichText::new("NULL").italics().color(muted)
                                            }
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
        });
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
