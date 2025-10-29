use eframe::egui;
use eframe::egui::text_edit::TextEditState;
use egui::text::{CCursor, CCursorRange};
// Using adapter for cursor state (removes direct TextEditState dependency from rest of file)
// syntax highlighting module temporarily disabled
use log::debug;

use crate::{
    connection, data_table, directory, editor, editor_autocomplete, models, sidebar_history,
    sidebar_query, window_egui,
};

// Tab management methods
pub(crate) fn create_new_tab(
    tabular: &mut window_egui::Tabular,
    title: String,
    content: String,
) -> usize {
    let tab_id = tabular.next_tab_id;
    tabular.next_tab_id += 1;

    let new_tab = models::structs::QueryTab {
        title,
        content: content.clone(),
        file_path: None,
        is_saved: false,
        is_modified: false,
        connection_id: None,       // No connection assigned by default
        database_name: None,       // No database assigned by default
        has_executed_query: false, // New tab hasn't executed any query yet
        result_headers: Vec::new(),
        result_rows: Vec::new(),
        result_all_rows: Vec::new(),
        result_table_name: String::new(),
        is_table_browse_mode: false,
        current_page: 0,
        page_size: 500, // default page size aligns with global default
        total_rows: 0,
        base_query: String::new(), // Empty base query initially
        dba_special_mode: None,
    };

    tabular.query_tabs.push(new_tab);
    let new_index = tabular.query_tabs.len() - 1;
    tabular.active_tab_index = new_index;

    // Update editor with new tab content
    tabular.editor.set_text(content.clone());
    // Clear global result state so a fresh tab starts clean (no lingering table below)
    tabular.current_table_headers.clear();
    tabular.current_table_data.clear();
    tabular.all_table_data.clear();
    tabular.current_table_name.clear();
    tabular.total_rows = 0;
    tabular.is_table_browse_mode = false;

    tab_id
}

// Convenience: create a new query tab and pre-assign a connection
pub(crate) fn create_new_tab_with_connection(
    tabular: &mut window_egui::Tabular,
    title: String,
    content: String,
    connection_id: Option<i64>,
) -> usize {
    let tab_id = create_new_tab(tabular, title, content);
    if let Some(active_tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
        active_tab.connection_id = connection_id;
        // Keep global current_connection_id in sync with the newly created tab
        tabular.current_connection_id = connection_id;
        // New tabs have no database selected by default here; leave as-is
    }
    tab_id
}

// Convenience: create a new query tab and pre-assign connection + database context
pub(crate) fn create_new_tab_with_connection_and_database(
    tabular: &mut window_egui::Tabular,
    title: String,
    content: String,
    connection_id: Option<i64>,
    database_name: Option<String>,
) -> usize {
    let tab_id = create_new_tab(tabular, title, content);
    if let Some(active_tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
        active_tab.connection_id = connection_id;
        active_tab.database_name = database_name.clone();
        // Sync global state with the tab's assigned connection
        tabular.current_connection_id = connection_id;
    }
    tab_id
}

pub(crate) fn close_tab(tabular: &mut window_egui::Tabular, tab_index: usize) {
    if tabular.query_tabs.len() <= 1 {
        // Don't close the last tab, just clear it
        if let Some(tab) = tabular.query_tabs.get_mut(0) {
            tab.content.clear();
            tab.title = "Untitled Query".to_string();
            tab.file_path = None;
            tab.is_saved = false;
            tab.is_modified = false;
            tab.connection_id = None; // Clear connection as well
            tab.database_name = None; // Clear database as well
            // Clear per-tab result state as well
            tab.result_headers.clear();
            tab.result_rows.clear();
            tab.result_all_rows.clear();
            tab.result_table_name.clear();
            tab.is_table_browse_mode = false;
            tab.current_page = 0;
            // Keep default page_size as-is (don't force overwrite)
            tab.total_rows = 0;
            tab.base_query.clear();
            tab.has_executed_query = false;
            tab.dba_special_mode = None;
        }
        tabular.editor.set_text(String::new());
        // Also clear global result state so the UI table area is reset
        tabular.current_table_headers.clear();
        tabular.current_table_data.clear();
        tabular.all_table_data.clear();
        tabular.current_table_name.clear();
        tabular.total_rows = 0;
        tabular.is_table_browse_mode = false;
        tabular.current_page = 0;
        // Keep the configured default page_size; don't override
        tabular.current_base_query.clear();
        tabular.current_connection_id = None;
        return;
    }

    if tab_index < tabular.query_tabs.len() {
        tabular.query_tabs.remove(tab_index);

        // Adjust active tab index
        if tabular.active_tab_index >= tabular.query_tabs.len() {
            tabular.active_tab_index = tabular.query_tabs.len() - 1;
        } else if tabular.active_tab_index > tab_index {
            tabular.active_tab_index -= 1;
        }

        // Update editor with active tab content
        if let Some(active_tab) = tabular.query_tabs.get(tabular.active_tab_index) {
            tabular.editor.set_text(active_tab.content.clone());
        }
    }
}

pub(crate) fn switch_to_tab(tabular: &mut window_egui::Tabular, tab_index: usize) {
    let mut need_connect: Option<i64> = None;
    if tab_index < tabular.query_tabs.len() {
        // Save current tab content
        if let Some(current_tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            if current_tab.content != tabular.editor.text {
                current_tab.content = tabular.editor.text.clone();
                current_tab.is_modified = true;
            }
            // Persist current global result state into the tab before switching (swap to avoid clones)
            std::mem::swap(
                &mut current_tab.result_headers,
                &mut tabular.current_table_headers,
            );
            std::mem::swap(
                &mut current_tab.result_rows,
                &mut tabular.current_table_data,
            );
            std::mem::swap(
                &mut current_tab.result_all_rows,
                &mut tabular.all_table_data,
            );
            std::mem::swap(
                &mut current_tab.result_table_name,
                &mut tabular.current_table_name,
            );
            current_tab.is_table_browse_mode = tabular.is_table_browse_mode;
            current_tab.current_page = tabular.current_page;
            current_tab.page_size = tabular.page_size;
            current_tab.total_rows = tabular.total_rows;
            std::mem::swap(&mut current_tab.base_query, &mut tabular.current_base_query);
            debug!(
                "💾 Saving tab {} state (swap): base_query='{}'",
                tabular.active_tab_index, current_tab.base_query
            );
            // dba_special_mode already resides on current_tab; no action required here
        }

        // Switch to new tab
        tabular.active_tab_index = tab_index;
        if let Some(new_tab) = tabular.query_tabs.get_mut(tab_index) {
            tabular.editor.set_text(new_tab.content.clone());
            // Restore per-tab result state into global display (swap to avoid clones)
            std::mem::swap(
                &mut tabular.current_table_headers,
                &mut new_tab.result_headers,
            );
            std::mem::swap(&mut tabular.current_table_data, &mut new_tab.result_rows);
            std::mem::swap(&mut tabular.all_table_data, &mut new_tab.result_all_rows);
            std::mem::swap(
                &mut tabular.current_table_name,
                &mut new_tab.result_table_name,
            );
            tabular.is_table_browse_mode = new_tab.is_table_browse_mode;
            tabular.current_page = new_tab.current_page;
            tabular.page_size = new_tab.page_size;
            tabular.total_rows = new_tab.total_rows;
            std::mem::swap(&mut tabular.current_base_query, &mut new_tab.base_query);
            debug!(
                "📂 Restoring tab {} state (swap): base_query='{}', connection_id={:?}",
                tab_index, tabular.current_base_query, new_tab.connection_id
            );
            // IMPORTANT: kembalikan connection id aktif sesuai tab baru
            tabular.current_connection_id = new_tab.connection_id;
            // dba_special_mode automatically follows with new_tab

            // Auto-connect restoration: jika tab memiliki connection_id dan pool belum siap, trigger creation
            if let Some(conn_id) = new_tab.connection_id {
                let has_pool = tabular.connection_pools.contains_key(&conn_id)
                    || tabular
                        .shared_connection_pools
                        .lock()
                        .map(|p| p.contains_key(&conn_id))
                        .unwrap_or(false);
                if !has_pool {
                    need_connect = Some(conn_id);
                }
            }

            // Jika user sedang berada di tampilan Structure dan tab tujuan adalah tab Table, reload struktur tabel tsb.
            if tabular.table_bottom_view == models::structs::TableBottomView::Structure
                && new_tab.title.starts_with("Table:")
            {
                // load_structure_info_for_current_table adalah metode pada Tabular (dibuat pub(crate))
                data_table::load_structure_info_for_current_table(tabular);
            }
        }
    }
    // Deferred connection attempt after borrows released: trigger background creation without blocking UI
    if let Some(conn_id) = need_connect {
        log::debug!(
            "Triggering background connection pool creation for {}",
            conn_id
        );
        crate::connection::ensure_background_pool_creation(tabular, conn_id);
        // Optionally, we can set a friendly status indicator here if needed.
    }
}

pub(crate) fn save_current_tab(tabular: &mut window_egui::Tabular) -> Result<(), String> {
    if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
        // Ensure content holds editor text plus metadata header (id + optional db)
        let mut final_content = tabular.editor.text.clone();
        // Prepare or update metadata header at top of file
        let (conn_meta, db_meta) = (tab.connection_id, tab.database_name.clone());
        let mut header_lines: Vec<String> = Vec::new();
        if conn_meta.is_some() || db_meta.is_some() {
            if let Some(id) = conn_meta {
                header_lines.push(format!("-- tabular: connection_id={}", id));
                // Also persist connection_name for resilience if IDs change later
                if let Some(conn_name) = tabular
                    .connections
                    .iter()
                    .find(|c| c.id == Some(id))
                    .map(|c| c.name.clone())
                {
                    header_lines.push(format!("-- tabular: connection_name={}", conn_name));
                }
            }
            if let Some(db) = db_meta.filter(|d| !d.trim().is_empty()) {
                header_lines.push(format!("-- tabular: database={}", db));
            }
        }
        if !header_lines.is_empty() {
            // Remove existing tabular header lines to avoid duplicates
            let filtered_existing: String = final_content
                .lines()
                .filter(|l| !l.trim_start().starts_with("-- tabular:"))
                .collect::<Vec<_>>()
                .join("\n");
            final_content = format!(
                "{}\n\n{}",
                header_lines.join("\n"),
                filtered_existing.trim_start_matches('\n')
            );
        }
        // Keep a clone for potential content-based file path resolution below
        tab.content = final_content.clone();

        // Best-effort: if file_path is missing but this tab likely comes from a query file,
        // try to resolve the path from the queries tree by matching the tab title (filename).
        if tab.file_path.is_none() {
            let title_name = tab.title.clone();
            if title_name.ends_with(".sql") {
                // Flatten queries_tree and find unique match by name
                fn collect_matches(
                    nodes: &Vec<crate::models::structs::TreeNode>,
                    name: &str,
                    out: &mut Vec<String>,
                ) {
                    for n in nodes {
                        if let Some(path) = &n.file_path
                            && n.node_type == crate::models::enums::NodeType::Query
                            && n.name == name
                        {
                            out.push(path.clone());
                        }
                        if !n.children.is_empty() {
                            collect_matches(&n.children, name, out);
                        }
                    }
                }
                let mut candidates = Vec::new();
                collect_matches(&tabular.queries_tree, &title_name, &mut candidates);
                if candidates.len() == 1 {
                    tab.file_path = Some(candidates.remove(0));
                } else if candidates.len() > 1 {
                    // Ambiguous by name; try content-based disambiguation (ignore tabular headers)
                    let strip_headers = |s: &str| -> String {
                        s.lines()
                            .filter(|l| !l.trim_start().starts_with("-- tabular:"))
                            .collect::<Vec<_>>()
                            .join("\n")
                            .trim_start_matches('\n')
                            .to_string()
                    };
                    let current_body = strip_headers(&final_content);
                    let mut match_path: Option<String> = None;
                    for p in candidates.iter() {
                        if let Ok(c) = std::fs::read_to_string(p)
                            && strip_headers(&c) == current_body
                        {
                            match_path = Some(p.clone());
                            break;
                        }
                    }
                    if let Some(p) = match_path {
                        tab.file_path = Some(p);
                    }
                }
            }
        }

        if tab.file_path.is_some() {
            // File already exists, save directly
            let file_path = tab.file_path.as_ref().unwrap().clone();
            std::fs::write(&file_path, &tab.content)
                .map_err(|e| format!("Failed to save file: {}", e))?;

            tab.is_saved = true;
            tab.is_modified = false;

            Ok(())
        } else {
            // Show save dialog for new file
            tabular.save_filename = tab.title.replace("Untitled Query", "").trim().to_string();
            if tabular.save_filename.is_empty() {
                tabular.save_filename = "new_query".to_string();
            }
            if !tabular.save_filename.ends_with(".sql") {
                tabular.save_filename.push_str(".sql");
            }

            // Initialize save directory with config query directory
            if tabular.save_directory.is_empty() {
                tabular.save_directory = crate::directory::get_query_dir()
                    .to_string_lossy()
                    .to_string();
            }

            tabular.show_save_dialog = true;
            Ok(())
        }
    } else {
        Err("No active tab".to_string())
    }
}

pub(crate) fn save_current_tab_with_name(
    tabular: &mut window_egui::Tabular,
    filename: String,
) -> Result<(), String> {
    if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
        // Mirror header injection as in save_current_tab
        let mut final_content = tabular.editor.text.clone();
        let (conn_meta, db_meta) = (tab.connection_id, tab.database_name.clone());
        let mut header_lines: Vec<String> = Vec::new();
        if conn_meta.is_some() || db_meta.is_some() {
            if let Some(id) = conn_meta {
                header_lines.push(format!("-- tabular: connection_id={}", id));
                // Also persist connection_name for resilience
                if let Some(conn_name) = tabular
                    .connections
                    .iter()
                    .find(|c| c.id == Some(id))
                    .map(|c| c.name.clone())
                {
                    header_lines.push(format!("-- tabular: connection_name={}", conn_name));
                }
            }
            if let Some(db) = db_meta.filter(|d| !d.trim().is_empty()) {
                header_lines.push(format!("-- tabular: database={}", db));
            }
        }
        if !header_lines.is_empty() {
            let filtered_existing: String = final_content
                .lines()
                .filter(|l| !l.trim_start().starts_with("-- tabular:"))
                .collect::<Vec<_>>()
                .join("\n");
            final_content = format!(
                "{}\n\n{}",
                header_lines.join("\n"),
                filtered_existing.trim_start_matches('\n')
            );
        }
        tab.content = final_content;
        // Use selected save directory or fallback to query directory
        let target_dir = if !tabular.save_directory.is_empty() {
            std::path::PathBuf::from(&tabular.save_directory)
        } else {
            directory::get_query_dir()
        };

        // Ensure the target directory exists
        std::fs::create_dir_all(&target_dir)
            .map_err(|e| format!("Failed to create target directory: {}", e))?;

        let mut clean_filename = filename.trim().to_string();
        if !clean_filename.ends_with(".sql") {
            clean_filename.push_str(".sql");
        }

        let file_path = target_dir.join(&clean_filename);

        std::fs::write(&file_path, &tab.content)
            .map_err(|e| format!("Failed to save file: {}", e))?;

        tab.file_path = Some(file_path.to_string_lossy().to_string());
        tab.title = clean_filename;
        tab.is_saved = true;
        tab.is_modified = false;

        // Refresh queries tree to show the new file
        sidebar_query::load_queries_from_directory(tabular);

        Ok(())
    } else {
        Err("No active tab".to_string())
    }
}

// Helper function to handle CMD+D / CTRL+D - Add next occurrence to multi-selection
// 
// This implements VSCode-style multi-cursor behavior:
// 1. First CMD+D: Select word under cursor (if no selection) or use current selection
// 2. Subsequent CMD+D: Find and add next occurrence of the selected text
// 3. All cursors stay active and typing applies to all positions
// 4. Press Escape or navigate with arrow keys to clear multi-selection
fn handle_add_next_occurrence(tabular: &mut window_egui::Tabular, ui: &egui::Ui) {
    let id = egui::Id::new("sql_editor");
    
    // Get current selection or word under cursor
    let (sel_start, sel_end) = if tabular.selection_start != tabular.selection_end {
        // Use existing selection
        (tabular.selection_start, tabular.selection_end)
    } else {
        // No selection: select word under cursor
        let pos = tabular.cursor_position.min(tabular.editor.text.len());
        let (word_start, word_end) = find_word_boundaries(&tabular.editor.text, pos);
        if word_start < word_end {
            // Update selection to the word
            tabular.selection_start = word_start;
            tabular.selection_end = word_end;
            (word_start, word_end)
        } else {
            // No word found, do nothing
            log::debug!("🎯 CMD+D: No word found under cursor");
            return;
        }
    };
    
    let selected_text = if sel_start < sel_end && sel_end <= tabular.editor.text.len() {
        tabular.editor.text[sel_start..sel_end].to_string()
    } else {
        return;
    };
    
    if selected_text.is_empty() {
        log::debug!("🎯 CMD+D: Empty selection, nothing to find");
        return;
    }
    
    log::debug!("🎯 CMD+D: Selected text='{}' at {}..{}", selected_text.escape_debug(), sel_start, sel_end);
    
    // Initialize multi-selection with current selection if it's the first occurrence
    if tabular.multi_selection.is_empty() {
        tabular.multi_selection.set_primary_range(sel_start, sel_end);
        log::debug!("🎯 Initialized multi-selection with primary range: {}..{}", sel_start, sel_end);
        
        // Store the selected text for visual feedback
        tabular.selected_text = selected_text.clone();
        ui.ctx().request_repaint();
        
        // Don't search for next occurrence on first CMD+D, just initialize
        return;
    }
    
    // Debug: print all existing regions
    log::debug!("🎯 Existing regions before add:");
    for (i, r) in tabular.multi_selection.regions().iter().enumerate() {
        let text_at_region = &tabular.editor.text[r.min()..r.max()];
        log::debug!("   [{}] {}..{} = '{}'", i, r.min(), r.max(), text_at_region.escape_debug());
    }
    
    // Add next occurrence
    let found = tabular.multi_selection.add_next_occurrence(&tabular.editor.text, &selected_text);
    
    if found {
        log::debug!("✅ Added next occurrence. Total selections: {}", tabular.multi_selection.len());
        
        // Debug: print all regions after add
        log::debug!("🎯 All regions after add:");
        for (i, r) in tabular.multi_selection.regions().iter().enumerate() {
            let text_at_region = &tabular.editor.text[r.min()..r.max()];
            log::debug!("   [{}] {}..{} = '{}'", i, r.min(), r.max(), text_at_region.escape_debug());
        }
        
        // Update visual feedback
        tabular.selected_text = selected_text.clone();
        
        // Get the last added selection to update cursor position
        if let Some(last_region) = tabular.multi_selection.regions().last() {
            let last_end = last_region.max();
            tabular.cursor_position = last_end;
            tabular.selection_start = last_region.min();
            tabular.selection_end = last_end;
            
            // Sync with egui state
            let to_char_index = |s: &str, byte_idx: usize| -> usize {
                let b = byte_idx.min(s.len());
                s[..b].chars().count()
            };
            
            let start_ci = to_char_index(&tabular.editor.text, last_region.min());
            let end_ci = to_char_index(&tabular.editor.text, last_end);
            crate::editor_state_adapter::EditorStateAdapter::set_selection(
                ui.ctx(),
                id,
                start_ci,
                end_ci,
                end_ci,
            );
        }
        
        ui.ctx().request_repaint();
    } else {
        log::debug!("ℹ️ No more occurrences found for '{}'", selected_text.escape_debug());
    }
}

// Helper: Find word boundaries at the given position (for selecting word under cursor)
fn find_word_boundaries(text: &str, pos: usize) -> (usize, usize) {
    use unicode_segmentation::UnicodeSegmentation;
    
    let pos = pos.min(text.len());
    
    // Find word boundaries using Unicode word segmentation
    let mut word_start = pos;
    let mut word_end = pos;
    
    // Find all word boundaries
    for (idx, word) in text.unicode_word_indices() {
        let start = idx;
        let end = start + word.len();
        
        // Check if position is within this word
        if pos >= start && pos <= end {
            word_start = start;
            word_end = end;
            break;
        }
    }
    
    (word_start, word_end)
}

pub(crate) fn render_advanced_editor(tabular: &mut window_egui::Tabular, ui: &mut egui::Ui) {
    // Find & Replace panel
    if tabular.advanced_editor.show_find_replace {
        ui.horizontal(|ui| {
            ui.label("Find:");
            ui.add_sized(
                [200.0, 20.0],
                egui::TextEdit::singleline(&mut tabular.advanced_editor.find_text),
            );

            ui.label("Replace:");
            ui.add_sized(
                [200.0, 20.0],
                egui::TextEdit::singleline(&mut tabular.advanced_editor.replace_text),
            );

            ui.checkbox(
                &mut tabular.advanced_editor.case_sensitive,
                "Case Sensitive",
            );
            ui.checkbox(&mut tabular.advanced_editor.use_regex, "Regex");

            if ui.button("Replace All").clicked() {
                perform_replace_all(tabular);
            }

            if ui.button("Find Next").clicked() {
                find_next(tabular);
            }

            if ui.button("✕").clicked() {
                tabular.advanced_editor.show_find_replace = false;
            }
        });
    }

    // ----- Pre-widget key handling & indentation (no active borrow of editor_text) -----
    let rows = if tabular.advanced_editor.desired_rows > 0 {
        tabular.advanced_editor.desired_rows
    } else {
        25
    };
    let mut tab_pressed_pre = ui.input(|i| i.key_pressed(egui::Key::Tab));
    let shift_pressed_pre = ui.input(|i| i.modifiers.shift);
    let (sel_start, sel_end) = (tabular.selection_start, tabular.selection_end);
    if tab_pressed_pre && sel_start < sel_end && sel_end <= tabular.editor.text.len() {
        let slice = &tabular.editor.text[sel_start..sel_end];
        if slice.contains('\n') {
            // multi-line
            // Find first line start
            let mut line_start = sel_start;
            while line_start > 0 && tabular.editor.text.as_bytes()[line_start - 1] != b'\n' {
                line_start -= 1;
            }
            let sel_end_clamped = sel_end.min(tabular.editor.text.len());
            let block = tabular.editor.text[line_start..sel_end_clamped].to_string();
            if !shift_pressed_pre {
                let mut indented = String::with_capacity(block.len() + 8);
                for line in block.split_inclusive('\n') {
                    if line == "\n" {
                        indented.push('\n');
                        continue;
                    }
                    let (content, nl) = if let Some(p) = line.rfind('\n') {
                        (&line[..p], &line[p..])
                    } else {
                        (line, "")
                    };
                    indented.push('\t');
                    indented.push_str(content);
                    indented.push_str(nl);
                }
                // Apply via rope edit API for consistency
                tabular
                    .editor
                    .apply_single_replace(line_start..sel_end_clamped, &indented);
                tabular.selection_start = line_start;
                tabular.selection_end = line_start + indented.len();
                tabular.cursor_position = tabular.selection_end;
            } else {
                let mut outdented = String::with_capacity(block.len());
                let mut changed = false;
                for line in block.split_inclusive('\n') {
                    if line == "\n" {
                        outdented.push('\n');
                        continue;
                    }
                    let (content, nl) = if let Some(p) = line.rfind('\n') {
                        (&line[..p], &line[p..])
                    } else {
                        (line, "")
                    };
                    let trimmed = if let Some(rest) = content.strip_prefix('\t') {
                        changed = true;
                        rest
                    } else if let Some(rest) = content.strip_prefix("    ") {
                        changed = true;
                        rest
                    } else {
                        content
                    };
                    outdented.push_str(trimmed);
                    outdented.push_str(nl);
                }
                if changed {
                    tabular
                        .editor
                        .apply_single_replace(line_start..sel_end_clamped, &outdented);
                    tabular.selection_start = line_start;
                    tabular.selection_end = line_start + outdented.len();
                    tabular.cursor_position = tabular.selection_end;
                }
            }
            // consume Tab key event so TextEdit tidak menambah tab baru
            ui.ctx().input_mut(|ri| {
                ri.events.retain(|e| {
                    !matches!(
                        e,
                        egui::Event::Key {
                            key: egui::Key::Tab,
                            ..
                        }
                    )
                })
            });
            if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                tab.content = tabular.editor.text.clone();
                tab.is_modified = true;
            }
        }
    }
    // ----- Handle autocomplete key interception and pre-acceptance BEFORE building TextEdit -----
    let mut enter_pressed_pre = ui.input(|i| i.key_pressed(egui::Key::Enter));
    let mut raw_tab = false;
    // VSCode-like navigation/action flags
    let mut word_left_pressed = false;
    let mut word_right_pressed = false;
    let mut multi_nav_left = false;
    let mut multi_nav_right = false;
    let mut word_nav_shift = false;
    let mut move_line_up = false;
    let mut move_line_down = false;
    let mut dup_line_up = false;
    let mut dup_line_down = false;
    // Intercept arrow keys when autocomplete popup shown so caret tidak ikut bergerak
    let mut arrow_down_pressed = false;
    let mut arrow_up_pressed = false;
    ui.input(|i| {
        for ev in &i.events {
            if let egui::Event::Key {
                key: egui::Key::Tab,
                pressed: true,
                ..
            } = ev
            {
                raw_tab = true;
            }
        }
    });
    // Defer actual accept application until after TextEdit is rendered to avoid borrow conflicts
    let mut defer_accept_autocomplete = false;
    if tabular.show_autocomplete {
        ui.ctx().input_mut(|ri| {
            // Drain & filter events: buang ArrowUp/ArrowDown pressed supaya TextEdit tidak memproses
            let mut kept = Vec::with_capacity(ri.events.len());
            let mut _enter_consumed = false; // renamed to silence unused warning
            for ev in ri.events.drain(..) {
                match ev {
                    egui::Event::Key {
                        key: egui::Key::ArrowDown,
                        pressed: true,
                        ..
                    } => {
                        arrow_down_pressed = true;
                        // user navigated popup
                        tabular.autocomplete_navigated = true;
                    }
                    egui::Event::Key {
                        key: egui::Key::ArrowUp,
                        pressed: true,
                        ..
                    } => {
                        arrow_up_pressed = true;
                        // user navigated popup
                        tabular.autocomplete_navigated = true;
                    }
                    // Smart Enter handling: only consume if we should accept autocomplete
                    e @ egui::Event::Key {
                        key: egui::Key::Enter,
                        pressed: true,
                        ..
                    } => {
                        // Heuristic: accept on Enter if user navigated OR
                        //  - there is only one suggestion OR
                        //  - selected suggestion extends current prefix (case-insensitive)
                        let mut should_accept = tabular.autocomplete_navigated;

                        if !should_accept {
                            let sugg_count = tabular.autocomplete_suggestions.len();
                            if sugg_count == 1 {
                                should_accept = true;
                            } else {
                                let prefix = tabular.autocomplete_prefix.clone();
                                if let Some(sugg) = tabular
                                    .autocomplete_suggestions
                                    .get(tabular.selected_autocomplete_index)
                                    && !prefix.is_empty()
                                {
                                    let p = prefix.to_lowercase();
                                    let s = sugg.to_lowercase();
                                    if s.starts_with(&p) {
                                        should_accept = true;
                                    }
                                }
                            }
                        }

                        if should_accept {
                            enter_pressed_pre = true; // we'll accept suggestion
                            _enter_consumed = true;
                        } else {
                            // don't consume: let TextEdit insert newline
                            kept.push(e);
                        }
                    }
                    // Jangan hilangkan release events agar repeat logic internal tidak stuck; hanya pressed yang kita konsumsi
                    other @ egui::Event::Key {
                        key: egui::Key::ArrowDown,
                        pressed: false,
                        ..
                    } => {
                        kept.push(other);
                    }
                    other @ egui::Event::Key {
                        key: egui::Key::ArrowUp,
                        pressed: false,
                        ..
                    } => {
                        kept.push(other);
                    }
                    other => kept.push(other),
                }
            }
            ri.events = kept;
        });
    }
    // VSCode-like word navigation & line operations (pre-TextEdit)
    // Helper: compute previous and next word boundaries using Unicode segmentation (UAX#29)
    use unicode_segmentation::UnicodeSegmentation;
    let prev_word_boundary = |s: &str, pos: usize| {
        let pos = pos.min(s.len());
        // Walk backward over word boundaries using unicode_words
        // Strategy: iterate words with their byte ranges, pick the last word whose end < pos; if pos inside a word, jump to its start
        let mut last_start = 0usize;
        for w in s.unicode_word_indices() {
            let (byte_idx, word) = w;
            let start = byte_idx;
            let end = start + word.len();
            if pos > end {
                last_start = start;
                continue;
            }
            if pos > start && pos <= end {
                // inside this word: jump to its start
                return start;
            }
            if pos <= start {
                break;
            }
        }
        // If not inside any word, jump to start of previous word if any, else 0
        last_start
    };
    let next_word_boundary = |s: &str, pos: usize| {
        let pos = pos.min(s.len());
        for w in s.unicode_word_indices() {
            let (byte_idx, word) = w;
            let start = byte_idx;
            let end = start + word.len();
            if pos < start {
                // next word found; jump to its start
                return start;
            }
            if pos >= start && pos < end {
                // inside this word; jump to its end
                return end;
            }
        }
        s.len()
    };
    // Helper: convert byte index -> char index for egui CCursor
    let to_char_index = |s: &str, byte_idx: usize| -> usize {
        let b = byte_idx.min(s.len());
        s[..b].chars().count()
    };
    // Helper: convert char index -> byte index for our rope/editor
    let to_byte_index = |s: &str, char_idx: usize| -> usize {
        match s.char_indices().nth(char_idx) {
            Some((b, _)) => b,
            None => s.len(),
        }
    };
    // Pre-handle Delete/Backspace when a selection exists: remove the whole selection (not just one char)
    // This ensures expected behavior “press Delete removes all selected text”.
    {
        let id = egui::Id::new("sql_editor");
        let mut do_delete_selection = false;
        let mut del_key_consumed = false;
        let mut has_selection = false;

        // Check for key presses first
        let (pressed_bs, pressed_del) = ui.input(|i| {
            (
                i.key_pressed(egui::Key::Backspace),
                i.key_pressed(egui::Key::Delete),
            )
        });

        if pressed_bs || pressed_del {
            // Method 1: Check egui state selection (char indices)
            if let Some(rng) =
                crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id)
                && rng.start != rng.end
            {
                has_selection = true;
                log::debug!(
                    "Selection detected via egui state: {} to {}",
                    rng.start,
                    rng.end
                );
            }

            // Method 2: Fallback to stored selection state (byte indices)
            if !has_selection && tabular.selection_start != tabular.selection_end {
                has_selection = true;
                log::debug!(
                    "Selection detected via stored state: {} to {}",
                    tabular.selection_start,
                    tabular.selection_end
                );
            }

            // Method 3: Check if there's selected text
            if !has_selection && !tabular.selected_text.is_empty() {
                has_selection = true;
                log::debug!(
                    "Selection detected via selected_text: '{}'",
                    tabular.selected_text
                );
            }

            // Only intercept and handle deletion if there's actually a selection
            // If no selection, let egui TextEdit handle normal Delete/Backspace behavior
            if has_selection {
                do_delete_selection = true;
                del_key_consumed = true;
                log::debug!(
                    "Will delete selection on {} key",
                    if pressed_del { "Delete" } else { "Backspace" }
                );

                // Remove the key event so TextEdit doesn't do additional mutation
                ui.ctx().input_mut(|ri| {
                    ri.events.retain(|e| {
                        !matches!(
                            e,
                            egui::Event::Key {
                                key: egui::Key::Backspace,
                                pressed: true,
                                ..
                            }
                        ) && !matches!(
                            e,
                            egui::Event::Key {
                                key: egui::Key::Delete,
                                pressed: true,
                                ..
                            }
                        )
                    });
                });
            } else {
                // No selection - let egui TextEdit handle normal Delete/Backspace
                log::debug!(
                    "No selection detected, letting TextEdit handle {} key normally",
                    if pressed_del { "Delete" } else { "Backspace" }
                );
                // Proactively request a repaint to avoid any visual lag/stale frame (outside locks)
                ui.ctx().request_repaint();
            }
        }

        if do_delete_selection {
            let mut start_b = 0;
            let mut end_b = 0;

            // Try to get selection range from egui state first
            if let Some(rng) =
                crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id)
                && rng.start != rng.end
            {
                start_b = to_byte_index(&tabular.editor.text, rng.start);
                end_b = to_byte_index(&tabular.editor.text, rng.end);
            }

            // Fallback to stored selection
            if start_b == end_b {
                start_b = tabular.selection_start;
                end_b = tabular.selection_end;
            }

            if start_b < end_b && end_b <= tabular.editor.text.len() {
                let selected_text = &tabular.editor.text[start_b..end_b];
                log::debug!(
                    "Deleting selection from {} to {}: '{}'",
                    start_b,
                    end_b,
                    selected_text
                );

                tabular.editor.apply_single_replace(start_b..end_b, "");
                tabular.cursor_position = start_b;
                tabular.selection_start = start_b;
                tabular.selection_end = start_b;
                tabular.selected_text.clear();
                // Mark for hard selection clear enforcement next frame
                tabular.selection_force_clear = true;

                // Sync egui caret to collapsed at start
                let ci = to_char_index(&tabular.editor.text, start_b);
                crate::editor_state_adapter::EditorStateAdapter::set_single(ui.ctx(), id, ci);

                // CRITICAL: Ensure editor maintains focus and cursor stays active for immediate typing
                ui.memory_mut(|m| m.request_focus(id));

                // Set focus boost to keep editor focused for several frames
                tabular.editor_focus_boost_frames = 10;

                // Mark tab as modified
                if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                    tab.content = tabular.editor.text.clone();
                    tab.is_modified = true;
                }

                log::debug!(
                    "Selection deleted successfully, cursor now at {} with focus maintained",
                    start_b
                );
                // Log remaining text preview
                {
                    let s = &tabular.editor.text;
                    let mut end = s.len();
                    for (count, (i, _)) in s.char_indices().enumerate() {
                        if count >= 200 {
                            end = i;
                            break;
                        }
                    }
                    let rem = if end < s.len() {
                        format!("{}… (len={})", s[..end].escape_debug(), s.len())
                    } else {
                        s.escape_debug().to_string()
                    };
                    log::debug!("Remaining text after selection delete: {}", rem);
                }
            }

            // If we consumed the key, request a repaint so UI reflects the change immediately
            if del_key_consumed {
                ui.ctx().request_repaint();
                // Double focus request to ensure it sticks
                ui.memory_mut(|m| m.request_focus(id));
            }
        }
    }
    // Special guard: Backspace on completely empty text -> consume and do nothing (avoid odd widget churn)
    {
        let id = egui::Id::new("sql_editor");
        let bs_pressed = ui.input(|i| i.key_pressed(egui::Key::Backspace));
        if bs_pressed && tabular.editor.text.is_empty() {
            // Ensure there's no selection
            let selection_exists = if let Some(rng) =
                crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id)
            {
                rng.start != rng.end
            } else {
                tabular.selection_start != tabular.selection_end
            };
            if !selection_exists {
                // Consume the Backspace key event and request a repaint outside the lock
                let mut consumed_bs_empty = false;
                ui.ctx().input_mut(|ri| {
                    let mut kept = Vec::with_capacity(ri.events.len());
                    for ev in ri.events.drain(..) {
                        if !consumed_bs_empty
                            && let egui::Event::Key {
                                key: egui::Key::Backspace,
                                pressed: true,
                                ..
                            } = ev
                        {
                            consumed_bs_empty = true;
                            continue;
                        }
                        kept.push(ev);
                    }
                    ri.events = kept;
                });
                if consumed_bs_empty {
                    log::debug!("Consumed Backspace on empty text (no-op)");
                    ui.ctx().request_repaint();
                }
            }
        }
    }
    // Forward Delete (no selection): delete the next grapheme to the right of the caret
    // On macOS laptops, this is typically triggered via Fn+Delete and should map to egui::Key::Delete
    {
        let id = egui::Id::new("sql_editor");
        let del_pressed_no_sel = ui.input(|i| i.key_pressed(egui::Key::Delete));
        if del_pressed_no_sel {
            // Determine if there's an active selection via egui state first, otherwise via stored state
            let mut has_selection = false;
            if let Some(rng) =
                crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id)
            {
                has_selection = rng.start != rng.end;
            } else if tabular.selection_start != tabular.selection_end {
                has_selection = true;
            }
            if !has_selection {
                // No selection: perform forward-delete of the next grapheme cluster
                let pos_b = tabular.cursor_position.min(tabular.editor.text.len());
                if pos_b < tabular.editor.text.len() {
                    use unicode_segmentation::UnicodeSegmentation;
                    let tail = &tabular.editor.text[pos_b..];
                    if let Some((_, first_gr)) = tail.grapheme_indices(true).next() {
                        let end_b = pos_b + first_gr.len();
                        let deleted_dbg = &tabular.editor.text[pos_b..end_b];
                        log::debug!(
                            "Forward Delete (no selection): removing '{}' at [{}..{}]",
                            deleted_dbg.escape_debug(),
                            pos_b,
                            end_b
                        );
                        tabular.editor.apply_single_replace(pos_b..end_b, "");
                        // Caret stays at pos_b
                        tabular.cursor_position = pos_b;
                        tabular.selection_start = pos_b;
                        tabular.selection_end = pos_b;
                        tabular.selected_text.clear();

                        // Sync egui caret to collapsed at pos_b (convert byte -> char)
                        let ci = {
                            let s = &tabular.editor.text;
                            let b = pos_b.min(s.len());
                            s[..b].chars().count()
                        };
                        crate::editor_state_adapter::EditorStateAdapter::set_single(
                            ui.ctx(),
                            id,
                            ci,
                        );

                        // Keep focus and mark modified
                        ui.memory_mut(|m| m.request_focus(id));
                        tabular.editor_focus_boost_frames = 6;
                        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                            tab.content = tabular.editor.text.clone();
                            tab.is_modified = true;
                        } else {
                            tabular.editor.mark_text_modified();
                        }

                        // Consume the Delete key event so TextEdit doesn't also process it
                        let mut consumed_delete_event = false;
                        ui.ctx().input_mut(|ri| {
                            let before = ri.events.len();
                            let mut kept = Vec::with_capacity(before);
                            for ev in ri.events.drain(..) {
                                if !consumed_delete_event
                                    && let egui::Event::Key {
                                        key: egui::Key::Delete,
                                        pressed: true,
                                        ..
                                    } = ev
                                {
                                    consumed_delete_event = true;
                                    continue;
                                }
                                kept.push(ev);
                            }
                            ri.events = kept;
                        });
                        if consumed_delete_event {
                            ui.ctx().request_repaint();
                        }
                        // Log remaining text preview
                        {
                            let s = &tabular.editor.text;
                            let mut end = s.len();
                            for (count, (i, _)) in s.char_indices().enumerate() {
                                if count >= 200 {
                                    end = i;
                                    break;
                                }
                            }
                            let rem = if end < s.len() {
                                format!("{}… (len={})", s[..end].escape_debug(), s.len())
                            } else {
                                s.escape_debug().to_string()
                            };
                            log::debug!("Remaining text after forward delete: {}", rem);
                        }
                    }
                } else if pos_b == tabular.editor.text.len() && pos_b > 0 {
                    // At end-of-text: treat Delete as Backspace (delete previous grapheme to the left)
                    use unicode_segmentation::UnicodeSegmentation;
                    let head = &tabular.editor.text[..pos_b];
                    // Find previous grapheme boundary by scanning the last grapheme in head
                    if let Some((start_off, prev_gr)) = head.grapheme_indices(true).next_back() {
                        let start_b = start_off;
                        let end_b = pos_b;
                        log::debug!(
                            "Delete at end -> backspace: removing '{}' at [{}..{}]",
                            prev_gr.escape_debug(),
                            start_b,
                            end_b
                        );
                        tabular.editor.apply_single_replace(start_b..end_b, "");
                        // Move caret left to start_b
                        tabular.cursor_position = start_b;
                        tabular.selection_start = start_b;
                        tabular.selection_end = start_b;
                        tabular.selected_text.clear();

                        // Sync egui caret to start_b
                        let ci = {
                            let s = &tabular.editor.text;
                            let b = start_b.min(s.len());
                            s[..b].chars().count()
                        };
                        crate::editor_state_adapter::EditorStateAdapter::set_single(
                            ui.ctx(),
                            id,
                            ci,
                        );
                        ui.memory_mut(|m| m.request_focus(id));
                        tabular.editor_focus_boost_frames = 6;
                        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                            tab.content = tabular.editor.text.clone();
                            tab.is_modified = true;
                        } else {
                            tabular.editor.mark_text_modified();
                        }
                        // Consume the Delete event
                        let mut consumed_delete_event = false;
                        ui.ctx().input_mut(|ri| {
                            let mut kept = Vec::with_capacity(ri.events.len());
                            for ev in ri.events.drain(..) {
                                if !consumed_delete_event
                                    && let egui::Event::Key {
                                        key: egui::Key::Delete,
                                        pressed: true,
                                        ..
                                    } = ev
                                {
                                    consumed_delete_event = true;
                                    continue;
                                }
                                kept.push(ev);
                            }
                            ri.events = kept;
                        });
                        if consumed_delete_event {
                            ui.ctx().request_repaint();
                        }
                        // Log remaining text preview
                        {
                            let s = &tabular.editor.text;
                            let mut end = s.len();
                            for (count, (i, _)) in s.char_indices().enumerate() {
                                if count >= 200 {
                                    end = i;
                                    break;
                                }
                            }
                            let rem = if end < s.len() {
                                format!("{}… (len={})", s[..end].escape_debug(), s.len())
                            } else {
                                s.escape_debug().to_string()
                            };
                            log::debug!("Remaining text after delete-at-end/backspace: {}", rem);
                        }
                    }
                }
            }
        }
    }
    // Note: Removed the confusing "Backspace at start -> forward delete" behavior
    // Now Backspace at start of text does nothing (standard behavior)
    // If users want to delete forward, they should use the Delete key (Fn+Delete on Mac)

    // [DISABLED] Special-case: Backspace at start-of-text with no selection -> perform forward delete of next grapheme
    // This entire block has been commented out because it was confusing - backspace at start should do nothing
    /*
    {
        let id = egui::Id::new("sql_editor");
        let bs_pressed = ui.input(|i| i.key_pressed(egui::Key::Backspace));
        if bs_pressed {
            // Determine selection
            let mut has_selection = false;
            let mut caret_b = tabular.cursor_position.min(tabular.editor.text.len());
            if let Some(rng) = crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id) {
                has_selection = rng.start != rng.end;
                // Prefer caret from egui state if available
                let to_b = |s: &str, ci: usize| -> usize {
                    match s.char_indices().nth(ci) { Some((b,_)) => b, None => s.len() }
                };
                caret_b = to_b(&tabular.editor.text, rng.primary).min(tabular.editor.text.len());
            } else if tabular.selection_start != tabular.selection_end {
                has_selection = true;
            }
            // The confusing forward delete behavior was here
        }
    }
    */

    // Helper: find line start and end byte indices for a given cursor
    let line_bounds = |s: &str, pos: usize| -> (usize, usize, usize) {
        let bytes = s.as_bytes();
        let mut start = pos.min(bytes.len());
        while start > 0 && bytes[start - 1] != b'\n' {
            start -= 1;
        }
        let mut end = pos.min(bytes.len());
        while end < bytes.len() && bytes[end] != b'\n' {
            end += 1;
        }
        // compute line number (slow but fine for pre-op)
        let mut ln = 0usize;
        let mut idx = 0usize;
        while idx < start {
            if bytes[idx] == b'\n' {
                ln += 1;
            }
            idx += 1;
        }
        (start, end, ln)
    };
    // Consume relevant events and set flags
    ui.ctx().input_mut(|ri| {
        let mut kept = Vec::with_capacity(ri.events.len());
        for ev in ri.events.drain(..) {
            match ev {
                egui::Event::Key {
                    key: egui::Key::ArrowLeft,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt => {
                    word_left_pressed = true;
                    word_nav_shift = modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowLeft,
                    pressed: true,
                    modifiers,
                    ..
                }
                    if tabular.multi_selection.len() > 1
                        && !modifiers.shift
                        && !modifiers.alt
                        && !modifiers.ctrl
                        && !modifiers.command =>
                {
                    multi_nav_left = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowRight,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt => {
                    word_right_pressed = true;
                    word_nav_shift = modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowRight,
                    pressed: true,
                    modifiers,
                    ..
                }
                    if tabular.multi_selection.len() > 1
                        && !modifiers.shift
                        && !modifiers.alt
                        && !modifiers.ctrl
                        && !modifiers.command =>
                {
                    multi_nav_right = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowUp,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt && !modifiers.shift => {
                    move_line_up = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowDown,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt && !modifiers.shift => {
                    move_line_down = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowUp,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt && modifiers.shift => {
                    dup_line_up = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowDown,
                    pressed: true,
                    modifiers,
                    ..
                } if modifiers.alt && modifiers.shift => {
                    dup_line_down = true;
                }
                other => kept.push(other),
            }
        }
        ri.events = kept;
    });
    // Apply word navigation immediately by updating egui TextEditState before widget is built
    if word_left_pressed || word_right_pressed {
        let id = egui::Id::new("sql_editor");
        let rng = crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id);
        let (start, end, primary) = if let Some(r) = rng {
            (r.start, r.end, r.primary)
        } else {
            let p = tabular.cursor_position.min(tabular.editor.text.len());
            (p, p, p)
        };
        let anchor = if primary == end { start } else { end };
        let cur = primary;
        let new_pos = if word_left_pressed {
            prev_word_boundary(&tabular.editor.text, cur)
        } else {
            next_word_boundary(&tabular.editor.text, cur)
        };
        if word_nav_shift {
            // Extend selection from anchor to new_pos, with primary at new_pos
            let start_ci = to_char_index(&tabular.editor.text, anchor.min(new_pos));
            let end_ci = to_char_index(&tabular.editor.text, anchor.max(new_pos));
            let primary_ci = to_char_index(&tabular.editor.text, new_pos);
            crate::editor_state_adapter::EditorStateAdapter::set_selection(
                ui.ctx(),
                id,
                start_ci,
                end_ci,
                primary_ci,
            );
            tabular.selection_start = anchor.min(new_pos);
            tabular.selection_end = anchor.max(new_pos);
            tabular.cursor_position = new_pos;
        } else {
            // Collapse to new_pos using direct set_ccursor_range equivalent
            let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
            let ci = to_char_index(&tabular.editor.text, new_pos);
            state
                .cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            state.store(ui.ctx(), id);
            tabular.selection_start = new_pos;
            tabular.selection_end = new_pos;
            tabular.cursor_position = new_pos;
        }
        ui.memory_mut(|m| m.request_focus(id));
    }
    if multi_nav_left || multi_nav_right {
        let id = egui::Id::new("sql_editor");
        if multi_nav_left {
            tabular
                .multi_selection
                .move_left(&tabular.editor.text);
        } else if multi_nav_right {
            tabular
                .multi_selection
                .move_right(&tabular.editor.text);
        }
        if let Some((start_b, end_b)) = tabular.multi_selection.primary_range() {
            // Primary caret collapses to start on left, end on right (equal when collapsed)
            let caret_b = if multi_nav_left { start_b } else { end_b };
            let to_char_index = |s: &str, byte_idx: usize| -> usize {
                let clamp = byte_idx.min(s.len());
                s[..clamp].chars().count()
            };
            crate::editor_state_adapter::EditorStateAdapter::set_single(
                ui.ctx(),
                id,
                to_char_index(&tabular.editor.text, caret_b),
            );
            tabular.selection_start = caret_b;
            tabular.selection_end = caret_b;
            tabular.cursor_position = caret_b;
            tabular.selected_text.clear();
        }
        ui.memory_mut(|m| m.request_focus(id));
        ui.ctx().request_repaint();
    }
    // Apply move/duplicate line operations pre-TextEdit (so content shows updated this frame)
    if move_line_up || move_line_down || dup_line_up || dup_line_down {
        let id = egui::Id::new("sql_editor");
        let text = &mut tabular.editor.text;
        let len = text.len();
        let rng = crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), id);
        let (sel_start, sel_end) = if let Some(r) = rng {
            (r.start, r.end)
        } else {
            (
                tabular.selection_start.min(len),
                tabular.selection_end.min(len),
            )
        };
        // Expand to whole lines
        let (line_start, _, _) = line_bounds(text, sel_start);
        let (_, mut line_end, _) = line_bounds(text, sel_end.max(sel_start));
        if line_end < len {
            line_end += 1; /* include trailing \n of last line if present */
        }
        // Extract block
        let block = text.get(line_start..line_end).unwrap_or("").to_string();
        // Locate neighbor line bounds
        if move_line_up || dup_line_up {
            if line_start == 0 {
                // Top-most; duplicate above still allowed
                if dup_line_up {
                    tabular
                        .editor
                        .apply_single_replace(line_start..line_start, &block);
                    let new_start = line_start;
                    let new_end = line_end + block.len();
                    let s_ci = to_char_index(&tabular.editor.text, new_start);
                    let e_ci = to_char_index(&tabular.editor.text, new_end);
                    crate::editor_state_adapter::EditorStateAdapter::set_selection(
                        ui.ctx(),
                        id,
                        s_ci,
                        e_ci,
                        e_ci,
                    );
                    tabular.selection_start = new_start;
                    tabular.selection_end = new_end;
                    tabular.cursor_position = new_end;
                }
            } else {
                // Find previous line start
                let prev_start = {
                    let bytes = text.as_bytes();
                    let mut s = line_start - 1; // currently at '\n' or char before current line
                    while s > 0 && bytes[s - 1] != b'\n' {
                        s -= 1;
                    }
                    s
                };
                if move_line_up {
                    // Remove block and insert before previous line
                    // First remove
                    let removed = block.clone();
                    tabular
                        .editor
                        .apply_single_replace(line_start..line_end, "");
                    // Adjust indices after removal
                    let insert_at = prev_start;
                    tabular
                        .editor
                        .apply_single_replace(insert_at..insert_at, &removed);
                    let new_start = insert_at;
                    let new_end = insert_at + removed.len();
                    let s_ci = to_char_index(&tabular.editor.text, new_start);
                    let e_ci = to_char_index(&tabular.editor.text, new_end);
                    crate::editor_state_adapter::EditorStateAdapter::set_selection(
                        ui.ctx(),
                        id,
                        s_ci,
                        e_ci,
                        e_ci,
                    );
                    tabular.selection_start = new_start;
                    tabular.selection_end = new_end;
                    tabular.cursor_position = new_end;
                } else if dup_line_up {
                    let insert_at = prev_start;
                    tabular
                        .editor
                        .apply_single_replace(insert_at..insert_at, &block);
                    let new_start = insert_at;
                    let new_end = insert_at + block.len();
                    let s_ci = to_char_index(&tabular.editor.text, new_start);
                    let e_ci = to_char_index(&tabular.editor.text, new_end);
                    crate::editor_state_adapter::EditorStateAdapter::set_selection(
                        ui.ctx(),
                        id,
                        s_ci,
                        e_ci,
                        e_ci,
                    );
                    tabular.selection_start = new_start;
                    tabular.selection_end = new_end;
                    tabular.cursor_position = new_end;
                }
            }
        } else if move_line_down || dup_line_down {
            // Find next line end start position
            let insert_after = line_end.min(text.len());
            if move_line_down {
                // Remove block, then insert after next line
                let removed = block.clone();
                tabular
                    .editor
                    .apply_single_replace(line_start..line_end, "");
                // After removal, the insertion point shifts left by removed.len()
                let mut after_next = insert_after - removed.len();
                // Move past the next line (find its end)
                let bytes2 = tabular.editor.text.as_bytes();
                let mut s = after_next;
                while s < bytes2.len() && bytes2[s] != b'\n' {
                    s += 1;
                }
                if s < bytes2.len() {
                    s += 1;
                }
                after_next = s;
                tabular
                    .editor
                    .apply_single_replace(after_next..after_next, &removed);
                let new_start = after_next;
                let new_end = after_next + block.len();
                let s_ci = to_char_index(&tabular.editor.text, new_start);
                let e_ci = to_char_index(&tabular.editor.text, new_end);
                crate::editor_state_adapter::EditorStateAdapter::set_selection(
                    ui.ctx(),
                    id,
                    s_ci,
                    e_ci,
                    e_ci,
                );
                tabular.selection_start = new_start;
                tabular.selection_end = new_end;
                tabular.cursor_position = new_end;
            } else if dup_line_down {
                tabular
                    .editor
                    .apply_single_replace(insert_after..insert_after, &block);
                let new_start = insert_after;
                let new_end = insert_after + block.len();
                let s_ci = to_char_index(&tabular.editor.text, new_start);
                let e_ci = to_char_index(&tabular.editor.text, new_end);
                crate::editor_state_adapter::EditorStateAdapter::set_selection(
                    ui.ctx(),
                    id,
                    s_ci,
                    e_ci,
                    e_ci,
                );
                tabular.selection_start = new_start;
                tabular.selection_end = new_end;
                tabular.cursor_position = new_end;
            }
        }
        ui.memory_mut(|m| m.request_focus(id));
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            tab.content = tabular.editor.text.clone();
            tab.is_modified = true;
        }
    }
    if raw_tab {
        tab_pressed_pre = true;
        log::debug!("Raw Tab event captured before editor render");
    }
    let accept_via_tab_pre = tab_pressed_pre && tabular.show_autocomplete;
    // Only accept via Enter if popup shown AND acceptance criteria met
    let accept_via_enter_pre = enter_pressed_pre && tabular.show_autocomplete;
    if accept_via_tab_pre || accept_via_enter_pre {
        // Remove Tab/Enter pressed events so TextEdit tidak menyisipkan tab/newline
        ui.ctx().input_mut(|ri| {
            let before = ri.events.len();
            ri.events.retain(|e| {
                !matches!(
                    e,
                    egui::Event::Key {
                        key: egui::Key::Tab,
                        pressed: true,
                        ..
                    }
                ) && !matches!(
                    e,
                    egui::Event::Key {
                        key: egui::Key::Enter,
                        pressed: true,
                        ..
                    }
                )
            });
            let removed = before - ri.events.len();
            if removed > 0 {
                log::debug!(
                    "Removed {} key event(s) (Tab/Enter) before autocomplete accept",
                    removed
                );
            }
        });
        // Mark acceptance to be applied after TextEdit render
        defer_accept_autocomplete = true;
    }

    // ----- Build widget after mutations -----
    // Re-enable syntax highlighting with cache
    let lang = tabular
        .query_tabs
        .get(tabular.active_tab_index)
        .and_then(|t| t.file_path.as_ref())
        .map(|p| crate::syntax_ts::detect_language_from_name(p))
        .unwrap_or(crate::syntax_ts::LanguageKind::Sql);
    let dark = matches!(
        tabular.advanced_editor.theme,
        models::structs::EditorColorTheme::GithubDark | models::structs::EditorColorTheme::Gruvbox
    );

    // Simple layouter; honor Word Wrap toggle by adjusting max_width
    let word_wrap = tabular.advanced_editor.word_wrap;
    let mut layouter = move |ui: &egui::Ui, text: &dyn egui::TextBuffer, wrap_width: f32| {
        let mut job = crate::syntax_ts::highlight_text(text.as_str(), lang, dark);
        job.wrap.max_width = if word_wrap { wrap_width } else { f32::INFINITY };
        ui.fonts(|f| f.layout_job(job))
    };

    // Pre-compute line count (immutable borrow) before creating mutable reference for TextEdit
    let pre_line_count = if tabular.advanced_editor.show_line_numbers {
        tabular.editor.text.lines().count().max(1)
    } else {
        0
    };
    // Pre-calc total_lines (used later for dynamic height) before mutable borrow by TextEdit
    let total_lines_for_layout = tabular.editor.text.lines().count().max(1);
    // Snapshot pre-change text and caret/selection for debug diff
    let pre_text_for_diff = tabular.editor.text.clone();
    let pre_state_for_diff = crate::editor_state_adapter::EditorStateAdapter::get_range(
        ui.ctx(),
        egui::Id::new("sql_editor"),
    );
    let (pre_sel_start_b, pre_sel_end_b, pre_cursor_b_for_diff) =
        if let Some(r) = pre_state_for_diff {
            let ss = to_byte_index(&tabular.editor.text, r.start);
            let ee = to_byte_index(&tabular.editor.text, r.end);
            let pp = to_byte_index(&tabular.editor.text, r.primary);
            (ss, ee, pp)
        } else {
            let p = tabular.cursor_position.min(tabular.editor.text.len());
            (
                tabular.selection_start.min(tabular.editor.text.len()),
                tabular.selection_end.min(tabular.editor.text.len()),
                p,
            )
        };

    // Calculate gutter width and editor rect
    let gutter_width = if tabular.advanced_editor.show_line_numbers {
        let digits = (pre_line_count as f32).log10().floor() as usize + 1;
        (digits as f32) * 8.0 + 16.0 // approximate monospace char width
    } else {
        0.0
    };

    let avail_rect = ui.available_rect_before_wrap();
    let line_height = ui.text_style_height(&egui::TextStyle::Monospace).max(1.0);
    let total_lines = total_lines_for_layout; // already computed before borrowing
    // Tinggi minimal mengikuti rows (tinggi viewport awal), tinggi maksimal mengikuti jumlah baris.
    // Tambahkan padding extra 3 * line_height agar baris terakhir tidak "nempel" pada panel bawah / handle.
    let min_height = line_height * rows as f32;
    let needed_height = line_height * total_lines as f32 + line_height * 3.0;
    let desired_height = needed_height.max(min_height);
    // Editor rect (tanpa gutter) – biarkan lebar penuh.
    let mut editor_rect = egui::Rect::from_min_size(
        avail_rect.min,
        egui::vec2(avail_rect.width(), desired_height),
    );

    if gutter_width > 0.0 {
        let gutter_rect = egui::Rect::from_min_max(
            editor_rect.min,
            egui::pos2(editor_rect.min.x + gutter_width, editor_rect.max.y),
        );
        editor_rect.min.x += gutter_width;
        ui.data_mut(|d| d.insert_temp::<egui::Rect>(egui::Id::new("gutter_rect"), gutter_rect));
    }

    // Build TextEdit widget directly and capture full output (galley, clip rect, etc.)
    let text_edit = egui::TextEdit::multiline(&mut tabular.editor.text)
        .font(egui::TextStyle::Monospace)
        .desired_rows(rows)
        .lock_focus(true)
        .desired_width(f32::INFINITY)
        .code_editor()
        .id_source("sql_editor")
        .layouter(&mut layouter);

    let egui::InnerResponse { inner: text_output, .. } =
        ui.allocate_ui_at_rect(editor_rect, |ui| text_edit.show(ui));
    let egui::text_edit::TextEditOutput {
        response,
        galley,
        galley_pos,
        text_clip_rect,
        ..
    } = text_output;

    // While focus boost is active, keep focus on the editor so typing works immediately after actions
    if tabular.editor_focus_boost_frames > 0 {
        ui.memory_mut(|m| m.request_focus(response.id));
        log::debug!(
            "Focus boost active: {} frames remaining",
            tabular.editor_focus_boost_frames
        );
    }
    // VSCode-like: subtle current line highlight
    if response.has_focus() {
        let cur = tabular.cursor_position.min(tabular.editor.text.len());
        let bytes = tabular.editor.text.as_bytes();
        let mut line_no = 0usize;
        let mut i = 0usize;
        while i < cur {
            if bytes[i] == b'\n' {
                line_no += 1;
            }
            i += 1;
        }
    let line_height = ui.text_style_height(&egui::TextStyle::Monospace);
    // Align with TextEdit internal top padding (approx. 6px)
    let y_top = response.rect.top() + 6.0 + (line_no as f32) * line_height;
        let gutter_w = if tabular.advanced_editor.show_line_numbers {
            gutter_width
        } else {
            0.0
        };
        let rect = egui::Rect::from_min_max(
            egui::pos2(response.rect.left() + gutter_w, y_top),
            egui::pos2(response.rect.right(), y_top + line_height),
        );
        let col = egui::Color32::from_rgba_unmultiplied(100, 100, 140, 30);
        ui.painter().rect_filled(rect, 0.0, col);
    }
    // Apply deferred autocomplete acceptance after TextEdit borrow is released
    if defer_accept_autocomplete {
        crate::editor_autocomplete::accept_current_suggestion(tabular);
        let clamped = tabular.cursor_position.min(tabular.editor.text.len());
        tabular.pending_cursor_set = Some(clamped);
        // Keep focus on editor so Tab/Enter doesn't move focus
        ui.memory_mut(|m| m.request_focus(response.id));
        // Immediately sync caret to the new end position in this frame as well
        let id = response.id;
        let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
        let ci = to_char_index(&tabular.editor.text, clamped);
        state
            .cursor
            .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
        state.store(ui.ctx(), id);
        ui.ctx().request_repaint();
        log::debug!(
            "Autocomplete accepted via {} (rope edit, post-render)",
            if accept_via_tab_pre { "Tab" } else { "Enter" }
        );
    }
    // Multi-cursor: key handling (Cmd+D / Ctrl+D for next occurrence) and Esc to clear
    let input_snapshot = ui.input(|i| i.clone());
    
    // Clear multi-selection on Escape
    if input_snapshot.key_pressed(egui::Key::Escape) && !tabular.multi_selection.is_empty() {
        tabular.multi_selection.clear();
        tabular.selected_text.clear();
        // Reset selection to single cursor at current position
        tabular.selection_start = tabular.cursor_position;
        tabular.selection_end = tabular.cursor_position;
        ui.ctx().request_repaint();
        log::debug!("🎯 Multi-selection cleared via Escape");
    }
    
    // Clear multi-selection when user navigates with arrow keys (without Shift)
    // This gives natural single-cursor behavior when moving around
    let navigation_clears = input_snapshot.key_pressed(egui::Key::ArrowUp)
        || input_snapshot.key_pressed(egui::Key::ArrowDown)
        || input_snapshot.key_pressed(egui::Key::Home)
        || input_snapshot.key_pressed(egui::Key::End)
        || input_snapshot.key_pressed(egui::Key::PageUp)
        || input_snapshot.key_pressed(egui::Key::PageDown);
    if !tabular.multi_selection.is_empty()
        && !input_snapshot.modifiers.shift
        && !input_snapshot.modifiers.alt // Don't clear on Alt+Arrow (word navigation)
        && navigation_clears
    {
        tabular.multi_selection.clear();
        tabular.selected_text.clear();
        ui.ctx().request_repaint();
        log::debug!("🎯 Multi-selection cleared due to navigation");
    }
    
    let cmd_or_ctrl = input_snapshot.modifiers.command || input_snapshot.modifiers.ctrl;
    if cmd_or_ctrl && input_snapshot.key_pressed(egui::Key::D) {
        // CMD+D / CTRL+D: Add next occurrence to multi-selection
        handle_add_next_occurrence(tabular, ui);
    }

    // Alt/Option + Click to add an extra caret (approximate hit-test on monospace grid)
    if input_snapshot.modifiers.alt
        && let Some(pos) = input_snapshot.pointer.interact_pos()
        && input_snapshot.pointer.primary_clicked()
        && response.rect.contains(pos)
    {
        // Compute line/column based on painter metrics
        let line_height = ui.text_style_height(&egui::TextStyle::Monospace);
        let char_w =
            ui.fonts(|f| f.glyph_width(&egui::TextStyle::Monospace.resolve(ui.style()), 'M'));
        let gutter_width = if tabular.advanced_editor.show_line_numbers {
            let total_lines = tabular.editor.text.lines().count().max(1);
            ui.fonts(|f| f.glyph_width(&egui::TextStyle::Monospace.resolve(ui.style()), '0'))
                * (total_lines.to_string().len() as f32)
        } else {
            0.0
        };
        let local = pos - response.rect.min;
        let mut line = ((local.y - 6.0) / line_height).floor() as isize;
        let mut col = ((local.x - gutter_width - 6.0) / char_w).floor() as isize;
        if line < 0 {
            line = 0;
        }
        if col < 0 {
            col = 0;
        }
        let line = line as usize;
        let col = col as usize;
        let start = tabular.editor.line_start(line);
        let end = if line + 1 < tabular.editor.line_count() {
            tabular
                .editor
                .line_start(line + 1)
                .min(tabular.editor.text.len())
        } else {
            tabular.editor.text.len()
        };
        let slice = &tabular.editor.text[start..end];
        let mut byte_off = start;
        let mut chars = 0usize;
        for (i, _) in slice.char_indices() {
            if chars >= col {
                break;
            }
            chars += 1;
            byte_off = start + i + 1;
        }
        // If requested column beyond line length, clamp to end
        if chars < col {
            byte_off = end;
        }
        tabular.add_cursor(byte_off);
        ui.ctx().request_repaint();
    }

    // Handle multi-cursor typing - apply changes to all cursors
    // Multi-selection typing compensations handled later in response.changed() branch now.
    if tabular.advanced_editor.show_line_numbers
        && let Some(gutter_rect) =
            ui.data(|d| d.get_temp::<egui::Rect>(egui::Id::new("gutter_rect")))
    {
        let line_height = ui.text_style_height(&egui::TextStyle::Monospace);
        let total_lines = tabular.editor.text.lines().count().max(1);
        let editor_height = response.rect.height();
        let needed_height = line_height * total_lines as f32;
        let final_rect = egui::Rect::from_min_size(
            gutter_rect.min,
            egui::vec2(gutter_rect.width(), editor_height.max(needed_height)),
        );
        let painter = ui.painter();
        painter.rect_filled(final_rect, 0.0, ui.visuals().faint_bg_color);
        
        // CRITICAL: Use SAME coordinate system as text editor (response.rect.top() + 6.0)
        // NOT final_rect.top() + 4.0!
        let mut y = response.rect.top() + 6.0;
        for (i, _) in tabular.editor.text.lines().enumerate() {
            painter.text(
                egui::pos2(final_rect.right() - 6.0, y),
                egui::Align2::RIGHT_TOP,
                (i + 1).to_string(),
                egui::TextStyle::Monospace.resolve(ui.style()),
                ui.visuals().weak_text_color(),
            );
            y += line_height;
            if y > final_rect.bottom() {
                break;
            }
        }
    }

    // Paint extra cursors and selection highlights (after gutter so they appear above text)
    if !tabular.multi_selection.is_empty() {
        let galley = galley.clone();
        let selection_painter = ui.painter().with_clip_rect(text_clip_rect);

        log::debug!(
            "🎨 Rendering multi-cursor highlights using galley_pos=({}, {})",
            galley_pos.x, galley_pos.y
        );

        let to_char_index = |s: &str, byte_idx: usize| -> usize {
            let clamped = byte_idx.min(s.len());
            s[..clamped].chars().count()
        };

        // First pass: Draw selection highlights for all regions
        for (idx, r) in tabular.multi_selection.regions().iter().enumerate() {
            let start_pos = r.min();
            let end_pos = r.max();

            if start_pos < end_pos {
                let start_ci = to_char_index(&tabular.editor.text, start_pos);
                let end_ci = to_char_index(&tabular.editor.text, end_pos);
                let start_cursor = CCursor::new(start_ci);
                let end_cursor = CCursor::new(end_ci);
                let range = CCursorRange::two(start_cursor, end_cursor);
                let [min_cursor, max_cursor] = range.sorted_cursors();
                let min_layout = galley.layout_from_cursor(min_cursor);
                let max_layout = galley.layout_from_cursor(max_cursor);

                let is_primary = idx == 0;
                let highlight_color = if is_primary {
                    egui::Color32::from_rgba_unmultiplied(70, 130, 180, 100)
                } else {
                    egui::Color32::from_rgba_unmultiplied(100, 150, 200, 80)
                };

                log::debug!(
                    "   Region [{}]: bytes {}..{} -> rows {}..{} (cols {} -> {})",
                    idx,
                    start_pos,
                    end_pos,
                    min_layout.row,
                    max_layout.row,
                    min_layout.column,
                    max_layout.column
                );

                for row_idx in min_layout.row..=max_layout.row {
                    let placed_row = &galley.rows[row_idx];
                    let row = &placed_row.row;

                    let left_local = if row_idx == min_layout.row {
                        row.x_offset(min_layout.column)
                    } else {
                        0.0
                    };
                    let right_local = if row_idx == max_layout.row {
                        row.x_offset(max_layout.column)
                    } else {
                        let newline_size = if row.ends_with_newline {
                            row.height() / 2.0
                        } else {
                            0.0
                        };
                        row.size.x + newline_size
                    };

                    let row_top = galley_pos.y + placed_row.min_y();
                    let row_bottom = galley_pos.y + placed_row.max_y();
                    let left = galley_pos.x + placed_row.pos.x + left_local;
                    let right = galley_pos.x + placed_row.pos.x + right_local;

                    let highlight_rect = egui::Rect::from_min_max(
                        egui::pos2(left, row_top),
                        egui::pos2(right, row_bottom),
                    );

                    if highlight_rect.is_positive() {
                        selection_painter.rect_filled(highlight_rect, 2.0, highlight_color);
                    }
                }
            }
        }

        // Second pass: Draw cursors on top of highlights
        for r in tabular.multi_selection.regions() {
            let caret_char_idx = to_char_index(&tabular.editor.text, r.max());
            let caret_cursor = CCursor::new(caret_char_idx);
            let caret_line_rect = galley
                .pos_from_cursor(caret_cursor)
                .translate(galley_pos.to_vec2());

            if caret_line_rect.height() > 0.0 {
                let caret_rect = egui::Rect::from_min_max(
                    egui::pos2(caret_line_rect.left(), caret_line_rect.top()),
                    egui::pos2(caret_line_rect.left() + 2.0, caret_line_rect.bottom()),
                );
                let color = egui::Color32::from_rgba_unmultiplied(100, 150, 255, 220);
                selection_painter.rect_filled(caret_rect, 1.0, color);
            }
        }
    }

    // After show(), apply any pending cursor via direct set_ccursor_range
    if let Some(pos) = tabular.pending_cursor_set {
        let id = response.id;
        let clamped = pos.min(tabular.editor.text.len());
        // Use a collapsed selection to set the caret directly
        let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
        let ci = to_char_index(&tabular.editor.text, clamped);
        state
            .cursor
            .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
        state.store(ui.ctx(), id);
        // Verify and re-assert if needed in the same frame
        let mut reapplied = false;
        if let Some(s2) = TextEditState::load(ui.ctx(), id)
            && let Some(rng) = s2.cursor.char_range()
            && rng.primary.index != ci
        {
            let mut s3 = s2;
            s3.cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            s3.store(ui.ctx(), id);
            reapplied = true;
        }
        tabular.cursor_position = clamped;
        tabular.pending_cursor_set = None;
        // Enforce for a few frames to fight any late overrides
        tabular.autocomplete_expected_cursor = Some(clamped);
        tabular.autocomplete_protection_frames = tabular.autocomplete_protection_frames.max(8);
        // Keep focus and repaint so the caret moves visually this frame
        ui.memory_mut(|m| m.request_focus(id));
        ui.ctx().request_repaint();
        debug!(
            "Applied pending cursor position {}{}",
            clamped,
            if reapplied { " (reapplied)" } else { "" }
        );
        // One more hard set right after the log to absolutely force the caret position in this frame
        if let Some(state_now) = TextEditState::load(ui.ctx(), id) {
            let mut st = state_now;
            let ci = to_char_index(&tabular.editor.text, clamped);
            st.cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            st.store(ui.ctx(), id);
            ui.memory_mut(|m| m.request_focus(id));
            ui.ctx().request_repaint();
            debug!("Forced caret to {} after pending apply log", clamped);
        }
    }
    // Enforce expected caret for a short window after autocomplete accept
    if tabular.autocomplete_protection_frames > 0 {
        if let Some(expected) = tabular.autocomplete_expected_cursor {
            let id = response.id;
            // Read current state
            if let Some(state) = TextEditState::load(ui.ctx(), id) {
                if let Some(rng) = state.cursor.char_range() {
                    let current = rng.primary.index;
                    let exp_ci = to_char_index(&tabular.editor.text, expected);
                    if current != exp_ci {
                        let mut st = state;
                        st.cursor
                            .set_char_range(Some(CCursorRange::one(CCursor::new(exp_ci))));
                        st.store(ui.ctx(), id);
                        ui.memory_mut(|m| m.request_focus(id));
                    }
                }
            } else {
                let mut st = TextEditState::default();
                let exp_ci = to_char_index(&tabular.editor.text, expected);
                st.cursor
                    .set_char_range(Some(CCursorRange::one(CCursor::new(exp_ci))));
                st.store(ui.ctx(), id);
                ui.memory_mut(|m| m.request_focus(id));
            }
        }
        tabular.autocomplete_protection_frames =
            tabular.autocomplete_protection_frames.saturating_sub(1);
        if tabular.autocomplete_protection_frames == 0 {
            tabular.autocomplete_expected_cursor = None;
        }
    }
    // Decrement focus boost window
    if tabular.editor_focus_boost_frames > 0 {
        tabular.editor_focus_boost_frames -= 1;
    }

    // Cleanup stray tab character inside the just-completed identifier (from Tab key) if any
    if accept_via_tab_pre {
        // Cursor currently at end of identifier after injection; scan backwards
        let mut idx = tabular.cursor_position.min(tabular.editor.text.len());
        let bytes = tabular.editor.text.as_bytes();
        while idx > 0 {
            let ch = bytes[idx - 1] as char;
            if ch.is_alphanumeric() || ch == '_' || ch == '\t' {
                idx -= 1;
            } else {
                break;
            }
        }
        // Now [idx .. cursor_position] spans the token (possibly including a tab)
        if idx < tabular.cursor_position {
            let token_range_end = tabular.cursor_position;
            let token_owned = tabular.editor.text[idx..token_range_end].to_string();
            if token_owned.contains('\t') {
                let cleaned: String = token_owned.chars().filter(|c| *c != '\t').collect();
                if cleaned != token_owned {
                    tabular
                        .editor
                        .text
                        .replace_range(idx..token_range_end, &cleaned);
                    let shift = token_owned.len() - cleaned.len();
                    tabular.cursor_position -= shift;
                    // Adjust egui state cursor (convert byte -> char index)
                    let id = egui::Id::new("sql_editor");
                    let ci = to_char_index(&tabular.editor.text, tabular.cursor_position);
                    crate::editor_state_adapter::EditorStateAdapter::set_single(ui.ctx(), id, ci);
                    log::debug!(
                        "Removed tab character from accepted token; new token='{}'",
                        cleaned
                    );
                }
            }
        }
    }

    // Try to capture selected text from the response
    // Note: This is a simplified approach. The actual implementation may vary depending on the CodeEditor version
    // Recover cursor + selection from TextEditState (single range only for now)
    if let Some(rng) =
        crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), response.id)
    {
        // Convert char indices from egui to byte indices for our buffer
        let primary_b = to_byte_index(&tabular.editor.text, rng.primary);
        let start_b = to_byte_index(&tabular.editor.text, rng.start);
        let end_b = to_byte_index(&tabular.editor.text, rng.end);
        tabular.cursor_position = primary_b;
        tabular.selection_start = start_b;
        tabular.selection_end = end_b;
        if start_b != end_b {
            if let Some(selected) = tabular.editor.text.get(start_b..end_b) {
                tabular.selected_text = selected.to_string();
            } else {
                tabular.selected_text.clear();
            }
        } else {
            tabular.selected_text.clear();
        }
    }

    // Enforce selection collapse visually if requested by a previous destructive action
    if tabular.selection_force_clear {
        let id = response.id;
        let caret_b = tabular.cursor_position.min(tabular.editor.text.len());
        // Convert byte index to char index for egui state
        let to_char_index = |s: &str, byte_idx: usize| -> usize {
            let b = byte_idx.min(s.len());
            s[..b].chars().count()
        };
        let ci = to_char_index(&tabular.editor.text, caret_b);
        crate::editor_state_adapter::EditorStateAdapter::set_single(ui.ctx(), id, ci);
        tabular.selection_start = caret_b;
        tabular.selection_end = caret_b;
        tabular.selected_text.clear();
        tabular.selection_force_clear = false;
        // keep focus and repaint to reflect collapse immediately
        ui.memory_mut(|m| m.request_focus(id));
        ui.ctx().request_repaint();
    }

    // Reset table focus flag when editor is interacted with
    if response.clicked() || response.has_focus() {
        tabular.table_recently_clicked = false;
    }

    // (Multi-line indent already handled pre-render if applied)

    // If you get a type error here, try:
    // let mut buffer = egui_code_editor::SimpleTextBuffer::from(&tabular.editor_text);
    // let response = editor.show(ui, &mut buffer);
    // tabular.editor_text = buffer.text().to_string();

    // Update tab content when editor changes (but skip autocomplete update if we're accepting via Tab)
    if response.changed() {
        // DEBUG: compute simple diff between pre and post text to log inserted/deleted content
        let post_text_for_diff = tabular.editor.text.clone();
        let pre_s = pre_text_for_diff.as_str();
        let post_s = post_text_for_diff.as_str();
        let pre_b = pre_s.as_bytes();
        let post_b = post_s.as_bytes();
        let mut pref = 0usize;
        let max_pref = pre_b.len().min(post_b.len());
        while pref < max_pref && pre_b[pref] == post_b[pref] {
            pref += 1;
        }
        let mut pre_suf = pre_b.len();
        let mut post_suf = post_b.len();
        while pre_suf > pref && post_suf > pref && pre_b[pre_suf - 1] == post_b[post_suf - 1] {
            pre_suf -= 1;
            post_suf -= 1;
        }
        let deleted_dbg = pre_s[pref..pre_suf].escape_debug().to_string();
        let inserted_dbg = post_s[pref..post_suf].escape_debug().to_string();
        let post_state_for_diff =
            crate::editor_state_adapter::EditorStateAdapter::get_range(ui.ctx(), response.id);
        let (post_sel_start_b, post_sel_end_b, post_cursor_b_for_diff) =
            if let Some(r) = post_state_for_diff {
                let ss = to_byte_index(&tabular.editor.text, r.start);
                let ee = to_byte_index(&tabular.editor.text, r.end);
                let pp = to_byte_index(&tabular.editor.text, r.primary);
                (ss, ee, pp)
            } else {
                let p = tabular.cursor_position.min(tabular.editor.text.len());
                (
                    tabular.selection_start.min(tabular.editor.text.len()),
                    tabular.selection_end.min(tabular.editor.text.len()),
                    p,
                )
            };
        let (bs_pressed, del_pressed, left_pressed, right_pressed) = ui.input(|i| {
            (
                i.key_pressed(egui::Key::Backspace),
                i.key_pressed(egui::Key::Delete),
                i.key_pressed(egui::Key::ArrowLeft),
                i.key_pressed(egui::Key::ArrowRight),
            )
        });
        log::debug!(
            "Δ edit: del='{}' ins='{}' @ [{}..{}] -> [{}..{}]; keys: BS={} DEL={} ←={} →={}; cursor {}->{}; sel {}..{} -> {}..{}",
            deleted_dbg,
            inserted_dbg,
            pref,
            pre_suf,
            pref,
            post_suf,
            bs_pressed,
            del_pressed,
            left_pressed,
            right_pressed,
            pre_cursor_b_for_diff,
            post_cursor_b_for_diff,
            pre_sel_start_b,
            pre_sel_end_b,
            post_sel_start_b,
            post_sel_end_b
        );
        // If this was a deletion (and not an insertion), log concise remaining text
        if !deleted_dbg.is_empty() && inserted_dbg.is_empty() {
            let s = &tabular.editor.text;
            let mut end = s.len();
            for (count, (i, _)) in s.char_indices().enumerate() {
                if count >= 200 {
                    end = i;
                    break;
                }
            }
            let rem = if end < s.len() {
                format!("{}… (len={})", s[..end].escape_debug(), s.len())
            } else {
                s.escape_debug().to_string()
            };
            log::debug!("Deleted chars: '{}' | Remaining text: {}", deleted_dbg, rem);
        }
        // Update rope text directly from current editor text to avoid any rebase/merge anomalies
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            let new_full_owned = tabular.editor.text.clone();
            tabular.editor.set_text(new_full_owned.clone());
            tab.content = new_full_owned;
            tab.is_modified = true;
            log::debug!(
                "Synced rope via set_text; incremental update disabled to prevent trailing-char resurfacing"
            );
        } else {
            // No tab? still mark rope dirty via full path
            tabular.editor.mark_text_modified();
        }

        // Apply multi-cursor editing only when there are truly multiple cursors
        // (avoid interfering with normal single-caret Delete/Backspace behavior)
        {
            if tabular.multi_selection.len() > 1 {
                // Multi-cursor mode is active: apply typing to all cursors
                // Use TextEditState to detect what got inserted (only handles uniform insert across collapsed carets)
                if let Some(rng) = crate::editor_state_adapter::EditorStateAdapter::get_range(
                    ui.ctx(),
                    response.id,
                ) {
                    // Convert char -> byte for comparisons and slicing
                    let new_primary_b = to_byte_index(&tabular.editor.text, rng.primary);
                    let old_primary = tabular.cursor_position;
                    
                    // Detect if this was a typing action (insertion)
                    if new_primary_b > old_primary {
                        if let Some(inserted_slice) =
                            tabular.editor.text.get(old_primary..new_primary_b)
                        {
                            let inserted = inserted_slice.to_string();
                            log::debug!("🎯 Multi-cursor typing: inserting '{}' at {} cursors", 
                                       inserted.escape_debug(), tabular.multi_selection.len());
                            
                            // Apply the insertion to all other cursors
                            tabular
                                .multi_selection
                                .apply_insert_text(&mut tabular.editor.text, &inserted);
                            tabular.cursor_position = new_primary_b;
                        }
                    } 
                    // Detect if this was a backspace action
                    else if new_primary_b < old_primary
                        && old_primary.saturating_sub(new_primary_b) == 1
                    {
                        log::debug!("🎯 Multi-cursor backspace at {} cursors", 
                                   tabular.multi_selection.len());
                        tabular
                            .multi_selection
                            .apply_backspace(&mut tabular.editor.text);
                        tabular.cursor_position = new_primary_b;
                    }
                }
            } else if tabular.multi_selection.len() == 1 {
                // User is typing with only one cursor active (after CMD+D once)
                // Keep the multi-selection active for potential next CMD+D
                // but update the primary cursor position
                if let Some(rng) = crate::editor_state_adapter::EditorStateAdapter::get_range(
                    ui.ctx(),
                    response.id,
                ) {
                    let new_primary_b = to_byte_index(&tabular.editor.text, rng.primary);
                    tabular.cursor_position = new_primary_b;
                    
                    // Update the selection range in multi_selection
                    let start_b = to_byte_index(&tabular.editor.text, rng.start);
                    let end_b = to_byte_index(&tabular.editor.text, rng.end);
                    tabular.multi_selection.set_primary_range(start_b, end_b);
                }
            } else {
                // No multi-cursor active: normal single-caret behavior
                // Skip multi-cursor compensation when only a single caret is active
                // to avoid misinterpreting normal Delete/Backspace edits.
            }
        }

        // Rebuild autocomplete suggestions on text changes unless we're in the middle of accepting via Tab/Enter
        // Also skip if the change was just a newline insertion (to avoid lag after Enter)
        if !accept_via_tab_pre && !accept_via_enter_pre {
            let just_inserted_newline = inserted_dbg == "\\n" && deleted_dbg.is_empty();
            if !just_inserted_newline {
                log::debug!("🔍 AUTOCOMPLETE DEBUG: Updating autocomplete after text change");
                editor_autocomplete::update_autocomplete(tabular);
            } else {
                log::debug!(
                    "🔍 AUTOCOMPLETE DEBUG: Skipping autocomplete update after newline insertion"
                );
            }
        }

        // Ensure rope stays in sync if multi-cursor logic modified editor.text directly
        // (the above multi-cursor branch may mutate tabular.editor.text without going through rope APIs)
        tabular.editor.mark_text_modified();
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            tab.content = tabular.editor.text.clone();
            tab.is_modified = true;
        }

        // Force a repaint after text changes to ensure visual sync (avoids any lingering glyphs)
        // BUT skip excessive repaints for simple newline insertions to reduce lag
        if !(inserted_dbg == "\\n" && deleted_dbg.is_empty()) {
            ui.ctx().request_repaint();
        }
    }

    // (Old forced replacement path removed; injection handles caret advance)

    // Keyboard handling for autocomplete
    let input = ui.input(|i| i.clone());
    if input.key_pressed(egui::Key::Space) && (input.modifiers.ctrl || input.modifiers.command) {
        editor_autocomplete::trigger_manual(tabular);
    }

    // Fallback: detect raw tab character insertion (editor consumed Tab key)
    if tabular.show_autocomplete && !tab_pressed_pre {
        // only if we didn't already detect it
        let cur = tabular.cursor_position.min(tabular.editor.text.len());
        if cur > 0 && tabular.editor.text.chars().nth(cur - 1) == Some('\t') {
            // Remove the inserted tab via rope edit
            let start = cur - 1;
            tabular.editor.apply_single_replace(start..cur, "");
            tabular.cursor_position = tabular.cursor_position.saturating_sub(1);
            log::debug!("Detected tab character insertion -> triggering autocomplete accept");
            editor_autocomplete::accept_current_suggestion(tabular);
            // Immediately set caret to new position and refocus
            let id = response.id;
            let clamped = tabular.cursor_position.min(tabular.editor.text.len());
            let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
            let ci = to_char_index(&tabular.editor.text, clamped);
            state
                .cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            state.store(ui.ctx(), id);
            ui.memory_mut(|m| m.request_focus(id));
        } else if cur >= 4 && &tabular.editor.text[cur - 4..cur] == "    " {
            // Remove inserted 4 spaces via rope edit
            let start = cur - 4;
            tabular.editor.apply_single_replace(start..cur, "");
            tabular.cursor_position = tabular.cursor_position.saturating_sub(4);
            log::debug!("Detected 4-space indentation -> triggering autocomplete accept");
            editor_autocomplete::accept_current_suggestion(tabular);
            // Immediately set caret to new position and refocus
            let id = response.id;
            let clamped = tabular.cursor_position.min(tabular.editor.text.len());
            let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
            let ci = to_char_index(&tabular.editor.text, clamped);
            state
                .cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            state.store(ui.ctx(), id);
            ui.memory_mut(|m| m.request_focus(id));
        }
    }
    if tabular.show_autocomplete {
        // Navigasi popup autocomplete: gunakan arrow yang sudah kita intercept sebelum render editor
        if arrow_down_pressed {
            editor_autocomplete::navigate(tabular, 1);
        }
        if arrow_up_pressed {
            editor_autocomplete::navigate(tabular, -1);
        }
        let mut accepted = false;
        if input.key_pressed(egui::Key::Enter) && !accept_via_enter_pre {
            // Apply same heuristic as pre-render
            let mut should_accept = tabular.autocomplete_navigated;
            if !should_accept {
                let sugg_count = tabular.autocomplete_suggestions.len();
                if sugg_count == 1 {
                    should_accept = true;
                } else {
                    let prefix = tabular.autocomplete_prefix.clone();
                    if let Some(sugg) = tabular
                        .autocomplete_suggestions
                        .get(tabular.selected_autocomplete_index)
                        && !prefix.is_empty()
                    {
                        let p = prefix.to_lowercase();
                        let s = sugg.to_lowercase();
                        if s.starts_with(&p) {
                            should_accept = true;
                        }
                    }
                }
            }

            if should_accept {
                editor_autocomplete::accept_current_suggestion(tabular);
                accepted = true;
            }
        }
        // Skip Tab acceptance here if already processed earlier
        if tab_pressed_pre && !accept_via_tab_pre {
            editor_autocomplete::accept_current_suggestion(tabular);
            accepted = true;
        }
        if accepted {
            log::debug!(
                "Autocomplete accepted via {}",
                if tab_pressed_pre {
                    "Tab"
                } else {
                    "Enter(post)"
                }
            );
            // Clean up potential inserted tab characters or spaces from editor before replacement
            // Detect diff compared to pre_text
            if tabular.editor.text.contains('\t') {
                // Remove a lone tab right before cursor via rope edit if exists
                let cur = tabular.cursor_position.min(tabular.editor.text.len());
                if cur > 0 && tabular.editor.text.chars().nth(cur - 1) == Some('\t') {
                    let start = cur - 1;
                    tabular.editor.apply_single_replace(start..cur, "");
                    tabular.cursor_position = tabular.cursor_position.saturating_sub(1);
                }
            }
            // Remove four leading spaces sequence before cursor (indent) if present
            let cur = tabular.cursor_position.min(tabular.editor.text.len());
            if cur >= 4 && &tabular.editor.text[cur - 4..cur] == "    " {
                let start = cur - 4;
                tabular.editor.apply_single_replace(start..cur, "");
                tabular.cursor_position = tabular.cursor_position.saturating_sub(4);
            }
            // Update internal egui state for cursor after Enter accept path
            let id = response.id;
            // Apply collapsed caret directly via set_ccursor_range equivalent
            let mut state = TextEditState::load(ui.ctx(), id).unwrap_or_default();
            let ci = to_char_index(&tabular.editor.text, tabular.cursor_position);
            state
                .cursor
                .set_char_range(Some(CCursorRange::one(CCursor::new(ci))));
            state.store(ui.ctx(), id);
            // Re-focus editor so Tab doesn't move focus away
            ui.memory_mut(|m| m.request_focus(id));
        }
        if input.key_pressed(egui::Key::Escape) {
            tabular.show_autocomplete = false;
        }
    }

    // Update suggestions saat kursor bergerak kiri/kanan (tanpa perubahan teks)
    let moved_lr =
        input.key_pressed(egui::Key::ArrowLeft) || input.key_pressed(egui::Key::ArrowRight);
    if moved_lr && !accept_via_tab_pre && !accept_via_enter_pre {
        if should_suppress_autocomplete_on_arrow(tabular) {
            log::trace!(
                "Skipping autocomplete update (suppress_once={}, table_clicked={}, has_cell={})",
                tabular.suppress_editor_arrow_once,
                tabular.table_recently_clicked,
                tabular.selected_cell.is_some()
            );
        } else {
            editor_autocomplete::update_autocomplete(tabular);
        }
        // Always reset suppression flag after one frame (even if we updated)
        tabular.suppress_editor_arrow_once = false;
    }

    // Render autocomplete popup positioned under cursor
    if tabular.show_autocomplete && !tabular.autocomplete_suggestions.is_empty() {
        // Approximate cursor line & column
        let cursor = tabular.cursor_position.min(tabular.editor.text.len());
        let mut line_start = 0usize;
        let mut line_no = 0usize;
        for (i, ch) in tabular.editor.text.char_indices() {
            if i >= cursor {
                break;
            }
            if ch == '\n' {
                line_no += 1;
                line_start = i + 1;
            }
        }
        let column = cursor - line_start;
        let char_w = 8.0_f32; // heuristic monospace width
        let line_h = ui.text_style_height(&egui::TextStyle::Monospace);
        let editor_rect = response.rect; // basic TextEdit rect
        let mut pos = egui::pos2(
            editor_rect.left() + (column as f32) * char_w,
            editor_rect.top() + 4.0 + (line_no as f32) * line_h,
        );
        // Clamp horizontally inside editor area
        if pos.x > editor_rect.right() - 150.0 {
            pos.x = editor_rect.right() - 150.0;
        }
        editor_autocomplete::render_autocomplete(tabular, ui, pos);
    }
}

pub(crate) fn perform_replace_all(tabular: &mut window_egui::Tabular) {
    if tabular.advanced_editor.find_text.is_empty() {
        return;
    }

    let find_text = &tabular.advanced_editor.find_text;
    let replace_text = &tabular.advanced_editor.replace_text;

    let new_text = if tabular.advanced_editor.use_regex {
        if let Ok(re) = regex::Regex::new(find_text) {
            re.replace_all(&tabular.editor.text, replace_text)
                .into_owned()
        } else {
            return;
        }
    } else if tabular.advanced_editor.case_sensitive {
        tabular.editor.text.replace(find_text, replace_text)
    } else {
        // case-insensitive simple replace
        let src = tabular.editor.text.clone();
        let find_lower = find_text.to_lowercase();
        let mut result = String::new();
        let mut last = 0;
        let src_lower = src.to_lowercase();
        let mut i = 0;
        while let Some(pos) = src_lower[i..].find(&find_lower) {
            let start = i + pos;
            result.push_str(&src[last..start]);
            result.push_str(replace_text);
            last = start + find_lower.len();
            i = last;
        }
        result.push_str(&src[last..]);
        result
    };

    // Bulk set text via buffer to keep rope in sync and record undo
    tabular.editor.set_text(new_text.clone());
    // Keep cursor within bounds
    tabular.cursor_position = tabular.cursor_position.min(tabular.editor.text.len());
    // Update current tab content
    if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
        tab.content = new_text;
        tab.is_modified = true;
    }
}

/// Returns true if an arrow-left/right movement in the editor this frame should NOT
/// trigger an autocomplete refresh. Conditions:
/// 1. A one-shot suppression flag set by table navigation (`suppress_editor_arrow_once`).
/// 2. The user is currently navigating a table (cell selected + table_recently_clicked).
#[inline]
fn should_suppress_autocomplete_on_arrow(tabular: &window_egui::Tabular) -> bool {
    (tabular.suppress_editor_arrow_once)
        || (tabular.table_recently_clicked && tabular.selected_cell.is_some())
}

pub(crate) fn find_next(tabular: &mut window_egui::Tabular) {
    // This is a simplified find implementation
    // In a real implementation, you'd want to track cursor position and highlight matches
    if !tabular.advanced_editor.find_text.is_empty()
        && let Some(_pos) = tabular.editor.text.find(&tabular.advanced_editor.find_text)
    {
        // In a full implementation, you would scroll to and highlight the match
        debug!("Found match for: {}", tabular.advanced_editor.find_text);
    }
}

pub(crate) fn open_command_palette(tabular: &mut window_egui::Tabular) {
    tabular.show_command_palette = true;
    tabular.command_palette_input.clear();
    tabular.show_theme_selector = false;
    tabular.command_palette_selected_index = 0;

    // Initialize command palette items
    tabular.command_palette_items = vec![
        "Preferences: Color Theme".to_string(),
        "View: Toggle Word Wrap".to_string(),
        "View: Toggle Line Numbers".to_string(),
        "View: Toggle Find and Replace".to_string(),
    ];
}

pub(crate) fn navigate_command_palette(tabular: &mut window_egui::Tabular, direction: i32) {
    // Filter commands based on current input
    let filtered_commands: Vec<String> = if tabular.command_palette_input.is_empty() {
        tabular.command_palette_items.clone()
    } else {
        tabular
            .command_palette_items
            .iter()
            .filter(|cmd| {
                cmd.to_lowercase()
                    .contains(&tabular.command_palette_input.to_lowercase())
            })
            .cloned()
            .collect()
    };

    if filtered_commands.is_empty() {
        return;
    }

    // Update selected index with wrapping
    if direction > 0 {
        // Down arrow
        tabular.command_palette_selected_index =
            (tabular.command_palette_selected_index + 1) % filtered_commands.len();
    } else {
        // Up arrow
        if tabular.command_palette_selected_index == 0 {
            tabular.command_palette_selected_index = filtered_commands.len() - 1;
        } else {
            tabular.command_palette_selected_index -= 1;
        }
    }
}

pub(crate) fn execute_selected_command(tabular: &mut window_egui::Tabular) {
    // Filter commands based on current input
    let filtered_commands: Vec<String> = if tabular.command_palette_input.is_empty() {
        tabular.command_palette_items.clone()
    } else {
        tabular
            .command_palette_items
            .iter()
            .filter(|cmd| {
                cmd.to_lowercase()
                    .contains(&tabular.command_palette_input.to_lowercase())
            })
            .cloned()
            .collect()
    };

    if tabular.command_palette_selected_index < filtered_commands.len() {
        let selected_command = filtered_commands[tabular.command_palette_selected_index].clone();
        execute_command(tabular, &selected_command);
    }
}

pub(crate) fn navigate_theme_selector(tabular: &mut window_egui::Tabular, direction: i32) {
    // There are 3 themes available
    let theme_count = 3;

    // Update selected index with wrapping
    if direction > 0 {
        // Down arrow
        tabular.theme_selector_selected_index =
            (tabular.theme_selector_selected_index + 1) % theme_count;
    } else {
        // Up arrow
        if tabular.theme_selector_selected_index == 0 {
            tabular.theme_selector_selected_index = theme_count - 1;
        } else {
            tabular.theme_selector_selected_index -= 1;
        }
    }
}

pub(crate) fn select_current_theme(tabular: &mut window_egui::Tabular) {
    // Map index to theme
    let theme = match tabular.theme_selector_selected_index {
        0 => models::structs::EditorColorTheme::GithubDark,
        1 => models::structs::EditorColorTheme::GithubLight,
        2 => models::structs::EditorColorTheme::Gruvbox,
        _ => models::structs::EditorColorTheme::GithubDark, // fallback
    };

    tabular.advanced_editor.theme = theme;
    tabular.show_theme_selector = false;
}

pub(crate) fn render_command_palette(tabular: &mut window_egui::Tabular, ctx: &egui::Context) {
    // Create a centered modal dialog
    egui::Area::new(egui::Id::new("command_palette"))
        .fixed_pos(egui::pos2(
            ctx.screen_rect().center().x - 300.0,
            ctx.screen_rect().center().y - 200.0,
        ))
        .show(ctx, |ui| {
            egui::Frame::default()
                .fill(ui.style().visuals.window_fill)
                .stroke(ui.style().visuals.window_stroke)
                .shadow(egui::epaint::Shadow::default())
                .inner_margin(egui::Margin::same(10))
                .show(ui, |ui| {
                    ui.vertical(|ui| {
                        // Search input
                        let response = ui.add_sized(
                            [580.0, 25.0],
                            egui::TextEdit::singleline(&mut tabular.command_palette_input)
                                .hint_text("Type command name..."),
                        );

                        // Reset selection when text changes
                        if response.changed() {
                            tabular.command_palette_selected_index = 0;
                        }

                        // Auto-focus the input when palette opens
                        if tabular.command_palette_input.is_empty() {
                            response.request_focus();
                        }

                        ui.separator();

                        // Filter commands based on input
                        let filtered_commands: Vec<String> =
                            if tabular.command_palette_input.is_empty() {
                                tabular.command_palette_items.clone()
                            } else {
                                tabular
                                    .command_palette_items
                                    .iter()
                                    .filter(|cmd| {
                                        cmd.to_lowercase()
                                            .contains(&tabular.command_palette_input.to_lowercase())
                                    })
                                    .cloned()
                                    .collect()
                            };

                        // Ensure selected index is within bounds when filtering
                        if tabular.command_palette_selected_index >= filtered_commands.len()
                            && !filtered_commands.is_empty()
                        {
                            tabular.command_palette_selected_index = 0;
                        }

                        // Command list
                        egui::ScrollArea::vertical()
                            .max_height(500.0)
                            .show(ui, |ui| {
                                for (index, command) in filtered_commands.iter().enumerate() {
                                    let is_selected =
                                        index == tabular.command_palette_selected_index;

                                    // Highlight selected item
                                    let text = if is_selected {
                                        egui::RichText::new(command)
                                            .background_color(ui.style().visuals.selection.bg_fill)
                                            .color(ui.style().visuals.selection.stroke.color)
                                    } else {
                                        egui::RichText::new(command)
                                    };

                                    if ui.selectable_label(is_selected, text).clicked() {
                                        execute_command(tabular, command);
                                    }
                                }
                            });
                    });
                });
        });
}

pub(crate) fn execute_command(tabular: &mut window_egui::Tabular, command: &str) {
    match command {
        "Preferences: Color Theme" => {
            tabular.show_command_palette = false;
            // Instead of directly setting show_theme_selector, use a flag
            tabular.request_theme_selector = true;
            tabular.theme_selector_selected_index = 0; // Reset to first theme
        }
        "View: Toggle Word Wrap" => {
            tabular.advanced_editor.word_wrap = !tabular.advanced_editor.word_wrap;
            tabular.show_command_palette = false;
        }
        "View: Toggle Line Numbers" => {
            tabular.advanced_editor.show_line_numbers = !tabular.advanced_editor.show_line_numbers;
            tabular.show_command_palette = false;
        }
        "View: Toggle Find and Replace" => {
            tabular.advanced_editor.show_find_replace = !tabular.advanced_editor.show_find_replace;
            tabular.show_command_palette = false;
        }
        _ => {
            debug!("Unknown command: {}", command);
            tabular.show_command_palette = false;
        }
    }
}

pub(crate) fn render_theme_selector(tabular: &mut window_egui::Tabular, ctx: &egui::Context) {
    // Create a centered modal dialog for theme selection
    egui::Area::new(egui::Id::new("theme_selector"))
        .fixed_pos(egui::pos2(
            ctx.screen_rect().center().x - 200.0,
            ctx.screen_rect().center().y - 150.0,
        ))
        .show(ctx, |ui| {
            egui::Frame::default()
                .fill(ui.style().visuals.window_fill)
                .stroke(ui.style().visuals.window_stroke)
                .shadow(egui::epaint::Shadow::default())
                .inner_margin(egui::Margin::same(15))
                .show(ui, |ui| {
                    ui.set_min_size(egui::vec2(400.0, 300.0));

                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Select Color Theme").heading());
                        ui.separator();

                        ui.spacing_mut().item_spacing.y = 8.0;

                        // Available themes with descriptions
                        let themes = [
                            (
                                models::structs::EditorColorTheme::GithubDark,
                                "GitHub Dark",
                                "Dark theme with blue accents",
                            ),
                            (
                                models::structs::EditorColorTheme::GithubLight,
                                "GitHub Light",
                                "Light theme with subtle colors",
                            ),
                            (
                                models::structs::EditorColorTheme::Gruvbox,
                                "Gruvbox",
                                "Retro warm theme with earthy colors",
                            ),
                        ];

                        for (index, (theme, name, description)) in themes.iter().enumerate() {
                            let is_current = tabular.advanced_editor.theme == *theme;
                            let is_selected = index == tabular.theme_selector_selected_index;

                            // Create horizontal layout for theme item
                            ui.horizontal(|ui| {
                                // Current theme indicator (checkmark)
                                if is_current {
                                    ui.label("✓");
                                } else {
                                    ui.label(" "); // Space for alignment
                                }

                                // Theme name with different styling based on selection
                                let text = if is_selected {
                                    // Highlight the selected item for keyboard navigation
                                    egui::RichText::new(*name)
                                        .size(16.0)
                                        .background_color(ui.style().visuals.selection.bg_fill)
                                        .color(ui.style().visuals.selection.stroke.color)
                                } else if is_current {
                                    // Bold text for current theme
                                    egui::RichText::new(*name)
                                        .size(16.0)
                                        .strong()
                                        .color(egui::Color32::from_rgb(0, 150, 255)) // Blue for current
                                } else {
                                    // Normal text for other themes
                                    egui::RichText::new(*name).size(16.0)
                                };

                                let response = ui.label(text);

                                // Handle click to select theme
                                if response.clicked() && !is_current {
                                    tabular.advanced_editor.theme = *theme;
                                    tabular.show_theme_selector = false;
                                }
                            });

                            // Show description with indentation
                            ui.horizontal(|ui| {
                                ui.add_space(20.0); // Indent description
                                ui.label(egui::RichText::new(*description).size(12.0).weak());
                            });
                            ui.add_space(5.0);
                        }

                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label("Use");
                            ui.code("↑↓");
                            ui.label("to navigate,");
                            ui.code("Enter");
                            ui.label("to select,");
                            ui.code("Escape");
                            ui.label("to close");
                        });
                    });
                });
        });
}

pub(crate) fn execute_query(tabular: &mut window_egui::Tabular) {
    tabular.is_table_browse_mode = false;
    // Priority: 1) Selected text, 2) Query from cursor position, 3) Full editor text
    let query = if !tabular.selected_text.trim().is_empty() {
        tabular.selected_text.trim().to_string()
    } else {
        let cursor_query = extract_query_from_cursor(tabular);
        if !cursor_query.trim().is_empty() {
            cursor_query
        } else {
            tabular.editor.text.trim().to_string()
        }
    };

    if query.is_empty() {
        tabular.current_table_name = "No query to execute".to_string();
        tabular.current_table_headers.clear();
        tabular.current_table_data.clear();
        return;
    }

    // We no longer branch on first execution; per-tab connection must always be set explicitly.

    // Strict per-tab connection: always use the tab's own connection_id (no global fallback)
    let connection_id = tabular
        .query_tabs
        .get(tabular.active_tab_index)
        .and_then(|t| t.connection_id);

    // If tab has no connection, show connection selector (first time OR after user cleared it)
    if connection_id.is_none() {
        debug!("Query execution requested but tab has no connection - opening selector");
        tabular.pending_query = query;
        tabular.auto_execute_after_connection = true;
        tabular.show_connection_selector = true;
        return;
    }

    // Check if we have an active connection
    if let Some(connection_id) = connection_id {
        // If a pool creation is already in progress for this connection, show loading and queue the query
        if tabular.pending_connection_pools.contains(&connection_id) {
            log::debug!(
                "⏳ Pool creation in progress for {}, queueing query and showing loading",
                connection_id
            );
            tabular.pool_wait_in_progress = true;
            tabular.pool_wait_connection_id = Some(connection_id);
            tabular.pool_wait_query = query.clone();
            tabular.pool_wait_started_at = Some(std::time::Instant::now());
            // Friendly status message; keep current data intact
            tabular.current_table_name = "Connecting… waiting for pool".to_string();
            // Do not execute now
            return;
        }

        // If no pool exists yet, try quick creation; if not immediately available, show loading
        if !tabular.connection_pools.contains_key(&connection_id) {
            // Attempt a quick creation via the runtime without capturing &mut tabular inside the future
            if let Some(rt) = tabular.runtime.clone() {
                // Use the non-blocking helper that handles pending state and background spawn
                let created = rt.block_on(async {
                    // SAFETY: try_get_connection_pool only briefly borrows tabular inside the await; we avoid
                    // capturing &mut tabular by doing only a readiness check here and letting the update loop handle execution.
                    // We return true only if a pool is immediately available; otherwise background creation will be started elsewhere.
                    crate::connection::try_get_connection_pool(tabular, connection_id)
                        .await
                        .is_some()
                });
                if !created {
                    // Not ready now; show loading and queue the query. Background creation will happen via get_or_create on demand.
                    log::debug!(
                        "🔧 Pool not ready for {}, queueing and showing loading",
                        connection_id
                    );
                    tabular.pool_wait_in_progress = true;
                    tabular.pool_wait_connection_id = Some(connection_id);
                    tabular.pool_wait_query = query.clone();
                    tabular.pool_wait_started_at = Some(std::time::Instant::now());
                    tabular.current_table_name = "Connecting… waiting for pool".to_string();
                    return;
                }
            } else {
                // No runtime configured yet; just set wait state
                tabular.pool_wait_in_progress = true;
                tabular.pool_wait_connection_id = Some(connection_id);
                tabular.pool_wait_query = query.clone();
                tabular.pool_wait_started_at = Some(std::time::Instant::now());
                tabular.current_table_name = "Connecting… waiting for pool".to_string();
                return;
            }
        }

        debug!("=== EXECUTING QUERY ===");
        debug!("Connection ID: {}", connection_id);
        debug!("Query: {}", query);

        // Auto-enable server-side pagination when the query does not specify LIMIT/TOP/OFFSET/FETCH
        // This prevents fetching huge result sets and uses paginated execution instead.
        {
            let upper = query.to_uppercase();
            // Broader detection: consider LIMIT/OFFSET/FETCH/TOP patterns without requiring trailing spaces
            let has_pagination_clause = upper.contains("LIMIT")
                || upper.contains("OFFSET")
                || upper.contains(" FETCH ")
                || upper.contains(" TOP ")
                || upper.contains("TOP(");

            // Heuristic: auto-paginate when there's at least one SELECT statement in the batch (e.g., "USE ...; SELECT ...")
            let is_select_like = upper
                .split(';')
                .any(|stmt| stmt.trim_start().starts_with("SELECT"));
            // Avoid auto-paginating for DML/DDL-only batches
            let is_mutating = upper.contains("INSERT ")
                || upper.contains("UPDATE ")
                || upper.contains("DELETE ")
                || upper.contains("MERGE ")
                || upper.contains("CREATE ")
                || upper.contains("ALTER ")
                || upper.contains("DROP ")
                || upper.contains("TRUNCATE ")
                || upper.contains("GRANT ")
                || upper.contains("REVOKE ")
                || upper.contains("EXEC ")
                || upper.contains("CALL ");

            if is_select_like && !has_pagination_clause && !is_mutating {
                // Prepare base query (trim trailing semicolon to avoid issues when appending LIMIT/OFFSET)
                let base_query = query.trim().trim_end_matches(';').to_string();

                // Force-enable server pagination for this execution as requested
                tabular.use_server_pagination = true;

                // Initialize server pagination state
                tabular.current_base_query = base_query.clone();
                tabular.current_page = 0;
                tabular.actual_total_rows = Some(10_000); // default total pages assumption

                // Persist base query into the active tab for consistent pagination behavior
                if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                    tab.base_query = base_query;
                    tab.current_page = tabular.current_page;
                    tab.page_size = tabular.page_size;
                }

                // Execute first page and exit normal path
                debug!(
                    "🚀 Auto server-pagination enabled (no LIMIT/TOP found). Executing first page..."
                );
                tabular.execute_paginated_query();
                return;
            }
        }

        let result =
            connection::execute_query_with_connection(tabular, connection_id, query.clone());

        debug!("Query execution result: {:?}", result.is_some());

        // Mark tab as having executed a query (regardless of success/failure)
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            tab.has_executed_query = true;
        }

        if let Some((headers, data)) = result {
            let is_error_result = headers.first().map(|h| h == "Error").unwrap_or(false);
            debug!("=== QUERY RESULT SUCCESS ===");
            debug!("Headers received: {} - {:?}", headers.len(), headers);
            debug!("Data rows received: {}", data.len());
            if !data.is_empty() {
                debug!("First row sample: {:?}", &data[0]);
            }

            tabular.current_table_headers = headers;

            // Use pagination for query results
            data_table::update_pagination_data(tabular, data);

            if tabular.total_rows == 0 {
                tabular.current_table_name = "Query executed successfully (no results)".to_string();
            } else {
                tabular.current_table_name = format!(
                    "Query Results ({} total rows, showing page {} of {})",
                    tabular.total_rows,
                    tabular.current_page + 1,
                    data_table::get_total_pages(tabular)
                );
            }
            debug!(
                "After update_pagination_data - total_rows: {}, all_table_data.len(): {}",
                tabular.total_rows,
                tabular.all_table_data.len()
            );
            debug!("============================");

            // Set the base query for pagination - this is crucial for regular queries!
            // For regular queries, we set the base query to the executed query (without LIMIT)
            let base_query_for_pagination = if !is_error_result && tabular.total_rows > 0 {
                // Simple LIMIT removal for pagination - remove LIMIT clause if present
                let mut clean_query = query.clone();
                if let Some(limit_pos) = clean_query.to_uppercase().rfind("LIMIT") {
                    // Find the end of the LIMIT clause (look for semicolon or end of string)
                    if let Some(semicolon_pos) = clean_query[limit_pos..].find(';') {
                        clean_query = format!(
                            "{}{}",
                            &clean_query[..limit_pos].trim(),
                            &clean_query[limit_pos + semicolon_pos..]
                        );
                    } else {
                        clean_query = clean_query[..limit_pos].trim().to_string();
                    }
                }
                clean_query
            } else {
                String::new()
            };
            tabular.current_base_query = base_query_for_pagination.clone();
            debug!(
                "📝 Set base_query for pagination: '{}'",
                base_query_for_pagination
            );

            // Save query to history hanya jika bukan hasil error
            if !is_error_result {
                sidebar_history::save_query_to_history(tabular, &query, connection_id);
            } else {
                debug!("Skip saving to history karena hasil error");
            }
            // Persist into tab state
            if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                tab.result_headers = tabular.current_table_headers.clone();
                tab.result_rows = tabular.current_table_data.clone();
                tab.result_all_rows = tabular.all_table_data.clone();
                tab.result_table_name = tabular.current_table_name.clone();
                tab.is_table_browse_mode = tabular.is_table_browse_mode;
                tab.current_page = tabular.current_page;
                tab.page_size = tabular.page_size;
                tab.total_rows = tabular.total_rows;
                tab.base_query = tabular.current_base_query.clone(); // Save the base query to the tab
            }
        } else {
            tabular.current_table_name = "Query execution failed".to_string();
            tabular.current_table_headers.clear();
            tabular.current_table_data.clear();
            tabular.all_table_data.clear();
            tabular.total_rows = 0;
            if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                tab.result_headers.clear();
                tab.result_rows.clear();
                tab.result_all_rows.clear();
                tab.result_table_name = tabular.current_table_name.clone();
                tab.total_rows = 0;
                tab.current_page = 0;
                tab.base_query.clear(); // Clear base query on failure
            }
        }
    }
}

pub(crate) fn extract_query_from_cursor(tabular: &mut window_egui::Tabular) -> String {
    if tabular.editor.text.is_empty() {
        return String::new();
    }

    let text_bytes = tabular.editor.text.as_bytes();
    let cursor_pos = tabular.cursor_position.min(text_bytes.len());

    // Find start position: go backwards from cursor to find the last semicolon (or start of file)
    let mut start_pos = 0;
    for i in (0..cursor_pos).rev() {
        if text_bytes[i] == b';' {
            // Start after the semicolon
            start_pos = i + 1;
            break;
        }
    }

    // Find end position: go forwards from cursor to find the next semicolon (or end of file)
    let mut end_pos = text_bytes.len();
    for (offset, &byte) in text_bytes[cursor_pos..].iter().enumerate() {
        if byte == b';' {
            // Include the semicolon
            end_pos = cursor_pos + offset + 1;
            break;
        }
    }

    // Extract the query text
    if let Ok(query_text) = std::str::from_utf8(&text_bytes[start_pos..end_pos]) {
        query_text.trim().to_string()
    } else {
        String::new()
    }
}

pub(crate) fn close_tabs_for_file(tabular: &mut window_egui::Tabular, file_path: &str) {
    // Find all tabs that have this file open and close them
    let mut indices_to_close = Vec::new();

    for (index, tab) in tabular.query_tabs.iter().enumerate() {
        if tab.file_path.as_deref() == Some(file_path) {
            indices_to_close.push(index);
        }
    }

    // Close tabs in reverse order to maintain correct indices
    for &index in indices_to_close.iter().rev() {
        editor::close_tab(tabular, index);
    }
}
