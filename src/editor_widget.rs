//! Experimental custom editor widget (Phase A skeleton).
//! Renders plain text from `EditorBuffer` and applies simple insert/backspace/newline.
//! Multi-cursor support (uniform insert/backspace) reused from `MultiSelection`.

use eframe::egui;
use egui::Ui;

use crate::{
    editor_buffer::EditorBuffer,
    editor_selection::MultiSelection,
    syntax::{LanguageKind, highlight_line},
};

#[derive(Default)]
pub struct EditorViewState {
    pub scroll_y: f32,
    pub scroll_x: f32,
    pub viewport_w: f32,
    pub viewport_h: f32,
    pub line_height: f32,
    pub desired_caret_x: Option<f32>,
}

#[derive(Default)]
pub struct EditorWidgetState {
    pub view: EditorViewState,
    pub show_line_numbers: bool,
}

impl EditorWidgetState {
    pub fn new() -> Self {
        Self {
            view: EditorViewState::default(),
            show_line_numbers: true,
        }
    }
}

#[derive(Default)]
pub struct EditorSignals {
    pub caret_moved: bool,
    pub text_changed: bool,
    pub inserted_char: Option<char>,
}

/// Phase A show function – minimal feature set.
pub fn show(
    ui: &mut Ui,
    state: &mut EditorWidgetState,
    buffer: &mut EditorBuffer,
    selection: &mut MultiSelection,
    lang: LanguageKind,
    dark: bool,
    line_cache: &mut std::collections::HashMap<(usize, u64), egui::text::LayoutJob>,
    _current_revision: u64, // deprecated param retained for compatibility; will use buffer.revision
) -> EditorSignals {
    let mut signals = EditorSignals::default();
    if selection.carets.is_empty() {
        selection.ensure_primary(0);
    }

    // --- Layout & interaction base ---
    let available = ui.available_rect_before_wrap();
    state.view.viewport_w = available.width();
    state.view.viewport_h = available.height();
    let line_height = ui.text_style_height(&egui::TextStyle::Monospace).max(1.0);
    state.view.line_height = line_height;
    let id = ui.make_persistent_id("custom_editor_phase_a");

    // --- Gather input BEFORE painting so edits show immediately ---
    let mut inserted_batch = String::new();
    let mut backspace = false;
    let mut move_left = false;
    let mut move_right = false;
    let mut move_up = false;
    let mut move_down = false;
    let mut shift = false;
    let mut undo_cmd = false;
    let mut redo_cmd = false;

    // We'll create a provisional rect; height will be recomputed after potential edits.
    // Reserve interaction space across available region for now.
    let provisional_rect = available;
    let response = ui.interact(provisional_rect, id, egui::Sense::click_and_drag());
    if response.has_focus() || response.clicked() {
        ui.memory_mut(|m| m.request_focus(id));
    }

    // Basic metrics for mapping pointer -> document offset (monospace assumption)
    let char_w = ui.fonts(|f| f.glyph_width(&egui::TextStyle::Monospace.resolve(ui.style()), 'M'));
    let line_count_now = buffer.line_count().max(1);
    let gutter_w = if state.show_line_numbers { 8.0 * (line_count_now.to_string().len() as f32 + 1.0) } else { 0.0 };
    let rect_left = available.min.x;
    let rect_top = available.min.y;
    let text_origin_for_hit = egui::pos2(
        rect_left + gutter_w + 6.0 - state.view.scroll_x,
        rect_top + 4.0 - state.view.scroll_y,
    );

    // Helper: map pointer position to nearest text offset (UTF-8 boundary safe)
    let pos_to_offset = |pos: egui::Pos2| -> usize {
        // Compute line index
        let y_rel = (pos.y - text_origin_for_hit.y).max(0.0);
        let mut line_idx = (y_rel / line_height).floor() as usize;
        line_idx = line_idx.min(buffer.line_count().saturating_sub(1));
        let line_start = buffer.line_start(line_idx).min(buffer.text.len());
        let line_end = if line_idx + 1 < buffer.line_count() {
            buffer.line_start(line_idx + 1).saturating_sub(1).min(buffer.text.len())
        } else {
            buffer.text.len()
        };
        let line_len = line_end.saturating_sub(line_start);
        // Compute column approximated by monospace width
        let x_rel = pos.x - text_origin_for_hit.x;
        let mut col = if x_rel <= 0.0 { 0 } else { (x_rel / char_w).round() as usize };
        col = col.min(line_len);
        let mut off = line_start + col;
        // Snap to char boundary
        if off > buffer.text.len() { off = buffer.text.len(); }
        while off > 0 && !buffer.text.is_char_boundary(off) { off -= 1; }
        off
    };

    // Mouse selection: click to set caret, drag to select
    if response.clicked() {
        if let Some(pos) = response.interact_pointer_pos() {
            let off = pos_to_offset(pos);
            let shift_pressed = ui.input(|i| i.modifiers.shift);
            if shift_pressed {
                selection.ensure_primary(off);
                if let Some(primary) = selection.primary_mut() {
                    primary.head = off; // anchor tetap
                }
            } else {
                selection.clear();
                selection.add_collapsed(off);
            }
            signals.caret_moved = true;
        }
    }

    // Double-click: select word under cursor
    if response.double_clicked() {
        if let Some(pos) = response.interact_pointer_pos() {
            let off = pos_to_offset(pos);
            let text = &buffer.text;
            let bytes = text.as_bytes();
            let is_word = |b: u8| -> bool { b.is_ascii_alphanumeric() || b == b'_' };
            let mut start = off.min(text.len());
            while start > 0 && !text.is_char_boundary(start) { start -= 1; }
            while start > 0 && {
                let p = start - 1; bytes.get(p).map(|&b| is_word(b)).unwrap_or(false)
            } {
                start -= 1;
                while start > 0 && !text.is_char_boundary(start) { start -= 1; }
            }
            let mut end = off.min(text.len());
            while end < text.len() && !text.is_char_boundary(end) { end += 1; }
            while end < text.len() && bytes.get(end).map(|&b| is_word(b)).unwrap_or(false) {
                end += 1;
                while end < text.len() && !text.is_char_boundary(end) { end += 1; }
            }
            if start < end {
                selection.clear();
                selection.add_caret(crate::editor_selection::Caret { anchor: start, head: end });
                signals.caret_moved = true;
            }
        }
    }
    if response.drag_started() {
        if let Some(pos) = response.interact_pointer_pos() {
            let off = pos_to_offset(pos);
            selection.ensure_primary(off);
            if let Some(primary) = selection.primary_mut() {
                primary.anchor = off;
                primary.head = off;
            }
            signals.caret_moved = true;
        }
    }
    if response.dragged() {
        if let Some(pos) = response.interact_pointer_pos() {
            // Auto-scroll while dragging near/over edges
            let margin = 12.0;
            let view_bottom = rect_top + state.view.viewport_h;
            if pos.y > view_bottom - margin {
                state.view.scroll_y += (pos.y - (view_bottom - margin)) * 0.5;
            } else if pos.y < rect_top + margin {
                state.view.scroll_y = state.view.scroll_y.saturating_sub_f32(((rect_top + margin) - pos.y) * 0.5);
            }
            let view_right = rect_left + state.view.viewport_w;
            if pos.x > view_right - margin {
                state.view.scroll_x += (pos.x - (view_right - margin)) * 0.5;
            } else if pos.x < rect_left + margin {
                state.view.scroll_x = (state.view.scroll_x - ((rect_left + margin) - pos.x) * 0.5).max(0.0);
            }
            let off = pos_to_offset(pos);
            if let Some(primary) = selection.primary_mut() {
                primary.head = off;
            }
            signals.caret_moved = true;
        }
    }

    ui.input(|i| {
        for ev in &i.events {
            match ev {
                egui::Event::Text(t) => {
                    if !t.chars().all(|c| c < ' ' && c != '\t') {
                        inserted_batch.push_str(t);
                    }
                }
                egui::Event::Key {
                    key: egui::Key::Enter,
                    pressed: true,
                    ..
                } => {
                    inserted_batch.push('\n');
                }
                egui::Event::Key {
                    key: egui::Key::Backspace,
                    pressed: true,
                    ..
                } => {
                    backspace = true;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowLeft,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    move_left = true;
                    shift = shift || modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowRight,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    move_right = true;
                    shift = shift || modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowUp,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    move_up = true;
                    shift = shift || modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::ArrowDown,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    move_down = true;
                    shift = shift || modifiers.shift;
                }
                egui::Event::Key {
                    key: egui::Key::Z,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    // Cmd/Ctrl+Z -> undo ; Shift+Cmd/Ctrl+Z -> redo
                    if modifiers.command || modifiers.ctrl {
                        // command covers mac_cmd on macOS
                        if modifiers.shift {
                            redo_cmd = true;
                        } else {
                            undo_cmd = true;
                        }
                    }
                }
                egui::Event::Key {
                    key: egui::Key::Y,
                    pressed: true,
                    modifiers,
                    ..
                } => {
                    // Ctrl+Y often redo on Windows/Linux
                    if modifiers.command || modifiers.ctrl {
                        redo_cmd = true;
                    }
                }
                _ => {}
            }
        }
    });

    // --- Text mutations ---
    if undo_cmd {
        if buffer.undo() {
            signals.text_changed = true;
        }
    } else if redo_cmd {
        if buffer.redo() {
            signals.text_changed = true;
        }
    } else if !inserted_batch.is_empty() {
        // Apply same insertion at each collapsed caret using rope-first replace
        let positions = selection.caret_positions();
        for &pos in positions.iter().rev() {
            let at = pos.min(buffer.text.len());
            buffer.apply_single_replace(at..at, &inserted_batch);
        }
        // Update selection positions after insertion: bump each caret/anchor >= its own pos
        let ins_len = inserted_batch.len();
        for &pos in &positions {
            selection.apply_simple_insert(pos, ins_len);
        }
        signals.text_changed = true;
        signals.inserted_char = inserted_batch.chars().last();
    } else if backspace {
        // Backspace one character to the left for each caret
        let positions = selection.caret_positions();
        // Determine deletion spans based on current text (before edits)
        let mut performed: Vec<(usize, usize)> = Vec::new(); // (start,len)
        for &pos in &positions {
            if pos == 0 { continue; }
            let mut real_start = pos - 1;
            while real_start > 0 && !buffer.text.is_char_boundary(real_start) {
                real_start -= 1;
            }
            let mut real_end = pos;
            while real_end < buffer.text.len() && !buffer.text.is_char_boundary(real_end) {
                real_end += 1;
            }
            if real_start < real_end && real_end <= buffer.text.len() {
                performed.push((real_start, real_end - real_start));
            }
        }
        // Apply rope edits from right to left to keep indices valid
        for &(start, len) in performed.iter().rev() {
            buffer.apply_single_replace(start..(start + len), "");
        }
        // Update selection using exact spans, from last to first
        performed.sort_by_key(|(s, _)| *s);
        for (start, len) in performed.into_iter().rev() {
            selection.apply_simple_delete(start, len);
        }
        signals.text_changed = true;
    }
    // No full clear; stale entries become unreachable because line_version changes.
    // Periodic pruning to avoid unbounded growth.
    if signals.text_changed && line_cache.len() > 10_000 {
        line_cache.retain(|(idx, ver), _| *ver == buffer.line_version(*idx));
    }

    // --- Movement (primary caret only for now) ---
    if let Some(primary) = selection.primary_mut() {
        let prev_head = primary.head;
        if move_left {
            if primary.head > 0 {
                primary.head -= 1;
                while primary.head > 0 && !buffer.text.is_char_boundary(primary.head) {
                    primary.head -= 1;
                }
            }
            if !shift {
                primary.anchor = primary.head;
            }
        }
        if move_right {
            if primary.head < buffer.text.len() {
                primary.head += 1;
                while primary.head < buffer.text.len()
                    && !buffer.text.is_char_boundary(primary.head)
                {
                    primary.head += 1;
                }
            }
            if !shift {
                primary.anchor = primary.head;
            }
        }
        if move_up || move_down {
            // Use cached offset translation
            let (line_idx, col) = buffer.offset_to_line_col(primary.head);
            let target_line = if move_up {
                line_idx.saturating_sub(1)
            } else {
                line_idx + 1
            };
            if target_line < buffer.line_count() {
                let start = buffer.line_start(target_line);
                let end = if target_line + 1 < buffer.line_count() {
                    buffer.line_start(target_line + 1) - 1
                } else {
                    buffer.text.len()
                };
                let new_col = col.min(end.saturating_sub(start));
                let new_pos = start + new_col;
                primary.head = new_pos.min(buffer.text.len());
                if !shift {
                    primary.anchor = primary.head;
                }
            } else if move_down {
                // beyond last line
                primary.head = buffer.text.len();
                if !shift {
                    primary.anchor = primary.head;
                }
            }
        }
        if primary.head != prev_head {
            signals.caret_moved = true;
        }
    }

    // --- After movement, ensure visible (primitive scroll) ---
    if let Some(primary) = selection.primary() {
        // compute logical line/col without extra allocation
        let (line_idx, col) = buffer.offset_to_line_col(primary.head.min(buffer.text.len()));
        let char_w =
            ui.fonts(|f| f.glyph_width(&egui::TextStyle::Monospace.resolve(ui.style()), 'M'));
        let caret_x = (col as f32) * char_w;
        let caret_y = (line_idx as f32) * line_height;
        let margin = 8.0;
        // vertical
        if caret_y < state.view.scroll_y {
            state.view.scroll_y = caret_y.saturating_sub_f32(margin);
        } else if caret_y + line_height > state.view.scroll_y + state.view.viewport_h {
            state.view.scroll_y = (caret_y + line_height) - state.view.viewport_h + margin;
        }
        // horizontal
        if caret_x < state.view.scroll_x {
            state.view.scroll_x = (caret_x - margin).max(0.0);
        } else if caret_x + char_w > state.view.scroll_x + state.view.viewport_w {
            state.view.scroll_x = caret_x + char_w - state.view.viewport_w + margin;
        }
    }

    // --- Recompute size & paint ---
    let line_count = buffer.line_count().max(1);
    let desired_h = line_height * line_count as f32 + line_height * 2.0;
    let rect = egui::Rect::from_min_size(available.min, egui::vec2(available.width(), desired_h));
    let painter = ui.painter();
    painter.rect_filled(rect, 0.0, ui.visuals().extreme_bg_color);

    let gutter_w = if state.show_line_numbers {
        8.0 * (line_count.to_string().len() as f32 + 1.0)
    } else {
        0.0
    };
    let text_origin = egui::pos2(
        rect.left() + gutter_w + 6.0 - state.view.scroll_x,
        rect.top() + 4.0 - state.view.scroll_y,
    );
    let char_w = ui.fonts(|f| f.glyph_width(&egui::TextStyle::Monospace.resolve(ui.style()), 'M'));
    let lines_vec = build_lines(&buffer.text); // Could be replaced by cached indices; keep for now (Stage2 optimization)

    // Selection highlighting (per caret range, merged duplicates internally by MultiSelection.ranges())
    let sel_color = ui.visuals().selection.bg_fill;
    for (start, end) in selection.ranges() {
        if start == end {
            continue;
        }
        highlight_range(
            painter,
            &buffer.text,
            &lines_vec,
            start,
            end,
            text_origin,
            line_height,
            char_w,
            sel_color,
            state.show_line_numbers,
            gutter_w,
        );
    }

    // Paint lines (with syntax highlight)
    let mut y = text_origin.y;
    for (idx, (lstart, lend)) in lines_vec.iter().enumerate() {
        let line_str = &buffer.text[*lstart..*lend];
        if state.show_line_numbers {
            painter.text(
                egui::pos2(rect.left() + 4.0, y),
                egui::Align2::LEFT_TOP,
                (idx + 1).to_string(),
                egui::TextStyle::Monospace.resolve(ui.style()),
                ui.visuals().weak_text_color(),
            );
        }
        // Use per-line version for more granular caching (fallback to global revision if line_versions not updated)
        let key = (idx, buffer.line_version(idx));
        let job = if let Some(cached) = line_cache.get(&key) {
            cached.clone()
        } else {
            let mut lj = highlight_line(line_str, lang, dark);
            // Set wrapping width large so we don't wrap inside line (horizontal scroll later)
            lj.wrap.max_width = f32::INFINITY;
            line_cache.insert(key, lj.clone());
            lj
        };
        let galley = ui.fonts(|f| f.layout_job(job));
        painter.galley(
            egui::pos2(text_origin.x, y),
            galley,
            ui.visuals().text_color(),
        );
        y += line_height;
    }

    // Draw carets (all)
    let caret_color = egui::Color32::from_rgb(120, 180, 250);
    for caret in &selection.carets {
        let head = caret.head.min(buffer.text.len());
        let (line_idx, col) = buffer.offset_to_line_col(head);
        let x = text_origin.x + (col as f32) * char_w;
        let y = text_origin.y + (line_idx as f32) * line_height;
        let caret_rect = egui::Rect::from_min_size(egui::pos2(x, y), egui::vec2(1.5, line_height));
        painter.rect_filled(caret_rect, 0.0, caret_color);
    }

    signals
}

// --- Helpers ---

// Removed local compute_line_starts/index_to_line_col in favor of buffer cached helpers.

fn build_lines(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, ch) in text.char_indices() {
        if ch == '\n' {
            out.push((start, i));
            start = i + 1;
        }
    }
    if start <= text.len() {
        out.push((start, text.len()));
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn highlight_range(
    painter: &egui::Painter,
    _text: &str,
    lines: &[(usize, usize)],
    start: usize,
    end: usize,
    origin: egui::Pos2,
    line_height: f32,
    char_w: f32,
    color: egui::Color32,
    _show_line_numbers: bool,
    _gutter_w: f32,
) {
    if start >= end {
        return;
    }
    for (idx, (ls, le)) in lines.iter().enumerate() {
        if *le <= start {
            continue;
        }
        if *ls >= end {
            break;
        }
        let seg_start = start.max(*ls);
        let seg_end = end.min(*le);
        if seg_start >= seg_end {
            continue;
        }
        let col_start = seg_start - *ls;
        let col_end = seg_end - *ls;
        let x0 = origin.x + (col_start as f32) * char_w;
        let width = (col_end - col_start) as f32 * char_w;
        let y = origin.y + (idx as f32) * line_height;
        let rect =
            egui::Rect::from_min_size(egui::pos2(x0, y), egui::vec2(width.max(1.0), line_height));
        painter.rect_filled(rect, 0.0, color.gamma_multiply(0.8));
    }
}

// Utility for f32 saturating subtraction
trait SaturatingSubF32 {
    fn saturating_sub_f32(self, other: f32) -> f32;
}
impl SaturatingSubF32 for f32 {
    fn saturating_sub_f32(self, other: f32) -> f32 {
        if self > other { self - other } else { 0.0 }
    }
}
