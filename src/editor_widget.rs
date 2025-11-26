//! Custom EGUI editor widget powered by lapce-core.
//! Renders text directly from lapce-core Buffer/Rope without egui::TextEdit.

use eframe::egui;
use egui::{Color32, FontId, Pos2, Rect, Response, Sense, Vec2};
use lapce_xi_rope::Rope;
use std::sync::Arc;

use crate::editor_buffer::EditorBuffer;

/// Custom layouter function type for syntax highlighting
/// Returns Arc<Galley> to match egui::Ui::fonts() output
pub type LayouterFn<'a> = Box<dyn FnMut(&egui::Ui, &str, f32) -> Arc<egui::Galley> + 'a>;

/// Custom editor widget that renders lapce-core buffer directly
pub struct LapceEditorWidget<'a> {
    buffer: &'a mut EditorBuffer,
    cursor_pos: &'a mut usize,
    selection_start: &'a mut usize,
    selection_end: &'a mut usize,
    desired_height_rows: usize,
    id: egui::Id,
    layouter: Option<LayouterFn<'a>>,
    show_line_numbers: bool,
    line_number_width: f32,
}

impl<'a> LapceEditorWidget<'a> {
    pub fn new(
        buffer: &'a mut EditorBuffer,
        cursor_pos: &'a mut usize,
        selection_start: &'a mut usize,
        selection_end: &'a mut usize,
    ) -> Self {
        Self {
            buffer,
            cursor_pos,
            selection_start,
            selection_end,
            desired_height_rows: 25,
            id: egui::Id::new("lapce_editor"),
            layouter: None,
            show_line_numbers: false,
            line_number_width: 0.0,
        }
    }

    pub fn id(mut self, id: impl Into<egui::Id>) -> Self {
        self.id = id.into();
        self
    }

    pub fn desired_rows(mut self, rows: usize) -> Self {
        self.desired_height_rows = rows;
        self
    }

    pub fn layouter(mut self, layouter: LayouterFn<'a>) -> Self {
        self.layouter = Some(layouter);
        self
    }

    pub fn line_numbers(mut self, show: bool, width: f32) -> Self {
        self.show_line_numbers = show;
        self.line_number_width = width;
        self
    }

    pub fn show(self, ui: &mut egui::Ui) -> Response {
        let Self {
            buffer,
            cursor_pos,
            selection_start,
            selection_end,
            desired_height_rows,
            id,
            layouter,
            show_line_numbers,
            line_number_width,
        } = self;

        // Calculate desired size
        let font_id = FontId::monospace(13.0);
        let row_height = ui.fonts(|f| f.row_height(&font_id));
        
        // Calculate actual height based on content
        let num_lines = buffer.line_count();
        let content_height = row_height * num_lines as f32 + row_height * 3.0; // Extra padding
        let min_height = row_height * desired_height_rows as f32;
        let desired_height = content_height.max(min_height);
        
        let full_width = ui.available_width();
        let (rect, mut response) = ui.allocate_exact_size(
            Vec2::new(full_width, desired_height),
            Sense::click_and_drag(),
        );
        
        // Override the response's ID with our custom ID for consistent focus tracking
        response.id = id;
        
        // CRITICAL: Request keyboard input capture
        response = response.on_hover_cursor(egui::CursorIcon::Text);
        
        // Calculate editor rect (excluding line numbers gutter)
        let gutter_width = if show_line_numbers { line_number_width } else { 0.0 };
        let editor_rect = if gutter_width > 0.0 {
            Rect::from_min_max(
                Pos2::new(rect.min.x + gutter_width, rect.min.y),
                rect.max,
            )
        } else {
            rect
        };

        // Handle focus - PERSISTENT focus management (per-widget id)
        // Default to true so brand-new editors can type immediately without an extra click.
        let focus_key = id.with("should_have_focus");
        let should_have_focus: bool = ui
            .data(|d| d.get_temp(focus_key))
            .unwrap_or(true);
        
        if response.clicked() || response.dragged() {
            eprintln!("[LAPCE_WIDGET] Click/drag detected, requesting focus for id={:?}", id);
            ui.memory_mut(|m| m.request_focus(id));
            // Mark that we should have focus
            ui.data_mut(|d| d.insert_temp(focus_key, true));
        }
        
        // AGGRESSIVE: If we should have focus, re-request it every frame!
        if should_have_focus {
            ui.memory_mut(|m| m.request_focus(id));
        }
        
        let has_focus = ui.memory(|m| m.has_focus(id));
        
        // Update persistent focus state
        ui.data_mut(|d| {
            d.insert_temp(focus_key, has_focus);
        });
        
        // If clicked outside, clear the focus flag
        if ui.input(|i| i.pointer.primary_clicked()) && !response.hovered() {
            ui.data_mut(|d| d.insert_temp(focus_key, false));
        }
        
        // CRITICAL: Tell EGUI we want keyboard input
        if has_focus {
            ui.ctx().request_repaint(); // Keep repainting for cursor blink
        }

        // Paint background
        let bg_color = if has_focus {
            ui.style().visuals.extreme_bg_color
        } else {
            ui.style().visuals.faint_bg_color
        };
        ui.painter().rect_filled(rect, 0.0, bg_color);

        // Paint line numbers gutter if enabled
        if show_line_numbers {
            let gutter_rect = Rect::from_min_max(
                rect.min,
                Pos2::new(rect.min.x + gutter_width, rect.max.y),
            );
            ui.painter().rect_filled(gutter_rect, 0.0, ui.style().visuals.faint_bg_color);
            
            // Paint line numbers
            let gutter_text_color = ui.style().visuals.weak_text_color();
            for line_idx in 0..num_lines {
                let y = editor_rect.min.y + (line_idx as f32 * row_height);
                if y > rect.max.y {
                    break;
                }
                let line_num = (line_idx + 1).to_string();
                ui.painter().text(
                    Pos2::new(rect.min.x + 4.0, y),
                    egui::Align2::LEFT_TOP,
                    line_num,
                    font_id.clone(),
                    gutter_text_color,
                );
            }
        }

        // Clamp cursor and selection
        let text_len = buffer.len();
        *cursor_pos = (*cursor_pos).min(text_len);
        *selection_start = (*selection_start).min(text_len);
        *selection_end = (*selection_end).min(text_len);

        // Handle mouse interaction (adjust for gutter)
        if response.clicked() {
            if let Some(pos) = response.interact_pointer_pos() {
                let adjusted_pos = Pos2::new(pos.x - gutter_width, pos.y);
                let byte_offset = pos_to_offset(buffer.rope(), adjusted_pos, editor_rect, row_height, &font_id, ui);
                *cursor_pos = byte_offset;
                *selection_start = byte_offset;
                *selection_end = byte_offset;
            }
        }

        if response.dragged() {
            if let Some(pos) = response.interact_pointer_pos() {
                let adjusted_pos = Pos2::new(pos.x - gutter_width, pos.y);
                let byte_offset = pos_to_offset(buffer.rope(), adjusted_pos, editor_rect, row_height, &font_id, ui);
                *selection_end = byte_offset;
                *cursor_pos = byte_offset;
            }
        }

        // Handle keyboard input (modifies buffer) via unified handler
        // This enables Enter, Delete, arrows, copy/paste, and selection edits.
        eprintln!("[LAPCE_WIDGET] id={:?}, has_focus={}, cursor_pos={}", id, has_focus, cursor_pos);
        // Store focus state for debugging
        ui.data_mut(|d| {
            let prev_focus: Option<bool> = d.get_temp(egui::Id::new("lapce_had_focus"));
            if prev_focus.unwrap_or(false) && !has_focus {
                eprintln!("[LAPCE_WIDGET] !!! FOCUS LOST !!! Previous frame had focus, now lost");
            }
            d.insert_temp(egui::Id::new("lapce_had_focus"), has_focus);
        });
        if has_focus {
            handle_input(
                ui,
                cursor_pos,
                selection_start,
                selection_end,
                buffer,
                &mut response,
            );
        }

        // Get rope for rendering (after all mutations)
        let rope = buffer.rope();
        
        // Render text with selections and optional syntax highlighting
        if let Some(mut layouter_fn) = layouter {
            render_text_with_layouter(
                ui,
                editor_rect,
                rope,
                *cursor_pos,
                *selection_start,
                *selection_end,
                has_focus,
                row_height,
                &font_id,
                &mut layouter_fn,
            );
        } else {
            render_text(
                ui,
                editor_rect,
                rope,
                *cursor_pos,
                *selection_start,
                *selection_end,
                has_focus,
                row_height,
                &font_id,
            );
        }

        response
    }
}

// Free functions to avoid borrow checker issues
fn pos_to_offset(
    rope: &Rope,
    screen_pos: Pos2,
    rect: Rect,
    row_height: f32,
    _font_id: &FontId,
    _ui: &egui::Ui,
) -> usize {
    let relative_y = screen_pos.y - rect.min.y;
    let line = (relative_y / row_height).floor() as usize;
    let num_lines = rope.measure::<lapce_xi_rope::LinesMetric>();
    let line = line.min(num_lines.saturating_sub(1));

    let line_start = rope.offset_of_line(line);
    let line_end = if line + 1 < num_lines {
        rope.offset_of_line(line + 1)
    } else {
        rope.len()
    };

    let line_text = rope.slice_to_cow(line_start..line_end);
    let relative_x = screen_pos.x - rect.min.x;

    // Calculate offset based on x position (simplified - just use character count)
    // For proper handling, would need to measure glyph widths, but that causes deadlock in nested fonts() calls
    let chars_per_pixel = if relative_x > 0.0 {
        (relative_x / 8.0) as usize // Approximate monospace width
    } else {
        0
    };
    
    let offset_in_line = chars_per_pixel.min(line_text.len());
    (line_start + offset_in_line).min(line_end)
}

fn handle_input(
    ui: &egui::Ui,
    cursor_pos: &mut usize,
    selection_start: &mut usize,
    selection_end: &mut usize,
    buffer: &mut EditorBuffer,
    response: &mut Response,
) {
    // CRITICAL: Need to consume events, not just read them
    let events = ui.input(|i| i.events.clone());
    
    for event in &events {
        match event {
            // Select All: Cmd/Ctrl+A
            egui::Event::Key { key: egui::Key::A, pressed: true, modifiers, .. }
                if modifiers.command || modifiers.ctrl || modifiers.mac_cmd =>
            {
                *selection_start = 0;
                *selection_end = buffer.len();
                *cursor_pos = *selection_end;
                // Selection change only; don't mark content changed. Just repaint.
                ui.ctx().request_repaint();
                continue;
            }
            egui::Event::Text(text) => {
                // Delete selection first if exists
                if *selection_start != *selection_end {
                    let start = (*selection_start).min(*selection_end);
                    let end = (*selection_start).max(*selection_end);
                    buffer.apply_single_replace(start..end, text);
                    *cursor_pos = start + text.len();
                    *selection_start = *cursor_pos;
                    *selection_end = *cursor_pos;
                } else {
                    buffer.apply_single_replace(*cursor_pos..*cursor_pos, text);
                    *cursor_pos += text.len();
                    *selection_start = *cursor_pos;
                    *selection_end = *cursor_pos;
                }
                response.mark_changed();
            }
            egui::Event::Key { key, pressed: true, modifiers, .. } => {
                match key {
                    egui::Key::Backspace => {
                        if *selection_start != *selection_end {
                            let start = (*selection_start).min(*selection_end);
                            let end = (*selection_start).max(*selection_end);
                            buffer.apply_single_replace(start..end, "");
                            *cursor_pos = start;
                            *selection_start = start;
                            *selection_end = start;
                        } else if *cursor_pos > 0 {
                            let prev = *cursor_pos - 1;
                            buffer.apply_single_replace(prev..*cursor_pos, "");
                            *cursor_pos = prev;
                            *selection_start = prev;
                            *selection_end = prev;
                        }
                        response.mark_changed();
                    }
                    egui::Key::Delete => {
                        if *selection_start != *selection_end {
                            let start = (*selection_start).min(*selection_end);
                            let end = (*selection_start).max(*selection_end);
                            buffer.apply_single_replace(start..end, "");
                            *cursor_pos = start;
                            *selection_start = start;
                            *selection_end = start;
                        } else if *cursor_pos < buffer.len() {
                            buffer.apply_single_replace(*cursor_pos..(*cursor_pos + 1), "");
                        }
                        response.mark_changed();
                    }
                    egui::Key::Enter => {
                        if *selection_start != *selection_end {
                            let start = (*selection_start).min(*selection_end);
                            let end = (*selection_start).max(*selection_end);
                            buffer.apply_single_replace(start..end, "\n");
                            *cursor_pos = start + 1;
                        } else {
                            buffer.apply_single_replace(*cursor_pos..*cursor_pos, "\n");
                            *cursor_pos += 1;
                        }
                        *selection_start = *cursor_pos;
                        *selection_end = *cursor_pos;
                        response.mark_changed();
                    }
                    egui::Key::ArrowLeft => {
                            if modifiers.shift {
                                if *cursor_pos > 0 {
                                    *cursor_pos -= 1;
                                    *selection_end = *cursor_pos;
                                }
                            } else {
                                if *cursor_pos > 0 {
                                    *cursor_pos -= 1;
                                }
                                *selection_start = *cursor_pos;
                                *selection_end = *cursor_pos;
                            }
                    }
                    egui::Key::ArrowRight => {
                            if modifiers.shift {
                                if *cursor_pos < buffer.len() {
                                    *cursor_pos += 1;
                                    *selection_end = *cursor_pos;
                                }
                            } else {
                                if *cursor_pos < buffer.len() {
                                    *cursor_pos += 1;
                                }
                                *selection_start = *cursor_pos;
                                *selection_end = *cursor_pos;
                            }
                    }
                    egui::Key::ArrowUp => {
                            let (line, col) = buffer.offset_to_line_col(*cursor_pos);
                            if line > 0 {
                                let new_line_start = buffer.line_start(line - 1);
                                let new_pos = (new_line_start + col).min(buffer.line_start(line) - 1);
                                *cursor_pos = new_pos;
                                if !modifiers.shift {
                                    *selection_start = *cursor_pos;
                                    *selection_end = *cursor_pos;
                                } else {
                                    *selection_end = *cursor_pos;
                                }
                            }
                    }
                    egui::Key::ArrowDown => {
                            let (line, col) = buffer.offset_to_line_col(*cursor_pos);
                            if line + 1 < buffer.line_count() {
                                let new_line_start = buffer.line_start(line + 1);
                                let new_line_len = if line + 2 < buffer.line_count() {
                                    buffer.line_start(line + 2) - new_line_start - 1
                                } else {
                                    buffer.len() - new_line_start
                                };
                                let new_pos = (new_line_start + col).min(new_line_start + new_line_len);
                                *cursor_pos = new_pos;
                                if !modifiers.shift {
                                    *selection_start = *cursor_pos;
                                    *selection_end = *cursor_pos;
                                } else {
                                    *selection_end = *cursor_pos;
                                }
                            }
                    }
                    _ => {}
                }
            }
            egui::Event::Paste(text) => {
                    if *selection_start != *selection_end {
                        let start = (*selection_start).min(*selection_end);
                        let end = (*selection_start).max(*selection_end);
                        buffer.apply_single_replace(start..end, text);
                        *cursor_pos = start + text.len();
                    } else {
                        buffer.apply_single_replace(*cursor_pos..*cursor_pos, text);
                        *cursor_pos += text.len();
                    }
                    *selection_start = *cursor_pos;
                    *selection_end = *cursor_pos;
                    response.mark_changed();
            }
            egui::Event::Copy | egui::Event::Cut => {
                    if *selection_start != *selection_end {
                        let start = (*selection_start).min(*selection_end);
                        let end = (*selection_start).max(*selection_end);
                        let selected_text = buffer.slice(start..end);
                        ui.ctx().copy_text(selected_text.to_string());
                        
                        if matches!(event, egui::Event::Cut) {
                            buffer.apply_single_replace(start..end, "");
                            *cursor_pos = start;
                            *selection_start = start;
                            *selection_end = start;
                            response.mark_changed();
                        }
                    }
            }
            _ => {}
        }
    }
}

fn render_text(
    ui: &egui::Ui,
    rect: Rect,
    rope: &Rope,
    cursor_pos: usize,
    selection_start: usize,
    selection_end: usize,
    has_focus: bool,
    row_height: f32,
    font_id: &FontId,
) {
    let painter = ui.painter();
    let mut y = rect.min.y;
    let num_lines = rope.measure::<lapce_xi_rope::LinesMetric>();

    // Render selection background first
    if selection_start != selection_end {
        let start = selection_start.min(selection_end);
        let end = selection_start.max(selection_end);
        let sel_color = ui.style().visuals.selection.bg_fill;
        paint_selection_simple(painter, rect, rope, start, end, row_height, sel_color);
    }

    // Render text line by line (outside fonts lock)
    let text_color = ui.style().visuals.text_color();
    
    for line_idx in 0..num_lines {
        let line_start = rope.offset_of_line(line_idx);
        let line_end = if line_idx + 1 < num_lines {
            rope.offset_of_line(line_idx + 1)
        } else {
            rope.len()
        };

        let line_text = rope.slice_to_cow(line_start..line_end);
        let line_text = line_text.trim_end_matches('\n');

        // Paint line text
        painter.text(
            Pos2::new(rect.min.x, y),
            egui::Align2::LEFT_TOP,
            line_text,
            font_id.clone(),
            text_color,
        );

        y += row_height;
        if y > rect.max.y {
            break;
        }
    }

    // Render cursor
    if has_focus {
        paint_cursor(painter, rect, rope, cursor_pos, row_height);
    }
}

// Render text with custom layouter for syntax highlighting
#[allow(clippy::too_many_arguments)]
fn render_text_with_layouter(
    ui: &egui::Ui,
    rect: Rect,
    rope: &Rope,
    cursor_pos: usize,
    selection_start: usize,
    selection_end: usize,
    has_focus: bool,
    row_height: f32,
    _font_id: &FontId,
    layouter: &mut LayouterFn,
) {
    let painter = ui.painter();
    let mut y = rect.min.y;
    let num_lines = rope.measure::<lapce_xi_rope::LinesMetric>();

    // Render selection background first
    if selection_start != selection_end {
        let start = selection_start.min(selection_end);
        let end = selection_start.max(selection_end);
        let sel_color = ui.style().visuals.selection.bg_fill;
        paint_selection_simple(painter, rect, rope, start, end, row_height, sel_color);
    }

    // Render text line by line with syntax highlighting
    for line_idx in 0..num_lines {
        let line_start = rope.offset_of_line(line_idx);
        let line_end = if line_idx + 1 < num_lines {
            rope.offset_of_line(line_idx + 1)
        } else {
            rope.len()
        };

        let line_text = rope.slice_to_cow(line_start..line_end);
        let line_str = line_text.trim_end_matches('\n');

        // Use layouter for syntax highlighting (returns Arc<Galley>)
        let galley = layouter(ui, line_str, f32::INFINITY);
        // Don't apply any tint - the layouter already provides proper themed colors
        // Using Color32::WHITE would override all syntax colors to white (invisible on light bg)
        painter.galley(Pos2::new(rect.min.x, y), galley, ui.style().visuals.text_color());

        y += row_height;
        if y > rect.max.y {
            break;
        }
    }

    // Render cursor
    if has_focus {
        paint_cursor(painter, rect, rope, cursor_pos, row_height);
    }
}

fn paint_cursor(
    painter: &egui::Painter,
    rect: Rect,
    rope: &Rope,
    cursor_pos: usize,
    row_height: f32,
) {
    let cursor_line = rope.line_of_offset(cursor_pos);
    let cursor_y = rect.min.y + (cursor_line as f32 * row_height);
    
    let line_start = rope.offset_of_line(cursor_line);
    let col_bytes = cursor_pos - line_start;
    
    // Approximate cursor X (8px per char for monospace)
    let cursor_x = rect.min.x + (col_bytes as f32 * 8.0);

    let cursor_rect = Rect::from_min_size(
        Pos2::new(cursor_x, cursor_y),
        Vec2::new(2.0, row_height),
    );

    // Use bright blue for better visibility
    painter.rect_filled(cursor_rect, 0.0, Color32::from_rgb(0, 150, 255));
}

// Simplified selection painting without nested fonts() calls
fn paint_selection_simple(
    painter: &egui::Painter,
    rect: Rect,
    rope: &Rope,
    start: usize,
    end: usize,
    row_height: f32,
    fill: Color32,
) {
    let start_line = rope.line_of_offset(start);
    let end_line = rope.line_of_offset(end);
    let num_lines = rope.measure::<lapce_xi_rope::LinesMetric>();

    for line_idx in start_line..=end_line {
        if line_idx >= num_lines {
            break;
        }

        let line_start = rope.offset_of_line(line_idx);
        let line_end = if line_idx + 1 < num_lines {
            rope.offset_of_line(line_idx + 1)
        } else {
            rope.len()
        };

        let sel_start_in_line = if line_idx == start_line {
            start - line_start
        } else {
            0
        };

        let sel_end_in_line = if line_idx == end_line {
            (end - line_start).min(line_end - line_start)
        } else {
            line_end - line_start
        };

        if sel_start_in_line >= sel_end_in_line {
            continue;
        }

        // Approximate selection width (8px per char)
        let x_start = rect.min.x + (sel_start_in_line as f32 * 8.0);
        let sel_width = ((sel_end_in_line - sel_start_in_line) as f32 * 8.0).max(2.0);
        let y_top = rect.min.y + (line_idx as f32 * row_height);

        let sel_rect = Rect::from_min_size(
            Pos2::new(x_start, y_top),
            Vec2::new(sel_width, row_height),
        );

        painter.rect_filled(sel_rect, 0.0, fill);
    }
}
