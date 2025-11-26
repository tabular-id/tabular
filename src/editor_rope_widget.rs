//! Custom text editor widget powered by Rope for egui
//! 
//! This widget provides a high-performance text editor using lapce-xi-rope
//! while maintaining compatibility with egui's UI framework.

use eframe::egui;
use lapce_xi_rope::{Rope, Interval, Delta, DeltaBuilder};

/// Rope-based text editor widget for egui
pub struct RopeEditor {
    id: egui::Id,
    rope: Rope,
    /// Cursor position (byte offset)
    cursor: usize,
    /// Selection anchor (for selection ranges)
    selection_anchor: Option<usize>,
    /// Scroll offset (in lines)
    scroll_offset: usize,
    /// Desired width
    desired_width: f32,
    /// Desired rows to show
    desired_rows: usize,
    /// Font style
    font: egui::TextStyle,
    /// Whether the widget is focused
    is_focused: bool,
}

impl RopeEditor {
    pub fn new(id: impl Into<egui::Id>, initial_text: &str) -> Self {
        Self {
            id: id.into(),
            rope: Rope::from(initial_text),
            cursor: 0,
            selection_anchor: None,
            scroll_offset: 0,
            desired_width: f32::INFINITY,
            desired_rows: 25,
            font: egui::TextStyle::Monospace,
            is_focused: false,
        }
    }

    /// Set desired width
    pub fn desired_width(mut self, width: f32) -> Self {
        self.desired_width = width;
        self
    }

    /// Set desired rows
    pub fn desired_rows(mut self, rows: usize) -> Self {
        self.desired_rows = rows;
        self
    }

    /// Get text as String
    pub fn text(&self) -> String {
        String::from(&self.rope)
    }

    /// Set text
    pub fn set_text(&mut self, text: String) {
        self.rope = Rope::from(&text);
        self.cursor = 0;
        self.selection_anchor = None;
    }

    /// Insert text at cursor
    pub fn insert_text(&mut self, text: &str) {
        let cursor = self.cursor.min(self.rope.len());
        self.rope.edit(Interval::new(cursor, cursor), text);
        self.cursor = cursor + text.len();
        self.selection_anchor = None;
    }

    /// Delete selection or character at cursor
    pub fn delete(&mut self) {
        if let Some(anchor) = self.selection_anchor {
            let start = anchor.min(self.cursor);
            let end = anchor.max(self.cursor);
            self.rope.edit(Interval::new(start, end), "");
            self.cursor = start;
            self.selection_anchor = None;
        } else if self.cursor > 0 {
            let cursor = self.cursor.min(self.rope.len());
            if cursor > 0 {
                self.rope.edit(Interval::new(cursor - 1, cursor), "");
                self.cursor = cursor - 1;
            }
        }
    }

    /// Get visible line range
    fn visible_line_range(&self, available_height: f32, line_height: f32) -> (usize, usize) {
        let visible_lines = (available_height / line_height).ceil() as usize;
        let start_line = self.scroll_offset;
        let end_line = (start_line + visible_lines).min(self.rope.measure::<lapce_xi_rope::LinesMetric>() + 1);
        (start_line, end_line)
    }

    /// Render the editor
    pub fn show(mut self, ui: &mut egui::Ui) -> egui::Response {
        let desired_size = egui::vec2(
            self.desired_width,
            ui.text_style_height(&self.font) * self.desired_rows as f32,
        );

        let (rect, mut response) = ui.allocate_exact_size(desired_size, egui::Sense::click_and_drag());

        // Handle focus
        if response.clicked() {
            response.request_focus();
            self.is_focused = true;
        }

        self.is_focused = response.has_focus();

        // Background
        let bg_color = if self.is_focused {
            ui.visuals().extreme_bg_color
        } else {
            ui.visuals().code_bg_color
        };
        ui.painter().rect_filled(rect, 4.0, bg_color);

        // Get text and render line by line
        let text = String::from(&self.rope);
        let line_height = ui.text_style_height(&self.font);
        let (start_line, end_line) = self.visible_line_range(rect.height(), line_height);

        let mut y_offset = rect.top();
        for (line_idx, line) in text.lines().enumerate().skip(start_line).take(end_line - start_line) {
            let pos = egui::pos2(rect.left() + 4.0, y_offset);
            
            ui.painter().text(
                pos,
                egui::Align2::LEFT_TOP,
                line,
                self.font.clone(),
                ui.visuals().text_color(),
            );
            
            y_offset += line_height;
        }

        // Render cursor if focused
        if self.is_focused {
            let cursor_pos = self.cursor.min(text.len());
            let (line_idx, col_idx) = self.offset_to_line_col(&text, cursor_pos);
            
            if line_idx >= start_line && line_idx < end_line {
                let line_y = rect.top() + (line_idx - start_line) as f32 * line_height;
                let char_width = ui.fonts(|f| {
                    f.glyph_width(&self.font.resolve(ui.style()), 'M')
                });
                let cursor_x = rect.left() + 4.0 + col_idx as f32 * char_width;
                
                let cursor_rect = egui::Rect::from_min_size(
                    egui::pos2(cursor_x, line_y),
                    egui::vec2(2.0, line_height),
                );
                
                ui.painter().rect_filled(
                    cursor_rect,
                    0.0,
                    ui.visuals().text_cursor.stroke.color,
                );
            }
        }

        // Handle keyboard input
        if self.is_focused {
            ui.input(|i| {
                for event in &i.events {
                    match event {
                        egui::Event::Text(text) => {
                            self.insert_text(text);
                            response.mark_changed();
                        }
                        egui::Event::Key { key: egui::Key::Backspace, pressed: true, .. } => {
                            self.delete();
                            response.mark_changed();
                        }
                        egui::Event::Key { key: egui::Key::Enter, pressed: true, .. } => {
                            self.insert_text("\n");
                            response.mark_changed();
                        }
                        _ => {}
                    }
                }
            });
        }

        response
    }

    /// Convert byte offset to (line, column)
    fn offset_to_line_col(&self, text: &str, offset: usize) -> (usize, usize) {
        let offset = offset.min(text.len());
        let mut line = 0;
        let mut col = 0;
        let mut current_offset = 0;
        
        for (idx, ch) in text.char_indices() {
            if idx >= offset {
                break;
            }
            if ch == '\n' {
                line += 1;
                col = 0;
            } else {
                col += 1;
            }
            current_offset = idx + ch.len_utf8();
        }
        
        (line, col)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rope_editor_basic() {
        let editor = RopeEditor::new("test_editor", "Hello World");
        assert_eq!(editor.text(), "Hello World");
    }

    #[test]
    fn test_rope_editor_insert() {
        let mut editor = RopeEditor::new("test_editor", "Hello");
        editor.cursor = 5;
        editor.insert_text(" World");
        assert_eq!(editor.text(), "Hello World");
    }

    #[test]
    fn test_rope_editor_delete() {
        let mut editor = RopeEditor::new("test_editor", "Hello World");
        editor.cursor = 5;
        editor.delete();
        assert_eq!(editor.text(), "Hell World");
    }
}
