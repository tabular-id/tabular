//! Simple demo of LapceEditorWidget - custom EGUI editor powered by lapce-core
//! 
//! Run with: cargo run --example lapce_editor_demo

use eframe::egui;
use tabular::editor_buffer::EditorBuffer;
use tabular::editor_widget::LapceEditorWidget;

fn main() -> Result<(), eframe::Error> {
    env_logger::init();
    
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([800.0, 600.0]),
        ..Default::default()
    };
    
    eframe::run_native(
        "Lapce Editor Demo",
        options,
        Box::new(|_cc| Ok(Box::new(DemoApp::default()))),
    )
}

struct DemoApp {
    buffer: EditorBuffer,
    cursor_pos: usize,
    selection_start: usize,
    selection_end: usize,
}

impl Default for DemoApp {
    fn default() -> Self {
        let initial_text = r#"-- SQL Editor powered by lapce-core!
-- Try typing, selecting, copy/paste, arrow keys, etc.

SELECT *
FROM users
WHERE name LIKE '%John%'
  AND age > 25
ORDER BY created_at DESC
LIMIT 10;

-- Features:
-- ✅ Rope-based editing (efficient for large files)
-- ✅ Unicode support via lapce-xi-rope
-- ✅ Direct rendering from Rope (no String cache overhead)
-- ✅ Custom selection & cursor rendering
-- ✅ Full keyboard navigation
-- ✅ Copy/paste support
-- 🚧 TODO: Syntax highlighting
-- 🚧 TODO: Multi-cursor
-- 🚧 TODO: Undo/redo
"#;
        
        Self {
            buffer: EditorBuffer::new(initial_text),
            cursor_pos: 0,
            selection_start: 0,
            selection_end: 0,
        }
    }
}

impl eframe::App for DemoApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("🚀 Lapce-Core Powered Editor Demo");
            ui.separator();
            
            ui.horizontal(|ui| {
                ui.label(format!("Lines: {}", self.buffer.line_count()));
                ui.separator();
                ui.label(format!("Bytes: {}", self.buffer.len()));
                ui.separator();
                ui.label(format!("Cursor: {}", self.cursor_pos));
                ui.separator();
                if self.selection_start != self.selection_end {
                    let len = self.selection_end.max(self.selection_start) 
                            - self.selection_start.min(self.selection_end);
                    ui.label(format!("Selection: {} bytes", len));
                }
            });
            
            ui.separator();
            
            // Main editor widget
            let response = LapceEditorWidget::new(
                &mut self.buffer,
                &mut self.cursor_pos,
                &mut self.selection_start,
                &mut self.selection_end,
            )
            .id(egui::Id::new("demo_editor"))
            .desired_rows(25)
            .show(ui);
            
            if response.changed() {
                // Sync legacy text field for compatibility
                self.buffer.text = self.buffer.text_snapshot();
            }
            
            ui.separator();
            ui.label("💡 Tip: This editor uses lapce-core Buffer instead of egui::TextEdit");
        });
    }
}
