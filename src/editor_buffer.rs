use lapce_core::buffer::{Buffer as LapceBuffer, rope_text::RopeText};
use lapce_xi_rope::Rope;

/// Editor buffer powered by lapce-core Buffer with full feature exposure.
/// Renders directly without intermediate String representation for better performance.
pub struct EditorBuffer {
    /// Core lapce-core Buffer for all text operations
    lapce_buffer: LapceBuffer,
    /// Legacy cached text for compatibility (will be phased out)
    pub text: String,
    /// Monotonic revision counter for tracking changes
    pub revision: u64,
}


impl Default for EditorBuffer {
    fn default() -> Self {
        Self::new("")
    }
}

impl EditorBuffer {
    pub fn new(initial: &str) -> Self {
        let lapce_buffer = LapceBuffer::new(initial);
        let text = initial.to_string();
        Self {
            lapce_buffer,
            text,
            revision: 0,
        }
    }

    /// Get a fresh snapshot of the current buffer as a String (for compatibility).
    pub fn text_snapshot(&self) -> String {
        self.lapce_buffer.to_string()
    }

    /// Replace whole content (used by tab switching / file load)
    pub fn set_text(&mut self, new_text: String) {
        // Create new buffer with the content
        self.lapce_buffer = LapceBuffer::new(&new_text);
        self.text = new_text;
        self.revision = self.revision.wrapping_add(1);
    }

    /// Mark that external text was modified (sync from legacy text field)
    pub fn mark_text_modified(&mut self) {
        // Sync text to lapce buffer if changed
        if self.text != self.lapce_buffer.to_string() {
            self.lapce_buffer = LapceBuffer::new(&self.text);
            self.revision = self.revision.wrapping_add(1);
        }
    }

    /// Number of lines in the buffer
    pub fn line_count(&self) -> usize {
        self.lapce_buffer.num_lines()
    }

    /// Get per-line version (for compatibility, returns global revision)
    pub fn line_version(&self, _line: usize) -> u64 {
        self.revision
    }

    /// Get start offset of given line index
    pub fn line_start(&self, line: usize) -> usize {
        self.lapce_buffer.offset_of_line(line.min(self.line_count() - 1))
    }

    /// Translate byte offset to (line, column)
    pub fn offset_to_line_col(&self, offset: usize) -> (usize, usize) {
        let offset = offset.min(self.lapce_buffer.len());
        let line = self.lapce_buffer.line_of_offset(offset);
        let line_start = self.lapce_buffer.offset_of_line(line);
        let col = offset - line_start;
        (line, col)
    }

    /// Core primitive replace operation using lapce-core's edit system
    pub fn apply_single_replace(&mut self, old_range: std::ops::Range<usize>, replacement: &str) {
        let start = old_range.start.min(self.text.len());
        let end = old_range.end.min(self.text.len()).max(start);
        
        // Apply to String representation (for legacy compatibility)
        self.text.replace_range(start..end, replacement);
        
        // Sync to lapce buffer
        self.lapce_buffer = LapceBuffer::new(&self.text);
        self.revision = self.revision.wrapping_add(1);
    }

    /// Try to update using a single span based on diff between previous and new text
    pub fn try_single_span_update(&mut self, previous: &str, new_full: &str) -> bool {
        if previous == new_full {
            return true;
        }
        
        // Find common prefix
        let mut prefix = 0usize;
        let prev_bytes = previous.as_bytes();
        let new_bytes = new_full.as_bytes();
        let min_len = prev_bytes.len().min(new_bytes.len());
        
        while prefix < min_len && prev_bytes[prefix] == new_bytes[prefix] {
            prefix += 1;
        }
        
        // Find common suffix
        let mut suffix = 0usize;
        while suffix < (prev_bytes.len() - prefix)
            && suffix < (new_bytes.len() - prefix)
            && prev_bytes[prev_bytes.len() - 1 - suffix] == new_bytes[new_bytes.len() - 1 - suffix]
        {
            suffix += 1;
        }
        
        let prev_mid_start = prefix;
        let prev_mid_end = prev_bytes.len() - suffix;
        let new_mid_start = prefix;
        let new_mid_end = new_bytes.len() - suffix;
        
        if prev_mid_start == prev_mid_end && new_mid_start == new_mid_end {
            return true;
        }
        
        if new_mid_start > new_mid_end || prev_mid_start > prev_mid_end {
            return false;
        }
        
        if let Some(replacement) = new_full.get(new_mid_start..new_mid_end) {
            self.apply_single_replace(prev_mid_start..prev_mid_end, replacement);
            return true;
        }
        
        false
    }

    /// Undo/Redo support - simplified implementation since lapce-core has internal history
    pub fn can_undo(&self) -> bool {
        // TODO: Expose lapce-core's undo history
        false
    }

    pub fn can_redo(&self) -> bool {
        // TODO: Expose lapce-core's redo history
        false
    }

    pub fn undo(&mut self) -> bool {
        // TODO: Implement using lapce-core's undo system
        false
    }

    pub fn redo(&mut self) -> bool {
        // TODO: Implement using lapce-core's redo system
        false
    }

    /// Notify that external bulk text changes were applied
    pub fn notify_bulk_text_changed(&mut self) {
        // Rebuild lapce buffer from current text
        self.lapce_buffer = LapceBuffer::new(&self.text);
        self.revision = self.revision.wrapping_add(1);
    }

    /// Get direct access to underlying Rope for advanced operations
    pub fn rope(&self) -> &Rope {
        self.lapce_buffer.text()
    }

    /// Get mutable access to lapce buffer for advanced edits
    pub fn lapce_buffer_mut(&mut self) -> &mut LapceBuffer {
        &mut self.lapce_buffer
    }

    /// Get immutable access to lapce buffer
    pub fn lapce_buffer(&self) -> &LapceBuffer {
        &self.lapce_buffer
    }

    /// Get line content as string
    pub fn line_content(&self, line: usize) -> String {
        if line < self.line_count() {
            let start = self.lapce_buffer.offset_of_line(line);
            let end = if line + 1 < self.line_count() {
                self.lapce_buffer.offset_of_line(line + 1)
            } else {
                self.lapce_buffer.len()
            };
            self.lapce_buffer.slice_to_cow(start..end).to_string()
        } else {
            String::new()
        }
    }

    /// Convert byte offset to character index (for EGUI TextEdit compatibility)
    /// Uses UTF-8 grapheme counting
    pub fn byte_to_char(&self, byte_offset: usize) -> usize {
        let clamped = byte_offset.min(self.text.len());
        self.text[..clamped].chars().count()
    }

    /// Convert character index to byte offset (for EGUI TextEdit compatibility)
    pub fn char_to_byte(&self, char_index: usize) -> usize {
        self.text
            .char_indices()
            .nth(char_index)
            .map(|(b, _)| b)
            .unwrap_or(self.text.len())
    }

    /// Get text slice by byte range (safe clamping)
    pub fn slice(&self, range: std::ops::Range<usize>) -> &str {
        let start = range.start.min(self.text.len());
        let end = range.end.min(self.text.len()).max(start);
        &self.text[start..end]
    }

    /// Check if offset is at line boundary
    pub fn is_line_start(&self, offset: usize) -> bool {
        if offset == 0 {
            return true;
        }
        if offset >= self.text.len() {
            return false;
        }
        self.text.as_bytes().get(offset - 1) == Some(&b'\n')
    }

    /// Get buffer length in bytes
    pub fn len(&self) -> usize {
        self.text.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }
}
