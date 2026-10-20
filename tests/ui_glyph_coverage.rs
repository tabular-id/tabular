//! Penjaga regresi: glyph yang tidak ada di font egui (tampil kotak "□") tidak boleh
//! muncul di string literal UI. Pakai `egui_icons::icons::ICON_*` sebagai gantinya.
//! Daftar glyph diambil dari audit cakupan font (Proportional + Monospace).

use std::path::Path;

/// Karakter yang terbukti tidak ada di font manapun yang dimuat Tabular.
const MISSING_GLYPHS: &[char] = &['\u{2715}', '\u{2325}', '\u{2303}'];

/// File yang memakai glyph itu sebagai input parser, bukan teks yang digambar.
const ALLOWED_FILES: &[&str] = &["keymap.rs"];

fn scan(dir: &Path, hits: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan(&path, hits);
        } else if path.extension().is_some_and(|e| e == "rs")
            && !path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| ALLOWED_FILES.contains(&n))
        {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for (no, line) in text.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                if let Some(c) = line.chars().find(|c| MISSING_GLYPHS.contains(c)) {
                    hits.push(format!("{}:{}: U+{:04X}", path.display(), no + 1, c as u32));
                }
            }
        }
    }
}

#[test]
fn ui_strings_do_not_use_missing_glyphs() {
    let mut hits = Vec::new();
    scan(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut hits,
    );
    assert!(
        hits.is_empty(),
        "Glyph tanpa font (tampil kotak) ditemukan; ganti dengan egui_icons::icons::ICON_*:\n{}",
        hits.join("\n")
    );
}
