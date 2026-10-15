//! File Viewer headless: klasifikasi jenis file, pembacaan teks dengan batas
//! ukuran, dekode gambar/SVG/PDF ke piksel RGBA, dan hex dump untuk file biner.
//!
//! Tidak bergantung pada `window_egui`; UI ada di
//! `window_egui/file_viewer_ui.rs`. Lihat `docs/FILE_VIEWER.md` dan ADR 0007.

pub mod decode;
pub mod lang;

use std::path::Path;

pub use lang::{CodeLang, CodeSyntax, SyntaxStyle, is_sensitive_file};

/// Teks di atas batas ini dibuka baca-saja dan dipotong.
pub const MAX_EDITABLE_TEXT_BYTES: u64 = 5 * 1024 * 1024;
/// Gambar dengan piksel lebih dari ini ditolak (melindungi memori GPU/RAM).
pub const MAX_IMAGE_PIXELS: u64 = 64_000_000;
/// Sisi terpanjang hasil rasterisasi SVG/PDF.
pub const MAX_RASTER_SIDE: u32 = 8192;
/// Baris pratinjau pertama untuk CSV/XLSX; sisanya lewat tombol "Load all".
pub const TABLE_PREVIEW_ROWS: usize = 100_000;
/// Byte awal yang ditampilkan hex dump untuk file biner.
pub const HEX_PREVIEW_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FileViewError {
    #[error("Cannot read {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Unsupported or corrupt image: {0}")]
    Image(String),
    #[error("Image is too large ({width}x{height} pixels)")]
    ImageTooLarge { width: u32, height: u32 },
    #[error("Invalid SVG: {0}")]
    Svg(String),
    #[error("Cannot open PDF: {0}")]
    Pdf(String),
    #[error("PDF support is not included in this build")]
    PdfUnsupported,
}

impl FileViewError {
    pub fn io(path: &Path, source: std::io::Error) -> Self {
        FileViewError::Io {
            path: path.display().to_string(),
            source,
        }
    }
}

/// Cara sebuah file ditampilkan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    /// Bitmap (PNG, JPEG, GIF, WebP, BMP, ICO, TIFF, ...).
    Image,
    Svg,
    Markdown,
    /// Data bertabel lewat `data_transfer::readers` (CSV, XLSX, ODS, Parquet, ...).
    Table,
    Pdf,
    /// Source code / konfigurasi / teks biasa, bisa diedit.
    Code(CodeLang),
    /// Tidak dikenali dan bukan teks.
    Binary,
}

impl FileKind {
    pub fn label(self) -> &'static str {
        match self {
            FileKind::Image => "Image",
            FileKind::Svg => "SVG",
            FileKind::Markdown => "Markdown",
            FileKind::Table => "Table",
            FileKind::Pdf => "PDF",
            FileKind::Code(lang) => lang.label(),
            FileKind::Binary => "Binary",
        }
    }
}

const IMAGE_EXTS: &[&str] = &[
    "png", "jpg", "jpeg", "jpe", "jfif", "gif", "webp", "bmp", "dib", "ico", "cur", "tif", "tiff",
    "tga", "qoi", "pnm", "ppm", "pgm", "pbm", "pam", "hdr", "exr", "dds", "ff",
];
const TABLE_EXTS: &[&str] = &[
    "csv", "tsv", "tab", "psv", "xlsx", "xlsm", "xlsb", "xls", "ods", "parquet", "pq", "ndjson",
    "jsonl",
];

fn extension_lower(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
}

/// Tentukan jenis file dari ekstensi, lalu dari isi (`head` = byte awal file).
pub fn classify(path: &Path, head: &[u8]) -> FileKind {
    if let Some(ext) = extension_lower(path) {
        let ext = ext.as_str();
        if IMAGE_EXTS.contains(&ext) {
            return FileKind::Image;
        }
        match ext {
            "svg" | "svgz" => return FileKind::Svg,
            "pdf" => return FileKind::Pdf,
            "md" | "markdown" | "mdx" | "mdown" => return FileKind::Markdown,
            _ => {}
        }
        if TABLE_EXTS.contains(&ext) {
            return FileKind::Table;
        }
    }
    if let Some(lang) = CodeLang::from_path(path) {
        return FileKind::Code(lang);
    }
    sniff(head)
}

/// Tebak jenis dari magic bytes, lalu apakah isinya teks.
pub fn sniff(head: &[u8]) -> FileKind {
    const MAGIC_IMAGE: &[&[u8]] = &[
        b"\x89PNG\r\n\x1a\n",
        b"\xff\xd8\xff",
        b"GIF87a",
        b"GIF89a",
        b"BM",
        b"II*\0",
        b"MM\0*",
        b"qoif",
    ];
    if head.starts_with(b"%PDF-") {
        return FileKind::Pdf;
    }
    if MAGIC_IMAGE.iter().any(|m| head.starts_with(m))
        || (head.len() >= 12 && &head[0..4] == b"RIFF" && &head[8..12] == b"WEBP")
    {
        return FileKind::Image;
    }
    if looks_like_text(head) {
        let trimmed = String::from_utf8_lossy(&head[..head.len().min(512)]);
        let t = trimmed.trim_start();
        if t.starts_with("<svg") || (t.starts_with("<?xml") && t.contains("<svg")) {
            return FileKind::Svg;
        }
        return FileKind::Code(CodeLang::PlainText);
    }
    FileKind::Binary
}

/// Teks bila tidak ada byte NUL dan sebagian besar byte adalah UTF-8 valid.
pub fn looks_like_text(head: &[u8]) -> bool {
    if head.is_empty() {
        return true;
    }
    if head.contains(&0) {
        // UTF-16 dengan BOM tetap teks.
        return head.starts_with(&[0xFF, 0xFE]) || head.starts_with(&[0xFE, 0xFF]);
    }
    match std::str::from_utf8(head) {
        Ok(_) => true,
        // Potongan `head` bisa memutus satu karakter multi-byte di ujung.
        Err(e) => e.valid_up_to() + 4 >= head.len() || e.error_len().is_none(),
    }
}

/// Baca byte awal file (untuk [`classify`]).
pub fn read_head(path: &Path, max: usize) -> Result<Vec<u8>, FileViewError> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|e| FileViewError::io(path, e))?;
    let mut buf = Vec::with_capacity(max.min(64 * 1024));
    file.take(max as u64)
        .read_to_end(&mut buf)
        .map_err(|e| FileViewError::io(path, e))?;
    Ok(buf)
}

/// Isi file teks yang sudah dinormalisasi untuk editor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextFile {
    /// Akhir baris selalu `\n`; `crlf` mencatat aslinya.
    pub text: String,
    pub crlf: bool,
    /// File diawali BOM UTF-8; ditulis kembali saat disimpan.
    pub bom: bool,
    /// File lebih besar dari batas; hanya bagian awal yang dimuat (baca-saja).
    pub truncated: bool,
    /// Ada byte yang bukan UTF-8 valid dan diganti `U+FFFD` (baca-saja).
    pub lossy: bool,
    pub size: u64,
}

impl TextFile {
    /// Aman diedit dan disimpan kembali tanpa merusak isi.
    pub fn editable(&self) -> bool {
        !self.truncated && !self.lossy
    }

    /// Teks yang ditulis ke disk (akhir baris dan BOM dikembalikan ke aslinya).
    pub fn to_disk(text: &str, crlf: bool, bom: bool) -> String {
        let body = if crlf {
            text.replace('\n', "\r\n")
        } else {
            text.to_string()
        };
        if bom { format!("\u{FEFF}{body}") } else { body }
    }
}

/// Baca file teks dengan batas `max_bytes`.
pub fn read_text(path: &Path, max_bytes: u64) -> Result<TextFile, FileViewError> {
    let size = std::fs::metadata(path)
        .map_err(|e| FileViewError::io(path, e))?
        .len();
    let bytes = read_head(path, max_bytes as usize)?;
    Ok(decode_text(&bytes, size, size > max_bytes))
}

/// Ubah byte menjadi [`TextFile`]: BOM UTF-8/UTF-16 dibuang, CRLF dinormalisasi.
pub fn decode_text(bytes: &[u8], size: u64, truncated: bool) -> TextFile {
    let bom = bytes.starts_with(&[0xEF, 0xBB, 0xBF]);
    let (raw, lossy) = if let Some(rest) = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
        let s = String::from_utf8_lossy(rest);
        let lossy = matches!(s, std::borrow::Cow::Owned(_));
        (s.into_owned(), lossy)
    } else if bytes.starts_with(&[0xFF, 0xFE]) || bytes.starts_with(&[0xFE, 0xFF]) {
        let enc = if bytes[0] == 0xFF {
            encoding_rs::UTF_16LE
        } else {
            encoding_rs::UTF_16BE
        };
        let (s, _, _) = enc.decode(&bytes[2..]);
        // Disimpan ulang sebagai UTF-8 akan mengubah encoding file: baca-saja.
        (s.into_owned(), true)
    } else {
        match std::str::from_utf8(bytes) {
            Ok(s) => (s.to_string(), false),
            Err(e) if truncated && e.error_len().is_none() => {
                // Potongan berhenti di tengah karakter multi-byte.
                (
                    String::from_utf8_lossy(&bytes[..e.valid_up_to()]).into_owned(),
                    false,
                )
            }
            Err(_) => (String::from_utf8_lossy(bytes).into_owned(), true),
        }
    };
    let crlf = raw.contains("\r\n");
    let text = if crlf { raw.replace("\r\n", "\n") } else { raw };
    TextFile {
        text,
        crlf,
        bom,
        truncated,
        lossy,
        size,
    }
}

/// Hex dump klasik: offset, 16 byte heksadesimal, lalu ASCII.
pub fn hex_dump(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4 + bytes.len() / 16 * 12);
    for (i, chunk) in bytes.chunks(16).enumerate() {
        out.push_str(&format!("{:08x}  ", i * 16));
        for j in 0..16 {
            match chunk.get(j) {
                Some(b) => out.push_str(&format!("{b:02x} ")),
                None => out.push_str("   "),
            }
            if j == 7 {
                out.push(' ');
            }
        }
        out.push(' ');
        for &b in chunk {
            out.push(if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            });
        }
        out.push('\n');
    }
    out
}

/// Ukuran file yang mudah dibaca: `1.5 MB`.
pub fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_by_extension() {
        let c = |p: &str| classify(Path::new(p), b"");
        assert_eq!(c("a.PNG"), FileKind::Image);
        assert_eq!(c("a.jpeg"), FileKind::Image);
        assert_eq!(c("logo.svg"), FileKind::Svg);
        assert_eq!(c("README.md"), FileKind::Markdown);
        assert_eq!(c("data.csv"), FileKind::Table);
        assert_eq!(c("book.xlsx"), FileKind::Table);
        assert_eq!(c("doc.pdf"), FileKind::Pdf);
        assert_eq!(c("main.go"), FileKind::Code(CodeLang::Go));
        assert_eq!(c(".env"), FileKind::Code(CodeLang::Env));
    }

    #[test]
    fn classify_by_content() {
        let p = Path::new("noext");
        assert_eq!(classify(p, b"%PDF-1.7\n"), FileKind::Pdf);
        assert_eq!(classify(p, b"\x89PNG\r\n\x1a\nxxxx"), FileKind::Image);
        assert_eq!(
            classify(p, b"hello world\n"),
            FileKind::Code(CodeLang::PlainText)
        );
        assert_eq!(classify(p, b"<svg xmlns=\"x\"></svg>"), FileKind::Svg);
        assert_eq!(classify(p, &[0u8, 1, 2, 3, 0xff, 0x00]), FileKind::Binary);
    }

    #[test]
    fn text_detection_tolerates_cut_multibyte() {
        let s = "héllo wörld".as_bytes();
        // Potong di tengah karakter 'ö' (2 byte).
        let cut = &s[..s.len() - 4];
        assert!(looks_like_text(cut));
    }

    #[test]
    fn decode_text_normalizes_crlf_and_bom() {
        let t = decode_text(b"\xEF\xBB\xBFa\r\nb\r\n", 9, false);
        assert_eq!(t.text, "a\nb\n");
        assert!(t.crlf);
        assert!(t.editable());
        assert!(t.bom);
        assert_eq!(
            TextFile::to_disk(&t.text, t.crlf, t.bom).as_bytes(),
            b"\xEF\xBB\xBFa\r\nb\r\n"
        );
        let plain = decode_text(b"x\n", 2, false);
        assert!(!plain.bom && !plain.crlf);
        assert_eq!(TextFile::to_disk(&plain.text, plain.crlf, plain.bom), "x\n");
    }

    #[test]
    fn decode_text_marks_invalid_utf8_lossy() {
        let t = decode_text(b"ab\xffcd", 5, false);
        assert!(t.lossy);
        assert!(!t.editable());
    }

    #[test]
    fn read_text_truncates_large_file() {
        let dir = std::env::temp_dir().join(format!("tabular-fv-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.txt");
        std::fs::write(&path, "x".repeat(100)).unwrap();
        let t = read_text(&path, 10).unwrap();
        assert_eq!(t.text.len(), 10);
        assert!(t.truncated);
        assert_eq!(t.size, 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hex_dump_formats_rows() {
        let dump = hex_dump(b"ABCDEFGHIJKLMNOPQ");
        let lines: Vec<&str> = dump.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("00000000  41 42"));
        assert!(lines[0].ends_with("ABCDEFGHIJKLMNOP"));
        assert!(lines[1].starts_with("00000010  51"));
    }

    #[test]
    fn human_size_units() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1536), "1.5 KB");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MB");
    }
}
