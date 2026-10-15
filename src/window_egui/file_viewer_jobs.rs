//! Job latar File Viewer: baca dan dekode file di thread terpisah, hasilnya
//! dikumpulkan di [`JobInbox`] milik tab dan diambil saat render.
//!
//! Inbox disimpan di state tab (bukan di `Tabular`) sehingga hasil job tidak
//! pernah salah alamat walaupun tab dipindah atau ditutup.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::data_transfer::readers::{self, LoadedFile, ReadOptions};
use crate::file_viewer::decode::{self, ImageInfo, PdfInfo, RgbaImage};
use crate::file_viewer::{self, TextFile};

/// Hasil satu job.
pub enum JobOutput {
    Text(Result<TextFile, String>),
    Image(Result<(Arc<RgbaImage>, ImageInfo), String>),
    Svg {
        scale: f32,
        result: Result<SvgRaster, String>,
    },
    PdfInfo(Result<(Arc<Vec<u8>>, PdfInfo), String>),
    PdfPage {
        index: usize,
        scale: f32,
        result: Result<RgbaImage, String>,
    },
    Table {
        all_rows: bool,
        result: Result<LoadedFile, String>,
    },
    Binary(Result<Vec<u8>, String>),
}

/// Hasil rasterisasi SVG beserta byte sumbernya (untuk render ulang saat zoom).
pub struct SvgRaster {
    pub bytes: Arc<Vec<u8>>,
    pub size: (f32, f32),
    pub image: RgbaImage,
}

/// Kotak masuk hasil job milik satu tab.
#[derive(Clone, Default)]
pub struct JobInbox {
    done: Arc<Mutex<Vec<JobOutput>>>,
    pending: Arc<AtomicUsize>,
}

impl JobInbox {
    /// Jalankan `work` di thread latar; hasilnya masuk ke inbox.
    pub fn spawn(&self, work: impl FnOnce() -> JobOutput + Send + 'static) {
        self.pending.fetch_add(1, Ordering::SeqCst);
        let done = self.done.clone();
        let pending = self.pending.clone();
        let spawned = std::thread::Builder::new()
            .name("file-viewer".into())
            .spawn(move || {
                let out = work();
                done.lock().unwrap_or_else(|e| e.into_inner()).push(out);
                pending.fetch_sub(1, Ordering::SeqCst);
            });
        if let Err(e) = spawned {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            log::error!("[FILE_VIEWER] cannot spawn worker thread: {e}");
        }
    }

    /// Ambil semua hasil yang sudah selesai.
    pub fn drain(&self) -> Vec<JobOutput> {
        std::mem::take(&mut *self.done.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// Jumlah job yang masih berjalan.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }
}

fn read_all(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| file_viewer::FileViewError::io(path, e).to_string())
}

pub fn load_text(inbox: &JobInbox, path: PathBuf) {
    inbox.spawn(move || {
        JobOutput::Text(
            file_viewer::read_text(&path, file_viewer::MAX_EDITABLE_TEXT_BYTES)
                .map_err(|e| e.to_string()),
        )
    });
}

/// Dekode bitmap; salinan tampilan diperkecil bila melebihi `max_side`
/// (batas tekstur GPU). Ukuran asli tetap ada di `ImageInfo`.
pub fn load_image(inbox: &JobInbox, path: PathBuf, max_side: u32) {
    inbox.spawn(move || {
        let result = read_all(&path).and_then(|bytes| {
            decode::decode_image(&bytes)
                .map(|(img, info)| (Arc::new(decode::fit_within(img, max_side)), info))
                .map_err(|e| e.to_string())
        });
        JobOutput::Image(result)
    });
}

/// Rasterisasi SVG. `bytes` = `None` berarti baca dari disk dulu.
pub fn render_svg(inbox: &JobInbox, path: PathBuf, bytes: Option<Arc<Vec<u8>>>, scale: f32) {
    inbox.spawn(move || {
        let result = (|| {
            let bytes = match bytes {
                Some(b) => b,
                None => Arc::new(read_all(&path)?),
            };
            let tree = decode::parse_svg(&bytes, path.parent().map(Path::to_path_buf))
                .map_err(|e| e.to_string())?;
            let size = decode::svg_size(&tree);
            let image = decode::rasterize_svg(&tree, scale).map_err(|e| e.to_string())?;
            Ok(SvgRaster { bytes, size, image })
        })();
        JobOutput::Svg { scale, result }
    });
}

pub fn load_pdf(inbox: &JobInbox, path: PathBuf) {
    inbox.spawn(move || {
        let result = read_all(&path).and_then(|bytes| {
            let bytes = Arc::new(bytes);
            decode::pdf_info(bytes.clone())
                .map(|info| (bytes, info))
                .map_err(|e| e.to_string())
        });
        JobOutput::PdfInfo(result)
    });
}

pub fn render_pdf_page(inbox: &JobInbox, bytes: Arc<Vec<u8>>, index: usize, scale: f32) {
    inbox.spawn(move || JobOutput::PdfPage {
        index,
        scale,
        result: decode::render_pdf_page(bytes, index, scale).map_err(|e| e.to_string()),
    });
}

pub fn load_table(inbox: &JobInbox, path: PathBuf, sheet: Option<String>, all_rows: bool) {
    inbox.spawn(move || {
        let opts = ReadOptions {
            sheet,
            max_rows: (!all_rows).then_some(file_viewer::TABLE_PREVIEW_ROWS),
            ..Default::default()
        };
        JobOutput::Table {
            all_rows,
            result: readers::read_file(&path, &opts).map_err(|e| e.to_string()),
        }
    });
}

pub fn load_binary(inbox: &JobInbox, path: PathBuf) {
    inbox.spawn(move || {
        JobOutput::Binary(
            file_viewer::read_head(&path, file_viewer::HEX_PREVIEW_BYTES)
                .map_err(|e| e.to_string()),
        )
    });
}
