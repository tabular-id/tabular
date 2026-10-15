//! Dekode gambar bitmap, rasterisasi SVG (resvg), dan render halaman PDF
//! (hayro, fitur `pdf`) menjadi piksel RGBA (alpha lurus). Semua fungsi di
//! sini berat dan dipanggil dari thread latar.

use std::sync::{Arc, OnceLock};

use super::{FileViewError, MAX_IMAGE_PIXELS, MAX_RASTER_SIDE};

/// Piksel RGBA 8-bit, alpha lurus (tidak premultiplied), baris demi baris.
#[derive(Clone, PartialEq, Eq)]
pub struct RgbaImage {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

impl std::fmt::Debug for RgbaImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RgbaImage({}x{})", self.width, self.height)
    }
}

impl RgbaImage {
    /// Warna piksel (r, g, b, a) di koordinat gambar.
    pub fn pixel(&self, x: u32, y: u32) -> Option<[u8; 4]> {
        if x >= self.width || y >= self.height {
            return None;
        }
        let i = ((y * self.width + x) * 4) as usize;
        self.pixels.get(i..i + 4).map(|p| [p[0], p[1], p[2], p[3]])
    }
}

/// Metadata bitmap untuk bar info.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageInfo {
    pub format: String,
    pub width: u32,
    pub height: u32,
    pub color: String,
    /// Jumlah frame (GIF/WebP animasi menampilkan frame pertama saja).
    pub animated: bool,
}

/// Dekode bitmap dari byte file. Gambar animasi dimuat frame pertamanya.
pub fn decode_image(bytes: &[u8]) -> Result<(RgbaImage, ImageInfo), FileViewError> {
    let reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| FileViewError::Image(e.to_string()))?;
    let format = reader.format();
    let (w, h) = reader
        .into_dimensions()
        .map_err(|e| FileViewError::Image(e.to_string()))?;
    if u64::from(w) * u64::from(h) > MAX_IMAGE_PIXELS {
        return Err(FileViewError::ImageTooLarge {
            width: w,
            height: h,
        });
    }
    let img = image::load_from_memory(bytes).map_err(|e| FileViewError::Image(e.to_string()))?;
    let color = format!("{:?}", img.color());
    let animated = matches!(format, Some(image::ImageFormat::Gif))
        && bytes.windows(3).filter(|w| *w == b"\x21\xF9\x04").count() > 1;
    let rgba = img.to_rgba8();
    let info = ImageInfo {
        format: format
            .map(|f| format!("{f:?}").to_uppercase())
            .unwrap_or_else(|| "Unknown".into()),
        width: rgba.width(),
        height: rgba.height(),
        color,
        animated,
    };
    Ok((
        RgbaImage {
            width: rgba.width(),
            height: rgba.height(),
            pixels: rgba.into_raw(),
        },
        info,
    ))
}

/// Perkecil gambar (rasio tetap) bila sisi terpanjang melebihi `max_side`,
/// mis. batas ukuran tekstur GPU. Gambar yang sudah muat dikembalikan apa adanya.
pub fn fit_within(img: RgbaImage, max_side: u32) -> RgbaImage {
    let longest = img.width.max(img.height);
    if longest <= max_side || max_side == 0 {
        return img;
    }
    let ratio = max_side as f64 / longest as f64;
    let w = ((img.width as f64 * ratio).round() as u32).max(1);
    let h = ((img.height as f64 * ratio).round() as u32).max(1);
    let Some(buf) = image::RgbaImage::from_raw(img.width, img.height, img.pixels) else {
        // Panjang buffer tidak cocok dengan dimensi: kembalikan gambar kosong.
        return RgbaImage {
            width: 1,
            height: 1,
            pixels: vec![0, 0, 0, 0],
        };
    };
    let small = image::imageops::resize(&buf, w, h, image::imageops::FilterType::Triangle);
    RgbaImage {
        width: w,
        height: h,
        pixels: small.into_raw(),
    }
}

/// Database font sistem untuk teks di SVG; dimuat sekali (mahal, ~100 ms).
fn svg_fontdb() -> Arc<resvg::usvg::fontdb::Database> {
    static DB: OnceLock<Arc<resvg::usvg::fontdb::Database>> = OnceLock::new();
    DB.get_or_init(|| {
        let mut db = resvg::usvg::fontdb::Database::new();
        db.load_system_fonts();
        Arc::new(db)
    })
    .clone()
}

/// Parse SVG (atau SVGZ). `resources_dir` dipakai untuk gambar relatif.
pub fn parse_svg(
    bytes: &[u8],
    resources_dir: Option<std::path::PathBuf>,
) -> Result<resvg::usvg::Tree, FileViewError> {
    let opt = resvg::usvg::Options {
        resources_dir,
        fontdb: svg_fontdb(),
        ..Default::default()
    };
    resvg::usvg::Tree::from_data(bytes, &opt).map_err(|e| FileViewError::Svg(e.to_string()))
}

/// Ukuran intrinsik SVG dalam piksel CSS.
pub fn svg_size(tree: &resvg::usvg::Tree) -> (f32, f32) {
    let s = tree.size();
    (s.width(), s.height())
}

/// Skala yang dipersempit agar sisi terpanjang tidak melebihi [`MAX_RASTER_SIDE`].
pub fn clamp_scale(width: f32, height: f32, scale: f32) -> f32 {
    let longest = width.max(height).max(1.0);
    let max_scale = MAX_RASTER_SIDE as f32 / longest;
    scale.clamp(0.01, max_scale.max(0.01))
}

/// Rasterisasi SVG pada skala `scale` (1.0 = ukuran intrinsik).
pub fn rasterize_svg(tree: &resvg::usvg::Tree, scale: f32) -> Result<RgbaImage, FileViewError> {
    let (w, h) = svg_size(tree);
    let scale = clamp_scale(w, h, scale);
    let pw = ((w * scale).ceil() as u32).max(1);
    let ph = ((h * scale).ceil() as u32).max(1);
    let mut pixmap = resvg::tiny_skia::Pixmap::new(pw, ph)
        .ok_or_else(|| FileViewError::Svg(format!("cannot allocate {pw}x{ph} canvas")))?;
    resvg::render(
        tree,
        resvg::tiny_skia::Transform::from_scale(scale, scale),
        &mut pixmap.as_mut(),
    );
    let mut pixels = Vec::with_capacity((pw * ph * 4) as usize);
    for p in pixmap.pixels() {
        let c = p.demultiply();
        pixels.extend_from_slice(&[c.red(), c.green(), c.blue(), c.alpha()]);
    }
    Ok(RgbaImage {
        width: pw,
        height: ph,
        pixels,
    })
}

/// Ukuran tiap halaman PDF dalam point (1/72 inci), sesudah rotasi.
#[derive(Clone, Debug, PartialEq)]
pub struct PdfInfo {
    pub pages: Vec<(f32, f32)>,
}

/// Baca jumlah dan ukuran halaman PDF.
#[cfg(feature = "pdf")]
pub fn pdf_info(bytes: Arc<Vec<u8>>) -> Result<PdfInfo, FileViewError> {
    let pdf = load_pdf(bytes)?;
    let pages = pdf.pages().iter().map(|p| p.render_dimensions()).collect();
    Ok(PdfInfo { pages })
}

#[cfg(not(feature = "pdf"))]
pub fn pdf_info(_bytes: Arc<Vec<u8>>) -> Result<PdfInfo, FileViewError> {
    Err(FileViewError::PdfUnsupported)
}

#[cfg(feature = "pdf")]
fn load_pdf(bytes: Arc<Vec<u8>>) -> Result<hayro::hayro_syntax::Pdf, FileViewError> {
    use hayro::hayro_syntax::LoadPdfError;
    hayro::hayro_syntax::Pdf::new(bytes).map_err(|e| match e {
        LoadPdfError::Decryption(_) => {
            FileViewError::Pdf("the document is encrypted or password-protected".into())
        }
        LoadPdfError::Invalid => FileViewError::Pdf("the file is not a valid PDF".into()),
    })
}

/// Render satu halaman (indeks 0) pada skala `scale` (1.0 = 72 DPI) di atas latar putih.
#[cfg(feature = "pdf")]
pub fn render_pdf_page(
    bytes: Arc<Vec<u8>>,
    index: usize,
    scale: f32,
) -> Result<RgbaImage, FileViewError> {
    let pdf = load_pdf(bytes)?;
    let pages = pdf.pages();
    let page = pages
        .get(index)
        .ok_or_else(|| FileViewError::Pdf(format!("page {} does not exist", index + 1)))?;
    let (w, h) = page.render_dimensions();
    let scale = clamp_scale(w, h, scale);
    let cache = hayro::RenderCache::new();
    let pixmap = hayro::render(
        page,
        &cache,
        &hayro::hayro_interpret::InterpreterSettings::default(),
        &hayro::RenderSettings::default(),
        &hayro::PixmapSettings {
            x_scale: scale,
            y_scale: scale,
            bg_color: hayro::vello_cpu::color::palette::css::WHITE,
        },
    );
    let (pw, ph) = (u32::from(pixmap.width()), u32::from(pixmap.height()));
    let mut pixels = Vec::with_capacity((pw * ph * 4) as usize);
    for p in pixmap.data() {
        // Latar putih buram: premultiplied sama dengan alpha lurus.
        pixels.extend_from_slice(&[p.r, p.g, p.b, p.a]);
    }
    Ok(RgbaImage {
        width: pw,
        height: ph,
        pixels,
    })
}

#[cfg(not(feature = "pdf"))]
pub fn render_pdf_page(
    _bytes: Arc<Vec<u8>>,
    _index: usize,
    _scale: f32,
) -> Result<RgbaImage, FileViewError> {
    Err(FileViewError::PdfUnsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png_2x1() -> Vec<u8> {
        let img = image::RgbaImage::from_raw(2, 1, vec![255, 0, 0, 255, 0, 0, 255, 128]).unwrap();
        let mut out = Vec::new();
        img.write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    #[test]
    fn decodes_png() {
        let (img, info) = decode_image(&png_2x1()).unwrap();
        assert_eq!((img.width, img.height), (2, 1));
        assert_eq!(img.pixel(0, 0), Some([255, 0, 0, 255]));
        assert_eq!(img.pixel(1, 0), Some([0, 0, 255, 128]));
        assert_eq!(img.pixel(2, 0), None);
        assert_eq!(info.format, "PNG");
    }

    #[test]
    fn fit_within_downscales_keeping_ratio() {
        let img = RgbaImage {
            width: 400,
            height: 100,
            pixels: vec![255; 400 * 100 * 4],
        };
        let small = fit_within(img.clone(), 100);
        assert_eq!((small.width, small.height), (100, 25));
        assert_eq!(small.pixels.len(), 100 * 25 * 4);
        let same = fit_within(img, 1000);
        assert_eq!((same.width, same.height), (400, 100));
    }

    #[test]
    fn rejects_garbage_image() {
        assert!(decode_image(b"not an image").is_err());
    }

    #[test]
    fn rasterizes_svg() {
        let svg = br##"<svg xmlns="http://www.w3.org/2000/svg" width="10" height="20">
            <rect width="10" height="20" fill="#00ff00"/></svg>"##;
        let tree = parse_svg(svg, None).unwrap();
        assert_eq!(svg_size(&tree), (10.0, 20.0));
        let img = rasterize_svg(&tree, 2.0).unwrap();
        assert_eq!((img.width, img.height), (20, 40));
        assert_eq!(img.pixel(5, 5), Some([0, 255, 0, 255]));
    }

    #[test]
    fn invalid_svg_is_error() {
        assert!(parse_svg(b"<svg", None).is_err());
    }

    #[test]
    fn clamp_scale_limits_longest_side() {
        let s = clamp_scale(10_000.0, 100.0, 4.0);
        assert!((10_000.0 * s) as u32 <= MAX_RASTER_SIDE);
        assert_eq!(clamp_scale(100.0, 100.0, 2.0), 2.0);
    }

    /// PDF satu halaman 100x50 pt dengan kotak hitam.
    #[cfg(feature = "pdf")]
    fn tiny_pdf() -> Vec<u8> {
        let content = b"0 0 0 rg 10 10 30 30 re f";
        let objs = [
            "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 50] /Contents 4 0 R >>".to_string(),
            format!(
                "<< /Length {} >>\nstream\n{}\nendstream",
                content.len(),
                std::str::from_utf8(content).unwrap()
            ),
        ];
        let mut out = b"%PDF-1.4\n".to_vec();
        let mut offsets = Vec::new();
        for (i, o) in objs.iter().enumerate() {
            offsets.push(out.len());
            out.extend_from_slice(format!("{} 0 obj\n{}\nendobj\n", i + 1, o).as_bytes());
        }
        let xref = out.len();
        out.extend_from_slice(
            format!("xref\n0 {}\n0000000000 65535 f \n", objs.len() + 1).as_bytes(),
        );
        for off in offsets {
            out.extend_from_slice(format!("{off:010} 00000 n \n").as_bytes());
        }
        out.extend_from_slice(
            format!(
                "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{}\n%%EOF\n",
                objs.len() + 1,
                xref
            )
            .as_bytes(),
        );
        out
    }

    #[cfg(feature = "pdf")]
    #[test]
    fn renders_pdf_page() {
        let bytes = Arc::new(tiny_pdf());
        let info = pdf_info(bytes.clone()).unwrap();
        assert_eq!(info.pages, vec![(100.0, 50.0)]);
        let img = render_pdf_page(bytes.clone(), 0, 2.0).unwrap();
        assert_eq!((img.width, img.height), (200, 100));
        // PDF memakai sumbu y ke atas: kotak (10..40, 10..40) pt ada di bawah-kiri.
        assert_eq!(img.pixel(40, 70), Some([0, 0, 0, 255]));
        assert_eq!(img.pixel(150, 20), Some([255, 255, 255, 255]));
        assert!(render_pdf_page(bytes, 3, 1.0).is_err());
    }

    #[cfg(feature = "pdf")]
    #[test]
    fn invalid_pdf_is_error() {
        assert!(pdf_info(Arc::new(b"%PDF-garbage".to_vec())).is_err());
    }
}
