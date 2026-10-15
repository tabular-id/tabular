# ADR 0007: File Viewer untuk file project

Tanggal: 2026-10-15 · Status: Diterima, diterapkan 2026-10-15

## Konteks

Programmer yang memakai Tabular bekerja dengan repository kode, koneksi database,
dan endpoint API dalam satu project. Selama ini Tabular hanya bisa membuka file
`.sql` di editor query dan file data lewat dialog Data Files. Gambar, PDF,
Markdown, konfigurasi (`.toml`, `.env`, `.yaml`), dan source code (Go, Rust, Java,
PHP, JavaScript, HTML, dan lainnya) harus dibuka di aplikasi lain.

## Keputusan

1. **Satu jenis tab baru, bukan modifikasi editor SQL.** `QueryTab` mendapat
   `file_viewer_state`, mengikuti pola tab Git, Compare, dan Integration Testing.
   File kode tidak dibuka di editor query karena editor itu membawa perilaku SQL:
   Run, autocomplete, lint, kartu koneksi, dan header `-- tabular:` saat save.
   Mematikan semua itu per tab lebih rawan daripada tab terpisah.
2. **Editor kode memakai ulang `render_code_view`.** Widget editor body REST client
   sudah punya gutter nomor baris, highlight hasil pencarian, dan cache layout.
   `Syntax` mendapat varian `Code(CodeLang)` yang memanggil highlighter
   `syntax_ts`, dan `render_code_view_scrolled` menambah gulir ke hasil pencarian.
3. **Highlighter heuristik per baris, tanpa grammar baru.** `LanguageKind` mendapat
   varian `Code(CodeLang)`. Aturan per bahasa (komentar, string, kata kunci, gaya
   key-value, markup, diff) ada di `file_viewer::lang` dan dieksekusi di
   `syntax_code.rs`. syntect dan grammar tree-sitter tambahan ditolak karena
   menambah ukuran biner dan waktu build untuk manfaat yang kecil di viewer.
4. **Tabel memakai grid hasil yang sudah ada.** CSV/XLSX/ODS/Parquet dibaca oleh
   `data_transfer::readers` di thread latar, lalu dimasukkan ke state grid tab
   aktif lewat `update_pagination_data`. Sort, find, dan paging langsung tersedia.
   File data yang di-drop tetap membuka dialog Data Files agar perilaku lama tidak
   berubah; File Viewer menerima file lain.
5. **SVG dengan resvg, PDF dengan hayro.** resvg 0.45 sudah ada di dependency tree
   lewat egui_extras, jadi dipakai langsung dengan versi yang sama. hayro adalah
   rasterizer PDF pure Rust tanpa toolchain C dan ditaruh di belakang fitur Cargo
   `pdf` (default aktif). PDF dirender per halaman secara lazy sesuai viewport,
   maksimal dua job bersamaan, dan tekstur halaman yang jauh dibuang.
6. **Semua dekode di thread latar dengan kotak masuk per tab.** `JobInbox`
   disimpan di state tab, bukan di `Tabular`, sehingga hasil job tidak bisa salah
   alamat saat tab dipindah atau ditutup. Pemuatan dimulai saat tab pertama kali
   dirender, karena butuh `egui::Context` (batas tekstur GPU, skala layar). Ini
   juga membuat pemulihan sesi bisa membuka tab viewer tanpa `Context`.
7. **Save setia pada file asli.** Akhir baris CRLF dan BOM UTF-8 dikembalikan saat
   save. File di atas 5 MB, file bukan UTF-8, dan UTF-16 dibuka baca-saja agar
   tidak rusak. Cmd+S global mendelegasikan ke File Viewer bila tab aktif adalah
   tab viewer.
8. **Sesi hanya menyimpan path.** `SessionTab.viewer` (default `false`, jadi sesi
   lama tetap terbaca) menandai tab viewer. Isi file, termasuk `.env`, tidak
   pernah ditulis ke `session.json`. Perubahan yang belum disimpan memicu dialog
   konfirmasi tutup tab yang sama dengan tab query.
9. **Daftar file repository lewat `git ls-files`.** Submenu Files di sidebar Git
   memakai `--cached --others --exclude-standard`, jadi mengikuti `.gitignore`,
   dan membuang file yang sudah dihapus dari disk. Logikanya headless di
   `git/files.rs` dan teruji dengan repository sementara.

## Konsekuensi

- Komentar blok multi-baris hanya dikenali lewat heuristik baris yang diawali
  `*`. Python docstring dan komentar `{- -}` multi-baris tidak diwarnai.
- Teks di atas 1 MB diedit tanpa warna, karena cache layout menghitung ulang
  seluruh teks setiap kali diketik.
- hayro belum mendukung PDF terenkripsi. User diarahkan ke aplikasi bawaan OS.
- Gambar animasi hanya menampilkan frame pertama.
- Tab strip tidak punya ikon per jenis tab, sehingga tab viewer hanya
  menampilkan nama file, sama seperti tab lain.
- Pratinjau Markdown mematikan `override_font_id` global di areanya sendiri agar
  heading tampil lebih besar.
