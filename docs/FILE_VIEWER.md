# File Viewer

Tabular opens common project files in their own tab, next to your queries, API requests
and Git views. Images, PDFs and Markdown are shown rendered. Spreadsheets and CSV files
open in the result grid. Source code and configuration files open in an editor with
syntax colors, line numbers, search and save.

## Opening a file

- **File menu > Open File…** or the Quick Open command **File: Open File…**. You can pick
  several files at once.
- **Drag and drop** a file onto the window. Data files that the import dialog handles
  (CSV, TSV, TXT, JSON, NDJSON, XLSX, XLS, ODS, Parquet, and compressed or encrypted
  exports) still open the **Data Files** import dialog, as before. Everything else opens in
  the File Viewer.
- **Git sidebar > Files**. Every repository section has a **Files** button next to
  Changes, Branches, History and Review. It lists tracked files plus new files that are not
  ignored, as a folder tree. Type in **Find file** to search by name. Click a file to open it.

`.sql` files always open in the query editor, so you can run them. A file that is already
open is brought to the front instead of opening a second tab. File Viewer tabs are reopened
when Tabular starts, if the files still exist.

## What each file type shows

| Type | Extensions | View |
|---|---|---|
| Image | png, jpg, gif, webp, bmp, ico, tiff, tga, qoi, pnm, hdr, exr, dds | Zoom, fit to window, actual size, checkerboard behind transparency, pixel color under the cursor |
| SVG | svg, svgz | Like images; redrawn sharply after you zoom |
| PDF | pdf | Continuous pages, page number box, previous and next, zoom, fit width |
| Markdown | md, markdown, mdx | Rendered preview with relative images, or **Edit source** |
| Table | csv, tsv, psv, xlsx, xlsm, xlsb, xls, ods, parquet, ndjson, jsonl | Result grid with sorting, find and paging; sheet picker; **Import to database…** |
| Code and config | about 50 languages and formats, see below | Editor with colors, line numbers, wrap, find, save |
| Binary | anything else that is not text | Size and a hex dump of the first 64 KB |

Code and config files include Rust, Go, Java, Kotlin, Scala, Groovy, Swift, C, C++, C#,
Objective-C, PHP, Python, Ruby, JavaScript, TypeScript, Dart, Lua, Perl, R, Elixir, Erlang,
Haskell, Zig, shell, PowerShell, batch, SQL, HTML, XML, Vue, Svelte, CSS, SCSS, JSON, YAML,
TOML, INI, `.env` files, Java properties, Dockerfile, Makefile, CMake, Terraform/HCL,
GraphQL, Protocol Buffers, Nginx config, ignore files and diffs. Files without an extension
are opened as text when their content is text.

Every view has **Reload from disk**, **Show in folder** and **Open with the default app** in
its header.

## Editing and saving

Text files are saved with **Cmd+S** (Ctrl+S) or the **Save** button. Tabular keeps the
file's line endings (LF or CRLF) and a UTF-8 byte order mark if the file had one. It does
not add the `-- tabular:` header that saved queries get. Closing a tab with unsaved changes
asks first, like query tabs do.

Some files open read-only, and the toolbar says why:

- Files larger than 5 MB show their first 5 MB.
- Files that are not valid UTF-8, including UTF-16, would change encoding if saved.

`.env`, private key and similar files are marked **Sensitive**. Their content is never
stored in the session file; only the path is remembered.

## Limits

- Syntax colors are line based. A block comment that spans several lines is recognized
  when its lines start with `*`, which covers the usual `/** … */` style.
- Files over 1 MB are edited without colors so typing stays responsive.
- Tables show the first 100,000 rows until you click **Load all rows**.
- Images over 64 megapixels are not opened. Large images are shown downscaled to the
  GPU texture limit; the info bar still shows the original size.
- Animated GIF and WebP show their first frame.
- Encrypted or password-protected PDFs are not supported. Use **Open with the default
  app** instead. PDF rendering sits behind the `pdf` Cargo feature, which is on by
  default; a build without it shows a message for PDF files.
- Markdown preview loads local images only, not images from the web.

## For developers

| Part | Path |
|---|---|
| File type detection, text reading, hex dump | `src/file_viewer/mod.rs` |
| Language map and syntax rules | `src/file_viewer/lang.rs` |
| Image, SVG and PDF decoding | `src/file_viewer/decode.rs` |
| Line highlighter for code | `src/syntax_code.rs` (sub-module of `syntax_ts`) |
| Tab UI and background jobs | `src/window_egui/file_viewer_ui.rs`, `file_viewer_jobs.rs` |
| Git Files list | `src/git/files.rs`, `GitSubMenu::Files` in `git_jobs.rs` and `git_sidebar.rs` |

The editor reuses `http_client_widgets::render_code_view`, and tables reuse the result
grid through `data_table::update_pagination_data`. See ADR 0007 for the decisions.
