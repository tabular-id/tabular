# ADR 0008: Konteks AI Assistant di sekitar tabel yang ditanyakan

Tanggal: 2026-10-23 · Status: Diterima, diterapkan 2026-10-23

## Konteks

Prompt AI Assistant hanya berisi skema dari `table_cache`/`column_cache`, memory project,
kutipan Obsidian, dan isi editor. Padahal Tabular sudah menyimpan jauh lebih banyak tentang
sebuah tabel: foreign key dan index di cache, endpoint HTTP yang membaca/menulisnya
(`SavedRequest.tables`, `EndpointLink` di diagram), proses bisnis (flow card), catatan
diagram, repository yang di-link ke group/folder API/Repo Setup, dan query history. Semua
itu hanya bisa diakses CLI agent lewat tool MCP terpisah, dan hanya bila agent memutuskan
memanggilnya. Backend HTTP API (OpenAI, Gemini, Anthropic) tidak melihatnya sama sekali.

## Keputusan

1. **Satu modul headless `agent::context`** merakit `ContextBundle` (Markdown + statistik +
   hint) dari data lokal untuk "tabel fokus" satu koneksi. Ia memakai ulang loader diagram
   (`load_any_diagram`), `flow_infos`, `repo_scan::grep_tables`,
   `vector_index::search_history_hits`, dan tidak menambah parser atau grep baru. Modul ini
   tidak bergantung pada `window_egui` dan dipanggil dari panel chat maupun tool MCP.
2. **Tabel fokus dideteksi, bukan seluruh skema.** Nama yang disebut di pertanyaan atau SQL
   tab aktif selalu pertama, lalu ranking indeks vektor lokal (`TABLE_MAX_DISTANCE`), maksimal
   8 tabel. Ini menjaga prompt kecil dan membuat bagian API/kode relevan.
3. **Setiap bagian punya budget byte dan jumlah item** (relasi 3 KB, API 6 KB, proses 6 KB,
   catatan 3 KB, kode 5 KB, git 3 KB, history 2,5 KB). Bagian kosong dilewati; kegagalan satu
   bagian (repo tidak ada, grep timeout) menjadi `hints`, bukan error, agar chat tetap jalan.
4. **Repository diresolusi dari tiga tautan yang sudah ada**: group diagram
   (`diagram_repo_paths` lalu clone cache), folder HTTP API yang request-nya menyentuh tabel,
   dan `RepoSetup` yang koneksi/project-nya cocok. Tidak pernah clone atau fetch dari chat.
   Git hanya dibaca: `git log -- <file>` (helper baru `git::log::page_for_paths`) dan
   `git status`; hanya subject commit yang dikirim, bukan body atau diff.
5. **Perakitan berjalan di thread latar** (`window_egui::ai_context_jobs`), bukan di
   `build_chat_prompts` yang berjalan di UI thread dengan `block_on`. `ChatPromptParts`
   memisahkan prompt supaya bundle disisipkan tepat sebelum `## Request`. Transkrip
   menampilkan langkah "Gathering context…", Stop membatalkan sebelum request terkirim.
6. **Privasi mengikuti aturan yang ada**: snippet kode di-`redact`, SQL history di-
   `redact_sql_secrets`, nilai env tidak pernah ikut. CLI agent dengan MCP hanya menerima
   `path:line` (bisa membaca file sendiri); provider API menerima snippet ≤160 karakter.
7. **Transparansi dan kontrol**: tiga preferensi `ai_context_api/git/history` (default on)
   di Settings dan sebagai chip API/Git/History di atas input chat; baris "Context: …" di
   bawah tiap jawaban (disimpan di riwayat sesi sebagai `context_summary`).
8. **Tool MCP `table_context`** mengembalikan bundle yang sama untuk agent luar, sehingga satu
   panggilan menggantikan `describe_diagram` + `find_table_usages` + `search_query_history`.

## Alternatif yang ditolak

- Memperluas `build_schema_context_for_prompt` saja: tetap berjalan di UI thread, grep repo
  akan membekukan jendela.
- Mengandalkan agent memanggil tool MCP: backend HTTP API tidak punya tool, dan agent sering
  tidak memanggilnya untuk pertanyaan singkat.
- Menyimpan konteks sebagai file Markdown yang di-embed: menggandakan data yang sudah ada di
  cache dan diagram, cepat basi.

## Konsekuensi

- Giliran chat mulai sedikit lebih lambat (query SQLite lokal plus grep repo kecil);
  repository besar dibatasi 60 detik per root dan maksimal 3 root.
- Prompt bertambah paling banyak ±28 KB saat semua bagian terisi; user bisa mematikan bagian
  per pertanyaan.
- Tes: `cargo test --lib agent::context::` (fixture SQLite in-memory + diagram di temp dir),
  `cargo test --lib git::ops::tests::log_for_paths`, dan tes MCP yang menghitung 30 tool.
