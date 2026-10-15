# ADR 0004: Repository Setup Otomatis dari Folder Git

Tanggal: 2026-10-14 · Status: Diterima, diterapkan 2026-10-14

## Konteks

Menyiapkan satu aplikasi di Tabular butuh banyak langkah manual: tambah koneksi satu
per satu (tipe, host, port, user, password), buat folder HTTP API, set repository,
jalankan Generate Endpoints, buka diagram, buat group, set repository group, lalu
Suggest Tables. Untuk microservices langkah itu diulang per service. Semua informasi
yang dibutuhkan sudah ada di repository: `.env`, docker-compose, dan konfigurasi
framework. Setelah disiapkan, isinya juga cepat basi saat kode berubah.

## Keputusan

1. **Satu titik masuk: folder git.** Discovery headless (`src/repo_setup/`) membaca
   file env, docker-compose, dan konfigurasi Spring/.NET/Rails/TypeORM/Prisma, lalu
   memindai route dengan `repo_endpoints::scan_routes`. Hasilnya `SetupPlan` murni
   data; penerapan ada di `window_egui::repo_setup_jobs`.
2. **Project ADR 0003 menjadi wadah.** Koneksi masuk folder koneksi project, variabel
   `.env` menjadi variabel environment project (kunci rahasia ke keychain), dan
   folder HTTP API masuk workspace project.
3. **Service = folder manifest/compose yang punya route.** Route dimiliki folder
   service terdalam. Satu service = monolith (dinamai sesuai repository), lebih dari
   satu = microservices (folder API dan group diagram per service).
4. **Penerapan idempoten dan tidak destruktif.** Identitas koneksi adalah
   `environment|file#KEY`, bukan nilai DSN, sehingga perubahan password menjadi
   update, bukan koneksi baru. Koneksi yang sama dengan milik user diadopsi. Hanya
   endpoint baru yang ditambahkan, supaya dokumentasi AI/manual tidak tertimpa. Item
   yang hilang ditandai stale, tidak pernah dihapus otomatis.
5. **Watcher berbasis sidik jari, bukan file watcher.** Sidik jari = HEAD + status
   porcelain (dengan ukuran/mtime file berubah) + isi file env/konfigurasi yang
   diawasi. Diperiksa per interval (bawaan 5 menit) dan saat jendela kembali fokus;
   discovery hanya jalan bila sidik jari berubah. Tidak ada fetch/pull otomatis.
6. **Parser YAML subset sendiri** (`repo_setup::yaml`) daripada dependensi baru:
   crate YAML serde yang umum sudah deprecated, dan yang dibutuhkan hanya map/list,
   anchor/merge, dan scalar sederhana.
7. **Diagram hanya disimpan lokal.** Group per service ditulis ke file diagram lokal;
   penyimpanan ke `diagram_by_tabular` di database target tetap menunggu user, agar
   setup otomatis tidak membuat tabel di database user tanpa diminta.
8. **Catatan setup personal** di `{data_dir}/repo_setups.json` (path lokal dan id
   koneksi hanya berlaku di mesin ini), tidak ikut sync.

## Konsekuensi

- Nilai `.env` produksi ikut menjadi koneksi Production (berwarna merah, password di
  keychain). Placeholder tetap dibuat tanpa password dan ditandai.
- Endpoint dari pemindaian teks hanya berisi method, path, dan parameter path;
  dokumentasi lengkap dan tautan endpoint-tabel tetap lewat Generate Endpoints (AI),
  opsional otomatis lewat preferensi.
- Konfigurasi yang hanya ada di Kubernetes/Helm/Terraform/secret manager tidak
  terbaca; file `*.env` yang namanya tidak menyebut environment (mis. `secrets.env`)
  dilewati.
- Monorepo besar memakai batas `MAX_FILES` pemindai route; peringatan ditampilkan
  bila batas tercapai.
