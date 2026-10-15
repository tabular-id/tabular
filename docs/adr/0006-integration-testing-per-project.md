# ADR 0006: Integration Testing per Project

Tanggal: 2026-10-15 · Status: Diterima, diterapkan 2026-10-15

## Konteks

Satu project Tabular menggabungkan koneksi database, folder HTTP API, dan
repository kode. Programmer butuh integration test atas alur bisnis yang
melintasi endpoint, misalnya alur ERP Purchasing → Plant → Sales. Urutan
langkah menentukan hasil, karena id yang dibuat satu langkah dipakai langkah
berikutnya. Data uji juga tidak boleh tertinggal di server.

Sebelumnya sudah ada suite yang di-generate AI (`http_tests`) dengan jendela
"Integration Tests". Jendela itu tidak terikat project, tidak punya cleanup yang
dijamin, dan tidak punya laporan.

## Keputusan

1. **Suite terikat project dan tampil di tab, bukan jendela.** Tombol di header,
   di sebelah nama project, membuka suite project aktif atau membuat suite
   kosong. Jendela lama dihapus. Menu "Integration Tests…" dan hasil generate
   dari folder sekarang membuka tab ini.
2. **Model suite yang sama dipakai ulang dan diperluas.** File JSON lama tetap
   terbaca. Field baru memakai `serde(default)`: `id` langkah, `phase`
   (Setup/Test/Cleanup), `group`, `cleanup_for`, `notes`, `project_id`,
   `groups` (urutan), `stop_on_failure`, dan `min_pass_rate`.
3. **Urutan eksekusi deterministik dan sama dengan tampilan.** Urutannya fase
   lebih dulu, lalu urutan group untuk Setup/Test, lalu urutan daftar. Cleanup
   memakai urutan daftar, dan **Auto-order cleanup** mengurutkan LIFO menurut
   langkah pembuat datanya.
4. **Relasi antar langkah dihitung dari variabel.** `{{var}}` yang dipakai
   dicocokkan dengan `extract` langkah sebelumnya. Hasilnya dipakai untuk lint
   (variabel dipakai sebelum disediakan, data tanpa cleanup, urutan cleanup),
   untuk kolom aliran data, dan untuk memilih langkah saat satu group dijalankan.
5. **Run satu group menarik prasyaratnya.** `select_for_groups` mengikutkan
   langkah group itu, prasyaratnya secara transitif dari group mana pun, Setup
   tanpa group, dan cleanup untuk semua langkah yang ikut.
6. **Cleanup dijamin.** Langkah yang variabelnya kosong ditandai Blocked dan
   tidak dikirim. Stop hanya menghentikan Setup/Test. Cleanup tetap berjalan
   untuk data yang benar-benar dibuat, dengan satu kali coba ulang untuk error
   jaringan. Hanya Abort yang melewati cleanup. Data yang gagal dibersihkan
   dicatat sebagai *residue* beserta id-nya.
7. **Verifikasi database bersifat read-only.** Setelah run, tabel yang ditautkan
   ke langkah pengubah data diperiksa lewat `HeadlessSession::run_query`. Query
   menghitung baris yang kolom teksnya memuat `run_id`. `run_id` divalidasi
   alfanumerik dan identifier dikutip per dialek.
8. **Laporan headless dan bisa dipakai CI.** `http_test_report` menghitung
   statistik dengan istilah ISTQB dan struktur ISO/IEC/IEEE 29119-3, serta
   verdict PASS / PASS WITH DEVIATIONS / FAIL / INCOMPLETE. Laporan memuat bukti
   bergaya GAMP 5: operator, waktu, SHA-256 definisi suite, expected vs actual,
   deviasi, traceability, dan tanda tangan. Formatnya HTML, Markdown, JUnit XML,
   dan JSON. `tabular test run` menjalankan suite yang sama dari terminal.
9. **Rahasia disamarkan di sumbernya.** Runner menyamarkan nilai variabel dan
   field JSON yang sensitif sebelum masuk hasil, riwayat, dan laporan.

## Konsekuensi

- Klaim standar terbatas pada *mengikuti struktur dan istilah*. Tabular tidak
  menyatakan sertifikasi 21 CFR Part 11. Tanda tangan elektronik dan audit
  trail yang tidak bisa diubah tetap tanggung jawab proses tim.
- Deteksi "membuat data" adalah heuristik: POST/PUT yang mengekstrak variabel
  bukan rahasia. Login yang hanya mengekstrak token tidak dihitung.
- Verifikasi database hanya menemukan data yang memuat `run_id`. Prompt AI
  meminta `{{run_id}}` di nama dan kode, tetapi langkah manual bisa saja tidak
  memakainya.
- Riwayat dibatasi 50 run per suite agar folder data tidak membengkak.
