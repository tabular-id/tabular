# ADR 0005: Multi-source Comparison

Tanggal: 2026-10-15 · Status: Diterima, diterapkan 2026-10-15

## Konteks

Data Compare (H11) membandingkan dua tabel SQL dengan struktur mirip. Kasus nyata
user lebih sulit: data yang sama tersimpan di MySQL, SQL Server, dan MongoDB. Di
MySQL dan SQL Server satu "baris" logis butuh `JOIN`, di MongoDB cukup satu koleksi.
Nama kolom dan kunci berbeda per sistem, dan perbandingan bisa memakan waktu lama.

## Keputusan

1. **Satu entri = satu query bebas di satu koneksi.** Bukan pasangan tabel. User
   menulis `JOIN`/filter sendiri; memilih tabel hanya mengisi `SELECT *` sesuai
   dialek. MongoDB memakai query JSON (`collection` + `filter`/`projection`/`sort`/
   `limit`, atau `pipeline`), karena eksekutor editor belum mendukung MongoDB dan
   JSON bisa divalidasi serta memuat Extended JSON.
2. **Variabel `${nama}` disubstitusi sebagai teks.** Sederhana dan sama untuk SQL
   maupun JSON. Konsekuensinya kutip ditulis user dan tidak ada parameter binding;
   variabel diisi pemilik comparison sendiri, bukan input pihak luar.
3. **Kunci dipasangkan per posisi, kolom nilai per nama + alias.** Nama kunci boleh
   berbeda antar engine (`id`, `order_no`, `_id`). Kolom nilai yang tidak ada di
   semua entri dilaporkan, tidak dibandingkan. Normalisasi nilai memakai ulang
   `compare::normalize` (angka, boolean, timestamp), dan tanggal MongoDB ditulis
   dalam bentuk timestamp SQL agar setara.
4. **Hanya kunci yang berbeda yang disimpan di hasil.** Kunci yang identik di semua
   sumber hanya dihitung. Tabel hasil: kunci, status, lalu tiap kolom sekali per
   entri. Ekspor memakai dialog Export Result yang sudah ada.
5. **Job background milik `Tabular`, bukan tab.** Job tetap jalan saat tab ditutup,
   dicerminkan ke panel Background Tasks (`TaskOwner::Compare`, bisa dibatalkan),
   dan hasilnya dikirim ke tab pemilik atau disimpan untuk dibuka lagi.
6. **Definisi dan statistik run terakhir disimpan di `connections.db`**
   (`multi_compare_saved`, JSON). Koneksi dirujuk lewat id, tidak ikut sync.

## Konsekuensi

- Tiap entri dimuat utuh ke memori; batas baris per entri (bawaan 100.000) menjaga
  pemakaian memori. Untuk SQL batas diterapkan setelah query karena membungkus query
  bebas dengan `LIMIT`/`TOP` tidak aman di semua dialek; MongoDB membatasi di server.
- Tidak ada skrip sinkronisasi; dengan lebih dari dua sumber tidak ada "sumber
  kebenaran" yang jelas.
- Kode headless (`data_transfer::multi_compare`, `mongo_source`,
  `multi_compare_saved`) bisa dipakai lapisan agent/MCP kelak tanpa `window_egui`.
