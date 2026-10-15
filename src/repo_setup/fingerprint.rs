//! Sidik jari repository untuk watcher: berubah bila HEAD berpindah, file
//! kerja berubah, atau file env/konfigurasi (sering di-`.gitignore`)
//! berubah. Watcher hanya menjalankan discovery ulang bila sidik jari beda.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::git::cli;

/// Hitung sidik jari `root`. `watched` = path relatif file env/konfigurasi
/// yang dipakai discovery terakhir. Folder root dan folder file-file itu juga
/// didaftar ulang untuk file `.env*` baru, karena file env biasanya
/// di-`.gitignore` sehingga tidak muncul di `git status`.
pub fn compute(root: &Path, watched: &[String]) -> String {
    let mut h = Sha256::new();
    let head = cli::run_text(root, &["rev-parse", "--verify", "-q", "HEAD"]).unwrap_or_default();
    h.update(head.trim().as_bytes());
    h.update(b"\0");
    let status = cli::run_text(
        root,
        &[
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignore-submodules",
        ],
    )
    .unwrap_or_default();
    for line in status.lines() {
        h.update(line.as_bytes());
        // File yang sudah berstatus "modified" bisa diubah lagi tanpa
        // mengubah baris status; ukuran + waktu ubah ikut dihitung.
        if let Some(rel) = line.get(3..) {
            let rel = rel.rsplit(" -> ").next().unwrap_or(rel).trim_matches('"');
            if let Ok(meta) = std::fs::metadata(root.join(rel)) {
                h.update(meta.len().to_le_bytes());
                if let Ok(m) = meta.modified()
                    && let Ok(d) = m.duration_since(std::time::UNIX_EPOCH)
                {
                    h.update(d.as_nanos().to_le_bytes());
                }
            }
        }
        h.update(b"\n");
    }
    h.update(b"\0");
    let mut files: Vec<String> = watched.to_vec();
    let mut dirs: Vec<String> = vec![String::new()];
    for rel in watched {
        let d = rel
            .rsplit_once('/')
            .map(|(d, _)| d)
            .unwrap_or("")
            .to_string();
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    }
    for d in dirs {
        let Ok(entries) = std::fs::read_dir(root.join(&d)) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if super::env_files::environment_for_file(&name).is_some() {
                files.push(if d.is_empty() {
                    name
                } else {
                    format!("{d}/{name}")
                });
            }
        }
    }
    files.sort();
    files.dedup();
    for rel in files {
        h.update(rel.as_bytes());
        match std::fs::read(root.join(rel)) {
            Ok(bytes) => h.update(Sha256::digest(&bytes)),
            Err(_) => h.update(b"missing"),
        }
    }
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_new_env_files() {
        let dir = std::env::temp_dir().join(format!("tabular-fp-new-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("svc")).expect("mkdir");
        std::fs::write(dir.join("svc/.env"), "A=1").expect("write");
        let watched = vec!["svc/.env".to_string()];
        let a = compute(&dir, &watched);
        std::fs::write(dir.join("svc/.env.production"), "A=2").expect("write");
        assert_ne!(a, compute(&dir, &watched));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn changes_when_watched_file_changes() {
        let dir = std::env::temp_dir().join(format!("tabular-fp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join(".env"), "A=1").expect("write");
        let watched = vec![".env".to_string()];
        let a = compute(&dir, &watched);
        assert_eq!(a, compute(&dir, &watched));
        std::fs::write(dir.join(".env"), "A=2").expect("write");
        assert_ne!(a, compute(&dir, &watched));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
