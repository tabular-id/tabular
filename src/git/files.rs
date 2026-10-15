//! Daftar file working tree repository untuk submenu "Files" di sidebar Git:
//! file yang di-track plus file baru yang tidak di-ignore (`git ls-files`),
//! disusun menjadi pohon folder.

use std::collections::BTreeMap;
use std::path::Path;

use super::{GitError, cli};

/// Batas hasil pencarian nama file yang ditampilkan.
pub const MAX_FILTER_RESULTS: usize = 500;

/// Path relatif (pemisah `/`) semua file working tree yang masih ada di disk.
pub fn list_files(repo: &Path) -> Result<Vec<String>, GitError> {
    let out = cli::run(
        Some(repo),
        &[
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        &cli::NEVER,
        cli::DEFAULT_TIMEOUT,
    )?;
    let mut files = parse_nul_list(&out);
    // File yang dihapus tapi belum di-commit masih ada di index.
    files.retain(|f| repo.join(f).is_file());
    Ok(files)
}

/// Pecah keluaran `-z` menjadi daftar path unik yang terurut.
pub fn parse_nul_list(out: &[u8]) -> Vec<String> {
    let mut files: Vec<String> = out
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    files.sort();
    files.dedup();
    files
}

/// Satu folder dalam pohon file. Path di `files` relatif terhadap root repo.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileTree {
    pub dirs: BTreeMap<String, FileTree>,
    pub files: Vec<String>,
}

impl FileTree {
    /// Susun pohon dari daftar path relatif.
    pub fn build(paths: &[String]) -> Self {
        let mut root = FileTree::default();
        for path in paths {
            let mut node = &mut root;
            let mut parts = path.split('/').peekable();
            while let Some(part) = parts.next() {
                if parts.peek().is_some() {
                    node = node.dirs.entry(part.to_string()).or_default();
                } else {
                    node.files.push(path.clone());
                }
            }
        }
        root.sort_files();
        root
    }

    fn sort_files(&mut self) {
        self.files
            .sort_by_key(|p| file_name(p).to_ascii_lowercase());
        for d in self.dirs.values_mut() {
            d.sort_files();
        }
    }
}

/// Nama file dari path relatif `a/b/c.rs` -> `c.rs`.
pub fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Path yang mengandung `query` (tanpa membedakan huruf besar/kecil), dibatasi
/// [`MAX_FILTER_RESULTS`]. Kecocokan pada nama file didahulukan.
pub fn filter(paths: &[String], query: &str) -> Vec<String> {
    let q = query.trim().to_lowercase();
    if q.is_empty() {
        return Vec::new();
    }
    let mut by_name = Vec::new();
    let mut by_path = Vec::new();
    for p in paths {
        let lower = p.to_lowercase();
        if file_name(&lower).contains(&q) {
            by_name.push(p.clone());
        } else if lower.contains(&q) {
            by_path.push(p.clone());
        }
        if by_name.len() >= MAX_FILTER_RESULTS {
            break;
        }
    }
    by_name.extend(by_path);
    by_name.truncate(MAX_FILTER_RESULTS);
    by_name
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn parses_nul_separated_output() {
        let out = b"src/main.rs\0README.md\0src/main.rs\0\0";
        assert_eq!(parse_nul_list(out), s(&["README.md", "src/main.rs"]));
    }

    #[test]
    fn builds_nested_tree() {
        let tree = FileTree::build(&s(&["README.md", "src/lib.rs", "src/a/b.rs", "Cargo.toml"]));
        assert_eq!(tree.files, s(&["Cargo.toml", "README.md"]));
        let src = &tree.dirs["src"];
        assert_eq!(src.files, s(&["src/lib.rs"]));
        assert_eq!(src.dirs["a"].files, s(&["src/a/b.rs"]));
    }

    #[test]
    fn filter_prefers_file_name_matches() {
        let paths = s(&["docs/main/notes.md", "src/main.rs", "src/lib.rs"]);
        assert_eq!(
            filter(&paths, "MAIN"),
            s(&["src/main.rs", "docs/main/notes.md"])
        );
        assert!(filter(&paths, "  ").is_empty());
    }

    #[test]
    fn lists_files_of_real_repo() {
        if cli::run_text(Path::new("."), &["--version"]).is_err() {
            return; // git tidak tersedia di mesin ini
        }
        let dir = std::env::temp_dir().join(format!("tabular-git-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        cli::run_text(&dir, &["init", "-q"]).unwrap();
        std::fs::write(dir.join(".gitignore"), "target/\n").unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::create_dir_all(dir.join("target")).unwrap();
        std::fs::write(dir.join("target/out.bin"), "x").unwrap();
        let files = list_files(&dir).unwrap();
        assert_eq!(files, s(&[".gitignore", "src/main.rs"]));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
