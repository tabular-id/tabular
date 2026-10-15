//! Perbaikan kode API dengan AI agar langkah integration test yang gagal
//! bekerja.
//!
//! Alurnya aman untuk repository user:
//! 1. Buat git worktree terpisah di branch baru `tabular/fix-…`, isinya
//!    disamakan dengan working copy (termasuk perubahan yang belum di-commit),
//!    lalu dicatat sebagai commit dasar di branch itu.
//! 2. AI menerima request, response server, dan kode handler, lalu menjawab
//!    pasangan "cari → ganti" per file. Teks yang dicari harus cocok persis satu
//!    kali; kalau tidak, AI diminta mengulang sekali.
//! 3. Perubahan diterapkan di worktree dan ditampilkan sebagai diff. User
//!    memilih: terapkan ke working copy (tanpa commit), simpan sebagai branch,
//!    atau buang. Branch user tidak pernah di-commit oleh Tabular.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use serde_json::Value;

use crate::agent::harness::ProgressStatus;
use crate::http_collection::SavedRequest;
use crate::http_test_validate::CodeSource;
use crate::http_tests::StepResult;
use crate::repo_scan::{RepoJobEvent, step};

/// Batas isi satu file yang dikirim utuh ke AI.
const MAX_FILE_CHARS: usize = 60_000;
/// Batas jumlah file utuh di prompt.
const MAX_FILES: usize = 6;
/// Batas ukuran file untracked yang disalin ke worktree.
const MAX_UNTRACKED_BYTES: u64 = 2 * 1024 * 1024;
/// Batas waktu cek build.
const CHECK_TIMEOUT: Duration = Duration::from_secs(900);

/// Satu langkah gagal yang ingin dibuat bekerja.
#[derive(Debug, Clone)]
pub struct CodeFixTarget {
    pub step_id: String,
    pub step_name: String,
    /// Request langkah (template `{{…}}`).
    pub request: SavedRequest,
    pub assertions: Vec<String>,
    pub result: StepResult,
    pub code: Option<CodeSource>,
}

pub struct CodeFixInput {
    pub backend: crate::ai_assistant::ChatBackend,
    pub backend_label: String,
    /// Folder repo (boleh sub-folder service di dalam repo git).
    pub repo_root: PathBuf,
    pub suite_name: String,
    pub targets: Vec<CodeFixTarget>,
    /// Catatan tambahan dari user (boleh kosong).
    pub note: String,
}

/// Satu perubahan file dari AI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEdit {
    /// Relatif terhadap root repo git.
    pub path: String,
    /// Teks yang diganti; kosong = file baru.
    pub search: String,
    pub replace: String,
}

/// Usulan perbaikan yang siap ditinjau.
#[derive(Debug, Clone, Default)]
pub struct CodeFixProposal {
    /// Root repo git user.
    pub repo: PathBuf,
    pub worktree: PathBuf,
    pub branch: String,
    pub summary: String,
    pub files: Vec<String>,
    /// Unified diff relatif terhadap working copy user.
    pub diff: String,
    /// Perintah cek build yang terdeteksi, mis. `cargo check`.
    pub check_command: Option<Vec<String>>,
    /// Folder tempat cek build dijalankan (di dalam worktree).
    pub check_dir: PathBuf,
    /// Langkah yang ingin diperbaiki.
    pub step_ids: Vec<String>,
}

pub type CodeFixEvent = RepoJobEvent<CodeFixProposal>;

pub struct CodeFixHandle {
    pub rx: mpsc::Receiver<CodeFixEvent>,
    cancel: Arc<AtomicBool>,
}

impl CodeFixHandle {
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

// ─── Git ────────────────────────────────────────────────────────────────────

fn git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    crate::git::cli::run_text(cwd, args).map_err(|e| e.to_string())
}

/// Commit dengan identitas Tabular, tanpa hook.
fn git_commit(cwd: &Path, message: &str) -> Result<(), String> {
    git(
        cwd,
        &[
            "-c",
            "user.name=Tabular",
            "-c",
            "user.email=tabular@localhost",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--no-verify",
            "--allow-empty",
            "-q",
            "-m",
            message,
        ],
    )
    .map(|_| ())
}

fn slug(text: &str) -> String {
    let s: String = text
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let s = s
        .split('-')
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let s: String = s.chars().take(30).collect();
    if s.is_empty() { "suite".into() } else { s }
}

/// Tulis patch ke file sementara dan jalankan `git apply` dengan argumen tambahan.
fn git_apply(cwd: &Path, patch: &str, extra: &[&str]) -> Result<(), String> {
    if patch.trim().is_empty() {
        return Ok(());
    }
    let file = std::env::temp_dir().join(format!(
        "tabular-patch-{}-{}.diff",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    std::fs::write(&file, patch).map_err(|e| e.to_string())?;
    let path = file.to_string_lossy().to_string();
    let mut args = vec!["apply", "--whitespace=nowarn"];
    args.extend_from_slice(extra);
    args.push(&path);
    let res = git(cwd, &args).map(|_| ());
    let _ = std::fs::remove_file(&file);
    res
}

/// Worktree baru di branch baru, isinya = working copy user.
fn prepare_worktree(repo: &Path, name: &str, base: &Path) -> Result<(PathBuf, String), String> {
    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S%3f").to_string();
    let branch = format!("tabular/fix-{}-{ts}", slug(name));
    let dir = base.join(format!("{}-{ts}", slug(name)));
    std::fs::create_dir_all(dir.parent().unwrap_or(&dir)).map_err(|e| e.to_string())?;
    let dir_s = dir.to_string_lossy().to_string();
    git(
        repo,
        &["worktree", "add", "-q", "-b", &branch, &dir_s, "HEAD"],
    )?;
    // Perubahan yang belum di-commit (tracked) ikut disalin.
    let pending = git(repo, &["diff", "HEAD", "--binary"])?;
    if let Err(e) = git_apply(&dir, &pending, &[]) {
        log::warn!("[CODEFIX] could not copy uncommitted changes: {e}");
    }
    // File baru yang belum di-track.
    let untracked = git(repo, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    for rel in untracked.split('\0').filter(|r| !r.is_empty()) {
        let src = repo.join(rel);
        if std::fs::metadata(&src).map(|m| m.len()).unwrap_or(u64::MAX) > MAX_UNTRACKED_BYTES {
            continue;
        }
        let dst = dir.join(rel);
        if let Some(parent) = dst.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::copy(&src, &dst);
    }
    git(&dir, &["add", "-A"])?;
    git_commit(&dir, "Tabular baseline: working copy before AI fix")?;
    Ok((dir, branch))
}

/// Hapus worktree dan branch usulan.
pub fn discard(p: &CodeFixProposal) -> Result<(), String> {
    let dir = p.worktree.to_string_lossy().to_string();
    let _ = git(&p.repo, &["worktree", "remove", "--force", &dir]);
    let _ = std::fs::remove_dir_all(&p.worktree);
    let _ = git(&p.repo, &["worktree", "prune"]);
    if !p.branch.is_empty() {
        git(&p.repo, &["branch", "-D", &p.branch])?;
    }
    Ok(())
}

/// Terapkan diff ke working copy user (tanpa commit), lalu buang worktree.
pub fn apply_to_working_copy(p: &CodeFixProposal) -> Result<(), String> {
    // Diff dibuat terhadap working copy persis, jadi apply biasa cukup; 3-way
    // hanya cadangan bila file sudah berubah lagi sejak usulan dibuat.
    git_apply(&p.repo, &p.diff, &[])
        .or_else(|_| git_apply(&p.repo, &p.diff, &["--3way"]))
        .map_err(|e| format!("Could not apply the fix to {}: {e}", p.repo.display()))?;
    if let Err(e) = discard(p) {
        log::warn!("[CODEFIX] cleanup after apply: {e}");
    }
    Ok(())
}

/// Commit perubahan di branch usulan dan lepas worktree; branch tetap ada.
pub fn keep_branch(p: &CodeFixProposal) -> Result<String, String> {
    git(&p.worktree, &["add", "-A"])?;
    let msg = format!(
        "fix: {}\n\nProposed by Tabular integration testing.",
        p.summary
            .lines()
            .next()
            .unwrap_or("make integration tests pass")
    );
    git_commit(&p.worktree, &msg)?;
    let dir = p.worktree.to_string_lossy().to_string();
    git(&p.repo, &["worktree", "remove", "--force", &dir])?;
    Ok(p.branch.clone())
}

/// Perintah cek build untuk folder service.
pub fn detect_check_command(dir: &Path) -> Option<Vec<String>> {
    let v = |a: &[&str]| Some(a.iter().map(|s| s.to_string()).collect());
    if dir.join("Cargo.toml").is_file() {
        v(&["cargo", "check", "--quiet"])
    } else if dir.join("go.mod").is_file() {
        v(&["go", "build", "./..."])
    } else if dir.join("tsconfig.json").is_file() && dir.join("node_modules").is_dir() {
        v(&["npx", "tsc", "--noEmit"])
    } else if dir.join("pom.xml").is_file() {
        v(&["mvn", "-q", "-o", "compile"])
    } else {
        None
    }
}

fn tail_chars(text: &str, n: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    chars[chars.len().saturating_sub(n)..].iter().collect()
}

/// Jalankan cek build di worktree. Hasil: Ok(output) bila sukses.
pub fn spawn_check(p: &CodeFixProposal) -> mpsc::Receiver<Result<String, String>> {
    let (tx, rx) = mpsc::channel();
    let Some(cmd) = p.check_command.clone() else {
        let _ = tx.send(Err("No build check is known for this project".into()));
        return rx;
    };
    let dir = p.check_dir.clone();
    std::thread::spawn(move || {
        let label = cmd.join(" ");
        let started = std::time::Instant::now();
        let bin = crate::agent::harness::resolve_binary(&cmd[0])
            .unwrap_or_else(|| PathBuf::from(&cmd[0]));
        let child = std::process::Command::new(bin)
            .args(&cmd[1..])
            .current_dir(&dir)
            .env("PATH", crate::agent::harness::augmented_path())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();
        let result = match child {
            Err(e) => Err(format!("Could not start {label}: {e}")),
            Ok(mut child) => loop {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        break match child.wait_with_output() {
                            Ok(o) => {
                                let text = format!(
                                    "{}{}",
                                    String::from_utf8_lossy(&o.stdout),
                                    String::from_utf8_lossy(&o.stderr)
                                );
                                let tail = tail_chars(&text, 4000);
                                if o.status.success() {
                                    Ok(if tail.trim().is_empty() {
                                        format!("{label} passed")
                                    } else {
                                        tail
                                    })
                                } else {
                                    Err(tail)
                                }
                            }
                            Err(e) => Err(e.to_string()),
                        };
                    }
                    Ok(None) if started.elapsed() > CHECK_TIMEOUT => {
                        let _ = child.kill();
                        break Err(format!("{label} timed out"));
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(250)),
                    Err(e) => break Err(e.to_string()),
                }
            },
        };
        let _ = tx.send(result);
    });
    rx
}

// ─── Edit ───────────────────────────────────────────────────────────────────

/// Urai jawaban AI: `{"summary": "...", "edits": [{path, search, replace}]}`.
pub fn parse_edits(text: &str) -> Result<(String, Vec<FileEdit>), String> {
    let v: Value = crate::repo_scan::slice_between(text, '{', '}')
        .and_then(|s| serde_json::from_str(s).ok())
        .ok_or_else(|| {
            format!(
                "AI reply is not JSON: {}",
                crate::repo_scan::truncate_chars(text.trim(), 200)
            )
        })?;
    let summary = v
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let str_of = |e: &Value, k: &str| e.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let edits = v
        .get("edits")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|e| FileEdit {
                    path: str_of(e, "path")
                        .trim()
                        .trim_start_matches("./")
                        .to_string(),
                    search: str_of(e, "search"),
                    replace: str_of(e, "replace"),
                })
                .filter(|e| !e.path.is_empty())
                .collect()
        })
        .unwrap_or_default();
    Ok((summary, edits))
}

/// Path aman di dalam `root` (tanpa `..`, tanpa path absolut, tanpa `.git`).
fn safe_join(root: &Path, rel: &str) -> Option<PathBuf> {
    let p = Path::new(rel);
    let bad = p.is_absolute()
        || p.components()
            .any(|c| matches!(c, std::path::Component::ParentDir) || c.as_os_str() == ".git");
    (!bad).then(|| root.join(p))
}

/// Terapkan edit di `root`. Mengembalikan pesan untuk edit yang gagal.
pub fn apply_edits(root: &Path, edits: &[FileEdit]) -> Vec<String> {
    let mut errors = Vec::new();
    for e in edits {
        let Some(path) = safe_join(root, &e.path) else {
            errors.push(format!("{}: path is outside the repository", e.path));
            continue;
        };
        if e.search.is_empty() {
            if path.exists() {
                errors.push(format!(
                    "{}: file exists; give a `search` text to change it",
                    e.path
                ));
                continue;
            }
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(err) = std::fs::write(&path, &e.replace) {
                errors.push(format!("{}: {err}", e.path));
            }
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            errors.push(format!("{}: file not found", e.path));
            continue;
        };
        match text.matches(&e.search).count() {
            1 => {
                let new = text.replacen(&e.search, &e.replace, 1);
                if let Err(err) = std::fs::write(&path, new) {
                    errors.push(format!("{}: {err}", e.path));
                }
            }
            0 => errors.push(format!("{}: search text not found", e.path)),
            n => errors.push(format!(
                "{}: search text matches {n} places; include more surrounding lines",
                e.path
            )),
        }
    }
    errors
}

// ─── Prompt ─────────────────────────────────────────────────────────────────

/// Path file yang disebut header konteks kode (`// path:line (…)`).
fn context_files(ctx: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for l in ctx.lines().filter_map(|l| l.strip_prefix("// ")) {
        if let Some((p, rest)) = l.split_once(':')
            && rest.contains('(')
            && !p.trim().is_empty()
            && !p.contains(' ')
            && !out.iter().any(|x| x == p.trim())
        {
            out.push(p.trim().to_string());
        }
    }
    out
}

pub fn build_prompts(
    targets: &[CodeFixTarget],
    note: &str,
    contexts: &[(String, String)],
    files: &[(String, String)],
) -> (String, String) {
    let system = String::from(
        "You are fixing the source code of an HTTP API so that its integration tests pass. \
         The tests were sent to the running server; you get each failing request, the server's \
         response and the relevant code.\n\
         Rules:\n\
         - Fix the API code, not the tests. Change only what is needed, keep the existing style, \
           and do not refactor unrelated code.\n\
         - If the test expectation itself is wrong (the API behaves correctly), return no edits and \
           say so in `summary`.\n\
         - Never add secrets, credentials or hosts to the code. Do not touch build output or \
           lock files.\n\
         - Edits are exact search/replace pairs. `search` must be copied exactly from the file \
           (including indentation) and match exactly one place; include enough surrounding lines. \
           Use an empty `search` only to create a new file. Paths are relative to the repository \
           root.\n\
         Reply with ONLY one JSON object, no prose and no markdown fence:\n\
         {\"summary\":\"one or two sentences: what was wrong and what you changed\",\
         \"edits\":[{\"path\":\"src/routes/x.rs\",\"search\":\"exact old text\",\"replace\":\"new text\"}]}\n",
    );
    let mut user = String::from("# Failing requests\n");
    for t in targets {
        let r = &t.result;
        user.push_str(&format!(
            "\n## {} ({} {})\nrequest URL template: {}\n",
            t.step_name,
            t.request.method.label(),
            r.url,
            t.request.url
        ));
        if !t.request.body_text.trim().is_empty() {
            user.push_str(&format!("request body:\n{}\n", t.request.body_text.trim()));
        }
        for a in &t.assertions {
            user.push_str(&format!("expected: {a}\n"));
        }
        match &r.error {
            Some(e) => user.push_str(&format!("result: no response ({e})\n")),
            None => user.push_str(&format!("result: HTTP {}\n", r.status)),
        }
        for a in r.assertions.iter().filter(|a| !a.passed) {
            user.push_str(&format!(
                "failed: {} (actual {})\n",
                a.assertion.describe(),
                a.actual.as_deref().unwrap_or("missing")
            ));
        }
        if !r.body_preview.trim().is_empty() {
            user.push_str(&format!(
                "response body:\n{}\n",
                crate::repo_scan::truncate_chars(r.body_preview.trim(), 1500)
            ));
        }
        if let Some((_, ctx)) = contexts.iter().find(|(id, _)| *id == t.step_id) {
            user.push_str(&format!("code for this route:\n{ctx}\n"));
        }
    }
    if !files.is_empty() {
        user.push_str("\n# Full files (copy `search` text from here)\n");
        for (path, text) in files {
            user.push_str(&format!("\n=== {path} ===\n{text}\n"));
        }
    }
    if !note.trim().is_empty() {
        user.push_str(&format!("\n# Note from the developer\n{}\n", note.trim()));
    }
    (system, user)
}

// ─── Job ────────────────────────────────────────────────────────────────────

pub fn spawn_code_fix(input: CodeFixInput) -> CodeFixHandle {
    let (tx, rx) = mpsc::channel();
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    std::thread::spawn(move || {
        let result = propose(input, &tx, &flag);
        if let Err(e) = &result {
            log::warn!("[CODEFIX] {e}");
        }
        let _ = tx.send(RepoJobEvent::Finished(result));
    });
    CodeFixHandle { rx, cancel }
}

fn read_files(root: &Path, paths: &[String]) -> Vec<(String, String)> {
    paths
        .iter()
        .take(MAX_FILES)
        .filter_map(|p| {
            let text = std::fs::read_to_string(safe_join(root, p)?).ok()?;
            Some((
                p.clone(),
                crate::repo_scan::truncate_chars(&text, MAX_FILE_CHARS),
            ))
        })
        .collect()
}

fn propose(
    input: CodeFixInput,
    tx: &mpsc::Sender<CodeFixEvent>,
    cancel: &AtomicBool,
) -> Result<CodeFixProposal, String> {
    let CodeFixInput {
        backend,
        backend_label,
        repo_root,
        suite_name,
        targets,
        note,
    } = input;
    let _ = tx.send(step(
        1,
        "Preparing an isolated branch",
        None,
        ProgressStatus::Active,
    ));
    let top = git(&repo_root, &["rev-parse", "--show-toplevel"])
        .map(|s| PathBuf::from(s.trim()))
        .map_err(|_| format!("{} is not inside a git repository", repo_root.display()))?;
    let base = crate::directory::get_app_data_dir().join("codefix");
    let (worktree, branch) = prepare_worktree(&top, &suite_name, &base)?;
    let mut proposal = CodeFixProposal {
        repo: top.clone(),
        worktree: worktree.clone(),
        branch: branch.clone(),
        step_ids: targets.iter().map(|t| t.step_id.clone()).collect(),
        ..Default::default()
    };
    let result = (|| -> Result<(), String> {
        let _ = tx.send(step(
            1,
            "Preparing an isolated branch",
            Some(branch.clone()),
            ProgressStatus::Done,
        ));
        // Konteks kode per langkah, dibaca dari worktree.
        let service_rel = repo_root
            .canonicalize()
            .ok()
            .and_then(|r| {
                top.canonicalize()
                    .ok()
                    .and_then(|t| r.strip_prefix(t).ok().map(Path::to_path_buf))
            })
            .unwrap_or_default();
        let service_dir = worktree.join(&service_rel);
        proposal.check_dir = service_dir.clone();
        proposal.check_command = detect_check_command(&service_dir);
        let file_list = crate::repo_scan::list_repo_files(&service_dir, cancel)
            .map(|(f, _)| f)
            .unwrap_or_default();
        let mut contexts: Vec<(String, String)> = Vec::new();
        let mut wanted: Vec<String> = Vec::new();
        for t in &targets {
            let Some(src) = &t.code else { continue };
            if let Some(ctx) =
                crate::http_test_validate::code_context(&service_dir, &src.source, &file_list)
            {
                for f in context_files(&ctx) {
                    let rel = service_rel.join(&f).to_string_lossy().to_string();
                    if !wanted.contains(&rel) {
                        wanted.push(rel);
                    }
                }
                contexts.push((t.step_id.clone(), ctx));
            }
        }
        let files = read_files(&worktree, &wanted);
        let label = format!("Asking {backend_label} for a code fix");
        let _ = tx.send(step(2, label.clone(), None, ProgressStatus::Active));
        let (system, user) = build_prompts(&targets, &note, &contexts, &files);
        let workspace = crate::http_test_validate::read_only_workspace(&backend, &worktree);
        let reply = crate::repo_scan::ask_ai(
            &backend,
            system.clone(),
            user.clone(),
            workspace.clone(),
            tx,
            cancel,
        )
        .map_err(|e| e.to_string())?;
        let (mut summary, edits) = parse_edits(&reply)?;
        let mut errors = apply_edits(&worktree, &edits);
        if !errors.is_empty() && !cancel.load(Ordering::SeqCst) {
            // Satu kali coba ulang dengan isi file terbaru.
            let _ = tx.send(step(
                2,
                label.clone(),
                Some(format!("{} edit(s) did not apply, retrying", errors.len())),
                ProgressStatus::Active,
            ));
            let paths: Vec<String> = edits.iter().map(|e| e.path.clone()).collect();
            let current: String = read_files(&worktree, &paths)
                .iter()
                .map(|(p, t)| format!("\n=== {p} (current) ===\n{t}\n"))
                .collect();
            let retry_user = format!(
                "{user}\n# Your previous edits could not be applied\n{}\n\
                 Some edits may already be applied; the files below show the current content. \
                 Reply again with only the edits that are still needed.\n{current}",
                errors.join("\n")
            );
            let reply =
                crate::repo_scan::ask_ai(&backend, system, retry_user, workspace, tx, cancel)
                    .map_err(|e| e.to_string())?;
            let (s2, edits2) = parse_edits(&reply)?;
            if !s2.is_empty() {
                summary = s2;
            }
            errors = apply_edits(&worktree, &edits2);
        }
        git(&worktree, &["add", "-A"])?;
        proposal.diff = git(&worktree, &["diff", "--cached", "HEAD"])?;
        proposal.files = git(&worktree, &["diff", "--cached", "HEAD", "--name-only"])?
            .lines()
            .map(str::to_string)
            .filter(|l| !l.is_empty())
            .collect();
        if !errors.is_empty() {
            summary.push_str(&format!("\n\nNot applied: {}", errors.join("; ")));
        }
        proposal.summary = summary;
        if proposal.diff.trim().is_empty() {
            return Err(if proposal.summary.trim().is_empty() {
                "The AI did not propose any code change".into()
            } else {
                format!("No code change proposed: {}", proposal.summary.trim())
            });
        }
        let _ = tx.send(step(
            2,
            label,
            Some(format!("{} file(s) changed", proposal.files.len())),
            ProgressStatus::Done,
        ));
        Ok(())
    })();
    match result {
        Ok(()) => Ok(proposal),
        Err(e) => {
            let _ = discard(&proposal);
            Err(e)
        }
    }
}

/// Repo root untuk langkah-langkah: root yang paling banyak dipakai.
pub fn pick_repo(targets: &[CodeFixTarget], fallback: Option<PathBuf>) -> Option<PathBuf> {
    let mut counts: HashMap<PathBuf, usize> = HashMap::new();
    for t in targets {
        if let Some(c) = &t.code {
            *counts.entry(c.root.clone()).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(p, _)| p)
        .or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "tabular-codefix-{name}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&d).expect("dir");
        d
    }

    #[test]
    fn parses_and_applies_exact_edits_safely() {
        let dir = tmp("edits");
        std::fs::create_dir_all(dir.join("src")).expect("src");
        std::fs::write(
            dir.join("src/a.rs"),
            "fn a() {\n    1\n}\nfn b() {\n    1\n}\n",
        )
        .expect("a");
        let (summary, edits) = parse_edits(
            r#"{"summary":"fix b","edits":[
              {"path":"src/a.rs","search":"fn b() {\n    1","replace":"fn b() {\n    2"},
              {"path":"src/a.rs","search":"}\n","replace":"x"},
              {"path":"../evil.rs","search":"","replace":"x"},
              {"path":"src/new.rs","search":"","replace":"pub fn n() {}\n"},
              {"path":"src/missing.rs","search":"y","replace":"z"}
            ]}"#,
        )
        .expect("parse");
        assert_eq!(summary, "fix b");
        let errors = apply_edits(&dir, &edits);
        let text = std::fs::read_to_string(dir.join("src/a.rs")).expect("read");
        assert!(text.contains("fn b() {\n    2"));
        assert!(
            text.contains("fn a() {\n    1"),
            "ambiguous edit must not apply"
        );
        assert!(dir.join("src/new.rs").is_file());
        assert_eq!(errors.len(), 3, "{errors:?}");
        assert!(errors.iter().any(|e| e.contains("matches 2 places")));
        assert!(errors.iter().any(|e| e.contains("outside the repository")));
        assert!(errors.iter().any(|e| e.contains("not found")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn finds_files_named_in_code_context() {
        let ctx = "// src/main.rs:84 (route)\nx\n// src/auth/routes.rs:33 (handler register)\ny\n// src/auth/routes.rs:10 (type RegisterBody)\n";
        assert_eq!(
            context_files(ctx),
            vec!["src/main.rs", "src/auth/routes.rs"]
        );
    }

    #[test]
    fn worktree_flow_keeps_user_branch_untouched() {
        if crate::agent::harness::resolve_binary("git").is_none() {
            return;
        }
        let repo = tmp("repo");
        let run = |args: &[&str]| git(&repo, args).expect("git");
        run(&["init", "-q"]);
        std::fs::write(repo.join("app.txt"), "status = 500\n").expect("write");
        run(&["add", "-A"]);
        git_commit(&repo, "init").expect("commit");
        // Perubahan belum di-commit milik user ikut ke worktree.
        std::fs::write(repo.join("app.txt"), "status = 500\nuser edit\n").expect("write");
        let head_before = run(&["rev-parse", "HEAD"]);

        let base = tmp("wt-base");
        let (wt, branch) = prepare_worktree(&repo, "My Suite", &base).expect("worktree");
        assert!(branch.starts_with("tabular/fix-my-suite-"));
        let in_wt = std::fs::read_to_string(wt.join("app.txt")).expect("read");
        assert!(in_wt.contains("user edit"));
        let errors = apply_edits(
            &wt,
            &[FileEdit {
                path: "app.txt".into(),
                search: "status = 500".into(),
                replace: "status = 200".into(),
            }],
        );
        assert!(errors.is_empty());
        let p = CodeFixProposal {
            repo: repo.clone(),
            worktree: wt.clone(),
            branch: branch.clone(),
            diff: git(&wt, &["diff", "HEAD"]).expect("diff"),
            summary: "return 200".into(),
            ..Default::default()
        };
        assert!(p.diff.contains("+status = 200"));
        assert!(
            !p.diff.contains("+user edit"),
            "baseline must hide user changes"
        );
        apply_to_working_copy(&p).expect("apply");
        let now = std::fs::read_to_string(repo.join("app.txt")).expect("read");
        assert_eq!(now, "status = 200\nuser edit\n");
        assert_eq!(
            run(&["rev-parse", "HEAD"]),
            head_before,
            "no commit on user branch"
        );
        assert!(!wt.exists());
        assert!(run(&["branch", "--list", &branch]).trim().is_empty());
        let _ = std::fs::remove_dir_all(&repo);
    }

    #[test]
    fn keep_branch_commits_only_on_the_fix_branch() {
        if crate::agent::harness::resolve_binary("git").is_none() {
            return;
        }
        let repo = tmp("keep");
        let run = |args: &[&str]| git(&repo, args).expect("git");
        run(&["init", "-q"]);
        std::fs::write(repo.join("a.txt"), "a\n").expect("write");
        run(&["add", "-A"]);
        git_commit(&repo, "init").expect("commit");
        let head = run(&["rev-parse", "HEAD"]);
        let base = tmp("wt-base2");
        let (wt, branch) = prepare_worktree(&repo, "s", &base).expect("wt");
        std::fs::write(wt.join("a.txt"), "b\n").expect("write");
        let p = CodeFixProposal {
            repo: repo.clone(),
            worktree: wt,
            branch: branch.clone(),
            summary: "change a".into(),
            ..Default::default()
        };
        assert_eq!(keep_branch(&p).expect("keep"), branch);
        assert_eq!(run(&["rev-parse", "HEAD"]), head);
        assert_eq!(
            std::fs::read_to_string(repo.join("a.txt")).expect("r"),
            "a\n"
        );
        let log = run(&["log", "--format=%s", &branch, "-1"]);
        assert!(log.starts_with("fix: change a"));
        let _ = std::fs::remove_dir_all(&repo);
    }
}
