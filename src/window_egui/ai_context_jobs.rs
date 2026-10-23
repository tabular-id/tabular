//! Job latar panel AI Assistant: merakit konteks tambahan (relasi dan index,
//! endpoint API, proses bisnis, catatan diagram, pemakaian di kode + commit
//! git, query history) untuk satu giliran chat, lalu memulai giliran itu
//! setelah konteks siap. Grep repository dan `git log` tidak pernah berjalan
//! di UI thread.

use std::sync::mpsc;
use std::time::Instant;

use crate::agent::context::{ContextBundle, ContextOptions, ContextRequest};
use crate::ai_assistant::{ChatBackend, ChatPromptParts};

use super::Tabular;

/// Satu perakitan konteks yang sedang berjalan beserta giliran yang menunggu.
pub struct AiContextJob {
    rx: mpsc::Receiver<Result<ContextBundle, String>>,
    cfg: ChatBackend,
    parts: ChatPromptParts,
    session_id: Option<String>,
    started: Instant,
}

const TOOL_NAME: &str = "context";

/// Permintaan konteks dari state UI. `None` bila tidak ada koneksi aktif.
pub fn context_request(
    t: &Tabular,
    cfg: &ChatBackend,
    question: &str,
    editor_text: &str,
) -> Option<ContextRequest> {
    let tab = t.query_tabs.get(t.active_tab_index);
    let connection_id = tab
        .and_then(|tab| tab.connection_id)
        .or(t.current_connection_id)?;
    let database = tab
        .and_then(|tab| tab.database_name.clone())
        .filter(|d| !d.trim().is_empty());
    let project_id = super::project_ui::current_project(t).map(|p| p.id.clone());
    Some(ContextRequest {
        connection_id,
        database,
        question: question.to_string(),
        editor_text: editor_text.to_string(),
        tables: Vec::new(),
        options: ContextOptions {
            api: t.ai_context_api,
            git: t.ai_context_git,
            history: t.ai_context_history,
            // Agent dengan MCP bisa membaca file sendiri; cukup `path:line`.
            code_snippets: !cfg.mcp_available,
            project_id,
        },
    })
}

/// Mulai perakitan konteks di thread latar. Bila tidak ada yang perlu dirakit
/// (tanpa koneksi, tanpa pool, atau tanpa runtime), giliran langsung dikirim.
pub fn start(
    t: &mut Tabular,
    cfg: ChatBackend,
    parts: ChatPromptParts,
    session_id: Option<String>,
) {
    let req = context_request(t, &cfg, &parts.request, &parts.editor_context);
    let (Some(req), Some(pool), Some(rt)) = (req, t.db_pool.clone(), t.runtime.clone()) else {
        let (system, user) = parts.join("");
        launch_turn(t, &cfg, system, user, session_id);
        return;
    };

    let (tx, rx) = mpsc::channel();
    let app_dir = crate::config::get_data_dir();
    let spawned = std::thread::Builder::new()
        .name("ai-context".into())
        .spawn(move || {
            let session =
                crate::agent::core::HeadlessSession::new((*pool).clone()).with_app_dir(app_dir);
            let result = rt
                .block_on(session.table_context(&req))
                .map_err(|e| e.to_string());
            let _ = tx.send(result);
        });
    if let Err(e) = spawned {
        log::warn!("[AI] cannot start context thread, sending without context: {e}");
        let (system, user) = parts.join("");
        launch_turn(t, &cfg, system, user, session_id);
        return;
    }

    if let Some(msg) = t.ai_chat.last_mut() {
        msg.progress_steps
            .push(crate::agent::harness::ProgressStep {
                step_index: Some(0),
                description: "Gathering context (relations, APIs, code, history)…".to_string(),
                detail: None,
                status: crate::agent::harness::ProgressStatus::Active,
                tool_name: Some(TOOL_NAME.to_string()),
            });
    }
    t.ai_is_loading = true;
    t.ai_context_job = Some(AiContextJob {
        rx,
        cfg,
        parts,
        session_id,
        started: Instant::now(),
    });
}

/// Buang job yang berjalan (Stop ditekan). Thread-nya selesai sendiri; hasilnya
/// diabaikan karena receiver sudah hilang.
pub fn cancel(t: &mut Tabular) -> bool {
    t.ai_context_job.take().is_some()
}

/// Periksa job tiap frame; begitu konteks siap, prompt dilengkapi dan giliran
/// dikirim ke backend.
pub fn poll(t: &mut Tabular, ctx: &eframe::egui::Context) {
    let Some(job) = t.ai_context_job.take() else {
        return;
    };
    match job.rx.try_recv() {
        Ok(result) => finish(t, job, result),
        Err(mpsc::TryRecvError::Disconnected) => {
            finish(t, job, Err("context thread stopped".to_string()))
        }
        Err(mpsc::TryRecvError::Empty) => {
            t.ai_context_job = Some(job);
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }
    }
}

fn finish(t: &mut Tabular, job: AiContextJob, result: Result<ContextBundle, String>) {
    let (extra, summary) = match result {
        Ok(bundle) => {
            let summary = if bundle.stats.is_empty() {
                "No extra context found for this request".to_string()
            } else {
                bundle.stats.summary()
            };
            if !bundle.hints.is_empty() {
                log::info!(
                    "[AI] context hints: {} ({} ms)",
                    bundle.hints.join("; "),
                    job.started.elapsed().as_millis()
                );
            }
            (bundle.markdown, summary)
        }
        Err(e) => {
            log::warn!("[AI] context assembly failed, sending without it: {e}");
            (String::new(), "Context unavailable".to_string())
        }
    };
    if let Some(msg) = t.ai_chat.last_mut() {
        if let Some(step) = msg
            .progress_steps
            .iter_mut()
            .find(|s| s.tool_name.as_deref() == Some(TOOL_NAME))
        {
            step.status = crate::agent::harness::ProgressStatus::Done;
            step.description = format!("Context: {summary}");
        }
        msg.context_summary = Some(summary);
    }
    let (system, user) = job.parts.join(&extra);
    launch_turn(t, &job.cfg, system, user, job.session_id);
}

/// Kirim giliran ke backend dan pasang receiver stream di state UI.
pub fn launch_turn(
    t: &mut Tabular,
    cfg: &ChatBackend,
    system: String,
    user: String,
    session_id: Option<String>,
) {
    match super::ai_mcp_ui::start_chat_turn(t, cfg, system, user, session_id) {
        Ok((rx, cancel)) => {
            t.ai_stream_receiver = Some(rx);
            t.ai_cancel = cancel;
            t.ai_is_loading = true;
            t.ai_live_edit_parser = Some(crate::agent::live_edit::LiveEditParser::default());
            t.ai_live_edit_active = None;
        }
        Err(e) => {
            t.ai_turn_target = None;
            t.ai_is_loading = false;
            log::warn!("[AGENT] failed to start chat turn: {e}");
            if let Some(msg) = t.ai_chat.last_mut() {
                msg.streaming = false;
                msg.error = Some(e);
            }
        }
    }
}
