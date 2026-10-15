//! Group diagram per service: setiap service repository menjadi satu group
//! (bertaut ke repository lewat URL git) berisi tabel yang dipakai kodenya.

use crate::models::structs::{DiagramGroup, DiagramState};
use crate::repo_scan::{Candidate, ScanReport};

/// Tabel yang dipakai satu service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTables {
    pub service: String,
    /// Folder service (absolut) untuk folder project group.
    pub dir: String,
    /// Id node diagram.
    pub table_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupSync {
    pub groups_created: usize,
    pub tables_added: usize,
    /// `(id group, folder service)` yang dikelola; folder project personal
    /// group diisi pemanggil.
    pub groups: Vec<(String, String)>,
}

impl GroupSync {
    pub fn changed(&self) -> bool {
        self.groups_created > 0 || self.tables_added > 0
    }
}

const PALETTE: [(u8, u8, u8); 8] = [
    (100, 149, 237),
    (46, 160, 67),
    (217, 119, 6),
    (168, 85, 247),
    (220, 38, 38),
    (13, 148, 136),
    (219, 39, 119),
    (101, 163, 13),
];

/// Kandidat pencarian teks dari node diagram.
pub fn candidates(state: &DiagramState) -> Vec<Candidate> {
    state
        .nodes
        .iter()
        .map(|n| Candidate {
            id: n.id.clone(),
            title: n.title.clone(),
        })
        .collect()
}

/// Tabel yang cukup yakin dipakai: ada bukti konteks SQL/ORM, atau dirujuk
/// berulang kali.
pub fn confident_ids(report: &ScanReport) -> Vec<String> {
    report
        .hits
        .iter()
        .filter(|h| h.evidence.iter().any(|e| e.strong) || h.refs >= 3)
        .map(|h| h.id.clone())
        .collect()
}

/// Buat/temukan group per service dan tambahkan tabelnya.
pub fn sync_groups(
    state: &mut DiagramState,
    repo_url: Option<&str>,
    services: &[ServiceTables],
) -> GroupSync {
    let mut out = GroupSync::default();
    for (i, svc) in services.iter().enumerate() {
        // Group yang sudah ada dipakai ulang: judul sama, atau (aplikasi
        // tunggal) group yang sudah menunjuk repository yang sama.
        let key = repo_url.and_then(crate::repo_scan::repo_key);
        let existing = state
            .groups
            .iter()
            .position(|g| g.title.eq_ignore_ascii_case(&svc.service))
            .or_else(|| {
                (services.len() == 1 && key.is_some())
                    .then(|| state.groups.iter().position(|g| g.repo_key() == key))
                    .flatten()
            });
        let gid = match existing {
            Some(idx) => {
                let g = &mut state.groups[idx];
                if g.shared_repo_url().is_none()
                    && let Some(url) = repo_url
                {
                    g.repo_url = Some(url.to_string());
                }
                g.id.clone()
            }
            None => {
                if svc.table_ids.is_empty() {
                    continue;
                }
                let id = format!(
                    "{:x}",
                    md5::compute(format!(
                        "{}:{}:{}",
                        repo_url.unwrap_or(&svc.dir),
                        svc.service,
                        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                    ))
                );
                let (r, g, b) = PALETTE[(state.groups.len() + i) % PALETTE.len()];
                state.groups.push(DiagramGroup {
                    id: id.clone(),
                    title: svc.service.clone(),
                    color: eframe::egui::Color32::from_rgb(r, g, b),
                    manual_pos: None,
                    repo_url: repo_url.map(str::to_string),
                });
                out.groups_created += 1;
                id
            }
        };
        let ids: Vec<String> = svc
            .table_ids
            .iter()
            .filter(|id| {
                state
                    .nodes
                    .iter()
                    .any(|n| &n.id == *id && !n.is_in_group(&gid))
            })
            .cloned()
            .collect();
        if !ids.is_empty() {
            out.tables_added += crate::diagram_repo::add_tables_to_group(state, &gid, &ids, true);
        }
        out.groups.push((gid, svc.dir.clone()));
    }
    out
}
