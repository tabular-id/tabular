//! Group diagram per service: setiap service repository menjadi satu group
//! (bertaut ke repository lewat URL git) berisi tabel yang dipakai kodenya.

use std::collections::HashMap;

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

/// Perubahan hasil [`sync_groups`] yang dihitung di thread latar atas
/// salinan state, lalu ditempel ke state asli (tab terbuka atau file) di
/// tempat lain. Hanya group yang dikelola dan node yang posisi/keanggotaan
/// group-nya berubah yang dibawa, sehingga suntingan user lain di state asli
/// tetap utuh.
#[derive(Debug, Clone, Default)]
pub struct GroupPatch {
    pub groups: Vec<DiagramGroup>,
    pub nodes: Vec<NodePatch>,
}

/// Posisi dan keanggotaan group satu node setelah sync.
#[derive(Debug, Clone)]
pub struct NodePatch {
    pub id: String,
    pub pos: eframe::egui::Pos2,
    pub group_id: Option<String>,
    pub group_ids: Vec<String>,
}

impl GroupPatch {
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.nodes.is_empty()
    }

    /// Jalankan [`sync_groups`] pada `state` (milik thread pemanggil) dan
    /// kembalikan hasilnya beserta patch terhadap state sebelum sync.
    pub fn compute(
        mut state: DiagramState,
        repo_url: Option<&str>,
        services: &[ServiceTables],
    ) -> (GroupSync, GroupPatch) {
        let before: HashMap<String, (eframe::egui::Pos2, Option<String>, Vec<String>)> = state
            .nodes
            .iter()
            .map(|n| {
                (
                    n.id.clone(),
                    (n.pos, n.group_id.clone(), n.group_ids.clone()),
                )
            })
            .collect();
        let sync = sync_groups(&mut state, repo_url, services);
        if !sync.changed() && sync.groups.is_empty() {
            return (sync, GroupPatch::default());
        }
        let managed: Vec<&str> = sync.groups.iter().map(|(g, _)| g.as_str()).collect();
        let groups = state
            .groups
            .iter()
            .filter(|g| managed.contains(&g.id.as_str()))
            .cloned()
            .collect();
        let nodes = state
            .nodes
            .into_iter()
            .filter(|n| {
                before.get(&n.id).is_none_or(|(pos, gid, gids)| {
                    *pos != n.pos || *gid != n.group_id || *gids != n.group_ids
                })
            })
            .map(|n| NodePatch {
                id: n.id,
                pos: n.pos,
                group_id: n.group_id,
                group_ids: n.group_ids,
            })
            .collect();
        (sync, GroupPatch { groups, nodes })
    }

    /// Tempel patch ke `state`. Node yang sudah tidak ada dilewati.
    pub fn apply(&self, state: &mut DiagramState) {
        for g in &self.groups {
            match state.groups.iter_mut().find(|x| x.id == g.id) {
                Some(existing) => {
                    if existing.repo_url.is_none() {
                        existing.repo_url = g.repo_url.clone();
                    }
                }
                None => state.groups.push(g.clone()),
            }
        }
        let by_id: HashMap<&str, &NodePatch> =
            self.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
        for node in &mut state.nodes {
            if let Some(p) = by_id.get(node.id.as_str()) {
                node.pos = p.pos;
                node.group_id = p.group_id.clone();
                node.group_ids = p.group_ids.clone();
            }
        }
        if !self.is_empty() {
            state.save_requested = true;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::structs::DiagramNode;
    use eframe::egui::{pos2, vec2};

    fn node(id: &str, x: f32) -> DiagramNode {
        DiagramNode {
            id: id.to_string(),
            title: id.to_string(),
            pos: pos2(x, 0.0),
            size: vec2(100.0, 80.0),
            ..Default::default()
        }
    }

    fn state() -> DiagramState {
        DiagramState {
            nodes: vec![
                node("users", 0.0),
                node("orders", 500.0),
                node("misc", 1000.0),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn patch_dari_salinan_menempatkan_tabel_tanpa_menimpa_suntingan_lain() {
        let services = vec![ServiceTables {
            service: "billing".into(),
            dir: "/repo/billing".into(),
            table_ids: vec!["users".into(), "orders".into()],
        }];
        let (sync, patch) = GroupPatch::compute(state(), None, &services);
        assert_eq!(sync.groups_created, 1);
        assert_eq!(sync.tables_added, 2);
        assert!(patch.nodes.iter().all(|n| n.id != "misc"));

        // State asli sempat disunting user selagi thread bekerja.
        let mut live = state();
        live.nodes[2].pos = pos2(42.0, 42.0);
        patch.apply(&mut live);

        let gid = &sync.groups[0].0;
        assert!(live.groups.iter().any(|g| &g.id == gid));
        assert!(live.nodes[0].is_in_group(gid));
        assert!(live.nodes[1].is_in_group(gid));
        assert_eq!(live.nodes[2].pos, pos2(42.0, 42.0));
        assert!(live.save_requested);
    }

    #[test]
    fn tanpa_tabel_tidak_ada_patch() {
        let services = vec![ServiceTables {
            service: "billing".into(),
            dir: "/repo/billing".into(),
            table_ids: vec![],
        }];
        let (sync, patch) = GroupPatch::compute(state(), None, &services);
        assert!(!sync.changed());
        assert!(patch.is_empty());
    }
}
