//! Perbandingan [`SetupPlan`] baru dengan catatan setup sebelumnya, untuk
//! ringkasan perubahan dan penandaan item yang hilang (stale).

use super::discover::SetupPlan;
use super::store::RepoSetup;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetupDiff {
    /// Indeks `plan.connections` yang belum tercatat.
    pub new_connections: Vec<usize>,
    /// Indeks `plan.connections` yang nilainya berubah.
    pub changed_connections: Vec<usize>,
    /// Identitas koneksi tercatat yang tidak ada lagi di repository.
    pub gone_connections: Vec<String>,
    /// Indeks `plan.services` yang belum tercatat.
    pub new_services: Vec<usize>,
    /// Folder relatif service tercatat yang hilang.
    pub gone_services: Vec<String>,
}

impl SetupDiff {
    pub fn is_empty(&self) -> bool {
        self.new_connections.is_empty()
            && self.changed_connections.is_empty()
            && self.gone_connections.is_empty()
            && self.new_services.is_empty()
            && self.gone_services.is_empty()
    }
}

pub fn diff(plan: &SetupPlan, setup: &RepoSetup) -> SetupDiff {
    let mut d = SetupDiff::default();
    for (i, c) in plan.connections.iter().enumerate() {
        match setup
            .connections
            .iter()
            .find(|t| t.identity == c.identity())
        {
            None => d.new_connections.push(i),
            Some(t) if t.digest != c.digest() || t.stale => d.changed_connections.push(i),
            Some(_) => {}
        }
    }
    for t in &setup.connections {
        if !t.stale && !plan.connections.iter().any(|c| c.identity() == t.identity) {
            d.gone_connections.push(t.identity.clone());
        }
    }
    for (i, s) in plan.services.iter().enumerate() {
        if !setup
            .services
            .iter()
            .any(|t| t.rel_dir == s.rel_dir && !t.stale)
        {
            d.new_services.push(i);
        }
    }
    for t in &setup.services {
        if !t.stale && !plan.services.iter().any(|s| s.rel_dir == t.rel_dir) {
            d.gone_services.push(t.rel_dir.clone());
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_env::Environment;
    use crate::deeplink::parse_dsn;
    use crate::repo_setup::discover::PlannedConnection;
    use crate::repo_setup::services::DiscoveredService;
    use crate::repo_setup::store::{TrackedConnection, TrackedService};

    fn conn(key: &str, url: &str) -> PlannedConnection {
        PlannedConnection {
            dsn: parse_dsn(url).expect("dsn"),
            environment: Environment::Development,
            source: ".env".into(),
            key: key.into(),
            service: None,
            needs_credentials: false,
            name: key.into(),
            note: None,
        }
    }

    fn plan(conns: Vec<PlannedConnection>, services: &[&str]) -> SetupPlan {
        SetupPlan {
            root: std::env::temp_dir(),
            name: "x".into(),
            key: "k".into(),
            remote_url: None,
            connections: conns,
            services: services
                .iter()
                .map(|d| DiscoveredService {
                    name: d.to_string(),
                    rel_dir: d.to_string(),
                    base_url: String::new(),
                    endpoints: Vec::new(),
                    framework: String::new(),
                })
                .collect(),
            env_vars: Vec::new(),
            watched_files: Vec::new(),
            fingerprint: String::new(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn detects_new_changed_and_gone_items() {
        let a = conn("A", "postgres://u:p@localhost/a");
        let b = conn("B", "mysql://u:p@localhost/b");
        let mut setup = RepoSetup::new("k", "/x", "x");
        setup.connections.push(TrackedConnection {
            identity: a.identity(),
            digest: "old".into(),
            ..Default::default()
        });
        setup.connections.push(TrackedConnection {
            identity: "development|.env#GONE".into(),
            ..Default::default()
        });
        setup.services.push(TrackedService {
            rel_dir: "old".into(),
            ..Default::default()
        });
        let p = plan(vec![a.clone(), b], &["svc"]);
        let d = diff(&p, &setup);
        assert_eq!(d.changed_connections, vec![0]);
        assert_eq!(d.new_connections, vec![1]);
        assert_eq!(
            d.gone_connections,
            vec!["development|.env#GONE".to_string()]
        );
        assert_eq!(d.new_services, vec![0]);
        assert_eq!(d.gone_services, vec!["old".to_string()]);

        setup.connections[0].digest = a.digest();
        setup.connections.truncate(1);
        setup.services = vec![TrackedService {
            rel_dir: "svc".into(),
            ..Default::default()
        }];
        let d = diff(&plan(vec![a], &["svc"]), &setup);
        assert!(d.is_empty(), "{d:?}");
    }
}
