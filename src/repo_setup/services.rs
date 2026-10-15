//! Pengelompokan route menjadi service: satu repository bisa berisi satu
//! aplikasi (monolith) atau beberapa microservice (monorepo).
//!
//! Folder service adalah folder yang punya manifest proyek (`package.json`,
//! `go.mod`, `pom.xml`, …) atau menjadi `build.context` di docker-compose.
//! Setiap route dimiliki folder service terdalam yang memuat file-nya.
//! Folder tanpa route (worker, library, frontend) tidak menjadi service API.

use std::collections::BTreeMap;
use std::path::Path;

use crate::repo_endpoints::{self, GeneratedEndpoint, RouteHit};

const MANIFESTS: &[&str] = &[
    "package.json",
    "go.mod",
    "Cargo.toml",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "pyproject.toml",
    "requirements.txt",
    "Pipfile",
    "setup.py",
    "composer.json",
    "Gemfile",
    "mix.exs",
];

/// Nama file manifest proyek.
pub fn is_manifest(name: &str) -> bool {
    MANIFESTS.contains(&name) || name.ends_with(".csproj") || name.ends_with(".fsproj")
}

/// Satu service API yang ditemukan.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredService {
    pub name: String,
    /// Folder relatif terhadap root repository (pemisah `/`, kosong = root).
    pub rel_dir: String,
    pub base_url: String,
    pub endpoints: Vec<GeneratedEndpoint>,
    pub framework: String,
}

/// Kandidat folder service dari docker-compose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeHint {
    pub service: String,
    pub rel_dir: String,
    pub base_url: Option<String>,
}

fn owns(dir: &str, file: &str) -> bool {
    dir.is_empty() || file.starts_with(&format!("{dir}/"))
}

/// Kelompokkan `hits` ke folder service. `manifest_dirs` = folder yang punya
/// manifest; `compose` = petunjuk nama dan base URL dari docker-compose.
pub fn group(
    root: &Path,
    repo_name: &str,
    manifest_dirs: &[String],
    compose: &[ComposeHint],
    hits: &[RouteHit],
) -> Vec<DiscoveredService> {
    let mut dirs: Vec<String> = manifest_dirs.to_vec();
    for c in compose {
        if !dirs.contains(&c.rel_dir) {
            dirs.push(c.rel_dir.clone());
        }
    }
    let mut by_dir: BTreeMap<String, Vec<RouteHit>> = BTreeMap::new();
    for hit in hits {
        let owner = dirs
            .iter()
            .filter(|d| owns(d, &hit.file))
            .max_by_key(|d| d.len())
            .cloned()
            .unwrap_or_default();
        by_dir.entry(owner).or_default().push(hit.clone());
    }
    let single = by_dir.len() == 1;
    let mut used_names: Vec<String> = Vec::new();
    by_dir
        .into_iter()
        .map(|(rel_dir, hits)| {
            let hint = compose.iter().find(|c| c.rel_dir == rel_dir);
            // Aplikasi tunggal selalu bernama repository: nama service
            // compose seperti `app`/`api` terlalu umum.
            let mut name = if single {
                repo_name.to_string()
            } else if let Some(h) = hint {
                h.service.clone()
            } else if rel_dir.is_empty() {
                repo_name.to_string()
            } else {
                rel_dir.rsplit('/').next().unwrap_or(&rel_dir).to_string()
            };
            if used_names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
                name = rel_dir.replace('/', "-");
            }
            used_names.push(name.clone());
            let abs = if rel_dir.is_empty() {
                root.to_path_buf()
            } else {
                root.join(&rel_dir)
            };
            let mut endpoints = repo_endpoints::merge_endpoints(
                Vec::new(),
                hits.iter().map(GeneratedEndpoint::from_hit).collect(),
            );
            repo_endpoints::enrich_flexurio_endpoints(&abs, &mut endpoints);
            let framework = top_framework(&hits);
            let base_url = hint
                .and_then(|h| h.base_url.clone())
                .or_else(|| base_url_from_env(&abs))
                .or_else(|| {
                    (framework == "flexurio")
                        .then(|| crate::flexurio_import::detect_flexurio_base_url(&abs))
                        .flatten()
                })
                .unwrap_or_else(|| repo_endpoints::default_base_url(&hits));
            DiscoveredService {
                name,
                rel_dir,
                base_url,
                endpoints,
                framework,
            }
        })
        .collect()
}

/// Base URL dari `.env` service: `APP_URL`/`BASE_URL` bila berupa URL
/// http, atau `PORT`/`APP_PORT`/`SERVER_PORT`/`HTTP_PORT`.
fn base_url_from_env(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join(".env")).ok()?;
    let vars = super::env_files::parse(&text);
    let get = |keys: &[&str]| {
        keys.iter().find_map(|k| {
            vars.iter()
                .find(|(vk, _)| vk.eq_ignore_ascii_case(k))
                .map(|(_, v)| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
    };
    if let Some(url) = get(&["BASE_URL", "API_URL", "APP_URL"])
        .filter(|u| u.starts_with("http://") || u.starts_with("https://"))
    {
        return Some(url.trim_end_matches('/').to_string());
    }
    get(&["PORT", "APP_PORT", "SERVER_PORT", "HTTP_PORT"])
        .filter(|p| p.chars().all(|c| c.is_ascii_digit()))
        .map(|p| format!("http://localhost:{p}"))
}

fn top_framework(hits: &[RouteHit]) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for h in hits {
        *counts.entry(h.framework).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(f, c)| (*c, *f))
        .map(|(f, _)| f.to_string())
        .unwrap_or_default()
}

/// Service pemilik file `rel` (folder terdalam), bila ada.
pub fn owner_of<'a>(services: &'a [DiscoveredService], rel: &str) -> Option<&'a DiscoveredService> {
    services
        .iter()
        .filter(|s| owns(&s.rel_dir, rel))
        .max_by_key(|s| s.rel_dir.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(file: &str, path: &str) -> RouteHit {
        RouteHit {
            method: "GET".into(),
            path: path.into(),
            file: file.into(),
            line: 1,
            framework: "express",
        }
    }

    #[test]
    fn single_app_is_named_after_repository() {
        let root = std::env::temp_dir().join(format!("tabular-svc-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("mkdir");
        let s = group(
            &root,
            "shop",
            &["".into()],
            &[],
            &[hit("src/a.ts", "/a"), hit("src/b.ts", "/b")],
        );
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].name, "shop");
        assert_eq!(s[0].endpoints.len(), 2);
        assert_eq!(s[0].base_url, "http://localhost:3000");
        std::fs::write(root.join(".env"), "APP_URL=http://shop.test/\nPORT=9000\n").expect("write");
        let s = group(&root, "shop", &["".into()], &[], &[hit("src/a.ts", "/a")]);
        assert_eq!(s[0].base_url, "http://shop.test");
        let _ = std::fs::remove_file(root.join(".env"));
    }

    #[test]
    fn monorepo_splits_routes_by_deepest_manifest() {
        let root = std::env::temp_dir();
        let s = group(
            &root,
            "platform",
            &[
                "".into(),
                "services/orders".into(),
                "services/users".into(),
                "web".into(),
            ],
            &[ComposeHint {
                service: "orders-api".into(),
                rel_dir: "services/orders".into(),
                base_url: Some("http://localhost:8081".into()),
            }],
            &[
                hit("services/orders/src/r.ts", "/orders"),
                hit("services/orders/src/r.ts", "/orders/{id}"),
                hit("services/users/main.go", "/users"),
            ],
        );
        assert_eq!(s.len(), 2, "{s:?}");
        let orders = s
            .iter()
            .find(|x| x.rel_dir == "services/orders")
            .expect("orders");
        assert_eq!(orders.name, "orders-api");
        assert_eq!(orders.base_url, "http://localhost:8081");
        assert_eq!(orders.endpoints.len(), 2);
        let users = s
            .iter()
            .find(|x| x.rel_dir == "services/users")
            .expect("users");
        assert_eq!(users.name, "users");
        assert_eq!(
            owner_of(&s, "services/users/.env").map(|x| x.name.as_str()),
            Some("users")
        );
        assert!(owner_of(&s, ".env").is_none());
    }
}
