//! Discovery lengkap satu repository menjadi [`SetupPlan`]: koneksi per
//! environment, variabel environment, service API beserta endpoint-nya,
//! dan file yang diawasi. Murni membaca disk dan menjalankan `git`; tidak
//! menyentuh `Tabular`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use sha2::{Digest, Sha256};

use super::RepoSetupError;
use super::compose::{self, ComposeInfo};
use super::config_files;
use super::dsn_detect::{self, FoundDsn};
use super::env_files;
use super::fingerprint;
use super::services::{self, ComposeHint, DiscoveredService};
use crate::connection_env::Environment;
use crate::deeplink::ParsedDsn;
use crate::models::enums::DatabaseType;

/// Kedalaman folder maksimum yang ditelusuri untuk file env/konfigurasi.
const MAX_DEPTH: usize = 6;
/// Batas entri yang ditelusuri (repository raksasa).
const MAX_ENTRIES: usize = 200_000;
/// Diagram yang dibuka otomatis paling banyak.
const MAX_DIAGRAMS: usize = 4;

/// Koneksi database yang akan dibuat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedConnection {
    pub dsn: ParsedDsn,
    pub environment: Environment,
    /// File asal relatif terhadap root repository.
    pub source: String,
    /// Variabel/kunci asal di file itu.
    pub key: String,
    pub service: Option<String>,
    pub needs_credentials: bool,
    pub name: String,
    /// Catatan untuk user (mis. port container tidak dipublikasikan).
    pub note: Option<String>,
}

impl PlannedConnection {
    /// Identitas stabil (tidak berubah walau nilai DSN berubah).
    pub fn identity(&self) -> String {
        format!("{}|{}#{}", self.environment.key(), self.source, self.key)
    }

    /// Hash DSN lengkap untuk mendeteksi perubahan nilai.
    pub fn digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.dsn.to_dsn_string(true).as_bytes());
        h.update([u8::from(self.dsn.ssl)]);
        hex::encode(h.finalize())
    }
}

/// Hasil discovery satu repository.
#[derive(Debug, Clone)]
pub struct SetupPlan {
    pub root: PathBuf,
    pub name: String,
    /// Kunci repository sama seperti tab Git.
    pub key: String,
    /// URL remote tanpa kredensial (ikut ke folder HTTP / group diagram).
    pub remote_url: Option<String>,
    pub connections: Vec<PlannedConnection>,
    pub services: Vec<DiscoveredService>,
    /// Variabel per environment untuk environment project.
    pub env_vars: Vec<(Environment, Vec<(String, String)>)>,
    pub watched_files: Vec<String>,
    pub fingerprint: String,
    pub warnings: Vec<String>,
}

impl SetupPlan {
    pub fn is_microservices(&self) -> bool {
        self.services.len() > 1
    }

    pub fn endpoint_count(&self) -> usize {
        self.services.iter().map(|s| s.endpoints.len()).sum()
    }

    /// Koneksi SQL non-production yang diagramnya dibentuk: satu per
    /// service (atau satu untuk repository), maksimal [`MAX_DIAGRAMS`].
    pub fn diagram_targets(&self) -> Vec<&PlannedConnection> {
        let rank = |e: Environment| match e {
            Environment::Development => 0,
            Environment::Local => 1,
            Environment::Testing => 2,
            Environment::Staging => 3,
            Environment::Production => 9,
        };
        let mut cands: Vec<&PlannedConnection> = self
            .connections
            .iter()
            .filter(|c| {
                c.environment != Environment::Production
                    && !c.needs_credentials
                    && (c.dsn.db_type == DatabaseType::SQLite || !c.dsn.database.trim().is_empty())
                    && matches!(
                        c.dsn.db_type,
                        DatabaseType::PostgreSQL
                            | DatabaseType::MySQL
                            | DatabaseType::MsSQL
                            | DatabaseType::SQLite
                    )
            })
            .collect();
        cands.sort_by_key(|c| rank(c.environment));
        let mut out: Vec<&PlannedConnection> = Vec::new();
        for c in cands {
            let same_service = out.iter().any(|o| o.service == c.service);
            let same_db = out.iter().any(|o| {
                dsn_detect::server_identity(&o.dsn) == dsn_detect::server_identity(&c.dsn)
            });
            if !same_service && !same_db && out.len() < MAX_DIAGRAMS {
                out.push(c);
            }
        }
        out
    }
}

/// File yang menarik bagi discovery, hasil satu kali penelusuran.
#[derive(Debug, Default)]
struct Files {
    env: Vec<String>,
    compose: Vec<String>,
    config: Vec<String>,
    manifest_dirs: Vec<String>,
}

fn walk(root: &Path, cancel: &AtomicBool) -> Result<Files, RepoSetupError> {
    let mut files = Files::default();
    let mut stack: Vec<(PathBuf, String, usize)> = vec![(root.to_path_buf(), String::new(), 0)];
    let mut seen = 0usize;
    'outer: while let Some((dir, rel, depth)) = stack.pop() {
        if cancel.load(Ordering::SeqCst) {
            return Err(RepoSetupError::Cancelled);
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > MAX_ENTRIES {
                break 'outer;
            }
            let Ok(ft) = entry.file_type() else { continue };
            let name = entry.file_name().to_string_lossy().to_string();
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if ft.is_dir() {
                if depth + 1 < MAX_DEPTH
                    && !name.starts_with('.')
                    && !crate::repo_scan::SKIP_DIRS.contains(&name.as_str())
                {
                    stack.push((entry.path(), child_rel, depth + 1));
                }
                continue;
            }
            if !ft.is_file() {
                continue;
            }
            if env_files::environment_for_path(&child_rel).is_some() {
                files.env.push(child_rel);
            } else if compose::is_compose_file(&name) && depth <= 2 {
                files.compose.push(child_rel);
            } else if config_files::classify(&child_rel).is_some() {
                // `application.yml` di folder test bukan konfigurasi aplikasi.
                if !child_rel.contains("/test/") && !child_rel.contains("/tests/") {
                    files.config.push(child_rel);
                }
            } else if services::is_manifest(&name) && !files.manifest_dirs.contains(&rel) {
                files.manifest_dirs.push(rel.clone());
            }
        }
    }
    files.env.sort();
    files.compose.sort();
    files.config.sort();
    files.manifest_dirs.sort();
    Ok(files)
}

fn parent_of(rel: &str) -> &str {
    rel.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

fn read(root: &Path, rel: &str) -> Option<String> {
    let path = root.join(rel);
    let meta = std::fs::metadata(&path).ok()?;
    if meta.len() > crate::repo_scan::MAX_FILE_BYTES {
        return None;
    }
    std::fs::read_to_string(path).ok()
}

/// Kunci repository seperti `crate::git::repos::merge`.
pub fn repo_key_of(root: &Path, remote: Option<&str>) -> String {
    remote
        .and_then(crate::repo_scan::repo_key)
        .unwrap_or_else(|| {
            let d = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
            format!("path:{}", d.display())
        })
}

type EnvFile = (String, Environment, Vec<(String, String)>);

/// Variabel Development terdekat untuk folder `dir` (untuk compose dan
/// placeholder konfigurasi).
fn dev_vars_for(env_parsed: &[EnvFile], dir: &str) -> Vec<(String, String)> {
    let mut best: Option<&EnvFile> = None;
    for e in env_parsed {
        let d = parent_of(&e.0);
        let fits = d.is_empty() || dir == d || dir.starts_with(&format!("{d}/"));
        if fits
            && e.1 == Environment::Development
            && best.is_none_or(|b| parent_of(&b.0).len() < d.len())
        {
            best = Some(e);
        }
    }
    best.map(|b| b.2.clone()).unwrap_or_default()
}

/// Discovery lengkap repository yang memuat `dir`.
pub fn discover(dir: &Path, cancel: &AtomicBool) -> Result<SetupPlan, RepoSetupError> {
    let root = crate::git::ops::discover_root(dir)
        .map_err(|_| RepoSetupError::NotARepo(dir.display().to_string()))?;
    let name = root
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "repository".to_string());
    let raw_remote = crate::repo_scan::git_remote_url(&root);
    let key = repo_key_of(&root, raw_remote.as_deref());
    let remote_url = raw_remote.filter(|u| !crate::repo_scan::has_embedded_credentials(u));
    let mut warnings = Vec::new();

    let files = walk(&root, cancel)?;

    // 1. File env: variabel per file.
    let mut env_parsed: Vec<EnvFile> = Vec::new();
    for rel in &files.env {
        let Some(env) = env_files::environment_for_path(rel) else {
            continue;
        };
        if let Some(text) = read(&root, rel) {
            env_parsed.push((rel.clone(), env, env_files::parse(&text)));
        }
    }

    // 2. docker-compose.
    let mut compose_all = ComposeInfo::default();
    let mut compose_hints: Vec<ComposeHint> = Vec::new();
    let mut compose_sources: Vec<String> = Vec::new();
    for rel in &files.compose {
        let Some(text) = read(&root, rel) else {
            continue;
        };
        let base = parent_of(rel);
        let info = compose::parse(&text, &dev_vars_for(&env_parsed, base));
        for app in &info.apps {
            let rel_dir = match (base.is_empty(), app.context.is_empty()) {
                (true, _) => app.context.clone(),
                (false, true) => base.to_string(),
                (false, false) => format!("{base}/{}", app.context),
            };
            if !compose_hints.iter().any(|h| h.rel_dir == rel_dir) {
                compose_hints.push(ComposeHint {
                    service: app.service.clone(),
                    rel_dir,
                    base_url: app.base_url.clone(),
                });
            }
        }
        for db in info.databases {
            compose_sources.push(rel.clone());
            compose_all.databases.push(db);
        }
        compose_all.apps.extend(info.apps);
    }

    if cancel.load(Ordering::SeqCst) {
        return Err(RepoSetupError::Cancelled);
    }

    // 3. Route → service.
    let scan = crate::repo_endpoints::scan_routes(&root, cancel).map_err(|e| match e {
        crate::repo_scan::RepoScanError::Cancelled => RepoSetupError::Cancelled,
        other => RepoSetupError::Scan(other.to_string()),
    })?;
    if scan.truncated {
        warnings.push("The repository is large; only part of it was scanned for routes.".into());
    }
    let services = services::group(
        &root,
        &name,
        &files.manifest_dirs,
        &compose_hints,
        &scan.hits,
    );

    // 4. Koneksi.
    let mut raw: Vec<(FoundDsn, Environment, String)> = Vec::new();
    for (rel, env, vars) in &env_parsed {
        for f in dsn_detect::from_vars(vars) {
            raw.push((f, *env, rel.clone()));
        }
    }
    for rel in &files.config {
        let (Some(kind), Some(text)) = (config_files::classify(rel), read(&root, rel)) else {
            continue;
        };
        let vars = dev_vars_for(&env_parsed, parent_of(rel));
        for c in config_files::extract(kind, rel, &text, &vars) {
            raw.push((c.found, c.environment, rel.clone()));
        }
    }

    let multi = services.len() > 1;
    let mut connections: Vec<PlannedConnection> = Vec::new();
    for (mut found, environment, source) in raw {
        let mut note = None;
        // `DB_HOST=postgres` menunjuk container compose: pakai port host.
        if environment != Environment::Production
            && let Some(db) = compose_all.db_by_host(&found.dsn.host)
        {
            found.dsn.host = "localhost".into();
            found.dsn.port = db.dsn.port.clone();
            if !db.published {
                note = Some(format!(
                    "Container '{}' does not publish its port in docker-compose",
                    db.service
                ));
            }
        }
        if found.dsn.db_type == DatabaseType::SQLite {
            absolutize_sqlite(&root, parent_of(&source), &mut found.dsn);
        }
        // Satu service: semua koneksi repository miliknya.
        let service = match services.as_slice() {
            [only] => Some(only.name.clone()),
            _ => services::owner_of(&services, &source).map(|s| s.name.clone()),
        };
        push_connection(
            &mut connections,
            PlannedConnection {
                needs_credentials: found.placeholder,
                dsn: found.dsn,
                environment,
                key: found.key,
                source,
                service,
                name: String::new(),
                note,
            },
        );
    }
    for (db, rel) in compose_all.databases.iter().zip(compose_sources) {
        let id = dsn_detect::server_identity(&db.dsn);
        if connections
            .iter()
            .any(|c| dsn_detect::server_identity(&c.dsn) == id)
        {
            continue;
        }
        push_connection(
            &mut connections,
            PlannedConnection {
                dsn: db.dsn.clone(),
                environment: Environment::Local,
                key: db.service.clone(),
                service: None,
                needs_credentials: dsn_detect::looks_placeholder(&db.dsn.password),
                name: String::new(),
                note: (!db.published).then(|| {
                    format!(
                        "Container '{}' does not publish its port in docker-compose",
                        db.service
                    )
                }),
                source: rel,
            },
        );
    }
    name_connections(&mut connections, multi);

    // 5. Variabel per environment (file dangkal didahulukan).
    let mut env_vars: Vec<(Environment, Vec<(String, String)>)> = Vec::new();
    let mut ordered: Vec<&EnvFile> = env_parsed.iter().collect();
    ordered.sort_by_key(|e| (e.0.matches('/').count(), e.0.clone()));
    for (_, env, vars) in ordered {
        let slot = match env_vars.iter().position(|(e, _)| e == env) {
            Some(i) => i,
            None => {
                env_vars.push((*env, Vec::new()));
                env_vars.len() - 1
            }
        };
        for (k, v) in vars {
            if !env_vars[slot].1.iter().any(|(ek, _)| ek == k) {
                env_vars[slot].1.push((k.clone(), v.clone()));
            }
        }
    }

    let mut watched_files: Vec<String> = files.env.clone();
    watched_files.extend(files.compose.iter().cloned());
    watched_files.extend(files.config.iter().cloned());
    let fingerprint = fingerprint::compute(&root, &watched_files);

    if connections.is_empty() {
        warnings.push(
            "No database connection was found in .env, docker-compose or framework config files."
                .into(),
        );
    }
    if services.is_empty() {
        warnings.push("No HTTP routes were found in the code.".into());
    }
    log::info!(
        "[REPO_SETUP] discovered '{}': {} connection(s), {} service(s), {} endpoint(s)",
        name,
        connections.len(),
        services.len(),
        services.iter().map(|s| s.endpoints.len()).sum::<usize>()
    );
    Ok(SetupPlan {
        root,
        name,
        key,
        remote_url,
        connections,
        services,
        env_vars,
        watched_files,
        fingerprint,
        warnings,
    })
}

fn absolutize_sqlite(root: &Path, source_dir: &str, dsn: &mut ParsedDsn) {
    let raw = dsn.database.trim().trim_start_matches("file:").to_string();
    if raw.is_empty() || raw == ":memory:" {
        return;
    }
    if Path::new(&raw).is_absolute() {
        dsn.database = raw;
        return;
    }
    let rel = raw.trim_start_matches("./");
    let base = if source_dir.is_empty() {
        root.to_path_buf()
    } else {
        root.join(source_dir)
    };
    let joined = base.join(rel);
    // `prisma/schema.prisma` memakai path relatif ke folder prisma; Laravel
    // relatif ke root aplikasi. Pakai yang benar-benar ada.
    dsn.database = if joined.exists() || !root.join(rel).exists() {
        joined.to_string_lossy().to_string()
    } else {
        root.join(rel).to_string_lossy().to_string()
    };
}

fn push_connection(out: &mut Vec<PlannedConnection>, c: PlannedConnection) {
    let id = dsn_detect::server_identity(&c.dsn);
    if let Some(existing) = out
        .iter_mut()
        .find(|o| o.environment == c.environment && dsn_detect::server_identity(&o.dsn) == id)
    {
        if existing.needs_credentials && !c.needs_credentials {
            *existing = c;
        }
        return;
    }
    out.push(c);
}

fn base_name(d: &ParsedDsn) -> String {
    let label = match d.db_type {
        DatabaseType::PostgreSQL => "postgres",
        DatabaseType::MySQL => "mysql",
        DatabaseType::MsSQL => "sqlserver",
        DatabaseType::Redis => "redis",
        DatabaseType::MongoDB => "mongodb",
        DatabaseType::SQLite => "sqlite",
        _ => "database",
    };
    match d.db_type {
        DatabaseType::SQLite => Path::new(&d.database)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "sqlite".into()),
        DatabaseType::Redis => format!("redis {}", d.host),
        _ if d.database.trim().is_empty() => format!("{label} {}", d.host),
        _ => d.database.clone(),
    }
}

/// Nama koneksi: `[service - ]database (ENV)`, unik dalam plan. Karakter
/// `/` dipakai sebagai pemisah folder, jadi diganti.
fn name_connections(conns: &mut [PlannedConnection], multi: bool) {
    let mut used: Vec<String> = Vec::new();
    for c in conns.iter_mut() {
        let base = base_name(&c.dsn);
        let stem = match (&c.service, multi) {
            (Some(s), true) if !base.eq_ignore_ascii_case(s) => format!("{s} - {base}"),
            _ => base,
        };
        let mut name = format!("{stem} ({})", c.environment.short()).replace('/', "-");
        let mut n = 2;
        while used.iter().any(|u| u.eq_ignore_ascii_case(&name)) {
            name = format!("{stem} {n} ({})", c.environment.short()).replace('/', "-");
            n += 1;
        }
        used.push(name.clone());
        c.name = name;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn write(root: &Path, rel: &str, text: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, text).expect("write");
    }

    fn git_repo(tag: &str) -> Option<PathBuf> {
        let dir =
            std::env::temp_dir().join(format!("tabular-discover-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        let ok = Command::new("git")
            .args(["init", "-q"])
            .current_dir(&dir)
            .status()
            .ok()?
            .success();
        ok.then_some(dir)
    }

    #[test]
    fn discovers_microservices_repository() {
        let Some(root) = git_repo("micro") else {
            eprintln!("git not available; skipping");
            return;
        };
        write(
            &root,
            "docker-compose.yml",
            r#"
services:
  orders-db:
    image: postgres:16
    environment:
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: pw
      POSTGRES_DB: orders
    ports: ["5433:5432"]
  orders:
    build: ./services/orders
    ports: ["8081:3000"]
  users:
    build: ./services/users
"#,
        );
        write(&root, "services/orders/package.json", "{}");
        write(
            &root,
            "services/orders/src/routes.js",
            "router.get('/orders', h);\nrouter.post('/orders', h);\n",
        );
        write(
            &root,
            "services/orders/.env",
            "DATABASE_URL=postgres://orders:pw@orders-db:5432/orders\n",
        );
        write(
            &root,
            "services/orders/.env.production",
            "DATABASE_URL=postgres://orders:<password>@prod.db:5432/orders\n",
        );
        write(
            &root,
            "services/orders/.env.example",
            "DATABASE_URL=postgres://x\n",
        );
        write(&root, "services/users/go.mod", "module users\n");
        write(&root, "services/users/main.go", "r.GET(\"/users\", h)\n");
        write(
            &root,
            "services/users/.env",
            "DB_CONNECTION=mysql\nDB_HOST=localhost\nDB_DATABASE=users\nDB_USERNAME=u\nDB_PASSWORD=p\n",
        );
        write(
            &root,
            "node_modules/x/.env",
            "DATABASE_URL=postgres://nope@h/x\n",
        );

        let plan = discover(&root, &AtomicBool::new(false)).expect("discover");
        assert!(plan.is_microservices(), "{:?}", plan.services);
        assert_eq!(plan.services.len(), 2);
        let orders = plan
            .services
            .iter()
            .find(|s| s.rel_dir == "services/orders")
            .expect("orders");
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.base_url, "http://localhost:8081");
        assert_eq!(orders.endpoints.len(), 2);

        // Dev orders (host compose → localhost:5433), prod orders
        // (placeholder), users mysql. Container compose sama dengan dev
        // orders sehingga tidak dobel.
        assert_eq!(plan.connections.len(), 3, "{:#?}", plan.connections);
        let dev = plan
            .connections
            .iter()
            .find(|c| {
                c.environment == Environment::Development
                    && c.dsn.db_type == DatabaseType::PostgreSQL
            })
            .expect("dev pg");
        assert_eq!(
            (dev.dsn.host.as_str(), dev.dsn.port.as_str()),
            ("localhost", "5433")
        );
        assert_eq!(dev.service.as_deref(), Some("orders"));
        assert_eq!(dev.name, "orders (DEV)");
        let prod = plan
            .connections
            .iter()
            .find(|c| c.environment == Environment::Production)
            .expect("prod");
        assert!(prod.needs_credentials);
        assert_eq!(prod.dsn.host, "prod.db");
        let users = plan
            .connections
            .iter()
            .find(|c| c.dsn.db_type == DatabaseType::MySQL)
            .expect("users");
        assert_eq!(users.name, "users (DEV)");
        assert_eq!(plan.diagram_targets().len(), 2);
        assert!(
            plan.watched_files
                .iter()
                .any(|f| f == "services/orders/.env")
        );
        assert!(
            !plan
                .watched_files
                .iter()
                .any(|f| f.contains("node_modules"))
        );
        assert!(
            plan.env_vars
                .iter()
                .any(|(e, _)| *e == Environment::Production)
        );
        assert!(plan.key.starts_with("path:"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn discovers_monolith_with_sqlite() {
        let Some(root) = git_repo("mono") else {
            return;
        };
        write(&root, "composer.json", "{}");
        write(&root, ".env", "APP_NAME=shop\nDB_CONNECTION=sqlite\n");
        write(
            &root,
            "routes/api.php",
            "Route::get('/products', [P::class, 'index']);\n",
        );
        let plan = discover(&root.join("routes"), &AtomicBool::new(false)).expect("discover");
        assert_eq!(plan.services.len(), 1);
        assert!(!plan.is_microservices());
        assert_eq!(plan.connections.len(), 1);
        let c = &plan.connections[0];
        assert_eq!(c.dsn.db_type, DatabaseType::SQLite);
        assert!(Path::new(&c.dsn.database).is_absolute());
        assert!(c.dsn.database.ends_with("database/database.sqlite"));
        let _ = std::fs::remove_dir_all(&root);
    }
}
