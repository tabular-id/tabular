//! `docker-compose.yml` / `compose.yaml`: container database (image
//! postgres/mysql/mongo/redis/mssql) menjadi koneksi ke port yang
//! dipublikasikan di localhost, dan service dengan `build:` menjadi kandidat
//! microservice beserta base URL-nya.

use super::env_files;
use super::yaml::{self, Yaml};
use crate::deeplink::ParsedDsn;
use crate::models::enums::DatabaseType;

/// Nama file compose yang dikenali.
pub fn is_compose_file(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    (n.starts_with("docker-compose") || n.starts_with("compose"))
        && (n.ends_with(".yml") || n.ends_with(".yaml"))
        && !n.contains("example")
}

/// Container database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeDb {
    pub service: String,
    pub dsn: ParsedDsn,
    /// Port dipublikasikan ke host; `false` = hanya terjangkau dari jaringan
    /// docker (koneksi tetap dicatat dengan port bawaan).
    pub published: bool,
}

/// Service aplikasi yang dibangun dari kode repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeApp {
    pub service: String,
    /// Folder build relatif terhadap folder file compose (tanpa `./`).
    pub context: String,
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComposeInfo {
    pub databases: Vec<ComposeDb>,
    pub apps: Vec<ComposeApp>,
}

impl ComposeInfo {
    /// Port host untuk container database bernama `service` (dipakai saat
    /// `.env` menulis `DB_HOST=postgres`).
    pub fn db_by_host(&self, host: &str) -> Option<&ComposeDb> {
        self.databases
            .iter()
            .find(|d| d.service.eq_ignore_ascii_case(host.trim()))
    }
}

/// Parse isi file compose. `vars` = isi `.env` di sebelahnya untuk
/// menyelesaikan `${VAR:-default}`.
pub fn parse(text: &str, vars: &[(String, String)]) -> ComposeInfo {
    let text = env_files::substitute(text, vars);
    let doc = yaml::parse(&text);
    let mut info = ComposeInfo::default();
    for (name, svc) in doc.get("services").map(Yaml::as_map).unwrap_or_default() {
        let image = svc.get("image").and_then(Yaml::as_str).unwrap_or_default();
        let env = service_env(svc);
        let db_type = image_type(image);
        if let Some(db_type) = db_type
            && svc.get("build").is_none()
        {
            let container_port = super::dsn_detect::default_port(&db_type);
            let host_port = published_port(svc, container_port);
            let dsn = container_dsn(
                db_type.clone(),
                &env,
                host_port
                    .clone()
                    .unwrap_or_else(|| container_port.to_string()),
            );
            info.databases.push(ComposeDb {
                service: name.clone(),
                dsn,
                published: host_port.is_some(),
            });
            continue;
        }
        let context = match svc.get("build") {
            Some(Yaml::Str(s)) => Some(s.clone()),
            Some(b @ Yaml::Map(_)) => Some(
                b.get("context")
                    .and_then(Yaml::as_str)
                    .unwrap_or(".")
                    .to_string(),
            ),
            _ => None,
        };
        if let Some(ctx) = context {
            let ctx = ctx.trim().trim_start_matches("./").trim_end_matches('/');
            let ctx = if ctx == "." { "" } else { ctx };
            let base_url = first_published(svc).map(|p| format!("http://localhost:{p}"));
            info.apps.push(ComposeApp {
                service: name.clone(),
                context: ctx.to_string(),
                base_url,
            });
        }
    }
    info
}

/// Tipe database dari nama image (`postgres:16`, `bitnami/mysql`).
pub fn image_type(image: &str) -> Option<DatabaseType> {
    let repo = image
        .split(':')
        .next()
        .unwrap_or(image)
        .to_ascii_lowercase();
    let last = repo.rsplit('/').next().unwrap_or(&repo);
    let t = super::dsn_detect::type_from_hint(last)?;
    // `redis-commander`, `mongo-express`, `pgadmin` bukan database.
    if last.contains("commander")
        || last.contains("express")
        || last.contains("admin")
        || last.contains("exporter")
        || last.contains("insight")
    {
        return None;
    }
    Some(t)
}

/// Variabel `environment:` (map atau list `K=V`).
pub fn service_env(svc: &Yaml) -> Vec<(String, String)> {
    match svc.get("environment") {
        Some(Yaml::Map(entries)) => entries
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .collect(),
        Some(Yaml::List(items)) => items
            .iter()
            .filter_map(Yaml::as_str)
            .map(|s| {
                let (k, v) = s.split_once('=').unwrap_or((s, ""));
                (k.trim().to_string(), v.trim().to_string())
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Port host yang memetakan `container_port`.
fn published_port(svc: &Yaml, container_port: &str) -> Option<String> {
    ports(svc)
        .into_iter()
        .find(|(_, target)| target == container_port)
        .map(|(host, _)| host)
}

fn first_published(svc: &Yaml) -> Option<String> {
    ports(svc).into_iter().next().map(|(h, _)| h)
}

/// Pasangan `(port host, port container)` dari `ports:`.
fn ports(svc: &Yaml) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in svc.get("ports").map(Yaml::as_list).unwrap_or_default() {
        match p {
            Yaml::Str(s) => {
                let s = s.split('/').next().unwrap_or(s);
                let parts: Vec<&str> = s.split(':').collect();
                let (host, target) = match parts.as_slice() {
                    [h, t] => (h.to_string(), t.to_string()),
                    [_ip, h, t] => (h.to_string(), t.to_string()),
                    // Hanya port container: port host acak, tidak bisa dipakai.
                    _ => continue,
                };
                // Rentang `8000-8010` diwakili port pertama.
                let first = |x: &str| x.split('-').next().unwrap_or(x).trim().to_string();
                out.push((first(&host), first(&target)));
            }
            Yaml::Map(_) => {
                let get = |k: &str| p.get(k).and_then(Yaml::as_str).map(str::to_string);
                if let (Some(h), Some(t)) = (get("published"), get("target")) {
                    out.push((h, t));
                }
            }
            _ => {}
        }
    }
    out
}

fn container_dsn(db_type: DatabaseType, env: &[(String, String)], port: String) -> ParsedDsn {
    let get = |keys: &[&str]| {
        keys.iter().find_map(|k| {
            env.iter()
                .find(|(ek, _)| ek == k)
                .map(|(_, v)| v.clone())
                .filter(|v| !v.is_empty())
        })
    };
    let (username, password, database) = match db_type {
        DatabaseType::PostgreSQL => {
            let user = get(&["POSTGRES_USER"]).unwrap_or_else(|| "postgres".into());
            let db = get(&["POSTGRES_DB"]).unwrap_or_else(|| user.clone());
            (user, get(&["POSTGRES_PASSWORD"]).unwrap_or_default(), db)
        }
        DatabaseType::MySQL => match get(&["MYSQL_USER", "MARIADB_USER"]) {
            Some(u) => (
                u,
                get(&["MYSQL_PASSWORD", "MARIADB_PASSWORD"]).unwrap_or_default(),
                get(&["MYSQL_DATABASE", "MARIADB_DATABASE"]).unwrap_or_default(),
            ),
            None => (
                "root".into(),
                get(&["MYSQL_ROOT_PASSWORD", "MARIADB_ROOT_PASSWORD"]).unwrap_or_default(),
                get(&["MYSQL_DATABASE", "MARIADB_DATABASE"]).unwrap_or_default(),
            ),
        },
        DatabaseType::MongoDB => (
            get(&["MONGO_INITDB_ROOT_USERNAME"]).unwrap_or_default(),
            get(&["MONGO_INITDB_ROOT_PASSWORD"]).unwrap_or_default(),
            get(&["MONGO_INITDB_DATABASE"]).unwrap_or_default(),
        ),
        DatabaseType::MsSQL => (
            "sa".into(),
            get(&["MSSQL_SA_PASSWORD", "SA_PASSWORD"]).unwrap_or_default(),
            "master".into(),
        ),
        DatabaseType::Redis => (
            String::new(),
            get(&["REDIS_PASSWORD"]).unwrap_or_default(),
            String::new(),
        ),
        _ => (String::new(), String::new(), String::new()),
    };
    ParsedDsn {
        db_type,
        host: "localhost".into(),
        port,
        username,
        password,
        database,
        ssl: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_compose_file_names() {
        assert!(is_compose_file("docker-compose.yml"));
        assert!(is_compose_file("docker-compose.override.yaml"));
        assert!(is_compose_file("compose.yaml"));
        assert!(!is_compose_file("compose.example.yml"));
        assert!(!is_compose_file("component.yml"));
    }

    #[test]
    fn extracts_databases_and_apps() {
        let src = r#"
services:
  orders-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: ${ORDERS_DB_PASSWORD:-devpass}
    ports:
      - "127.0.0.1:5433:5432"
  cache:
    image: redis:7
  mysql:
    image: mysql:8
    environment:
      - MYSQL_ROOT_PASSWORD=root
      - MYSQL_DATABASE=billing
    ports:
      - target: 3306
        published: 3307
  adminer:
    image: adminer
    ports: ["8081:8080"]
  redis-ui:
    image: rediscommander/redis-commander
  orders:
    build: ./services/orders
    ports: ["8080:3000"]
  gateway:
    build:
      context: .
      dockerfile: Dockerfile
"#;
        let info = parse(src, &[]);
        assert_eq!(info.databases.len(), 3, "{:?}", info.databases);
        let pg = &info.databases[0];
        assert_eq!(pg.service, "orders-db");
        assert_eq!(pg.dsn.port, "5433");
        assert_eq!(pg.dsn.database, "orders");
        assert_eq!(pg.dsn.password, "devpass");
        assert!(pg.published);
        let redis = &info.databases[1];
        assert!(!redis.published);
        let my = &info.databases[2];
        assert_eq!(
            (my.dsn.port.as_str(), my.dsn.username.as_str()),
            ("3307", "root")
        );
        assert_eq!(my.dsn.database, "billing");
        assert_eq!(info.apps.len(), 2);
        assert_eq!(info.apps[0].context, "services/orders");
        assert_eq!(
            info.apps[0].base_url.as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(info.apps[1].context, "");
        assert!(info.db_by_host("ORDERS-DB").is_some());
    }
}
