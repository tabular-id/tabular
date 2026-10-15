//! Deteksi koneksi database dari kumpulan variabel (`.env`, environment
//! docker-compose, properti Spring yang sudah diratakan).
//!
//! Tiga bentuk dikenali:
//! 1. URL utuh (`DATABASE_URL=postgres://…`, `jdbc:mysql://…`, `mongodb+srv://…`)
//!    lewat [`crate::deeplink::parse_dsn`];
//! 2. connection string ADO.NET (`Server=…;Database=…;User Id=…`);
//! 3. grup variabel dengan prefix sama (`DB_HOST`, `DB_PORT`, `DB_DATABASE`,
//!    `DB_USERNAME`, `DB_PASSWORD`, `DB_CONNECTION`; `PGHOST`…; `POSTGRES_*`).

use std::collections::HashMap;

use crate::deeplink::{ParsedDsn, parse_dsn};
use crate::models::enums::DatabaseType;

/// Satu koneksi yang ditemukan di kumpulan variabel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundDsn {
    pub dsn: ParsedDsn,
    /// Variabel asal (`DATABASE_URL`, atau prefix grup seperti `DB_*`).
    pub key: String,
    /// Nilai masih berupa contoh/placeholder (`<password>`, `changeme`,
    /// `${VAR}` yang tidak terselesaikan): koneksi tetap dibuat tetapi
    /// ditandai perlu kredensial.
    pub placeholder: bool,
}

/// Prefix grup `*_HOST` yang jelas bukan database.
const NON_DB_PREFIXES: &[&str] = &[
    "SMTP",
    "MAIL",
    "EMAIL",
    "FTP",
    "SSH",
    "APP",
    "API",
    "HTTP",
    "PROXY",
    "KAFKA",
    "RABBIT",
    "AMQP",
    "ELASTIC",
    "OPENSEARCH",
    "S3",
    "MINIO",
    "SENTRY",
    "OTEL",
    "JAEGER",
    "VITE",
    "NEXT_PUBLIC",
    "MEMCACHE",
    "NATS",
    "MQTT",
    "LDAP",
    "SERVER",
    "FRONTEND",
    "BACKEND",
    "WEB",
    "GRPC",
    "CONSUL",
    "VAULT",
    "ZIPKIN",
    "PUSHER",
    "REVERB",
    "LOKI",
    "PROMETHEUS",
    "GRAFANA",
    "KEYCLOAK",
    "STATSD",
    "DOCKER",
    "NGINX",
];

/// Port bawaan untuk tipe `t`.
pub fn default_port(t: &DatabaseType) -> &'static str {
    match t {
        DatabaseType::MySQL => "3306",
        DatabaseType::PostgreSQL => "5432",
        DatabaseType::MsSQL => "1433",
        DatabaseType::Redis => "6379",
        DatabaseType::MongoDB => "27017",
        DatabaseType::SQLite | DatabaseType::ApiHttp | DatabaseType::Plugin(_) => "",
    }
}

/// Tipe database dari petunjuk teks (driver Laravel, dialect, adapter
/// Rails, nama image docker, prefix variabel).
pub fn type_from_hint(hint: &str) -> Option<DatabaseType> {
    let h = hint.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| h.contains(w));
    if has(&[
        "postgis",
        "postgres",
        "pgsql",
        "timescale",
        "cockroach",
        "supabase",
    ]) || h == "pg"
    {
        Some(DatabaseType::PostgreSQL)
    } else if has(&["mysql", "mariadb", "percona", "trilogy"]) {
        Some(DatabaseType::MySQL)
    } else if has(&["sqlserver", "sql-server", "mssql", "azure-sql", "sqlsrv"]) {
        Some(DatabaseType::MsSQL)
    } else if has(&["mongo"]) {
        Some(DatabaseType::MongoDB)
    } else if has(&["redis", "valkey", "keydb", "dragonfly"]) {
        Some(DatabaseType::Redis)
    } else if has(&["sqlite"]) {
        Some(DatabaseType::SQLite)
    } else {
        None
    }
}

fn type_from_port(port: &str) -> Option<DatabaseType> {
    match port.trim() {
        "5432" => Some(DatabaseType::PostgreSQL),
        "3306" => Some(DatabaseType::MySQL),
        "1433" => Some(DatabaseType::MsSQL),
        "6379" => Some(DatabaseType::Redis),
        "27017" => Some(DatabaseType::MongoDB),
        _ => None,
    }
}

/// Nilai contoh/placeholder, bukan nilai sungguhan.
pub fn looks_placeholder(value: &str) -> bool {
    let v = value.trim().to_ascii_lowercase();
    (v.contains('<') && v.contains('>'))
        || v.contains("${")
        || v.contains("your_")
        || v.contains("your-")
        || v.contains("changeme")
        || v.contains("change_me")
        || v.contains("xxxx")
        || v.contains("example.com")
        || v.contains("placeholder")
        || v.contains("%env(")
}

fn dsn_placeholder(d: &ParsedDsn) -> bool {
    [&d.host, &d.database, &d.username, &d.password]
        .iter()
        .any(|v| looks_placeholder(v))
        || (d.db_type != DatabaseType::SQLite && d.host.trim().is_empty())
}

/// Parse URL atau connection string ADO.NET menjadi DSN.
pub fn parse_value(value: &str) -> Option<ParsedDsn> {
    let v = value.trim().trim_matches('"').trim_matches('\'');
    if v.is_empty() {
        return None;
    }
    if v.contains("://") || v.starts_with("jdbc:") || v.starts_with("sqlite:") {
        let mut dsn = parse_dsn(v).ok()?;
        if dsn.db_type == DatabaseType::MongoDB && v.starts_with("mongodb+srv") {
            // SRV tidak punya port; parse_dsn mengisi port bawaan.
            dsn.port = String::new();
        }
        return Some(dsn);
    }
    let lower = v.to_ascii_lowercase();
    let ado_start = [
        "server=",
        "data source=",
        "datasource=",
        "host=",
        "address=",
    ]
    .iter()
    .any(|p| lower.starts_with(p));
    if v.contains('=') && (v.contains(';') || ado_start) {
        return parse_ado(v);
    }
    None
}

/// Connection string ADO.NET (`Server=host,1433;Database=db;User Id=sa;Password=x`).
pub fn parse_ado(value: &str) -> Option<ParsedDsn> {
    let mut map: HashMap<String, String> = HashMap::new();
    for part in value.split(';') {
        if let Some((k, v)) = part.split_once('=') {
            map.insert(
                k.trim().to_ascii_lowercase().replace(' ', ""),
                v.trim().to_string(),
            );
        }
    }
    let get = |keys: &[&str]| keys.iter().find_map(|k| map.get(*k).cloned());
    let raw_host = get(&[
        "server",
        "datasource",
        "host",
        "address",
        "addr",
        "networkaddress",
    ])?;
    let database = get(&["database", "initialcatalog", "dbname"]).unwrap_or_default();
    let username = get(&["userid", "uid", "user", "username"]).unwrap_or_default();
    let password = get(&["password", "pwd"]).unwrap_or_default();
    let lower_host = raw_host.to_ascii_lowercase();
    if lower_host.ends_with(".db")
        || lower_host.ends_with(".sqlite")
        || lower_host.ends_with(".sqlite3")
    {
        return Some(ParsedDsn {
            db_type: DatabaseType::SQLite,
            host: String::new(),
            port: String::new(),
            username: String::new(),
            password: String::new(),
            database: raw_host,
            ssl: false,
        });
    }
    let db_type = if map.contains_key("host") {
        DatabaseType::PostgreSQL
    } else if map.contains_key("uid")
        && !map.contains_key("initialcatalog")
        && !map.contains_key("trustservercertificate")
    {
        DatabaseType::MySQL
    } else {
        DatabaseType::MsSQL
    };
    let host_part = raw_host.trim_start_matches("tcp:");
    let (host, port) = match host_part.split_once(',') {
        Some((h, p)) => (h.to_string(), p.trim().to_string()),
        None => match host_part.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
                (h.to_string(), p.to_string())
            }
            _ => (host_part.to_string(), String::new()),
        },
    };
    let port = get(&["port"])
        .filter(|p| !p.is_empty())
        .unwrap_or(if port.is_empty() {
            default_port(&db_type).to_string()
        } else {
            port
        });
    let host = if host == "." || host.eq_ignore_ascii_case("(local)") {
        "localhost".to_string()
    } else {
        host
    };
    let ssl = matches!(
        get(&["encrypt", "sslmode", "ssl"])
            .map(|s| s.to_ascii_lowercase())
            .as_deref(),
        Some("true" | "yes" | "require" | "required" | "verify-full" | "mandatory")
    );
    Some(ParsedDsn {
        db_type,
        host,
        port,
        username,
        password,
        database,
        ssl,
    })
}

/// Variabel yang namanya pantas berisi URL koneksi database.
fn is_db_url_key(key: &str) -> bool {
    let k = key.to_ascii_uppercase();
    if k.contains("SHADOW")
        || k.contains("BROKER")
        || k.contains("QUEUE")
        || k.contains("CACHE_URL") && !k.contains("REDIS")
    {
        return false;
    }
    [
        "URL",
        "URI",
        "DSN",
        "CONNECTION",
        "CONN",
        "DATABASE",
        "DB",
        "MONGO",
        "REDIS",
        "POSTGRES",
        "MYSQL",
        "SQL",
    ]
    .iter()
    .any(|m| k.contains(m))
}

/// Cari semua koneksi di `vars`. Hasil dideduplikasi per server/database.
pub fn from_vars(vars: &[(String, String)]) -> Vec<FoundDsn> {
    let mut upper: HashMap<String, String> = HashMap::new();
    for (k, v) in vars {
        upper.insert(k.to_ascii_uppercase().replace('.', "_"), v.clone());
    }
    let mut out: Vec<FoundDsn> = Vec::new();

    // 1–2. URL utuh dan connection string.
    for (k, v) in vars {
        if !is_db_url_key(k) {
            continue;
        }
        if let Some(dsn) = parse_value(v) {
            push_unique(
                &mut out,
                FoundDsn {
                    placeholder: dsn_placeholder(&dsn),
                    dsn,
                    key: k.clone(),
                },
            );
        }
    }

    // 3. Grup variabel ber-prefix.
    let mut prefixes: Vec<String> = Vec::new();
    for k in upper.keys() {
        for suffix in ["HOST", "HOSTNAME", "CONNECTION", "DATABASE", "DB"] {
            if let Some(p) = k.strip_suffix(suffix)
                && !prefixes.iter().any(|x| x == p)
            {
                prefixes.push(p.to_string());
            }
        }
    }
    prefixes.sort();
    let mut implicit: Vec<FoundDsn> = Vec::new();
    for prefix in prefixes {
        match from_group(&prefix, &upper) {
            Some((found, false)) => push_unique(&mut out, found),
            Some((found, true)) => implicit.push(found),
            None => {}
        }
    }
    // Variabel container (`MYSQL_DATABASE` tanpa host) biasanya hanya untuk
    // inisialisasi container; abaikan bila database yang sama sudah punya
    // koneksi eksplisit (mis. `DATABASE_URL`).
    for found in implicit {
        let dup = out
            .iter()
            .any(|f| f.dsn.db_type == found.dsn.db_type && f.dsn.database == found.dsn.database);
        if !dup {
            push_unique(&mut out, found);
        }
    }
    out
}

/// Koneksi dari grup variabel `prefix*`. Flag kedua = host tidak ditulis
/// (diasumsikan localhost).
fn from_group(prefix: &str, vars: &HashMap<String, String>) -> Option<(FoundDsn, bool)> {
    let bare = prefix.trim_end_matches('_');
    if NON_DB_PREFIXES
        .iter()
        .any(|p| bare == *p || bare.starts_with(&format!("{p}_")))
    {
        return None;
    }
    let get = |suffixes: &[&str]| {
        suffixes.iter().find_map(|s| {
            vars.get(&format!("{prefix}{s}"))
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty() && !v.eq_ignore_ascii_case("null"))
        })
    };
    let host = get(&["HOST", "HOSTNAME"]);
    let explicit = get(&[
        "CONNECTION",
        "DRIVER",
        "TYPE",
        "DIALECT",
        "ENGINE",
        "CLIENT",
        "ADAPTER",
    ])
    .and_then(|h| type_from_hint(&h));
    let port = get(&["PORT"]);
    let prefix_hint = type_from_hint(bare).or(if bare == "PG" {
        Some(DatabaseType::PostgreSQL)
    } else {
        None
    });
    let database = get(&[
        "DATABASE",
        "NAME",
        "DB",
        "DBNAME",
        "DB_NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
    ]);
    let db_type = explicit
        .clone()
        .or(prefix_hint.clone())
        .or_else(|| port.as_deref().and_then(type_from_port))?;

    if db_type == DatabaseType::SQLite {
        let path = database.unwrap_or_else(|| "database/database.sqlite".to_string());
        if path.contains("://") {
            return None;
        }
        return Some((
            FoundDsn {
                placeholder: looks_placeholder(&path),
                dsn: ParsedDsn {
                    db_type,
                    host: String::new(),
                    port: String::new(),
                    username: String::new(),
                    password: String::new(),
                    database: path,
                    ssl: false,
                },
                key: format!("{prefix}*"),
            },
            false,
        ));
    }

    let implicit_host = host.is_none();
    let host = match host {
        Some(h) => h,
        // Variabel container (`POSTGRES_DB` tanpa host) → server lokal.
        None if prefix_hint.is_some() && database.is_some() => "localhost".to_string(),
        // `DB_CONNECTION=mysql` tanpa `DB_HOST`: bawaan framework localhost.
        None if explicit.is_some() && database.is_some() => "localhost".to_string(),
        None => return None,
    };
    // `*_HOST` berisi URL (mis. `REDIS_HOST=redis://…`) sudah ditangani di atas.
    if host.contains("://") {
        return None;
    }
    let (host, port) = match (host.rsplit_once(':'), port) {
        (Some((h, p)), None) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.to_string(), p.to_string())
        }
        (_, Some(p)) => (host, p),
        (_, None) => (host, default_port(&db_type).to_string()),
    };
    let username = get(&["USERNAME", "USER", "LOGIN", "UID"])
        .or_else(|| {
            (db_type == DatabaseType::MongoDB)
                .then(|| get(&["INITDB_ROOT_USERNAME"]))
                .flatten()
        })
        .unwrap_or_default();
    let password = get(&["PASSWORD", "PASS", "PWD", "PASSWD", "ROOT_PASSWORD"])
        .or_else(|| get(&["INITDB_ROOT_PASSWORD", "SA_PASSWORD"]))
        .unwrap_or_default();
    let username = if username.is_empty() && password.is_empty() {
        username
    } else if username.is_empty() {
        match db_type {
            DatabaseType::MySQL => "root".to_string(),
            DatabaseType::PostgreSQL => "postgres".to_string(),
            DatabaseType::MsSQL => "sa".to_string(),
            _ => username,
        }
    } else {
        username
    };
    let database = database
        .or_else(|| get(&["INITDB_DATABASE"]))
        .or_else(|| {
            (db_type == DatabaseType::PostgreSQL && !username.is_empty()).then(|| username.clone())
        })
        .unwrap_or_default();
    let ssl = matches!(
        get(&["SSLMODE", "SSL_MODE", "SSL", "TLS"])
            .map(|s| s.to_ascii_lowercase())
            .as_deref(),
        Some("true" | "1" | "require" | "required" | "verify-full" | "verify-ca" | "yes")
    );
    let dsn = ParsedDsn {
        db_type,
        host,
        port,
        username,
        password,
        database,
        ssl,
    };
    Some((
        FoundDsn {
            placeholder: dsn_placeholder(&dsn),
            dsn,
            key: format!("{prefix}*"),
        },
        implicit_host,
    ))
}

/// Kunci identitas server/database (tanpa password) untuk deduplikasi.
pub fn server_identity(d: &ParsedDsn) -> String {
    let host = match d.host.trim().to_ascii_lowercase().as_str() {
        "127.0.0.1" | "::1" | "0.0.0.0" | "" => "localhost".to_string(),
        h => h.to_string(),
    };
    format!(
        "{}|{}|{}|{}",
        d.db_type.as_db_str(),
        host,
        d.port.trim(),
        d.database.trim()
    )
}

fn push_unique(out: &mut Vec<FoundDsn>, found: FoundDsn) {
    let id = server_identity(&found.dsn);
    match out.iter_mut().find(|f| server_identity(&f.dsn) == id) {
        // Versi tanpa placeholder / dengan kredensial lebih lengkap menang.
        Some(existing) => {
            if existing.placeholder && !found.placeholder
                || existing.dsn.password.is_empty() && !found.dsn.password.is_empty()
            {
                *existing = found;
            }
        }
        None => out.push(found),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn detects_url_variables_and_dedups() {
        let found = from_vars(&vars(&[
            ("DATABASE_URL", "postgres://app:pw@localhost:5432/orders"),
            ("REDIS_URL", "redis://127.0.0.1:6379/0"),
            ("APP_URL", "http://localhost:3000"),
            (
                "SHADOW_DATABASE_URL",
                "postgres://app:pw@localhost:5432/shadow",
            ),
            ("DB_HOST", "127.0.0.1"),
            ("DB_PORT", "5432"),
            ("DB_DATABASE", "orders"),
            ("DB_USERNAME", "app"),
        ]));
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[0].dsn.db_type, DatabaseType::PostgreSQL);
        assert_eq!(found[0].dsn.password, "pw");
        assert!(!found[0].placeholder);
        assert_eq!(found[1].dsn.db_type, DatabaseType::Redis);
    }

    #[test]
    fn detects_laravel_group() {
        let found = from_vars(&vars(&[
            ("DB_CONNECTION", "mysql"),
            ("DB_HOST", "127.0.0.1"),
            ("DB_PORT", "3306"),
            ("DB_DATABASE", "shop"),
            ("DB_USERNAME", "root"),
            ("DB_PASSWORD", "secret"),
            ("MAIL_HOST", "smtp.mailtrap.io"),
            ("MAIL_PORT", "2525"),
        ]));
        assert_eq!(found.len(), 1, "{found:?}");
        let d = &found[0].dsn;
        assert_eq!(d.db_type, DatabaseType::MySQL);
        assert_eq!((d.host.as_str(), d.port.as_str()), ("127.0.0.1", "3306"));
        assert_eq!(d.database, "shop");
        assert_eq!(d.password, "secret");
        assert_eq!(found[0].key, "DB_*");
    }

    #[test]
    fn detects_laravel_sqlite_and_pg_env_and_container_vars() {
        let found = from_vars(&vars(&[("DB_CONNECTION", "sqlite")]));
        assert_eq!(found[0].dsn.db_type, DatabaseType::SQLite);
        assert_eq!(found[0].dsn.database, "database/database.sqlite");

        let found = from_vars(&vars(&[
            ("PGHOST", "db.internal"),
            ("PGUSER", "u"),
            ("PGDATABASE", "x"),
        ]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].dsn.port, "5432");
        assert_eq!(found[0].dsn.host, "db.internal");

        let found = from_vars(&vars(&[
            ("POSTGRES_USER", "app"),
            ("POSTGRES_PASSWORD", "pw"),
            ("POSTGRES_DB", "orders"),
        ]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].dsn.host, "localhost");
        assert_eq!(found[0].dsn.database, "orders");
    }

    #[test]
    fn container_vars_do_not_duplicate_explicit_url() {
        let found = from_vars(&vars(&[
            ("DATABASE_URL", "mysql://app:pw@db.example.org:3306/shop"),
            ("MYSQL_DATABASE", "shop"),
            ("MYSQL_USER", "app"),
            ("MYSQL_PASSWORD", "pw"),
        ]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].dsn.host, "db.example.org");
    }

    #[test]
    fn marks_placeholders() {
        let found = from_vars(&vars(&[(
            "DATABASE_URL",
            "postgres://user:<password>@localhost:5432/app",
        )]));
        assert!(found[0].placeholder);
        let found = from_vars(&vars(&[("MONGO_URI", "mongodb://${MONGO_HOST}:27017/app")]));
        assert!(found.is_empty() || found[0].placeholder);
    }

    #[test]
    fn parses_ado_connection_strings() {
        let d = parse_value(
            "Server=tcp:sql.local,1444;Database=Shop;User Id=sa;Password=P@ss;Encrypt=True",
        )
        .expect("mssql");
        assert_eq!(d.db_type, DatabaseType::MsSQL);
        assert_eq!((d.host.as_str(), d.port.as_str()), ("sql.local", "1444"));
        assert_eq!(d.database, "Shop");
        assert!(d.ssl);
        let d =
            parse_value("Host=localhost;Port=5433;Database=app;Username=u;Password=p").expect("pg");
        assert_eq!(d.db_type, DatabaseType::PostgreSQL);
        assert_eq!(d.port, "5433");
        let d = parse_value("Data Source=app.db").expect("sqlite");
        assert_eq!(d.db_type, DatabaseType::SQLite);
        assert!(parse_value("just text").is_none());
    }

    #[test]
    fn ignores_non_database_hosts() {
        let found = from_vars(&vars(&[
            ("SMTP_HOST", "mail"),
            ("SMTP_PORT", "587"),
            ("KAFKA_HOST", "k"),
            ("APP_HOST", "0.0.0.0"),
            ("APP_PORT", "5432"),
        ]));
        assert!(found.is_empty(), "{found:?}");
    }
}
