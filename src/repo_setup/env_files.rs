//! File `.env` di repository: pengenalan nama file, environment yang
//! diwakilinya, dan parser `KEY=value`.
//!
//! Parser ditulis sendiri (bukan `dotenvy`) supaya substitusi `${VAR}` hanya
//! memakai variabel dari file yang sama dan tidak pernah membaca environment
//! proses Tabular.

use crate::connection_env::{self, Environment};

/// Nama file `.env` yang hanya berisi contoh/template, bukan nilai sungguhan.
const TEMPLATE_MARKERS: &[&str] = &["example", "sample", "template", "dist", "defaults", "tpl"];

/// Environment yang diwakili file bernama `name`. `None` = bukan file env
/// atau hanya template (`.env.example`).
pub fn environment_for_file(name: &str) -> Option<Environment> {
    let lower = name.to_ascii_lowercase();
    if lower == ".env" || lower == ".envrc" {
        return Some(Environment::Development);
    }
    // `prod.env` hanya dikenali bila namanya menyebut environment;
    // `secrets.env` / `app.env` (sering untuk Helm/Kubernetes) dilewati.
    if !lower.starts_with(".env")
        && let Some(stem) = lower.strip_suffix(".env").filter(|s| !s.is_empty())
    {
        return connection_env::detect_from_name(stem);
    }
    let suffix = lower
        .strip_prefix(".env.")
        .or_else(|| lower.strip_prefix(".env-"))?;
    let tokens: Vec<&str> = suffix
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    if tokens.iter().any(|t| TEMPLATE_MARKERS.contains(t)) {
        return None;
    }
    // Ekstensi file lain (`.env.ts`, `.env.d`) bukan file env.
    if tokens.iter().any(|t| {
        matches!(
            *t,
            "js" | "ts" | "json" | "d" | "py" | "go" | "rs" | "bak" | "vault"
        )
    }) {
        return None;
    }
    Some(connection_env::detect_from_name(suffix).unwrap_or(
        if tokens.iter().any(|t| matches!(*t, "docker" | "compose")) {
            Environment::Local
        } else {
            Environment::Development
        },
    ))
}

/// Environment file `rel` (path relatif): dari namanya, lalu dari nama
/// folder bila namanya hanya `.env` (`deploy/production/.env`).
pub fn environment_for_path(rel: &str) -> Option<Environment> {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    let env = environment_for_file(name)?;
    let lower = name.to_ascii_lowercase();
    if (lower == ".env" || lower == ".envrc")
        && let Some((dir, _)) = rel.rsplit_once('/')
        && let Some(from_dir) = connection_env::detect_from_name(dir)
    {
        return Some(from_dir);
    }
    Some(env)
}

/// Nama environment project untuk `env` (selalu English, tidak
/// dilokalkan, karena tersimpan di file project).
pub fn project_env_name(env: Environment) -> &'static str {
    match env {
        Environment::Production => "Production",
        Environment::Staging => "Staging",
        Environment::Development => "Development",
        Environment::Testing => "Testing",
        Environment::Local => "Local",
    }
}

/// Parse isi file `.env`. Urutan dipertahankan; kunci ganda memakai nilai
/// terakhir.
pub fn parse(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
        {
            continue;
        }
        let value = value.trim_start();
        let parsed = if let Some(rest) = value.strip_prefix('\'') {
            rest.split_once('\'')
                .map(|(v, _)| v)
                .unwrap_or(rest)
                .to_string()
        } else if let Some(rest) = value.strip_prefix('"') {
            substitute(&unescape_double(rest), &out)
        } else {
            let v = match value.find(" #") {
                Some(i) => &value[..i],
                None => value,
            };
            substitute(v.trim(), &out)
        };
        match out.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = parsed,
            None => out.push((key.to_string(), parsed)),
        }
    }
    out
}

fn unescape_double(rest: &str) -> String {
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => break,
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some(o) => out.push(o),
                None => break,
            },
            _ => out.push(c),
        }
    }
    out
}

/// Ganti `${VAR}`, `${VAR:-default}`, `${VAR-default}` dan `$VAR` dengan
/// nilai dari `vars`. Variabel yang tidak dikenal dibiarkan apa adanya
/// (kecuali punya default), supaya pemanggil bisa menandainya placeholder.
pub fn substitute(value: &str, vars: &[(String, String)]) -> String {
    let lookup = |name: &str| {
        vars.iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let mut out = String::with_capacity(value.len());
    let chars: Vec<char> = value.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '$' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'{') {
            let Some(end) = chars[i + 2..].iter().position(|&c| c == '}') else {
                out.extend(&chars[i..]);
                break;
            };
            let inner: String = chars[i + 2..i + 2 + end].iter().collect();
            let (name, default) = match inner.split_once(":-").or_else(|| inner.split_once('-')) {
                Some((n, d)) => (n.to_string(), Some(d.to_string())),
                None => (inner.clone(), None),
            };
            match lookup(&name).filter(|v| !v.is_empty()).or(default) {
                Some(v) => out.push_str(&v),
                None => {
                    out.push_str("${");
                    out.push_str(&inner);
                    out.push('}');
                }
            }
            i += end + 3;
            continue;
        }
        let name: String = chars[i + 1..]
            .iter()
            .take_while(|c| c.is_ascii_alphanumeric() || **c == '_')
            .collect();
        if name.is_empty() {
            out.push('$');
            i += 1;
            continue;
        }
        match lookup(&name) {
            Some(v) => out.push_str(&v),
            None => {
                out.push('$');
                out.push_str(&name);
            }
        }
        i += 1 + name.chars().count();
    }
    out
}

/// Variabel rahasia: dari namanya, atau dari nilainya (URL dengan
/// `user:password@`, connection string berisi `Password=`).
pub fn is_secret(key: &str, value: &str) -> bool {
    if is_secret_key(key) {
        return true;
    }
    let v = value.trim();
    if let Some((_, rest)) = v.split_once("://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
        if let Some((userinfo, _)) = authority.rsplit_once('@')
            && userinfo.contains(':')
        {
            return true;
        }
    }
    let lower = v.to_ascii_lowercase();
    lower.contains("password=") || lower.contains("pwd=")
}

/// Kunci yang nilainya rahasia (disimpan di keychain, bukan file project).
pub fn is_secret_key(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    [
        "PASSWORD",
        "PASSWD",
        "PWD",
        "SECRET",
        "TOKEN",
        "PRIVATE",
        "CREDENTIAL",
        "API_KEY",
        "APIKEY",
        "ACCESS_KEY",
        "AUTH",
    ]
    .iter()
    .any(|m| upper.contains(m))
        || upper.ends_with("_KEY")
        || upper.ends_with("_URL") && upper.contains("DATABASE")
        || upper.ends_with("_DSN")
        || upper.ends_with("_SALT")
        || upper.ends_with("_PASS")
        || upper.ends_with("_URI") && (upper.contains("MONGO") || upper.contains("DB"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_file_names_to_environments() {
        assert_eq!(environment_for_file(".env"), Some(Environment::Development));
        assert_eq!(
            environment_for_file(".env.production"),
            Some(Environment::Production)
        );
        assert_eq!(
            environment_for_file(".env.prod"),
            Some(Environment::Production)
        );
        assert_eq!(
            environment_for_file(".env.staging"),
            Some(Environment::Staging)
        );
        assert_eq!(
            environment_for_file(".env.test"),
            Some(Environment::Testing)
        );
        assert_eq!(environment_for_file(".env.local"), Some(Environment::Local));
        assert_eq!(
            environment_for_file(".env.development.local"),
            Some(Environment::Development)
        );
        assert_eq!(
            environment_for_file(".env.docker"),
            Some(Environment::Local)
        );
        assert_eq!(
            environment_for_file("prod.env"),
            Some(Environment::Production)
        );
        assert_eq!(environment_for_file("secrets.env"), None);
        assert_eq!(environment_for_file(".env.example"), None);
        assert_eq!(environment_for_file(".env.production.sample"), None);
        assert_eq!(environment_for_file(".env.ts"), None);
        assert_eq!(environment_for_file("README.md"), None);
    }

    #[test]
    fn uses_folder_names_for_plain_env_files() {
        assert_eq!(
            environment_for_path("deploy/production/.env"),
            Some(Environment::Production)
        );
        assert_eq!(
            environment_for_path("services/devices/.env"),
            Some(Environment::Development)
        );
        assert_eq!(
            environment_for_path("k8s/staging/.env.local"),
            Some(Environment::Local)
        );
        assert_eq!(environment_for_path("helm/secrets.env"), None);
    }

    #[test]
    fn parses_quotes_comments_exports_and_substitution() {
        let src = r#"
# comment
export DB_HOST=localhost
DB_PORT=5432 # inline
DB_USER='app user'
DB_PASSWORD="p\"w#1"
DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/app
FALLBACK=${MISSING:-def}
UNKNOWN=$NOPE/x
EMPTY=
bad line
DB_PORT=5433
"#;
        let vars = parse(src);
        let get = |k: &str| vars.iter().find(|(x, _)| x == k).map(|(_, v)| v.as_str());
        assert_eq!(get("DB_HOST"), Some("localhost"));
        assert_eq!(get("DB_PORT"), Some("5433"));
        assert_eq!(get("DB_USER"), Some("app user"));
        assert_eq!(get("DB_PASSWORD"), Some("p\"w#1"));
        assert_eq!(
            get("DATABASE_URL"),
            Some("postgres://app user:p\"w#1@localhost:5432/app")
        );
        assert_eq!(get("FALLBACK"), Some("def"));
        assert_eq!(get("UNKNOWN"), Some("$NOPE/x"));
        assert_eq!(get("EMPTY"), Some(""));
        assert_eq!(vars.iter().filter(|(k, _)| k == "DB_PORT").count(), 1);
    }

    #[test]
    fn detects_secret_keys() {
        assert!(is_secret_key("DB_PASSWORD"));
        assert!(is_secret_key("JWT_SECRET"));
        assert!(is_secret_key("STRIPE_API_KEY"));
        assert!(is_secret_key("DATABASE_URL"));
        assert!(!is_secret_key("DB_HOST"));
        assert!(!is_secret_key("APP_NAME"));
        assert!(is_secret("REDIS_URL", "redis://:pw@cache:6379"));
        assert!(is_secret("BROKER_URL", "amqp://u:p@h/vhost"));
        assert!(is_secret("ConnectionStrings__Db", "Server=x;Password=y"));
        assert!(!is_secret("APP_URL", "https://user@example.org/path"));
        assert!(!is_secret("APP_URL", "http://localhost:3000"));
    }
}
