//! Koneksi database dari file konfigurasi framework:
//! - Spring Boot `application*.properties` / `application*.yml`,
//! - .NET `appsettings*.json` (`ConnectionStrings`),
//! - Rails `config/database.yml` (ERB `ENV[...]` diselesaikan),
//! - TypeORM `ormconfig.json`,
//! - Prisma `schema.prisma` (hanya URL literal; `env("X")` sudah tercakup
//!   oleh `.env`).
//!
//! Placeholder `${X}` / `${X:default}` diselesaikan dari variabel `.env`
//! environment yang sama bila ada, lalu nilai bawaannya.

use std::sync::OnceLock;

use regex::Regex;

use super::dsn_detect::{self, FoundDsn};
use super::yaml::{self, Yaml};
use crate::connection_env::{self, Environment};
use crate::deeplink::ParsedDsn;
use crate::models::enums::DatabaseType;

/// Jenis file konfigurasi yang dikenali dari namanya.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigKind {
    SpringProperties,
    SpringYaml,
    AppSettings,
    RailsDatabase,
    OrmConfig,
    Prisma,
}

/// Kenali file konfigurasi dari path relatifnya (pemisah `/`).
pub fn classify(rel: &str) -> Option<ConfigKind> {
    let name = rel.rsplit('/').next().unwrap_or(rel).to_ascii_lowercase();
    if name.starts_with("application") && name.ends_with(".properties") {
        return Some(ConfigKind::SpringProperties);
    }
    if name.starts_with("application") && (name.ends_with(".yml") || name.ends_with(".yaml")) {
        return Some(ConfigKind::SpringYaml);
    }
    if name.starts_with("appsettings") && name.ends_with(".json") {
        return Some(ConfigKind::AppSettings);
    }
    if name == "database.yml" && rel.ends_with("config/database.yml") {
        return Some(ConfigKind::RailsDatabase);
    }
    if name == "ormconfig.json" {
        return Some(ConfigKind::OrmConfig);
    }
    if name == "schema.prisma" {
        return Some(ConfigKind::Prisma);
    }
    None
}

/// Environment dari nama file profil (`application-prod.yml`,
/// `appsettings.Staging.json`); tanpa profil = Development.
pub fn environment_of(rel: &str) -> Environment {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    let stem = name.split('.').next().unwrap_or(name);
    let profile = stem
        .strip_prefix("application-")
        .map(str::to_string)
        .or_else(|| {
            name.strip_prefix("appsettings.")
                .and_then(|r| r.strip_suffix(".json"))
                .map(str::to_string)
        })
        .unwrap_or_default();
    connection_env::detect_from_name(&profile).unwrap_or(Environment::Development)
}

/// Satu koneksi dari file konfigurasi, dengan environment-nya.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDsn {
    pub found: FoundDsn,
    pub environment: Environment,
}

/// Baca koneksi dari isi file `kind`. `vars` = variabel `.env` untuk
/// menyelesaikan placeholder.
pub fn extract(
    kind: ConfigKind,
    rel: &str,
    text: &str,
    vars: &[(String, String)],
) -> Vec<ConfigDsn> {
    let env = environment_of(rel);
    let wrap = |found: Vec<FoundDsn>, environment: Environment| {
        found
            .into_iter()
            .map(|found| ConfigDsn { found, environment })
            .collect::<Vec<_>>()
    };
    match kind {
        ConfigKind::SpringProperties => wrap(spring(&parse_properties(text), vars), env),
        ConfigKind::SpringYaml => wrap(spring(&yaml::parse(text).flatten(), vars), env),
        ConfigKind::AppSettings => wrap(appsettings(text), env),
        ConfigKind::RailsDatabase => rails(text, vars),
        ConfigKind::OrmConfig => wrap(ormconfig(text), env),
        ConfigKind::Prisma => wrap(prisma(text), env),
    }
}

/// `key=value` / `key: value` gaya `.properties`.
pub fn parse_properties(text: &str) -> Vec<(String, String)> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('!'))
        .filter_map(|l| {
            let i = l.find(['=', ':'])?;
            Some((l[..i].trim().to_string(), l[i + 1..].trim().to_string()))
        })
        .collect()
}

fn spring_placeholder_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\$\{([A-Za-z0-9_.\-]+)(?::([^}]*))?\}").expect("regex"))
}

/// Selesaikan `${X}` / `${X:default}` gaya Spring.
pub fn resolve_spring(value: &str, vars: &[(String, String)]) -> String {
    spring_placeholder_re()
        .replace_all(value, |c: &regex::Captures<'_>| {
            let name = &c[1];
            let env_name = name.to_ascii_uppercase().replace(['.', '-'], "_");
            vars.iter()
                .find(|(k, _)| k == name || *k == env_name)
                .map(|(_, v)| v.clone())
                .or_else(|| c.get(2).map(|m| m.as_str().to_string()))
                .unwrap_or_else(|| c[0].to_string())
        })
        .into_owned()
}

fn spring(flat: &[(String, String)], vars: &[(String, String)]) -> Vec<FoundDsn> {
    let get = |key: &str| {
        flat.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| resolve_spring(v, vars))
    };
    let mut out = Vec::new();
    for prefix in ["spring.datasource", "spring.r2dbc"] {
        if let Some(url) = get(&format!("{prefix}.url"))
            && let Some(mut dsn) =
                dsn_detect::parse_value(&url.replace("r2dbc:", "").replace("r2dbcs:", ""))
        {
            if let Some(u) = get(&format!("{prefix}.username")) {
                dsn.username = u;
            }
            if let Some(p) = get(&format!("{prefix}.password")) {
                dsn.password = p;
            }
            out.push(finish(dsn, &format!("{prefix}.url")));
        }
    }
    for prefix in ["spring.data.mongodb", "spring.mongodb"] {
        if let Some(uri) = get(&format!("{prefix}.uri"))
            && let Some(dsn) = dsn_detect::parse_value(&uri)
        {
            out.push(finish(dsn, &format!("{prefix}.uri")));
        } else if let Some(host) = get(&format!("{prefix}.host")) {
            let dsn = ParsedDsn {
                db_type: DatabaseType::MongoDB,
                host,
                port: get(&format!("{prefix}.port")).unwrap_or_else(|| "27017".into()),
                username: get(&format!("{prefix}.username")).unwrap_or_default(),
                password: get(&format!("{prefix}.password")).unwrap_or_default(),
                database: get(&format!("{prefix}.database")).unwrap_or_default(),
                ssl: false,
            };
            out.push(finish(dsn, &format!("{prefix}.host")));
        }
    }
    for prefix in ["spring.data.redis", "spring.redis"] {
        if let Some(url) = get(&format!("{prefix}.url"))
            && let Some(dsn) = dsn_detect::parse_value(&url)
        {
            out.push(finish(dsn, &format!("{prefix}.url")));
        } else if let Some(host) = get(&format!("{prefix}.host")) {
            let dsn = ParsedDsn {
                db_type: DatabaseType::Redis,
                host,
                port: get(&format!("{prefix}.port")).unwrap_or_else(|| "6379".into()),
                username: get(&format!("{prefix}.username")).unwrap_or_default(),
                password: get(&format!("{prefix}.password")).unwrap_or_default(),
                database: get(&format!("{prefix}.database")).unwrap_or_default(),
                ssl: false,
            };
            out.push(finish(dsn, &format!("{prefix}.host")));
        }
    }
    out
}

fn finish(dsn: ParsedDsn, key: &str) -> FoundDsn {
    let placeholder = [&dsn.host, &dsn.database, &dsn.username, &dsn.password]
        .iter()
        .any(|v| dsn_detect::looks_placeholder(v))
        || (dsn.db_type != DatabaseType::SQLite && dsn.host.trim().is_empty());
    FoundDsn {
        dsn,
        key: key.to_string(),
        placeholder,
    }
}

fn appsettings(text: &str) -> Vec<FoundDsn> {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(strip_json_comments(text).as_str())
    else {
        return Vec::new();
    };
    let Some(map) = json
        .as_object()
        .and_then(|o| {
            o.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("ConnectionStrings"))
        })
        .and_then(|(_, v)| v.as_object())
    else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(name, v)| {
            let dsn = dsn_detect::parse_value(v.as_str()?)?;
            Some(finish(dsn, &format!("ConnectionStrings:{name}")))
        })
        .collect()
}

/// `appsettings.json` sering berisi komentar `//`.
fn strip_json_comments(text: &str) -> String {
    text.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn erb_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"<%=\s*ENV(?:\[\s*["']([A-Za-z0-9_]+)["']\s*\]|\.fetch\(\s*["']([A-Za-z0-9_]+)["']\s*(?:,\s*["']?([^"')]*)["']?\s*)?\)(?:\s*\{\s*["']?([^"'}]*)["']?\s*\})?)\s*(?:\|\|\s*["']?([^"'%]*)["']?\s*)?%>"#,
        )
        .expect("regex")
    })
}

/// Selesaikan ERB `<%= ENV['X'] %>`, `<%= ENV.fetch("X") { "d" } %>`,
/// `<%= ENV.fetch('X', 'd') %>`, `<%= ENV['X'] || 'd' %>`.
pub fn resolve_erb(text: &str, vars: &[(String, String)]) -> String {
    erb_re()
        .replace_all(text, |c: &regex::Captures<'_>| {
            let name = c.get(1).or(c.get(2)).map(|m| m.as_str()).unwrap_or("");
            vars.iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .or_else(|| {
                    c.get(3)
                        .or(c.get(4))
                        .or(c.get(5))
                        .map(|m| m.as_str().trim().to_string())
                })
                .unwrap_or_else(|| format!("${{{name}}}"))
        })
        .into_owned()
}

fn rails(text: &str, vars: &[(String, String)]) -> Vec<ConfigDsn> {
    let doc = yaml::parse(&resolve_erb(text, vars));
    let mut out = Vec::new();
    for (env_name, cfg) in doc.as_map() {
        if env_name == "default" || env_name.starts_with("x-") {
            continue;
        }
        let environment =
            connection_env::detect_from_name(env_name).unwrap_or(Environment::Development);
        // Rails 6+: konfigurasi multi-database (`primary:`, `cache:`).
        let configs: Vec<(String, &Yaml)> =
            if cfg.get("adapter").is_some() || cfg.get("url").is_some() {
                vec![(String::new(), cfg)]
            } else {
                cfg.as_map().iter().map(|(n, c)| (n.clone(), c)).collect()
            };
        for (sub, c) in configs {
            let s = |k: &str| c.get(k).and_then(Yaml::as_str).map(str::to_string);
            let key = if sub.is_empty() {
                env_name.clone()
            } else {
                format!("{env_name}.{sub}")
            };
            if let Some(url) = s("url")
                && let Some(dsn) = dsn_detect::parse_value(&url)
            {
                out.push(ConfigDsn {
                    found: finish(dsn, &key),
                    environment,
                });
                continue;
            }
            let Some(db_type) = s("adapter").as_deref().and_then(dsn_detect::type_from_hint) else {
                continue;
            };
            let database = s("database").unwrap_or_default();
            let dsn = if db_type == DatabaseType::SQLite {
                ParsedDsn {
                    db_type,
                    host: String::new(),
                    port: String::new(),
                    username: String::new(),
                    password: String::new(),
                    database,
                    ssl: false,
                }
            } else {
                ParsedDsn {
                    port: s("port").unwrap_or_else(|| dsn_detect::default_port(&db_type).into()),
                    db_type,
                    host: s("host").unwrap_or_else(|| "localhost".into()),
                    username: s("username").or_else(|| s("user")).unwrap_or_default(),
                    password: s("password").unwrap_or_default(),
                    database,
                    ssl: false,
                }
            };
            out.push(ConfigDsn {
                found: finish(dsn, &key),
                environment,
            });
        }
    }
    out
}

fn ormconfig(text: &str) -> Vec<FoundDsn> {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    let items: Vec<&serde_json::Value> = match &json {
        serde_json::Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    items
        .into_iter()
        .filter_map(|o| {
            let s = |k: &str| {
                o.get(k).and_then(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
            };
            if let Some(url) = s("url") {
                return dsn_detect::parse_value(&url).map(|d| finish(d, "ormconfig.url"));
            }
            let db_type = dsn_detect::type_from_hint(&s("type")?)?;
            let dsn = ParsedDsn {
                port: s("port").unwrap_or_else(|| dsn_detect::default_port(&db_type).into()),
                host: if db_type == DatabaseType::SQLite {
                    String::new()
                } else {
                    s("host").unwrap_or_else(|| "localhost".into())
                },
                db_type,
                username: s("username").unwrap_or_default(),
                password: s("password").unwrap_or_default(),
                database: s("database").unwrap_or_default(),
                ssl: false,
            };
            Some(finish(dsn, "ormconfig"))
        })
        .collect()
}

fn prisma(text: &str) -> Vec<FoundDsn> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r#"(?m)^\s*url\s*=\s*"([^"]+)""#).expect("regex"));
    re.captures_iter(text)
        .filter_map(|c| dsn_detect::parse_value(&c[1]).map(|d| finish(d, "schema.prisma")))
        .collect()
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
    fn classifies_and_detects_profiles() {
        assert_eq!(
            classify("src/main/resources/application.yml"),
            Some(ConfigKind::SpringYaml)
        );
        assert_eq!(
            classify("application-prod.properties"),
            Some(ConfigKind::SpringProperties)
        );
        assert_eq!(
            classify("Api/appsettings.Development.json"),
            Some(ConfigKind::AppSettings)
        );
        assert_eq!(
            classify("config/database.yml"),
            Some(ConfigKind::RailsDatabase)
        );
        assert_eq!(classify("prisma/schema.prisma"), Some(ConfigKind::Prisma));
        assert_eq!(classify("src/app.ts"), None);
        assert_eq!(
            environment_of("application-prod.yml"),
            Environment::Production
        );
        assert_eq!(
            environment_of("appsettings.Staging.json"),
            Environment::Staging
        );
        assert_eq!(environment_of("application.yml"), Environment::Development);
    }

    #[test]
    fn reads_spring_yaml_with_placeholders() {
        let src = "spring:\n  datasource:\n    url: ${DB_URL:jdbc:postgresql://localhost:5432/orders}\n    username: ${DB_USER}\n    password: secret\n  data:\n    redis:\n      host: localhost\n";
        let found = extract(
            ConfigKind::SpringYaml,
            "application.yml",
            src,
            &vars(&[("DB_USER", "app")]),
        );
        assert_eq!(found.len(), 2, "{found:?}");
        let pg = &found[0].found.dsn;
        assert_eq!(pg.db_type, DatabaseType::PostgreSQL);
        assert_eq!(pg.database, "orders");
        assert_eq!(pg.username, "app");
        assert_eq!(pg.password, "secret");
        assert_eq!(found[1].found.dsn.db_type, DatabaseType::Redis);
    }

    #[test]
    fn reads_spring_properties() {
        let src =
            "spring.datasource.url=jdbc:mysql://db:3306/shop\nspring.datasource.username=root\n";
        let found = extract(
            ConfigKind::SpringProperties,
            "application-staging.properties",
            src,
            &[],
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].environment, Environment::Staging);
        assert_eq!(found[0].found.dsn.db_type, DatabaseType::MySQL);
    }

    #[test]
    fn reads_appsettings_connection_strings() {
        let src = r#"{
  // komentar
  "ConnectionStrings": { "Default": "Server=localhost,1433;Database=Shop;User Id=sa;Password=x" },
  "Logging": {}
}"#;
        let found = extract(ConfigKind::AppSettings, "appsettings.json", src, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].found.dsn.db_type, DatabaseType::MsSQL);
        assert_eq!(found[0].found.key, "ConnectionStrings:Default");
    }

    #[test]
    fn reads_rails_database_yml_with_erb_and_anchors() {
        let src = r#"
default: &default
  adapter: postgresql
  host: <%= ENV.fetch("DB_HOST") { "localhost" } %>
  username: <%= ENV['DB_USER'] %>
development:
  <<: *default
  database: shop_dev
test:
  <<: *default
  database: shop_test
production:
  url: <%= ENV['DATABASE_URL'] %>
"#;
        let found = extract(
            ConfigKind::RailsDatabase,
            "config/database.yml",
            src,
            &vars(&[("DB_USER", "rails")]),
        );
        assert_eq!(found.len(), 2, "{found:?}");
        let dev = found
            .iter()
            .find(|f| f.environment == Environment::Development)
            .expect("dev");
        assert_eq!(dev.found.dsn.host, "localhost");
        assert_eq!(dev.found.dsn.username, "rails");
        assert_eq!(dev.found.dsn.database, "shop_dev");
        assert!(found.iter().any(|f| f.environment == Environment::Testing));
    }

    #[test]
    fn reads_ormconfig_and_prisma() {
        let found = extract(
            ConfigKind::OrmConfig,
            "ormconfig.json",
            r#"{"type":"mysql","host":"localhost","port":3307,"username":"u","database":"d"}"#,
            &[],
        );
        assert_eq!(found[0].found.dsn.port, "3307");
        let found = extract(
            ConfigKind::Prisma,
            "schema.prisma",
            "datasource db {\n  provider = \"postgresql\"\n  url = \"postgresql://u:p@localhost:5432/app\"\n}\n",
            &[],
        );
        assert_eq!(found.len(), 1);
        assert!(
            extract(
                ConfigKind::Prisma,
                "schema.prisma",
                "  url = env(\"DATABASE_URL\")",
                &[]
            )
            .is_empty()
        );
    }
}
