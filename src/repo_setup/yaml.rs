//! Parser subset YAML untuk file konfigurasi yang dibaca repository setup
//! (`docker-compose.yml`, `application.yml` Spring, `config/database.yml`
//! Rails).
//!
//! Sengaja kecil dan tanpa dependensi: mendukung map dan list blok berbasis
//! indentasi, scalar polos/berkutip, flow `[a, b]` dan `{k: v}` sederhana,
//! block scalar `|`/`>`, anchor `&x`, alias `*x`, dan merge key `<<`. Hanya
//! dokumen pertama yang dibaca. Fitur YAML lain (tag, kunci kompleks, scalar
//! multi-baris polos) diabaikan tanpa error, karena file yang tidak terbaca
//! cukup dilewati oleh pemanggil.

use std::collections::HashMap;

/// Kedalaman bersarang maksimum (blok maupun flow). File repository bisa
/// dibuat sengaja dalam; lebih dari ini dianggap `Null`/teks mentah supaya
/// thread discovery tidak kehabisan stack.
const MAX_DEPTH: usize = 64;
/// Total node yang boleh dihasilkan dari alias (`*x`), mencegah ledakan
/// eksponensial ala "billion laughs".
const ALIAS_BUDGET: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Yaml {
    #[default]
    Null,
    Str(String),
    List(Vec<Yaml>),
    Map(Vec<(String, Yaml)>),
}

impl Yaml {
    /// Nilai kunci `key` pada map.
    pub fn get(&self, key: &str) -> Option<&Yaml> {
        match self {
            Yaml::Map(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Jumlah node (untuk anggaran alias).
    fn size(&self) -> usize {
        match self {
            Yaml::Null | Yaml::Str(_) => 1,
            Yaml::List(items) => 1 + items.iter().map(Yaml::size).sum::<usize>(),
            Yaml::Map(entries) => 1 + entries.iter().map(|(_, v)| v.size()).sum::<usize>(),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Yaml::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_map(&self) -> &[(String, Yaml)] {
        match self {
            Yaml::Map(entries) => entries,
            _ => &[],
        }
    }

    pub fn as_list(&self) -> &[Yaml] {
        match self {
            Yaml::List(items) => items,
            _ => &[],
        }
    }

    /// Ratakan menjadi pasangan `a.b.c = nilai` (gaya properties Spring).
    /// Elemen list memakai indeks `a[0]`.
    pub fn flatten(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        flatten_into(self, String::new(), &mut out);
        out
    }
}

fn flatten_into(v: &Yaml, prefix: String, out: &mut Vec<(String, String)>) {
    match v {
        Yaml::Null => {}
        Yaml::Str(s) => out.push((prefix, s.clone())),
        Yaml::List(items) => {
            for (i, item) in items.iter().enumerate() {
                flatten_into(item, format!("{prefix}[{i}]"), out);
            }
        }
        Yaml::Map(entries) => {
            for (k, item) in entries {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_into(item, key, out);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Line {
    indent: usize,
    text: String,
}

/// Parse dokumen pertama `text`. Input yang tidak dikenali menghasilkan
/// [`Yaml::Null`] atau map sebagian, tidak pernah panic.
pub fn parse(text: &str) -> Yaml {
    let mut lines = Vec::new();
    let mut started = false;
    for raw in text.lines() {
        let raw = raw.trim_end_matches('\r');
        let trimmed = raw.trim_start();
        if trimmed == "---" || trimmed.starts_with("--- ") {
            if started {
                break;
            }
            continue;
        }
        if trimmed == "..." {
            break;
        }
        let indent = raw.len() - trimmed.len();
        // Baris kosong dan komentar dipertahankan sebagai penanda untuk block
        // scalar; parser blok melewatinya.
        lines.push(Line {
            indent,
            text: trimmed.to_string(),
        });
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            started = true;
        }
    }
    let mut p = Parser {
        lines,
        pos: 0,
        anchors: HashMap::new(),
        depth: 0,
        alias_budget: ALIAS_BUDGET,
    };
    p.skip_blank();
    if p.pos >= p.lines.len() {
        return Yaml::Null;
    }
    let indent = p.lines[p.pos].indent;
    p.block(indent)
}

struct Parser {
    lines: Vec<Line>,
    pos: usize,
    anchors: HashMap<String, Yaml>,
    depth: usize,
    alias_budget: usize,
}

impl Parser {
    fn skip_blank(&mut self) {
        while self.pos < self.lines.len() {
            let t = &self.lines[self.pos].text;
            if t.is_empty() || t.starts_with('#') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&mut self) -> Option<(usize, String)> {
        self.skip_blank();
        self.lines.get(self.pos).map(|l| (l.indent, l.text.clone()))
    }

    fn block(&mut self, indent: usize) -> Yaml {
        if self.depth >= MAX_DEPTH {
            // Lewati seluruh blok supaya pemanggil tetap maju.
            while self.peek().is_some_and(|(i, _)| i >= indent) {
                self.pos += 1;
            }
            return Yaml::Null;
        }
        self.depth += 1;
        let out = match self.peek() {
            Some((_, text)) if is_list_item(&text) => self.list(indent),
            Some(_) => self.map(indent),
            None => Yaml::Null,
        };
        self.depth -= 1;
        out
    }

    fn list(&mut self, indent: usize) -> Yaml {
        let mut items = Vec::new();
        while let Some((ind, text)) = self.peek() {
            if ind != indent || !is_list_item(&text) {
                break;
            }
            let rest = text[1..].trim_start().to_string();
            let offset = text.len() - rest.len();
            if rest.is_empty() {
                self.pos += 1;
                items.push(self.child(indent));
                continue;
            }
            if split_key(&rest).is_some() && !rest.starts_with('{') && !rest.starts_with('[') {
                // `- key: value` membuka map dengan indentasi kolom isi.
                self.lines[self.pos] = Line {
                    indent: indent + offset,
                    text: rest,
                };
                items.push(self.block(indent + offset));
                continue;
            }
            self.pos += 1;
            let (anchor, rest) = take_anchor(&rest);
            let value = self.inline_value(&rest, indent);
            if let Some(a) = anchor {
                self.anchors.insert(a, value.clone());
            }
            items.push(value);
        }
        Yaml::List(items)
    }

    fn map(&mut self, indent: usize) -> Yaml {
        let mut entries: Vec<(String, Yaml)> = Vec::new();
        let mut merges: Vec<Yaml> = Vec::new();
        while let Some((ind, text)) = self.peek() {
            if ind < indent {
                break;
            }
            if ind > indent {
                // Indentasi tak terduga: lewati blok anaknya.
                let _ = self.block(ind);
                continue;
            }
            if is_list_item(&text) {
                break;
            }
            self.pos += 1;
            let Some((key, rest)) = split_key(&text) else {
                continue;
            };
            let (anchor, rest) = take_anchor(rest.trim());
            let value = if rest.is_empty() {
                // Nilai blok: anak lebih menjorok, atau list sejajar kunci.
                match self.peek() {
                    Some((ni, _)) if ni > indent => self.block(ni),
                    Some((ni, nt)) if ni == indent && is_list_item(&nt) => self.list(indent),
                    _ => Yaml::Null,
                }
            } else {
                self.inline_value(&rest, indent)
            };
            if let Some(a) = anchor {
                self.anchors.insert(a, value.clone());
            }
            if key == "<<" {
                merges.push(value);
                continue;
            }
            match entries.iter_mut().find(|(k, _)| *k == key) {
                Some(slot) => slot.1 = value,
                None => entries.push((key, value)),
            }
        }
        for m in merges {
            let sources = match m {
                Yaml::List(items) => items,
                other => vec![other],
            };
            for src in sources {
                for (k, v) in src.as_map() {
                    if !entries.iter().any(|(ek, _)| ek == k) {
                        entries.push((k.clone(), v.clone()));
                    }
                }
            }
        }
        Yaml::Map(entries)
    }

    /// Anak sebuah `- ` kosong: blok yang lebih menjorok, atau `Null`.
    fn child(&mut self, indent: usize) -> Yaml {
        match self.peek() {
            Some((ni, _)) if ni > indent => self.block(ni),
            _ => Yaml::Null,
        }
    }

    fn inline_value(&mut self, rest: &str, indent: usize) -> Yaml {
        let rest = strip_comment(rest);
        if let Some(name) = rest.strip_prefix('*') {
            let Some(v) = self.anchors.get(name.trim()) else {
                return Yaml::Null;
            };
            let size = v.size();
            if size > self.alias_budget {
                self.alias_budget = 0;
                return Yaml::Null;
            }
            self.alias_budget -= size;
            return v.clone();
        }
        if rest.starts_with('|') || rest.starts_with('>') {
            return Yaml::Str(self.block_scalar(indent, rest.starts_with('>')));
        }
        scalar_or_flow(rest, 0)
    }

    fn block_scalar(&mut self, indent: usize, folded: bool) -> String {
        let mut parts: Vec<String> = Vec::new();
        let mut base: Option<usize> = None;
        while let Some(line) = self.lines.get(self.pos) {
            if line.text.is_empty() {
                parts.push(String::new());
                self.pos += 1;
                continue;
            }
            if line.indent <= indent {
                break;
            }
            let b = *base.get_or_insert(line.indent);
            let pad = " ".repeat(line.indent.saturating_sub(b));
            parts.push(format!("{pad}{}", line.text));
            self.pos += 1;
        }
        while parts.last().is_some_and(String::is_empty) {
            parts.pop();
        }
        if folded {
            parts.join(" ")
        } else {
            parts.join("\n")
        }
    }
}

fn is_list_item(text: &str) -> bool {
    text == "-" || text.starts_with("- ")
}

/// Pisahkan `kunci: nilai` di luar kutipan. Kunci boleh berkutip.
fn split_key(text: &str) -> Option<(String, &str)> {
    let bytes = text.as_bytes();
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None if (b == b'"' || b == b'\'') && i == 0 => quote = Some(b),
            None if b == b'#' && (i == 0 || bytes[i - 1] == b' ') => return None,
            None if b == b':' && (i + 1 == bytes.len() || bytes[i + 1] == b' ') => {
                let key = unquote(text[..i].trim());
                if key.is_empty() {
                    return None;
                }
                return Some((key, &text[i + 1..]));
            }
            None => {}
        }
    }
    None
}

fn take_anchor(text: &str) -> (Option<String>, String) {
    if let Some(rest) = text.strip_prefix('&') {
        let (name, after) = rest.split_once(' ').unwrap_or((rest, ""));
        return (Some(name.to_string()), after.trim().to_string());
    }
    (None, text.to_string())
}

/// Buang komentar ` # ...` di luar kutipan.
fn strip_comment(text: &str) -> &str {
    let bytes = text.as_bytes();
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None if b == b'"' || b == b'\'' => quote = Some(b),
            None if b == b'#' && (i == 0 || bytes[i - 1] == b' ' || bytes[i - 1] == b'\t') => {
                return text[..i].trim_end();
            }
            None => {}
        }
    }
    text.trim_end()
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        let inner = &s[1..s.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        }
        return out;
    }
    if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
        return s[1..s.len() - 1].replace("''", "'");
    }
    s.to_string()
}

fn scalar_or_flow(s: &str, depth: usize) -> Yaml {
    let s = s.trim();
    if depth >= MAX_DEPTH {
        return Yaml::Str(s.to_string());
    }
    if s.is_empty() || s == "~" || s == "null" {
        return Yaml::Null;
    }
    if s.starts_with('[') && s.ends_with(']') {
        return Yaml::List(
            split_flow(&s[1..s.len() - 1])
                .into_iter()
                .map(|p| scalar_or_flow(&p, depth + 1))
                .collect(),
        );
    }
    if s.starts_with('{') && s.ends_with('}') {
        let mut entries = Vec::new();
        for part in split_flow(&s[1..s.len() - 1]) {
            match split_key(&part) {
                Some((k, v)) => entries.push((k, scalar_or_flow(v, depth + 1))),
                None => entries.push((unquote(&part), Yaml::Null)),
            }
        }
        return Yaml::Map(entries);
    }
    Yaml::Str(unquote(s))
}

/// Pisahkan isi flow collection di koma tingkat atas.
fn split_flow(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for c in s.chars() {
        match quote {
            Some(q) if c == q => {
                quote = None;
                cur.push(c);
            }
            Some(_) => cur.push(c),
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '[' | '{' => {
                    depth += 1;
                    cur.push(c);
                }
                ']' | '}' => {
                    depth -= 1;
                    cur.push(c);
                }
                ',' if depth == 0 => {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compose_file() {
        let src = r#"
version: "3.8"
x-db-env: &dbenv
  POSTGRES_USER: app
  POSTGRES_PASSWORD: "s3cr#t"   # komentar
services:
  db:
    image: postgres:16
    environment:
      <<: *dbenv
      POSTGRES_DB: orders
    ports:
      - "5433:5432"
  api:
    build:
      context: ./services/orders
    environment:
      - DATABASE_URL=postgres://app:x@db:5432/orders
    ports: ["8080:8080"]
"#;
        let y = parse(src);
        let db = y.get("services").and_then(|s| s.get("db")).expect("db");
        assert_eq!(db.get("image").and_then(Yaml::as_str), Some("postgres:16"));
        let env = db.get("environment").expect("env");
        assert_eq!(env.get("POSTGRES_USER").and_then(Yaml::as_str), Some("app"));
        assert_eq!(
            env.get("POSTGRES_PASSWORD").and_then(Yaml::as_str),
            Some("s3cr#t")
        );
        assert_eq!(
            env.get("POSTGRES_DB").and_then(Yaml::as_str),
            Some("orders")
        );
        assert_eq!(
            db.get("ports").map(|p| p.as_list().to_vec()),
            Some(vec![Yaml::Str("5433:5432".into())])
        );
        let api = y.get("services").and_then(|s| s.get("api")).expect("api");
        assert_eq!(
            api.get("build")
                .and_then(|b| b.get("context"))
                .and_then(Yaml::as_str),
            Some("./services/orders")
        );
        assert_eq!(api.get("environment").map(|e| e.as_list().len()), Some(1));
        assert_eq!(api.get("ports").map(|e| e.as_list().len()), Some(1));
    }

    #[test]
    fn parses_list_of_maps_and_block_scalars() {
        let src =
            "items:\n- name: a\n  port: 1\n- name: b\nnote: |\n  line one\n  line two\nend: x\n";
        let y = parse(src);
        let items = y.get("items").expect("items").as_list();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get("port").and_then(Yaml::as_str), Some("1"));
        assert_eq!(items[1].get("name").and_then(Yaml::as_str), Some("b"));
        assert_eq!(
            y.get("note").and_then(Yaml::as_str),
            Some("line one\nline two")
        );
        assert_eq!(y.get("end").and_then(Yaml::as_str), Some("x"));
    }

    #[test]
    fn flattens_spring_style_keys() {
        let src =
            "spring:\n  datasource:\n    url: jdbc:postgresql://localhost/app\n    username: sa\n";
        let flat = parse(src).flatten();
        assert!(flat.contains(&(
            "spring.datasource.url".to_string(),
            "jdbc:postgresql://localhost/app".to_string()
        )));
        assert!(flat.contains(&("spring.datasource.username".to_string(), "sa".to_string())));
    }

    #[test]
    fn survives_deep_nesting_and_alias_bombs() {
        let deep = format!("a: {}{}\n", "[".repeat(200_000), "]".repeat(200_000));
        let _ = parse(&deep);
        let mut block = String::new();
        for i in 0..2_000 {
            block.push_str(&" ".repeat(i));
            block.push_str("k:\n");
        }
        let _ = parse(&block);
        let mut bomb = String::from("a: &a [x, x, x, x, x, x, x, x, x, x]\n");
        let names = ["b", "c", "d", "e", "f", "g", "h", "i", "j"];
        let mut prev = "a";
        for n in names {
            bomb.push_str(&format!("{n}: &{n}\n"));
            for _ in 0..10 {
                bomb.push_str(&format!("  - *{prev}\n"));
            }
            prev = n;
        }
        let y = parse(&bomb);
        assert!(y.size() < 2 * ALIAS_BUDGET, "{}", y.size());
    }

    #[test]
    fn reads_only_first_document_and_survives_garbage() {
        let y = parse("---\na: 1\n---\nb: 2\n");
        assert_eq!(y.get("a").and_then(Yaml::as_str), Some("1"));
        assert!(y.get("b").is_none());
        let _ = parse(":::\n  - - -\n\t{[}");
        assert_eq!(parse(""), Yaml::Null);
    }
}
