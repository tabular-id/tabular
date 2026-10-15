//! Highlighter heuristik per baris untuk source code dan file konfigurasi di
//! File Viewer. Sub-modul `syntax_ts` agar memakai palet warna yang sama.
//!
//! Batasan yang disengaja: highlighter bekerja per baris (hasilnya di-cache
//! per baris), sehingga komentar blok yang melintasi banyak baris hanya
//! dikenali lewat heuristik baris yang diawali `*` pada bahasa mirip C.

use eframe::egui::text::LayoutJob;
use eframe::egui::{Color32, TextFormat};

use crate::file_viewer::{CodeLang, CodeSyntax, SyntaxStyle};

fn push(job: &mut LayoutJob, text: &str, color: Color32) {
    if text.is_empty() {
        return;
    }
    job.append(
        text,
        0.0,
        TextFormat {
            color,
            ..Default::default()
        },
    );
}

fn type_color(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(120, 220, 170)
    } else {
        Color32::from_rgb(0, 120, 110)
    }
}

fn diff_add_color(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(110, 220, 120)
    } else {
        Color32::from_rgb(20, 130, 40)
    }
}

fn diff_del_color(dark: bool) -> Color32 {
    if dark {
        Color32::from_rgb(255, 110, 110)
    } else {
        Color32::from_rgb(190, 30, 30)
    }
}

/// Highlight satu baris `line` sesuai bahasa `lang`.
pub(super) fn highlight_code_line(line: &str, lang: CodeLang, dark: bool, job: &mut LayoutJob) {
    let syntax = lang.syntax();
    match syntax.style {
        SyntaxStyle::Plain => push(job, line, super::normal_color(dark)),
        SyntaxStyle::Json => super::highlight_json_like_line(line, dark, job),
        SyntaxStyle::Diff => diff_line(line, dark, job),
        SyntaxStyle::KeyValue => key_value_line(line, &syntax, lang, dark, job),
        SyntaxStyle::Markup => markup_line(line, &syntax, dark, job),
        SyntaxStyle::Generic => generic_line(line, &syntax, dark, job),
    }
}

fn diff_line(line: &str, dark: bool, job: &mut LayoutJob) {
    let color = if line.starts_with("+++") || line.starts_with("---") {
        super::keyword_color(dark)
    } else if line.starts_with('+') {
        diff_add_color(dark)
    } else if line.starts_with('-') {
        diff_del_color(dark)
    } else if line.starts_with("@@") {
        type_color(dark)
    } else if line.starts_with("diff ") || line.starts_with("index ") {
        super::comment_color(dark)
    } else {
        super::normal_color(dark)
    };
    push(job, line, color);
}

fn starts_comment<'a>(rest: &str, syntax: &'a CodeSyntax) -> Option<&'a str> {
    syntax
        .line_comments
        .iter()
        .copied()
        .find(|c| rest.starts_with(c))
}

fn is_keyword(word: &str, syntax: &CodeSyntax) -> bool {
    if syntax.case_insensitive {
        syntax.keywords.iter().any(|k| k.eq_ignore_ascii_case(word))
    } else {
        syntax.keywords.contains(&word)
    }
}

/// Panjang byte string literal yang dimulai di `rest` (termasuk kutip penutup).
fn string_len(rest: &str, quote: char) -> usize {
    let mut escaped = false;
    for (i, c) in rest.char_indices().skip(1) {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' && quote != '`' {
            escaped = true;
            continue;
        }
        if c == quote {
            return i + c.len_utf8();
        }
    }
    rest.len()
}

fn number_len(rest: &str) -> usize {
    let mut end = 0;
    for (i, c) in rest.char_indices() {
        if c.is_ascii_alphanumeric() || c == '.' || c == '_' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    end
}

fn ident_len(rest: &str) -> usize {
    let mut end = 0;
    for (i, c) in rest.char_indices() {
        if c.is_alphanumeric() || c == '_' || (i > 0 && c == '$') {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    end
}

/// Tokenisasi bahasa mirip C/skrip.
fn generic_line(line: &str, syntax: &CodeSyntax, dark: bool, job: &mut LayoutJob) {
    let trimmed = line.trim_start();
    // Lanjutan komentar blok gaya `/** ... */` (heuristik per baris).
    if syntax.block_comment == Some(("/*", "*/"))
        && (trimmed.starts_with("* ") || trimmed == "*" || trimmed.starts_with("*/"))
    {
        push(job, line, super::comment_color(dark));
        return;
    }
    let mut i = 0;
    let mut plain_start = 0;
    let flush = |job: &mut LayoutJob, from: usize, to: usize| {
        push(job, &line[from..to], super::normal_color(dark));
    };
    while i < line.len() {
        let rest = &line[i..];
        let c = rest.chars().next().unwrap_or(' ');
        // Komentar baris: sisa baris.
        if starts_comment(rest, syntax).is_some() {
            flush(job, plain_start, i);
            push(job, rest, super::comment_color(dark));
            return;
        }
        // Komentar blok yang dibuka di baris ini.
        if let Some((open, close)) = syntax.block_comment
            && rest.starts_with(open)
        {
            flush(job, plain_start, i);
            let end = rest[open.len()..]
                .find(close)
                .map(|p| open.len() + p + close.len())
                .unwrap_or(rest.len());
            push(job, &rest[..end], super::comment_color(dark));
            i += end;
            plain_start = i;
            continue;
        }
        if syntax.string_delims.contains(&c) {
            flush(job, plain_start, i);
            let len = string_len(rest, c);
            push(job, &rest[..len], super::string_color(dark));
            i += len;
            plain_start = i;
            continue;
        }
        if c.is_ascii_digit() {
            // Angka hanya bila tidak menempel pada identifier sebelumnya.
            let prev_ident = line[..i]
                .chars()
                .next_back()
                .is_some_and(|p| p.is_alphanumeric() || p == '_');
            if !prev_ident {
                flush(job, plain_start, i);
                let len = number_len(rest);
                push(job, &rest[..len], super::number_color(dark));
                i += len;
                plain_start = i;
                continue;
            }
        }
        if c.is_alphabetic() || c == '_' || c == '@' || c == '$' || c == '#' {
            let lead = c.len_utf8();
            let len = if c.is_alphabetic() || c == '_' {
                ident_len(rest)
            } else {
                lead + ident_len(&rest[lead..])
            };
            let word = &rest[..len.max(lead)];
            let core = word.trim_start_matches(['@', '$', '#']);
            let color = if is_keyword(core, syntax) || is_keyword(word, syntax) {
                Some(super::keyword_color(dark))
            } else if c == '@' || c == '#' {
                // Anotasi / atribut / direktif preprocessor.
                Some(type_color(dark))
            } else if core.chars().next().is_some_and(|f| f.is_uppercase())
                && core.chars().any(|x| x.is_lowercase())
            {
                Some(type_color(dark))
            } else {
                None
            };
            if let Some(color) = color {
                flush(job, plain_start, i);
                push(job, word, color);
                i += word.len();
                plain_start = i;
            } else {
                i += word.len();
            }
            continue;
        }
        i += c.len_utf8();
    }
    flush(job, plain_start, line.len());
}

/// `[section]`, `key = value`, `key: value`, `export KEY=value`.
fn key_value_line(
    line: &str,
    syntax: &CodeSyntax,
    lang: CodeLang,
    dark: bool,
    job: &mut LayoutJob,
) {
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];
    if trimmed.is_empty() {
        push(job, line, super::normal_color(dark));
        return;
    }
    if starts_comment(trimmed, syntax).is_some() {
        push(job, line, super::comment_color(dark));
        return;
    }
    if lang == CodeLang::Ignore {
        let color = if trimmed.starts_with('!') {
            super::keyword_color(dark)
        } else {
            super::normal_color(dark)
        };
        push(job, line, color);
        return;
    }
    if trimmed.starts_with('[') {
        push(job, line, super::keyword_color(dark));
        return;
    }
    push(job, indent, super::normal_color(dark));
    let mut body = trimmed;
    // Penanda daftar YAML.
    if let Some(after) = body.strip_prefix("- ") {
        push(job, "- ", super::punctuation_color(dark));
        body = after;
    }
    if let Some(after) = body.strip_prefix("export ") {
        push(job, "export ", super::keyword_color(dark));
        body = after;
    }
    // Pemisah kunci: `=` atau `:` pertama di luar kutip.
    let sep = body.char_indices().find_map(|(i, c)| match c {
        '"' | '\'' => Some(None),
        '=' => Some(Some(i)),
        ':' if lang == CodeLang::Yaml || lang == CodeLang::Properties => {
            let next = body[i + 1..].chars().next();
            (next.is_none() || next == Some(' ')).then_some(Some(i))
        }
        _ => None,
    });
    match sep.flatten() {
        Some(pos) => {
            push(job, &body[..pos], super::json_property_color(dark));
            push(job, &body[pos..pos + 1], super::punctuation_color(dark));
            value_part(&body[pos + 1..], syntax, dark, job);
        }
        None => value_part(body, syntax, dark, job),
    }
}

/// Nilai di kanan `=`/`:` dengan komentar di belakang (`value # catatan`).
fn value_part(value: &str, syntax: &CodeSyntax, dark: bool, job: &mut LayoutJob) {
    let trimmed = value.trim();
    let lead = &value[..value.len() - value.trim_start().len()];
    push(job, lead, super::normal_color(dark));
    let rest = value.trim_start();
    if rest.is_empty() {
        return;
    }
    let first = rest.chars().next().unwrap_or(' ');
    if syntax.string_delims.contains(&first) {
        let len = string_len(rest, first);
        push(job, &rest[..len], super::string_color(dark));
        generic_tail(&rest[len..], syntax, dark, job);
        return;
    }
    // Komentar di belakang nilai butuh spasi sebelum penanda.
    let comment_at = syntax
        .line_comments
        .iter()
        .filter_map(|c| rest.find(&format!(" {c}")))
        .min();
    let (val, tail) = match comment_at {
        Some(p) => (&rest[..p], &rest[p..]),
        None => (rest, ""),
    };
    let val_trim = val.trim();
    let color = if is_keyword(val_trim, syntax) {
        super::keyword_color(dark)
    } else if !val_trim.is_empty()
        && val_trim
            .trim_start_matches(['-', '+'])
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '_')
        && val_trim.chars().any(|c| c.is_ascii_digit())
    {
        super::number_color(dark)
    } else if trimmed.starts_with('[') || trimmed.starts_with('{') {
        super::punctuation_color(dark)
    } else {
        super::string_color(dark)
    };
    push(job, val, color);
    if !tail.is_empty() {
        let ws = tail.len() - tail.trim_start().len();
        push(job, &tail[..ws], super::normal_color(dark));
        push(job, &tail[ws..], super::comment_color(dark));
    }
}

fn generic_tail(rest: &str, syntax: &CodeSyntax, dark: bool, job: &mut LayoutJob) {
    if !rest.is_empty() {
        generic_line(rest, syntax, dark, job);
    }
}

/// Tag markup: nama tag, atribut, nilai string, komentar `<!-- -->`.
fn markup_line(line: &str, syntax: &CodeSyntax, dark: bool, job: &mut LayoutJob) {
    let mut i = 0;
    let mut text_start = 0;
    let bytes = line.as_bytes();
    while i < line.len() {
        if line[i..].starts_with("<!--") {
            push(job, &line[text_start..i], super::normal_color(dark));
            let end = line[i + 4..]
                .find("-->")
                .map(|p| i + 4 + p + 3)
                .unwrap_or(line.len());
            push(job, &line[i..end], super::comment_color(dark));
            i = end;
            text_start = i;
            continue;
        }
        if bytes[i] == b'<' {
            push(job, &line[text_start..i], super::normal_color(dark));
            i = markup_tag(line, i, syntax, dark, job);
            text_start = i;
            continue;
        }
        i += line[i..].chars().next().map_or(1, char::len_utf8);
    }
    push(job, &line[text_start..], super::normal_color(dark));
}

/// Tulis satu tag mulai `<` di `start`; kembalikan posisi sesudahnya.
fn markup_tag(
    line: &str,
    start: usize,
    syntax: &CodeSyntax,
    dark: bool,
    job: &mut LayoutJob,
) -> usize {
    let punct = super::punctuation_color(dark);
    let mut i = start + 1;
    let lead_end = line[i..]
        .char_indices()
        .find(|(_, c)| !matches!(c, '/' | '!' | '?'))
        .map_or(line.len(), |(p, _)| i + p);
    push(job, &line[start..lead_end], punct);
    i = lead_end;
    let name_len = line[i..]
        .char_indices()
        .find(|(_, c)| !(c.is_alphanumeric() || matches!(c, '-' | ':' | '_' | '.')))
        .map_or(line.len() - i, |(p, _)| p);
    push(job, &line[i..i + name_len], super::keyword_color(dark));
    i += name_len;
    while i < line.len() {
        let c = line[i..].chars().next().unwrap_or(' ');
        if c == '>' {
            push(job, ">", punct);
            return i + 1;
        }
        if syntax.string_delims.contains(&c) {
            let len = string_len(&line[i..], c);
            push(job, &line[i..i + len], super::string_color(dark));
            i += len;
            continue;
        }
        if c.is_alphabetic() || matches!(c, '_' | ':' | '@' | '#' | 'v') {
            let len = line[i..]
                .char_indices()
                .find(|(_, x)| {
                    !(x.is_alphanumeric() || matches!(x, '-' | ':' | '_' | '.' | '@' | '#'))
                })
                .map_or(line.len() - i, |(p, _)| p)
                .max(c.len_utf8());
            push(job, &line[i..i + len], super::json_property_color(dark));
            i += len;
            continue;
        }
        let len = c.len_utf8();
        push(job, &line[i..i + len], punct);
        i += len;
    }
    line.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sections(line: &str, lang: CodeLang) -> Vec<(String, Color32)> {
        let mut job = LayoutJob::default();
        highlight_code_line(line, lang, true, &mut job);
        // Teks yang di-highlight harus sama persis dengan masukan.
        assert_eq!(job.text, line);
        job.sections
            .iter()
            .map(|s| {
                (
                    job.text[s.byte_range.start.0..s.byte_range.end.0].to_string(),
                    s.format.color,
                )
            })
            .collect()
    }

    fn color_of(secs: &[(String, Color32)], token: &str) -> Color32 {
        secs.iter()
            .find(|(t, _)| t == token)
            .unwrap_or_else(|| panic!("token {token:?} not found in {secs:?}"))
            .1
    }

    #[test]
    fn rust_keywords_strings_comments() {
        let s = sections(r#"fn main() { let x = "hi"; } // done"#, CodeLang::Rust);
        assert_eq!(color_of(&s, "fn"), super::super::keyword_color(true));
        assert_eq!(color_of(&s, "let"), super::super::keyword_color(true));
        assert_eq!(color_of(&s, "\"hi\""), super::super::string_color(true));
        assert_eq!(color_of(&s, "// done"), super::super::comment_color(true));
    }

    #[test]
    fn python_hash_comment_and_number() {
        let s = sections("x = 42  # answer", CodeLang::Python);
        assert_eq!(color_of(&s, "42"), super::super::number_color(true));
        assert_eq!(color_of(&s, "# answer"), super::super::comment_color(true));
    }

    #[test]
    fn identifier_digits_are_not_numbers() {
        let s = sections("var abc123 = 1", CodeLang::JavaScript);
        assert!(s.iter().all(|(t, _)| t != "123"));
    }

    #[test]
    fn env_key_value() {
        let s = sections("DB_HOST=localhost # local", CodeLang::Env);
        assert_eq!(
            color_of(&s, "DB_HOST"),
            super::super::json_property_color(true)
        );
        assert_eq!(color_of(&s, "localhost"), super::super::string_color(true));
        assert_eq!(color_of(&s, "# local"), super::super::comment_color(true));
    }

    #[test]
    fn toml_section_and_number() {
        let s = sections("[package]", CodeLang::Toml);
        assert_eq!(color_of(&s, "[package]"), super::super::keyword_color(true));
        let s = sections("version = 3", CodeLang::Toml);
        assert_eq!(color_of(&s, "3"), super::super::number_color(true));
    }

    #[test]
    fn yaml_url_value_is_not_split() {
        let s = sections("url: http://x:80/a", CodeLang::Yaml);
        assert_eq!(color_of(&s, "url"), super::super::json_property_color(true));
        assert_eq!(
            color_of(&s, "http://x:80/a"),
            super::super::string_color(true)
        );
    }

    #[test]
    fn html_tag_attributes() {
        let s = sections(r#"<a href="/x">link</a> <!-- c -->"#, CodeLang::Html);
        assert_eq!(color_of(&s, "a"), super::super::keyword_color(true));
        assert_eq!(
            color_of(&s, "href"),
            super::super::json_property_color(true)
        );
        assert_eq!(color_of(&s, "\"/x\""), super::super::string_color(true));
        assert_eq!(
            color_of(&s, "<!-- c -->"),
            super::super::comment_color(true)
        );
    }

    #[test]
    fn diff_lines() {
        assert_eq!(
            sections("+added", CodeLang::Diff)[0].1,
            diff_add_color(true)
        );
        assert_eq!(
            sections("-removed", CodeLang::Diff)[0].1,
            diff_del_color(true)
        );
    }

    #[test]
    fn unicode_and_unterminated_strings_do_not_panic() {
        for lang in [CodeLang::Go, CodeLang::Html, CodeLang::Yaml, CodeLang::Php] {
            sections("héllo \"ünterminated ✓ <tag attr='x", lang);
            sections("", lang);
            sections("   ", lang);
        }
    }
}
