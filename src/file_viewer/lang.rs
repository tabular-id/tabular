//! Peta ekstensi/nama file ke bahasa pemrograman dan spesifikasi sintaks
//! sederhana untuk highlighter heuristik per baris (`syntax_ts`).
//!
//! Highlighter bekerja per baris (cache per baris), jadi spesifikasi di sini
//! hanya memuat hal yang bisa dikenali dalam satu baris: komentar baris,
//! komentar blok yang dibuka dan ditutup di baris yang sama, string, angka, dan
//! kata kunci.

use std::path::Path;

/// Bahasa source code / konfigurasi yang dikenali File Viewer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CodeLang {
    Rust,
    Go,
    Java,
    Kotlin,
    Scala,
    Groovy,
    Swift,
    C,
    Cpp,
    CSharp,
    ObjC,
    Php,
    Python,
    Ruby,
    JavaScript,
    TypeScript,
    Dart,
    Lua,
    Perl,
    R,
    Elixir,
    Erlang,
    Haskell,
    Zig,
    Shell,
    PowerShell,
    Batch,
    Sql,
    Html,
    Xml,
    Vue,
    Css,
    Scss,
    Json,
    Yaml,
    Toml,
    Ini,
    Env,
    Properties,
    Dockerfile,
    Makefile,
    CMake,
    Hcl,
    Graphql,
    Protobuf,
    Nginx,
    Ignore,
    Diff,
    Markdown,
    PlainText,
}

/// Gaya highlighter yang dipakai untuk satu bahasa.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyntaxStyle {
    /// Bahasa mirip C/skrip: komentar, string, angka, kata kunci.
    Generic,
    /// `key = value` / `key: value` / `[section]` (TOML, INI, .env, YAML).
    KeyValue,
    /// Tag `<...>` (HTML, XML, Vue).
    Markup,
    /// JSON memakai highlighter JSON yang sudah ada.
    Json,
    /// Baris `+`, `-`, `@@` pada diff/patch.
    Diff,
    /// Tanpa warna.
    Plain,
}

/// Spesifikasi sintaks satu bahasa.
#[derive(Clone, Copy, Debug)]
pub struct CodeSyntax {
    pub style: SyntaxStyle,
    pub line_comments: &'static [&'static str],
    pub block_comment: Option<(&'static str, &'static str)>,
    pub string_delims: &'static [char],
    pub keywords: &'static [&'static str],
    /// Kata kunci dicocokkan tanpa memedulikan huruf besar/kecil.
    pub case_insensitive: bool,
}

const C_LIKE_COMMENT: &[&str] = &["//"];
const HASH_COMMENT: &[&str] = &["#"];
const QUOTES: &[char] = &['"', '\''];
const QUOTES_BT: &[char] = &['"', '\'', '`'];
const DQ: &[char] = &['"'];

const KW_RUST: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
    "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
    "unsafe", "use", "where", "while", "Some", "None", "Ok", "Err",
];
const KW_GO: &[&str] = &[
    "break",
    "case",
    "chan",
    "const",
    "continue",
    "default",
    "defer",
    "else",
    "fallthrough",
    "for",
    "func",
    "go",
    "goto",
    "if",
    "import",
    "interface",
    "map",
    "package",
    "range",
    "return",
    "select",
    "struct",
    "switch",
    "type",
    "var",
    "nil",
    "true",
    "false",
];
const KW_JAVA: &[&str] = &[
    "abstract",
    "boolean",
    "break",
    "byte",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extends",
    "final",
    "finally",
    "float",
    "for",
    "if",
    "implements",
    "import",
    "instanceof",
    "int",
    "interface",
    "long",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "short",
    "static",
    "super",
    "switch",
    "synchronized",
    "this",
    "throw",
    "throws",
    "try",
    "void",
    "volatile",
    "while",
    "var",
    "record",
    "null",
    "true",
    "false",
];
const KW_KOTLIN: &[&str] = &[
    "as",
    "break",
    "class",
    "continue",
    "do",
    "else",
    "false",
    "for",
    "fun",
    "if",
    "in",
    "interface",
    "is",
    "null",
    "object",
    "package",
    "return",
    "super",
    "this",
    "throw",
    "true",
    "try",
    "typealias",
    "val",
    "var",
    "when",
    "while",
    "import",
    "data",
    "sealed",
    "override",
    "private",
    "public",
    "internal",
    "suspend",
    "companion",
    "def",
    "trait",
    "extends",
    "with",
    "new",
    "match",
    "case",
    "implicit",
    "lazy",
];
const KW_SWIFT: &[&str] = &[
    "class",
    "struct",
    "enum",
    "protocol",
    "extension",
    "func",
    "let",
    "var",
    "if",
    "else",
    "guard",
    "switch",
    "case",
    "default",
    "for",
    "while",
    "return",
    "import",
    "init",
    "self",
    "nil",
    "true",
    "false",
    "public",
    "private",
    "static",
    "throws",
    "try",
    "async",
    "await",
];
const KW_C: &[&str] = &[
    "auto",
    "break",
    "case",
    "char",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extern",
    "float",
    "for",
    "goto",
    "if",
    "inline",
    "int",
    "long",
    "register",
    "return",
    "short",
    "signed",
    "sizeof",
    "static",
    "struct",
    "switch",
    "typedef",
    "union",
    "unsigned",
    "void",
    "volatile",
    "while",
    "class",
    "namespace",
    "template",
    "typename",
    "public",
    "private",
    "protected",
    "virtual",
    "override",
    "new",
    "delete",
    "this",
    "using",
    "nullptr",
    "true",
    "false",
    "bool",
    "auto",
    "constexpr",
    "include",
    "define",
    "ifdef",
    "ifndef",
    "endif",
    "pragma",
];
const KW_CSHARP: &[&str] = &[
    "abstract",
    "async",
    "await",
    "bool",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "decimal",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "false",
    "finally",
    "float",
    "for",
    "foreach",
    "get",
    "if",
    "in",
    "int",
    "interface",
    "internal",
    "is",
    "long",
    "namespace",
    "new",
    "null",
    "object",
    "out",
    "override",
    "private",
    "protected",
    "public",
    "readonly",
    "record",
    "ref",
    "return",
    "set",
    "static",
    "string",
    "struct",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "using",
    "var",
    "virtual",
    "void",
    "while",
];
const KW_PHP: &[&str] = &[
    "abstract",
    "array",
    "as",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "echo",
    "else",
    "elseif",
    "extends",
    "false",
    "final",
    "finally",
    "fn",
    "for",
    "foreach",
    "function",
    "if",
    "implements",
    "interface",
    "namespace",
    "new",
    "null",
    "private",
    "protected",
    "public",
    "require",
    "require_once",
    "return",
    "static",
    "switch",
    "throw",
    "trait",
    "true",
    "try",
    "use",
    "while",
    "match",
    "readonly",
];
const KW_PYTHON: &[&str] = &[
    "and", "as", "assert", "async", "await", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "False", "finally", "for", "from", "global", "if", "import", "in", "is",
    "lambda", "None", "nonlocal", "not", "or", "pass", "raise", "return", "True", "try", "while",
    "with", "yield", "self",
];
const KW_RUBY: &[&str] = &[
    "alias",
    "and",
    "begin",
    "break",
    "case",
    "class",
    "def",
    "do",
    "else",
    "elsif",
    "end",
    "ensure",
    "false",
    "for",
    "if",
    "in",
    "module",
    "next",
    "nil",
    "not",
    "or",
    "redo",
    "rescue",
    "retry",
    "return",
    "self",
    "super",
    "then",
    "true",
    "unless",
    "until",
    "when",
    "while",
    "yield",
    "require",
    "attr_accessor",
];
const KW_JS: &[&str] = &[
    "async",
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "from",
    "function",
    "if",
    "import",
    "in",
    "instanceof",
    "let",
    "new",
    "null",
    "of",
    "return",
    "static",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "undefined",
    "var",
    "void",
    "while",
    "yield",
    "interface",
    "type",
    "enum",
    "implements",
    "private",
    "public",
    "protected",
    "readonly",
    "as",
    "namespace",
    "declare",
];
const KW_DART: &[&str] = &[
    "abstract", "async", "await", "break", "case", "catch", "class", "const", "continue",
    "default", "else", "enum", "extends", "false", "final", "for", "if", "import", "in", "is",
    "late", "new", "null", "required", "return", "static", "super", "switch", "this", "throw",
    "true", "try", "var", "void", "while", "with",
];
const KW_LUA: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "if", "in", "local",
    "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];
const KW_PERL: &[&str] = &[
    "my", "our", "sub", "if", "elsif", "else", "unless", "while", "for", "foreach", "return",
    "use", "package", "require", "last", "next",
];
const KW_R: &[&str] = &[
    "function", "if", "else", "for", "while", "repeat", "return", "TRUE", "FALSE", "NULL", "NA",
    "library", "in", "next", "break",
];
const KW_ELIXIR: &[&str] = &[
    "def",
    "defp",
    "defmodule",
    "do",
    "end",
    "fn",
    "if",
    "else",
    "case",
    "cond",
    "with",
    "true",
    "false",
    "nil",
    "import",
    "alias",
    "use",
    "require",
    "when",
    "receive",
    "after",
];
const KW_ERLANG: &[&str] = &[
    "after", "case", "catch", "end", "fun", "if", "of", "receive", "try", "when", "module",
    "export", "true", "false",
];
const KW_HASKELL: &[&str] = &[
    "case", "class", "data", "deriving", "do", "else", "if", "import", "in", "instance", "let",
    "module", "newtype", "of", "then", "type", "where", "True", "False",
];
const KW_ZIG: &[&str] = &[
    "const",
    "var",
    "fn",
    "pub",
    "return",
    "if",
    "else",
    "while",
    "for",
    "switch",
    "struct",
    "enum",
    "union",
    "try",
    "catch",
    "defer",
    "errdefer",
    "comptime",
    "null",
    "true",
    "false",
    "undefined",
];
const KW_SHELL: &[&str] = &[
    "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done", "case", "esac",
    "in", "function", "return", "export", "local", "readonly", "set", "unset", "source", "echo",
    "exit", "shift", "true", "false",
];
const KW_POWERSHELL: &[&str] = &[
    "function", "param", "if", "else", "elseif", "foreach", "for", "while", "return", "switch",
    "try", "catch", "finally", "throw", "begin", "process", "end",
];
const KW_BATCH: &[&str] = &[
    "echo", "set", "if", "else", "goto", "call", "exit", "for", "in", "do", "not", "exist", "rem",
    "setlocal", "endlocal",
];
const KW_SQL: &[&str] = &[
    "select",
    "from",
    "where",
    "insert",
    "into",
    "values",
    "update",
    "set",
    "delete",
    "create",
    "table",
    "alter",
    "drop",
    "index",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "on",
    "and",
    "or",
    "not",
    "null",
    "as",
    "group",
    "by",
    "order",
    "having",
    "limit",
    "offset",
    "union",
    "distinct",
    "primary",
    "key",
    "foreign",
    "references",
    "default",
    "begin",
    "commit",
    "rollback",
    "view",
    "with",
    "case",
    "when",
    "then",
    "else",
    "end",
    "is",
    "in",
    "exists",
];
const KW_CSS: &[&str] = &[
    "important",
    "media",
    "import",
    "keyframes",
    "from",
    "to",
    "root",
    "hover",
    "active",
    "focus",
    "before",
    "after",
    "include",
    "mixin",
    "extend",
    "if",
    "else",
    "each",
    "use",
];
const KW_DOCKER: &[&str] = &[
    "FROM",
    "RUN",
    "CMD",
    "LABEL",
    "EXPOSE",
    "ENV",
    "ADD",
    "COPY",
    "ENTRYPOINT",
    "VOLUME",
    "USER",
    "WORKDIR",
    "ARG",
    "ONBUILD",
    "STOPSIGNAL",
    "HEALTHCHECK",
    "SHELL",
    "AS",
];
const KW_MAKE: &[&str] = &[
    "ifeq", "ifneq", "ifdef", "ifndef", "else", "endif", "include", "define", "endef", "export",
    "override", ".PHONY",
];
const KW_CMAKE: &[&str] = &[
    "cmake_minimum_required",
    "project",
    "add_executable",
    "add_library",
    "target_link_libraries",
    "set",
    "if",
    "else",
    "endif",
    "foreach",
    "endforeach",
    "include",
    "find_package",
    "option",
    "message",
    "function",
    "endfunction",
];
const KW_HCL: &[&str] = &[
    "resource",
    "variable",
    "output",
    "provider",
    "module",
    "data",
    "locals",
    "terraform",
    "true",
    "false",
    "null",
    "for",
    "in",
    "if",
];
const KW_GRAPHQL: &[&str] = &[
    "query",
    "mutation",
    "subscription",
    "fragment",
    "on",
    "type",
    "input",
    "enum",
    "interface",
    "union",
    "scalar",
    "schema",
    "extend",
    "implements",
    "directive",
    "true",
    "false",
    "null",
];
const KW_PROTO: &[&str] = &[
    "syntax", "package", "import", "option", "message", "enum", "service", "rpc", "returns",
    "repeated", "optional", "required", "oneof", "map", "reserved", "string", "int32", "int64",
    "bool", "bytes", "double", "float", "uint32", "uint64",
];
const KW_NGINX: &[&str] = &[
    "server",
    "location",
    "listen",
    "server_name",
    "root",
    "index",
    "proxy_pass",
    "include",
    "upstream",
    "return",
    "rewrite",
    "if",
    "set",
    "http",
    "events",
    "worker_processes",
];
const KW_YAML: &[&str] = &["true", "false", "null", "yes", "no", "on", "off", "~"];

const fn spec(
    style: SyntaxStyle,
    line_comments: &'static [&'static str],
    block_comment: Option<(&'static str, &'static str)>,
    string_delims: &'static [char],
    keywords: &'static [&'static str],
) -> CodeSyntax {
    CodeSyntax {
        style,
        line_comments,
        block_comment,
        string_delims,
        keywords,
        case_insensitive: false,
    }
}

const C_BLOCK: Option<(&str, &str)> = Some(("/*", "*/"));

impl CodeLang {
    /// Nama tampilan (UI, bahasa Inggris).
    pub fn label(self) -> &'static str {
        match self {
            CodeLang::Rust => "Rust",
            CodeLang::Go => "Go",
            CodeLang::Java => "Java",
            CodeLang::Kotlin => "Kotlin",
            CodeLang::Scala => "Scala",
            CodeLang::Groovy => "Groovy",
            CodeLang::Swift => "Swift",
            CodeLang::C => "C",
            CodeLang::Cpp => "C++",
            CodeLang::CSharp => "C#",
            CodeLang::ObjC => "Objective-C",
            CodeLang::Php => "PHP",
            CodeLang::Python => "Python",
            CodeLang::Ruby => "Ruby",
            CodeLang::JavaScript => "JavaScript",
            CodeLang::TypeScript => "TypeScript",
            CodeLang::Dart => "Dart",
            CodeLang::Lua => "Lua",
            CodeLang::Perl => "Perl",
            CodeLang::R => "R",
            CodeLang::Elixir => "Elixir",
            CodeLang::Erlang => "Erlang",
            CodeLang::Haskell => "Haskell",
            CodeLang::Zig => "Zig",
            CodeLang::Shell => "Shell",
            CodeLang::PowerShell => "PowerShell",
            CodeLang::Batch => "Batch",
            CodeLang::Sql => "SQL",
            CodeLang::Html => "HTML",
            CodeLang::Xml => "XML",
            CodeLang::Vue => "Vue",
            CodeLang::Css => "CSS",
            CodeLang::Scss => "SCSS",
            CodeLang::Json => "JSON",
            CodeLang::Yaml => "YAML",
            CodeLang::Toml => "TOML",
            CodeLang::Ini => "INI",
            CodeLang::Env => "Environment",
            CodeLang::Properties => "Properties",
            CodeLang::Dockerfile => "Dockerfile",
            CodeLang::Makefile => "Makefile",
            CodeLang::CMake => "CMake",
            CodeLang::Hcl => "HCL",
            CodeLang::Graphql => "GraphQL",
            CodeLang::Protobuf => "Protocol Buffers",
            CodeLang::Nginx => "Nginx",
            CodeLang::Ignore => "Ignore file",
            CodeLang::Diff => "Diff",
            CodeLang::Markdown => "Markdown",
            CodeLang::PlainText => "Plain text",
        }
    }

    /// Spesifikasi sintaks untuk highlighter.
    pub fn syntax(self) -> CodeSyntax {
        use SyntaxStyle::*;
        match self {
            CodeLang::Rust => spec(Generic, C_LIKE_COMMENT, C_BLOCK, DQ, KW_RUST),
            CodeLang::Go => spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES_BT, KW_GO),
            CodeLang::Java => spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_JAVA),
            CodeLang::Kotlin | CodeLang::Scala | CodeLang::Groovy => {
                spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_KOTLIN)
            }
            CodeLang::Swift => spec(Generic, C_LIKE_COMMENT, C_BLOCK, DQ, KW_SWIFT),
            CodeLang::C | CodeLang::Cpp | CodeLang::ObjC => {
                spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_C)
            }
            CodeLang::CSharp => spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_CSHARP),
            CodeLang::Php => spec(Generic, &["//", "#"], C_BLOCK, QUOTES, KW_PHP),
            CodeLang::Python => spec(Generic, HASH_COMMENT, None, QUOTES, KW_PYTHON),
            CodeLang::Ruby => spec(Generic, HASH_COMMENT, None, QUOTES, KW_RUBY),
            CodeLang::JavaScript | CodeLang::TypeScript => {
                spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES_BT, KW_JS)
            }
            CodeLang::Dart => spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_DART),
            CodeLang::Lua => spec(Generic, &["--"], Some(("--[[", "]]")), QUOTES, KW_LUA),
            CodeLang::Perl => spec(Generic, HASH_COMMENT, None, QUOTES, KW_PERL),
            CodeLang::R => spec(Generic, HASH_COMMENT, None, QUOTES, KW_R),
            CodeLang::Elixir => spec(Generic, HASH_COMMENT, None, QUOTES, KW_ELIXIR),
            CodeLang::Erlang => spec(Generic, &["%"], None, QUOTES, KW_ERLANG),
            CodeLang::Haskell => spec(Generic, &["--"], Some(("{-", "-}")), DQ, KW_HASKELL),
            CodeLang::Zig => spec(Generic, C_LIKE_COMMENT, None, QUOTES, KW_ZIG),
            CodeLang::Shell => spec(Generic, HASH_COMMENT, None, QUOTES_BT, KW_SHELL),
            CodeLang::PowerShell => CodeSyntax {
                case_insensitive: true,
                ..spec(
                    Generic,
                    HASH_COMMENT,
                    Some(("<#", "#>")),
                    QUOTES,
                    KW_POWERSHELL,
                )
            },
            CodeLang::Batch => CodeSyntax {
                case_insensitive: true,
                ..spec(Generic, &["::", "REM ", "rem "], None, DQ, KW_BATCH)
            },
            CodeLang::Sql => CodeSyntax {
                case_insensitive: true,
                ..spec(Generic, &["--"], C_BLOCK, QUOTES, KW_SQL)
            },
            CodeLang::Html | CodeLang::Xml | CodeLang::Vue => {
                spec(Markup, &[], Some(("<!--", "-->")), QUOTES, &[])
            }
            CodeLang::Css | CodeLang::Scss => spec(Generic, &["//"], C_BLOCK, QUOTES, KW_CSS),
            CodeLang::Json => spec(Json, &[], None, DQ, &[]),
            CodeLang::Yaml => spec(KeyValue, HASH_COMMENT, None, QUOTES, KW_YAML),
            CodeLang::Toml | CodeLang::Env | CodeLang::Properties | CodeLang::Ignore => {
                spec(KeyValue, &["#", "!"], None, QUOTES, KW_YAML)
            }
            CodeLang::Ini => spec(KeyValue, &[";", "#"], None, QUOTES, KW_YAML),
            CodeLang::Dockerfile => spec(Generic, HASH_COMMENT, None, QUOTES, KW_DOCKER),
            CodeLang::Makefile => spec(Generic, HASH_COMMENT, None, QUOTES, KW_MAKE),
            CodeLang::CMake => CodeSyntax {
                case_insensitive: true,
                ..spec(Generic, HASH_COMMENT, None, DQ, KW_CMAKE)
            },
            CodeLang::Hcl => spec(Generic, &["#", "//"], C_BLOCK, DQ, KW_HCL),
            CodeLang::Graphql => spec(Generic, HASH_COMMENT, None, DQ, KW_GRAPHQL),
            CodeLang::Protobuf => spec(Generic, C_LIKE_COMMENT, C_BLOCK, QUOTES, KW_PROTO),
            CodeLang::Nginx => spec(Generic, HASH_COMMENT, None, QUOTES, KW_NGINX),
            CodeLang::Diff => spec(Diff, &[], None, &[], &[]),
            CodeLang::Markdown | CodeLang::PlainText => spec(Plain, &[], None, &[], &[]),
        }
    }

    /// Tebak bahasa dari nama/path file. `None` bila bukan file teks yang dikenal.
    pub fn from_path(path: &Path) -> Option<CodeLang> {
        let name = path.file_name()?.to_str()?;
        Self::from_file_name(name)
    }

    /// Seperti [`CodeLang::from_path`] tapi dari nama file saja.
    pub fn from_file_name(name: &str) -> Option<CodeLang> {
        let lower = name.to_ascii_lowercase();
        // Nama khusus tanpa ekstensi yang bermakna.
        let by_name = match lower.as_str() {
            "dockerfile" | "containerfile" => Some(CodeLang::Dockerfile),
            "makefile" | "gnumakefile" | "justfile" => Some(CodeLang::Makefile),
            "cmakelists.txt" => Some(CodeLang::CMake),
            "gemfile" | "rakefile" | "podfile" | "vagrantfile" | "fastfile" => Some(CodeLang::Ruby),
            "jenkinsfile" => Some(CodeLang::Groovy),
            ".gitignore" | ".dockerignore" | ".npmignore" | ".prettierignore" | ".eslintignore"
            | ".gitattributes" | ".hgignore" | "codeowners" => Some(CodeLang::Ignore),
            ".editorconfig" | ".npmrc" | ".gitconfig" | ".gitmodules" => Some(CodeLang::Ini),
            ".bashrc" | ".zshrc" | ".profile" | ".bash_profile" | ".zprofile" => {
                Some(CodeLang::Shell)
            }
            "license" | "licence" | "readme" | "authors" | "changelog" | "notice" => {
                Some(CodeLang::PlainText)
            }
            "go.mod" | "go.sum" => Some(CodeLang::Go),
            "cargo.lock" | "pipfile" | "poetry.lock" => Some(CodeLang::Toml),
            _ => None,
        };
        if by_name.is_some() {
            return by_name;
        }
        // `.env`, `.env.local`, `.env.production`, `app.env`.
        if lower == ".env" || lower.starts_with(".env.") || lower.ends_with(".env") {
            return Some(CodeLang::Env);
        }
        if lower.starts_with("dockerfile.") || lower.ends_with(".dockerfile") {
            return Some(CodeLang::Dockerfile);
        }
        let ext = lower.rsplit_once('.').map(|(_, e)| e)?;
        let lang = match ext {
            "rs" => CodeLang::Rust,
            "go" => CodeLang::Go,
            "java" => CodeLang::Java,
            "kt" | "kts" => CodeLang::Kotlin,
            "scala" | "sc" | "sbt" => CodeLang::Scala,
            "groovy" | "gradle" => CodeLang::Groovy,
            "swift" => CodeLang::Swift,
            "c" | "h" => CodeLang::C,
            "cc" | "cpp" | "cxx" | "hpp" | "hh" | "hxx" | "ino" => CodeLang::Cpp,
            "cs" | "csx" => CodeLang::CSharp,
            "m" | "mm" => CodeLang::ObjC,
            "php" | "phtml" | "blade" => CodeLang::Php,
            "py" | "pyw" | "pyi" => CodeLang::Python,
            "rb" | "erb" | "gemspec" | "rake" => CodeLang::Ruby,
            "js" | "mjs" | "cjs" | "jsx" => CodeLang::JavaScript,
            "ts" | "tsx" | "mts" | "cts" => CodeLang::TypeScript,
            "dart" => CodeLang::Dart,
            "lua" => CodeLang::Lua,
            "pl" | "pm" => CodeLang::Perl,
            "r" => CodeLang::R,
            "ex" | "exs" => CodeLang::Elixir,
            "erl" | "hrl" => CodeLang::Erlang,
            "hs" => CodeLang::Haskell,
            "zig" => CodeLang::Zig,
            "sh" | "bash" | "zsh" | "fish" | "ksh" => CodeLang::Shell,
            "ps1" | "psm1" | "psd1" => CodeLang::PowerShell,
            "bat" | "cmd" => CodeLang::Batch,
            "sql" | "ddl" | "dml" | "psql" => CodeLang::Sql,
            "html" | "htm" | "xhtml" | "twig" | "jinja" | "j2" | "hbs" | "ejs" | "svelte" => {
                CodeLang::Html
            }
            "xml" | "xsd" | "xsl" | "xslt" | "plist" | "csproj" | "fsproj" | "pom" | "wsdl"
            | "storyboard" | "xib" | "resx" | "svg" => CodeLang::Xml,
            "vue" => CodeLang::Vue,
            "css" => CodeLang::Css,
            "scss" | "sass" | "less" | "styl" => CodeLang::Scss,
            "json" | "jsonc" | "json5" | "geojson" | "webmanifest" | "har" | "ipynb" => {
                CodeLang::Json
            }
            "yaml" | "yml" => CodeLang::Yaml,
            "toml" | "lock" => CodeLang::Toml,
            "ini" | "cfg" | "conf" | "cnf" | "inf" | "desktop" | "service" => CodeLang::Ini,
            "properties" => CodeLang::Properties,
            "mk" | "mak" => CodeLang::Makefile,
            "cmake" => CodeLang::CMake,
            "tf" | "tfvars" | "hcl" | "nomad" => CodeLang::Hcl,
            "graphql" | "gql" => CodeLang::Graphql,
            "proto" => CodeLang::Protobuf,
            "nginx" => CodeLang::Nginx,
            "diff" | "patch" => CodeLang::Diff,
            "md" | "markdown" | "mdx" | "mdown" => CodeLang::Markdown,
            "txt" | "text" | "log" | "out" | "rst" | "adoc" | "tex" | "csv" | "tsv" | "srt"
            | "vtt" | "ndjson" | "jsonl" => CodeLang::PlainText,
            _ => return None,
        };
        Some(lang)
    }
}

/// File yang lazim berisi rahasia (kredensial, token). Isinya tidak boleh ikut
/// ke konteks AI atau indeks vektor.
pub fn is_sensitive_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let lower = name.to_ascii_lowercase();
    lower == ".env"
        || lower.starts_with(".env.")
        || lower.ends_with(".env")
        || lower.ends_with(".pem")
        || lower.ends_with(".key")
        || lower.ends_with(".p12")
        || lower.ends_with(".pfx")
        || lower == ".npmrc"
        || lower == ".pypirc"
        || lower == ".netrc"
        || lower == "credentials"
        || lower.starts_with("id_rsa")
        || lower.starts_with("id_ed25519")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_extensions() {
        let cases = [
            ("main.rs", CodeLang::Rust),
            ("server.go", CodeLang::Go),
            ("App.java", CodeLang::Java),
            ("index.php", CodeLang::Php),
            ("index.html", CodeLang::Html),
            ("app.js", CodeLang::JavaScript),
            ("app.tsx", CodeLang::TypeScript),
            ("Cargo.toml", CodeLang::Toml),
            ("config.yml", CodeLang::Yaml),
            ("package.json", CodeLang::Json),
            ("script.py", CodeLang::Python),
            ("query.SQL", CodeLang::Sql),
        ];
        for (name, lang) in cases {
            assert_eq!(CodeLang::from_file_name(name), Some(lang), "{name}");
        }
    }

    #[test]
    fn maps_special_names() {
        assert_eq!(CodeLang::from_file_name(".env"), Some(CodeLang::Env));
        assert_eq!(CodeLang::from_file_name(".env.local"), Some(CodeLang::Env));
        assert_eq!(
            CodeLang::from_file_name("Dockerfile"),
            Some(CodeLang::Dockerfile)
        );
        assert_eq!(
            CodeLang::from_file_name("Dockerfile.dev"),
            Some(CodeLang::Dockerfile)
        );
        assert_eq!(
            CodeLang::from_file_name("Makefile"),
            Some(CodeLang::Makefile)
        );
        assert_eq!(
            CodeLang::from_file_name(".gitignore"),
            Some(CodeLang::Ignore)
        );
        assert_eq!(
            CodeLang::from_file_name("CMakeLists.txt"),
            Some(CodeLang::CMake)
        );
        assert_eq!(CodeLang::from_file_name("go.mod"), Some(CodeLang::Go));
    }

    #[test]
    fn unknown_extension_is_none() {
        assert_eq!(CodeLang::from_file_name("photo.png"), None);
        assert_eq!(CodeLang::from_file_name("archive.bin"), None);
        assert_eq!(CodeLang::from_file_name("noext"), None);
    }

    #[test]
    fn sensitive_files_detected() {
        assert!(is_sensitive_file(Path::new("/app/.env")));
        assert!(is_sensitive_file(Path::new("/app/.env.production")));
        assert!(is_sensitive_file(Path::new("server.key")));
        assert!(!is_sensitive_file(Path::new("main.rs")));
        assert!(!is_sensitive_file(Path::new("environment.ts")));
    }
}
