//! Sumber data MongoDB untuk Multi-source Comparison: query JSON → dokumen →
//! tabel datar. Field bersarang menjadi kolom `a.b`; array dan nilai khusus
//! ditulis sebagai Extended JSON (relaxed).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};

use super::TableData;

/// Query MongoDB yang ditulis user sebagai JSON:
/// `{"collection": "orders", "filter": {...}, "projection": {...},
/// "sort": {...}, "limit": 100}` atau
/// `{"collection": "orders", "pipeline": [...]}`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MongoQuery {
    pub collection: String,
    pub filter: Document,
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub pipeline: Option<Vec<Document>>,
    pub limit: Option<u64>,
}

const QUERY_HINT: &str =
    "MongoDB query must be JSON like {\"collection\": \"orders\", \"filter\": {}}";

fn to_document(value: serde_json::Value, field: &str) -> Result<Document, String> {
    match Bson::try_from(value).map_err(|e| format!("\"{field}\": {e}"))? {
        Bson::Document(d) => Ok(d),
        _ => Err(format!("\"{field}\" must be a JSON object")),
    }
}

fn optional_document(
    map: &mut serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<Document>, String> {
    match map.remove(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(v) => to_document(v, field).map(Some),
    }
}

/// Baca teks query (setelah variabel diganti). Extended JSON didukung, mis.
/// `{"_id": {"$oid": "..."}}` atau `{"$date": "2026-01-01T00:00:00Z"}`.
pub fn parse_mongo_query(text: &str) -> Result<MongoQuery, String> {
    use serde_json::Value;
    let value: Value =
        serde_json::from_str(text.trim()).map_err(|e| format!("{QUERY_HINT}: {e}"))?;
    let Value::Object(mut map) = value else {
        return Err(QUERY_HINT.to_string());
    };
    let collection = match map.remove("collection") {
        Some(Value::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
        _ => return Err("MongoDB query needs a \"collection\" name".to_string()),
    };
    let mut q = MongoQuery {
        collection,
        filter: optional_document(&mut map, "filter")?.unwrap_or_default(),
        projection: optional_document(&mut map, "projection")?,
        sort: optional_document(&mut map, "sort")?,
        ..Default::default()
    };
    match map.remove("pipeline") {
        None | Some(Value::Null) => {}
        Some(Value::Array(items)) => {
            q.pipeline = Some(
                items
                    .into_iter()
                    .map(|stage| to_document(stage, "pipeline"))
                    .collect::<Result<_, _>>()?,
            );
        }
        Some(_) => return Err("\"pipeline\" must be an array of stages".to_string()),
    }
    match map.remove("limit") {
        None | Some(Value::Null) => {}
        Some(Value::Number(n)) => q.limit = n.as_u64().filter(|n| *n > 0),
        Some(_) => return Err("\"limit\" must be a number".to_string()),
    }
    if let Some(field) = map.keys().next() {
        return Err(format!(
            "Unknown field \"{field}\"; use collection, filter, projection, sort, limit or pipeline"
        ));
    }
    if q.pipeline.is_some() && (!q.filter.is_empty() || q.projection.is_some() || q.sort.is_some())
    {
        return Err("Use either \"pipeline\" or filter/projection/sort, not both".to_string());
    }
    Ok(q)
}

fn format_datetime(dt: mongodb::bson::DateTime) -> String {
    let ms = dt.timestamp_millis();
    match chrono::DateTime::from_timestamp_millis(ms) {
        // Bentuk yang sama dengan timestamp SQL agar opsi toleran menyamakannya.
        Some(d) => d.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        None => ms.to_string(),
    }
}

/// Teks satu nilai BSON; `None` = null/undefined.
pub fn cell_text(value: &Bson) -> Option<String> {
    Some(match value {
        Bson::Null | Bson::Undefined => return None,
        Bson::String(s) => s.clone(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Int32(n) => n.to_string(),
        Bson::Int64(n) => n.to_string(),
        Bson::Double(d) => d.to_string(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::DateTime(dt) => format_datetime(*dt),
        Bson::Decimal128(d) => d.to_string(),
        other => other.clone().into_relaxed_extjson().to_string(),
    })
}

fn walk(prefix: &str, doc: &Document, out: &mut Vec<(String, Option<String>)>) {
    for (key, value) in doc {
        let name = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            Bson::Document(inner) if !inner.is_empty() => walk(&name, inner, out),
            _ => out.push((name, cell_text(value))),
        }
    }
}

/// Ratakan dokumen menjadi tabel. Kolom urut kemunculan pertama; field yang
/// tidak ada di sebuah dokumen bernilai NULL.
pub fn flatten_documents(docs: &[Document]) -> TableData {
    let mut headers: Vec<String> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(docs.len());
    let mut fields = Vec::new();
    for doc in docs {
        fields.clear();
        walk("", doc, &mut fields);
        let mut row: Vec<Option<String>> = vec![None; headers.len()];
        for (name, value) in fields.drain(..) {
            let idx = *positions.entry(name.clone()).or_insert_with(|| {
                headers.push(name);
                headers.len() - 1
            });
            if row.len() <= idx {
                row.resize(idx + 1, None);
            }
            row[idx] = value;
        }
        rows.push(row);
    }
    TableData::from_cells(headers, rows)
}

/// Jalankan query di `database` dan kembalikan tabelnya. `row_limit`
/// membatasi jumlah dokumen; `true` = ada dokumen yang terpotong.
pub async fn fetch(
    client: &mongodb::Client,
    database: &str,
    query: &MongoQuery,
    row_limit: Option<u64>,
    cancel: &AtomicBool,
) -> Result<(TableData, bool), String> {
    let database = database.trim();
    if database.is_empty() {
        return Err("Choose the MongoDB database".to_string());
    }
    let coll = client
        .database(database)
        .collection::<Document>(&query.collection);
    // Satu dokumen ekstra untuk tahu apakah batas memotong data.
    let cap = row_limit.map(|l| l.saturating_add(1));
    let effective = match (query.limit, cap) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let as_i64 = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
    let mut cursor = match &query.pipeline {
        Some(pipeline) => {
            let mut stages = pipeline.clone();
            if let Some(n) = effective {
                stages.push(doc! { "$limit": as_i64(n) });
            }
            coll.aggregate(stages).await
        }
        None => {
            let mut find = coll.find(query.filter.clone());
            if let Some(p) = &query.projection {
                find = find.projection(p.clone());
            }
            if let Some(s) = &query.sort {
                find = find.sort(s.clone());
            }
            if let Some(n) = effective {
                find = find.limit(as_i64(n));
            }
            find.await
        }
    }
    .map_err(|e| e.to_string())?;

    let mut docs = Vec::new();
    while let Some(doc) = cursor.try_next().await.map_err(|e| e.to_string())? {
        docs.push(doc);
        if docs.len() % 1000 == 0 && cancel.load(Ordering::Relaxed) {
            return Err("Cancelled".to_string());
        }
    }
    let truncated = match row_limit {
        Some(limit) => {
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            let over = docs.len() > limit;
            docs.truncate(limit);
            over
        }
        None => false,
    };
    Ok((flatten_documents(&docs), truncated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    #[test]
    fn parses_find_and_pipeline_queries() {
        let q = parse_mongo_query(
            r#"{"collection":"orders","filter":{"_id":{"$oid":"65a1b2c3d4e5f60718293a4b"}},"sort":{"x":1},"limit":5}"#,
        )
        .unwrap();
        assert_eq!(q.collection, "orders");
        assert!(matches!(q.filter.get("_id"), Some(Bson::ObjectId(_))));
        assert_eq!(q.limit, Some(5));
        assert!(q.sort.is_some());

        let p = parse_mongo_query(r#"{"collection":"o","pipeline":[{"$match":{"a":1}}]}"#).unwrap();
        assert_eq!(p.pipeline.unwrap().len(), 1);
    }

    #[test]
    fn rejects_bad_queries() {
        assert!(parse_mongo_query("db.orders.find()").is_err());
        assert!(parse_mongo_query(r#"{"filter":{}}"#).is_err());
        assert!(parse_mongo_query(r#"{"collection":"o","bogus":1}"#).is_err());
        assert!(parse_mongo_query(r#"{"collection":"o","filter":{"a":1},"pipeline":[]}"#).is_err());
        assert!(parse_mongo_query(r#"{"collection":"o","filter":[1]}"#).is_err());
    }

    #[test]
    fn flattens_nested_documents() {
        let oid = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
        let docs = vec![
            doc! {
                "_id": oid,
                "total": 10.5,
                "customer": { "id": 7, "name": "Ann" },
                "tags": ["a", "b"],
                "at": mongodb::bson::DateTime::from_millis(1_767_225_600_000),
                "note": Bson::Null,
            },
            doc! { "_id": 2, "extra": true },
        ];
        let t = flatten_documents(&docs);
        assert_eq!(
            t.headers,
            vec![
                "_id",
                "total",
                "customer.id",
                "customer.name",
                "tags",
                "at",
                "note",
                "extra"
            ]
        );
        assert_eq!(t.value(0, 0), Some("65a1b2c3d4e5f60718293a4b"));
        assert_eq!(t.value(0, 1), Some("10.5"));
        assert_eq!(t.value(0, 2), Some("7"));
        assert_eq!(t.value(0, 4), Some(r#"["a","b"]"#));
        assert_eq!(t.value(0, 5), Some("2026-01-01 00:00:00.000"));
        assert_eq!(t.value(0, 6), None);
        assert_eq!(t.value(0, 7), None);
        assert_eq!(t.value(1, 0), Some("2"));
        assert_eq!(t.value(1, 1), None);
        assert_eq!(t.value(1, 7), Some("true"));
    }
}
