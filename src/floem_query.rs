//! Query execution for Floem UI
//! 
//! Handles SQL query execution without egui dependencies

use sqlx::{Row, Column};
use std::time::Instant;

use crate::floem_connection::ConnectionInfo;
use crate::models::enums::DatabaseType;

/// Query execution result
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub execution_time_ms: u128,
    pub error: Option<String>,
}

impl QueryResult {
    pub fn error(msg: String) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: 0,
            error: Some(msg),
        }
    }
    
    pub fn success(columns: Vec<String>, rows: Vec<Vec<String>>, execution_time_ms: u128) -> Self {
        let row_count = rows.len();
        Self {
            columns,
            rows,
            row_count,
            execution_time_ms,
            error: None,
        }
    }
}

/// Execute SQL query
pub async fn execute_query(conn_info: &ConnectionInfo, query: String) -> QueryResult {
    let start = Instant::now();
    
    match conn_info.db_type {
        DatabaseType::SQLite => execute_sqlite_query(conn_info, query, start).await,
        DatabaseType::PostgreSQL => execute_postgres_query(conn_info, query, start).await,
        DatabaseType::MySQL => execute_mysql_query(conn_info, query, start).await,
        _ => QueryResult::error("Database type not yet supported".to_string()),
    }
}

async fn execute_sqlite_query(conn_info: &ConnectionInfo, query: String, start: Instant) -> QueryResult {
    let conn_str = format!("sqlite://{}", conn_info.database);
    
    match sqlx::sqlite::SqlitePool::connect(&conn_str).await {
        Ok(pool) => {
            match sqlx::query(&query).fetch_all(&pool).await {
                Ok(rows) => {
                    if rows.is_empty() {
                        return QueryResult::success(vec![], vec![], start.elapsed().as_millis());
                    }
                    
                    // Get column names from first row
                    let columns: Vec<String> = rows[0]
                        .columns()
                        .iter()
                        .map(|col| col.name().to_string())
                        .collect();
                    
                    // Extract data
                    let mut data_rows = Vec::new();
                    for row in rows {
                        let mut data_row = Vec::new();
                        for i in 0..columns.len() {
                            let value: Option<String> = row.try_get(i).ok();
                            data_row.push(value.unwrap_or_else(|| "NULL".to_string()));
                        }
                        data_rows.push(data_row);
                    }
                    
                    QueryResult::success(columns, data_rows, start.elapsed().as_millis())
                }
                Err(e) => QueryResult::error(format!("Query error: {}", e)),
            }
        }
        Err(e) => QueryResult::error(format!("Connection error: {}", e)),
    }
}

async fn execute_postgres_query(conn_info: &ConnectionInfo, query: String, start: Instant) -> QueryResult {
    let conn_str = format!(
        "postgres://{}:{}@{}:{}/{}",
        conn_info.username, conn_info.password, conn_info.host, conn_info.port, conn_info.database
    );
    
    match sqlx::postgres::PgPool::connect(&conn_str).await {
        Ok(pool) => {
            match sqlx::query(&query).fetch_all(&pool).await {
                Ok(rows) => {
                    if rows.is_empty() {
                        return QueryResult::success(vec![], vec![], start.elapsed().as_millis());
                    }
                    
                    let columns: Vec<String> = rows[0]
                        .columns()
                        .iter()
                        .map(|col| col.name().to_string())
                        .collect();
                    
                    let mut data_rows = Vec::new();
                    for row in rows {
                        let mut data_row = Vec::new();
                        for i in 0..columns.len() {
                            let value: Option<String> = row.try_get(i).ok();
                            data_row.push(value.unwrap_or_else(|| "NULL".to_string()));
                        }
                        data_rows.push(data_row);
                    }
                    
                    QueryResult::success(columns, data_rows, start.elapsed().as_millis())
                }
                Err(e) => QueryResult::error(format!("Query error: {}", e)),
            }
        }
        Err(e) => QueryResult::error(format!("Connection error: {}", e)),
    }
}

async fn execute_mysql_query(conn_info: &ConnectionInfo, query: String, start: Instant) -> QueryResult {
    let conn_str = format!(
        "mysql://{}:{}@{}:{}/{}",
        conn_info.username, conn_info.password, conn_info.host, conn_info.port, conn_info.database
    );
    
    match sqlx::mysql::MySqlPool::connect(&conn_str).await {
        Ok(pool) => {
            match sqlx::query(&query).fetch_all(&pool).await {
                Ok(rows) => {
                    if rows.is_empty() {
                        return QueryResult::success(vec![], vec![], start.elapsed().as_millis());
                    }
                    
                    let columns: Vec<String> = rows[0]
                        .columns()
                        .iter()
                        .map(|col| col.name().to_string())
                        .collect();
                    
                    let mut data_rows = Vec::new();
                    for row in rows {
                        let mut data_row = Vec::new();
                        for i in 0..columns.len() {
                            let value: Option<String> = row.try_get(i).ok();
                            data_row.push(value.unwrap_or_else(|| "NULL".to_string()));
                        }
                        data_rows.push(data_row);
                    }
                    
                    QueryResult::success(columns, data_rows, start.elapsed().as_millis())
                }
                Err(e) => QueryResult::error(format!("Query error: {}", e)),
            }
        }
        Err(e) => QueryResult::error(format!("Connection error: {}", e)),
    }
}
