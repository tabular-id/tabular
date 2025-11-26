//! Connection management for Floem UI
//! 
//! Simplified connection handling without egui dependencies

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::models::enums::{DatabaseType, DatabasePool};

/// Connection information for Floem UI
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub id: i64,
    pub name: String,
    pub db_type: DatabaseType,
    pub host: String,
    pub port: String,
    pub database: String,
    pub username: String,
    #[serde(skip)]
    pub password: String,
}

impl ConnectionInfo {
    pub fn new_sqlite(name: String, path: String) -> Self {
        Self {
            id: 0,
            name,
            db_type: DatabaseType::SQLite,
            host: String::new(),
            port: String::new(),
            database: path,
            username: String::new(),
            password: String::new(),
        }
    }
    
    pub fn new_postgres(name: String, host: String, port: String, database: String, username: String, password: String) -> Self {
        Self {
            id: 0,
            name,
            db_type: DatabaseType::PostgreSQL,
            host,
            port,
            database,
            username,
            password,
        }
    }
}

/// Connection state manager
#[derive(Clone)]
pub struct ConnectionManager {
    connections: Arc<Mutex<Vec<ConnectionInfo>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    pub fn add_connection(&mut self, conn: ConnectionInfo) {
        let mut conns = self.connections.lock().unwrap();
        conns.push(conn);
    }
    
    pub fn get_connections(&self) -> Vec<ConnectionInfo> {
        self.connections.lock().unwrap().clone()
    }
    
    pub async fn test_connection(&self, conn: &ConnectionInfo) -> Result<String, String> {
        match conn.db_type {
            DatabaseType::SQLite => {
                // Test SQLite connection
                match sqlx::sqlite::SqlitePool::connect(&format!("sqlite://{}", conn.database)).await {
                    Ok(_) => Ok("SQLite connection successful".to_string()),
                    Err(e) => Err(format!("SQLite error: {}", e)),
                }
            }
            DatabaseType::PostgreSQL => {
                // Test PostgreSQL connection
                let conn_str = format!(
                    "postgres://{}:{}@{}:{}/{}",
                    conn.username, conn.password, conn.host, conn.port, conn.database
                );
                match sqlx::postgres::PgPool::connect(&conn_str).await {
                    Ok(_) => Ok("PostgreSQL connection successful".to_string()),
                    Err(e) => Err(format!("PostgreSQL error: {}", e)),
                }
            }
            _ => Err("Database type not yet supported in Floem UI".to_string()),
        }
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}
