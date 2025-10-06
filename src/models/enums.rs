use std::sync::Arc;

use mongodb::Client as MongoClient;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use sqlx::{MySqlPool, PgPool, SqlitePool};

#[derive(Clone, PartialEq, Debug)]
pub enum NodeType {
    #[allow(dead_code)]
    Database,
    Table,
    Column,
    Query,
    QueryHistItem,
    // Folder/grouping node for query history by execution date (YYYY-MM-DD)
    HistoryDateFolder,
    Connection,
    DatabasesFolder,
    TablesFolder,
    ViewsFolder,
    StoredProceduresFolder,
    UserFunctionsFolder,
    TriggersFolder,
    EventsFolder,
    DBAViewsFolder,
    UsersFolder,
    PrivilegesFolder,
    ProcessesFolder,
    StatusFolder,
    MetricsUserActiveFolder,
    // MySQL specific DBA quick views
    ReplicationStatusFolder,
    MasterStatusFolder,
    View,
    StoredProcedure,
    UserFunction,
    Trigger,
    Event,
    MySQLFolder,      // Folder untuk koneksi MySQL
    MsSQLFolder,      // Folder untuk koneksi MsSQL
    MongoDBFolder,    // Folder untuk koneksi MongoDB
    PostgreSQLFolder, // Folder untuk koneksi PostgreSQL
    SQLiteFolder,     // Folder untuk koneksi SQLite
    RedisFolder,      // Folder untuk koneksi Redis
    CustomFolder,     // Folder custom yang bisa dinamai user
    QueryFolder,      // Folder untuk mengelompokkan query files
    // New table subfolders and items
    ColumnsFolder,
    IndexesFolder,
    PrimaryKeysFolder,
    Index,
}

// Special DBA quick view context (used to apply post-processing without embedding markers in SQL)
#[derive(Clone, PartialEq, Debug)]
pub enum DBASpecialMode {
    ReplicationStatus,
    MasterStatus,
}

#[derive(Debug, Clone)]
pub enum BackgroundTask {
    RefreshConnection { connection_id: i64 },
    CheckForUpdates,
    StartPrefetch { 
        connection_id: i64,
        show_progress: bool, // Whether to show progress in UI
    },
}

#[derive(Debug, Clone)]
pub enum BackgroundResult {
    RefreshComplete {
        connection_id: i64,
        success: bool,
    },
    UpdateCheckComplete {
        result: Result<crate::self_update::UpdateInfo, String>,
    },
    PrefetchProgress {
        connection_id: i64,
        completed: usize,
        total: usize,
    },
    PrefetchComplete {
        connection_id: i64,
    },
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    SQLite,
    Redis,
    MsSQL,
    MongoDB,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutocompleteKind {
    Table,
    Column,
    Syntax,
}

// Enum untuk berbagai jenis database pool - sqlx pools are already thread-safe
#[derive(Clone)]
pub enum DatabasePool {
    MySQL(Arc<MySqlPool>),
    PostgreSQL(Arc<PgPool>),
    SQLite(Arc<SqlitePool>),
    Redis(Arc<ConnectionManager>),
    // For MsSQL we store a lightweight config (connections opened per query for now)
    MsSQL(Arc<crate::driver_mssql::MssqlConfigWrapper>),
    MongoDB(Arc<MongoClient>),
}
