use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug)]
pub enum NodeType {
    #[allow(dead_code)]
    Database,
    Table,
    Column,
    Query,
    QueryHistItem,
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
    View,
    StoredProcedure,
    UserFunction,
    Trigger,
    Event,
    MySQLFolder,       // Folder untuk koneksi MySQL
    PostgreSQLFolder,  // Folder untuk koneksi PostgreSQL
    SQLiteFolder,      // Folder untuk koneksi SQLite
    RedisFolder,       // Folder untuk koneksi Redis
    CustomFolder,      // Folder custom yang bisa dinamai user
    QueryFolder,       // Folder untuk mengelompokkan query files
}

#[derive(Debug, Clone)]
pub enum BackgroundTask {
    RefreshConnection { connection_id: i64 },
}

#[derive(Debug, Clone)]
pub enum BackgroundResult {
    RefreshComplete { connection_id: i64, success: bool },
}


#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    SQLite,
    Redis,
}