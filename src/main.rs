use eframe::{egui, App, Frame};
use serde::{Deserialize, Serialize};
use sqlx::{SqlitePool, MySqlPool, PgPool, Row, Column, mysql::MySqlPoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};

fn main() -> Result<(), eframe::Error> {
    let mut options = eframe::NativeOptions::default();
    
    // Set window icon
    if let Some(icon) = load_icon() {
        options.viewport.icon = Some(std::sync::Arc::new(icon));
    }
    
    eframe::run_native(
        "Tabular",
        options,
        Box::new(|_cc| Box::new(MyApp::new())),
    )
}

fn load_icon() -> Option<egui::IconData> {
    let icon_bytes = include_bytes!("../assets/logo.png");
    
    match image::load_from_memory(icon_bytes) {
        Ok(image) => {
            let rgba = image.to_rgba8();
            let (width, height) = rgba.dimensions();
            Some(egui::IconData {
                rgba: rgba.into_raw(),
                width,
                height,
            })
        }
        Err(e) => {
            eprintln!("Failed to load icon: {}", e);
            None
        }
    }
}


#[derive(Clone)]
struct TreeNode {
    name: String,
    children: Vec<TreeNode>,
    is_expanded: bool,
    node_type: NodeType,
    connection_id: Option<i64>, // For connection nodes
    is_loaded: bool, // For tracking if tables/columns are loaded
    database_name: Option<String>, // For storing database context
    file_path: Option<String>, // For query files
}

#[derive(Clone, PartialEq, Debug)]
enum NodeType {
    #[allow(dead_code)]
    Database,
    Table,
    Column,
    Query,
    HistoryItem,
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
}

#[derive(Clone, Debug)]
struct QueryTab {
    title: String,
    content: String,
    file_path: Option<String>,
    is_saved: bool,
    is_modified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistoryItem {
    id: Option<i64>,
    query: String,
    connection_id: i64,
    connection_name: String,
    executed_at: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct ConnectionConfig {
    id: Option<i64>,
    name: String,
    host: String,
    port: String,
    username: String,
    password: String,
    database: String,
    connection_type: DatabaseType,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
enum DatabaseType {
    MySQL,
    PostgreSQL,
    SQLite,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            id: None,
            name: String::new(),
            host: "localhost".to_string(),
            port: "3306".to_string(),
            username: String::new(),
            password: String::new(),
            database: String::new(),
            connection_type: DatabaseType::MySQL,
        }
    }
}

impl TreeNode {
    fn new(name: String, node_type: NodeType) -> Self {
        Self {
            name,
            children: Vec::new(),
            is_expanded: false,
            node_type,
            connection_id: None,
            is_loaded: true, // Regular nodes are always loaded
            database_name: None,
            file_path: None,
        }
    }

    #[allow(dead_code)]
    fn with_children(name: String, node_type: NodeType, children: Vec<TreeNode>) -> Self {
        Self {
            name,
            children,
            is_expanded: false,
            node_type,
            connection_id: None,
            is_loaded: true,
            database_name: None,
            file_path: None,
        }
    }

    fn new_connection(name: String, connection_id: i64) -> Self {
        Self {
            name,
            children: Vec::new(),
            is_expanded: false,
            node_type: NodeType::Connection,
            connection_id: Some(connection_id),
            is_loaded: false, // Connection nodes need to load tables
            database_name: None,
            file_path: None,
        }
    }
}

// Enum untuk berbagai jenis database pool - sqlx pools are already thread-safe
#[derive(Clone)]
enum DatabasePool {
    MySQL(Arc<MySqlPool>),
    PostgreSQL(Arc<PgPool>),
    SQLite(Arc<SqlitePool>),
}

struct MyApp {
    editor_text: String,
    selected_menu: String,
    items_tree: Vec<TreeNode>,
    queries_tree: Vec<TreeNode>,
    history_tree: Vec<TreeNode>,
    history_items: Vec<HistoryItem>, // Actual history data
    connections: Vec<ConnectionConfig>,
    show_add_connection: bool,
    new_connection: ConnectionConfig,
    db_pool: Option<Arc<SqlitePool>>,
    // Connection cache untuk menghindari membuat koneksi berulang
    connection_pools: HashMap<i64, DatabasePool>,
    // Context menu and edit connection fields
    show_edit_connection: bool,
    edit_connection: ConnectionConfig,
    // UI refresh flag
    needs_refresh: bool,
    // Table data display
    current_table_data: Vec<Vec<String>>,
    current_table_headers: Vec<String>,
    current_table_name: String,
    current_connection_id: Option<i64>,
    // Splitter position for resizable table view (0.0 to 1.0)
    table_split_ratio: f32,
    // Table sorting state
    sort_column: Option<usize>,
    sort_ascending: bool,
    // Test connection status
    test_connection_status: Option<(bool, String)>, // (success, message)
    test_connection_in_progress: bool,
    // Background processing channels
    background_sender: Option<Sender<BackgroundTask>>,
    background_receiver: Option<Receiver<BackgroundResult>>,
    // Background refresh status tracking
    refreshing_connections: std::collections::HashSet<i64>,
    // Query tab system
    query_tabs: Vec<QueryTab>,
    active_tab_index: usize,
    next_tab_id: usize,
    // Save dialog
    show_save_dialog: bool,
    save_filename: String,
    // Connection selection dialog
    show_connection_selector: bool,
    pending_query: String, // Store query to execute after connection is selected
    auto_execute_after_connection: bool, // Flag to auto-execute after connection selected
    // Error message display
    error_message: String,
    show_error_message: bool,
}

#[derive(Debug, Clone)]
struct ExpansionRequest {
    node_type: NodeType,
    connection_id: i64,
    database_name: Option<String>,
}

#[derive(Debug, Clone)]
enum BackgroundTask {
    RefreshConnection { connection_id: i64 },
}

#[derive(Debug, Clone)]
enum BackgroundResult {
    RefreshComplete { connection_id: i64, success: bool },
}

impl MyApp {
    fn get_app_data_dir() -> std::path::PathBuf {
        let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
        home_dir.join(".tabular")
    }
    
    fn get_data_dir() -> std::path::PathBuf {
        Self::get_app_data_dir().join("data")
    }
    
    fn get_query_dir() -> std::path::PathBuf {
        Self::get_app_data_dir().join("query")
    }
    
    fn ensure_app_directories() -> Result<(), std::io::Error> {
        let app_dir = Self::get_app_data_dir();
        let data_dir = Self::get_data_dir();
        let query_dir = Self::get_query_dir();
        
        // Create directories if they don't exist
        std::fs::create_dir_all(&app_dir)?;
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&query_dir)?;
        
        println!("Created app directories:");
        println!("  App: {}", app_dir.display());
        println!("  Data: {}", data_dir.display());
        println!("  Query: {}", query_dir.display());
        
        Ok(())
    }

    fn url_encode(input: &str) -> String {
        input
            .replace("%", "%25")  // Must be first
            .replace("#", "%23")
            .replace("&", "%26")
            .replace("@", "%40")
            .replace("?", "%3F")
            .replace("=", "%3D")
            .replace("+", "%2B")
            .replace(" ", "%20")
            .replace(":", "%3A")
            .replace("/", "%2F")
    }

    fn new() -> Self {
        // Create background processing channels
        let (background_sender, background_receiver) = mpsc::channel::<BackgroundTask>();
        let (result_sender, result_receiver) = mpsc::channel::<BackgroundResult>();

        let mut app = Self {
            editor_text: String::new(),
            selected_menu: "Database".to_string(),
            items_tree: Vec::new(),
            queries_tree: Vec::new(),
            history_tree: Vec::new(),
            history_items: Vec::new(),
            connections: Vec::new(),
            show_add_connection: false,
            new_connection: ConnectionConfig::default(),
            db_pool: None,
            connection_pools: HashMap::new(), // Start with empty cache
            show_edit_connection: false,
            edit_connection: ConnectionConfig::default(),
            needs_refresh: false,
            current_table_data: Vec::new(),
            current_table_headers: Vec::new(),
            current_table_name: String::new(),
            current_connection_id: None,
            table_split_ratio: 0.6, // Default 60% for editor, 40% for table
            sort_column: None,
            sort_ascending: true,
            test_connection_status: None,
            test_connection_in_progress: false,
            background_sender: Some(background_sender),
            background_receiver: Some(result_receiver),
            refreshing_connections: std::collections::HashSet::new(),
            query_tabs: Vec::new(),
            active_tab_index: 0,
            next_tab_id: 1,
            show_save_dialog: false,
            save_filename: String::new(),
            show_connection_selector: false,
            pending_query: String::new(),
            auto_execute_after_connection: false,
            error_message: String::new(),
            show_error_message: false,
        };
        
        // Clear any old cached pools
        app.connection_pools.clear();
        println!("Application started with fresh connection pool cache");
        
        // Initialize database and sample data FIRST
        app.initialize_database();
        app.initialize_sample_data();
        
        // Load saved queries from directory
        app.load_queries_from_directory();
        
        // Create initial query tab
        app.create_new_tab("Untitled Query".to_string(), String::new());
        
        // Start background thread AFTER database is initialized
        app.start_background_worker(background_receiver, result_sender);
        
        app
    }

    fn start_background_worker(&self, task_receiver: Receiver<BackgroundTask>, result_sender: Sender<BackgroundResult>) {
        // Get the current db_pool for cache operations
        let db_pool = self.db_pool.clone();
        
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            while let Ok(task) = task_receiver.recv() {
                match task {
                    BackgroundTask::RefreshConnection { connection_id } => {
                        println!("Background worker processing refresh for connection_id: {}", connection_id);
                        
                        let success = Self::refresh_connection_background_with_db(
                            connection_id,
                            &db_pool,
                            &rt
                        );
                        
                        let _ = result_sender.send(BackgroundResult::RefreshComplete {
                            connection_id,
                            success,
                        });
                    }
                }
            }
        });
    }

    fn refresh_connection_background_with_db(
        connection_id: i64,
        db_pool: &Option<Arc<SqlitePool>>,
        rt: &tokio::runtime::Runtime,
    ) -> bool {
        println!("Background refresh started for connection_id: {}", connection_id);
        
        // Get connection from database
        if let Some(cache_pool_arc) = db_pool {
            let connection_result = rt.block_on(async {
                sqlx::query_as::<_, (i64, String, String, String, String, String, String, String)>(
                    "SELECT id, name, host, port, username, password, database_name, connection_type FROM connections WHERE id = ?"
                )
                .bind(connection_id)
                .fetch_optional(cache_pool_arc.as_ref())
                .await
            });
            
            println!("Query result for connection_id {}: {:?}", connection_id, connection_result.is_ok());
            match &connection_result {
                Ok(Some(data)) => println!("Found connection data: {} - {}", data.0, data.1),
                Ok(None) => println!("Query succeeded but no connection found with ID: {}", connection_id),
                Err(e) => println!("Database query error: {}", e),
            }
            
            if let Ok(Some((id, name, host, port, username, password, database_name, connection_type))) = connection_result {
                let connection = ConnectionConfig {
                    id: Some(id),
                    name,
                    host,
                    port,
                    username,
                    password,
                    database: database_name,
                    connection_type: match connection_type.as_str() {
                        "MySQL" => DatabaseType::MySQL,
                        "PostgreSQL" => DatabaseType::PostgreSQL,
                        _ => DatabaseType::SQLite,
                    },
                };
                
                println!("Found connection: {}", connection.name);
                
                let result = rt.block_on(async {
                    // Clear cache
                    println!("Clearing cache for connection_id: {}", connection_id);
                    let _ = sqlx::query("DELETE FROM database_cache WHERE connection_id = ?")
                        .bind(connection_id)
                        .execute(cache_pool_arc.as_ref())
                        .await;
                    
                    let _ = sqlx::query("DELETE FROM table_cache WHERE connection_id = ?")
                        .bind(connection_id)
                        .execute(cache_pool_arc.as_ref())
                        .await;
                    
                    let _ = sqlx::query("DELETE FROM column_cache WHERE connection_id = ?")
                        .bind(connection_id)
                        .execute(cache_pool_arc.as_ref())
                        .await;

                    // Create new connection pool
                    println!("Creating new pool for connection_id: {}", connection_id);
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30), // 30 second timeout
                        Self::create_database_pool(&connection)
                    ).await {
                        Ok(Some(new_pool)) => {
                            println!("Created new pool for connection_id: {}", connection_id);
                            Self::fetch_and_cache_all_data(connection_id, &connection, &new_pool, cache_pool_arc.as_ref()).await
                        }
                        Ok(None) => {
                            println!("Failed to create pool for connection_id: {}", connection_id);
                            false
                        }
                        Err(_) => {
                            println!("Timeout creating pool for connection_id: {}", connection_id);
                            false
                        }
                    }
                });
                println!("Background refresh result for connection_id {}: {}", connection_id, result);
                result
            } else {
                println!("Connection not found in database for ID: {}", connection_id);
                false
            }
        } else {
            println!("No cache database available for connection_id: {}", connection_id);
            false
        }
    }

    async fn create_database_pool(connection: &ConnectionConfig) -> Option<DatabasePool> {
        println!("Creating database pool for connection: {}", connection.name);
        
        match connection.connection_type {
            DatabaseType::MySQL => {
                let encoded_username = Self::url_encode(&connection.username);
                let encoded_password = Self::url_encode(&connection.password);
                let connection_string = format!(
                    "mysql://{}:{}@{}:{}/{}",
                    encoded_username, encoded_password, connection.host, connection.port, connection.database
                );
                
                println!("Attempting MySQL connection to: {}:{}@{}", connection.username, connection.host, connection.port);
                
                match MySqlPoolOptions::new()
                    .max_connections(3) // Reduced from 5 to 3
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        println!("Successfully created MySQL pool for connection: {}", connection.name);
                        Some(DatabasePool::MySQL(Arc::new(pool)))
                    }
                    Err(e) => {
                        println!("Failed to create MySQL pool for '{}': {}", connection.name, e);
                        None
                    }
                }
            }
            DatabaseType::PostgreSQL => {
                let connection_string = format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    connection.username, connection.password, connection.host, connection.port, connection.database
                );
                
                println!("Attempting PostgreSQL connection to: {}:{}@{}", connection.username, connection.host, connection.port);
                
                match PgPoolOptions::new()
                    .max_connections(3)
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        println!("Successfully created PostgreSQL pool for connection: {}", connection.name);
                        Some(DatabasePool::PostgreSQL(Arc::new(pool)))
                    }
                    Err(e) => {
                        println!("Failed to create PostgreSQL pool for '{}': {}", connection.name, e);
                        None
                    }
                }
            }
            DatabaseType::SQLite => {
                let connection_string = format!("sqlite:{}", connection.host);
                
                println!("Attempting SQLite connection to: {}", connection.host);
                
                match SqlitePoolOptions::new()
                    .max_connections(3)
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        println!("Successfully created SQLite pool for connection: {}", connection.name);
                        Some(DatabasePool::SQLite(Arc::new(pool)))
                    }
                    Err(e) => {
                        println!("Failed to create SQLite pool for '{}': {}", connection.name, e);
                        None
                    }
                }
            }
        }
    }

    async fn fetch_and_cache_all_data(
        connection_id: i64,
        connection: &ConnectionConfig,
        pool: &DatabasePool,
        cache_pool: &SqlitePool,
    ) -> bool {
        match &connection.connection_type {
            DatabaseType::MySQL => {
                if let DatabasePool::MySQL(mysql_pool) = pool {
                    Self::fetch_mysql_data(connection_id, mysql_pool, cache_pool).await
                } else {
                    false
                }
            }
            DatabaseType::SQLite => {
                if let DatabasePool::SQLite(sqlite_pool) = pool {
                    Self::fetch_sqlite_data(connection_id, sqlite_pool, cache_pool).await
                } else {
                    false
                }
            }
            DatabaseType::PostgreSQL => {
                if let DatabasePool::PostgreSQL(postgres_pool) = pool {
                    Self::fetch_postgres_data(connection_id, postgres_pool, cache_pool).await
                } else {
                    false
                }
            }
        }
    }

    async fn fetch_mysql_data(connection_id: i64, pool: &MySqlPool, cache_pool: &SqlitePool) -> bool {
        // Fetch databases
        if let Ok(rows) = sqlx::query("SHOW DATABASES")
            .fetch_all(pool)
            .await 
        {
            for row in rows {
                if let Ok(db_name) = row.try_get::<String, _>(0) {
                    // Cache database
                    let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                        .bind(connection_id)
                        .bind(&db_name)
                        .execute(cache_pool)
                        .await;

                    // Fetch tables for this database
                    let query = format!("SHOW TABLES FROM `{}`", db_name);
                    if let Ok(table_rows) = sqlx::query(&query).fetch_all(pool).await {
                        for table_row in table_rows {
                            if let Ok(table_name) = table_row.try_get::<String, _>(0) {
                                // Cache table
                                let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name) VALUES (?, ?, ?)")
                                    .bind(connection_id)
                                    .bind(&db_name)
                                    .bind(&table_name)
                                    .execute(cache_pool)
                                    .await;

                                // Fetch columns for this table
                                let col_query = format!("DESCRIBE `{}`.`{}`", db_name, table_name);
                                if let Ok(col_rows) = sqlx::query(&col_query).fetch_all(pool).await {
                                    for col_row in col_rows {
                                        if let (Ok(col_name), Ok(col_type)) = (
                                            col_row.try_get::<String, _>(0),
                                            col_row.try_get::<String, _>(1)
                                        ) {
                                            // Cache column
                                            let _ = sqlx::query("INSERT OR REPLACE INTO column_cache (connection_id, database_name, table_name, column_name, data_type, ordinal_position) VALUES (?, ?, ?, ?, ?, ?)")
                                                .bind(connection_id)
                                                .bind(&db_name)
                                                .bind(&table_name)
                                                .bind(&col_name)
                                                .bind(&col_type)
                                                .bind(0) // MySQL DESCRIBE doesn't provide ordinal position easily
                                                .execute(cache_pool)
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            true
        } else {
            false
        }
    }

    async fn fetch_sqlite_data(connection_id: i64, pool: &SqlitePool, cache_pool: &SqlitePool) -> bool {
        // For SQLite, we typically work with the main database, but we can get table info
        if let Ok(rows) = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            .fetch_all(pool)
            .await 
        {
            // Cache the main database
            let db_name = "main";
            let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                .bind(connection_id)
                .bind(db_name)
                .execute(cache_pool)
                .await;

            // Cache tables
            for row in rows {
                if let Ok(table_name) = row.try_get::<String, _>(0) {
                    // Cache table
                    let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                        .bind(connection_id)
                        .bind(db_name)
                        .bind(&table_name)
                        .bind("table")
                        .execute(cache_pool)
                        .await;

                    // Fetch columns for this table
                    let col_query = format!("PRAGMA table_info({})", table_name);
                    if let Ok(col_rows) = sqlx::query(&col_query).fetch_all(pool).await {
                        for col_row in col_rows {
                            if let (Ok(col_name), Ok(col_type)) = (
                                col_row.try_get::<String, _>("name"),
                                col_row.try_get::<String, _>("type")
                            ) {
                                // Cache column
                                let _ = sqlx::query("INSERT OR REPLACE INTO column_cache (connection_id, database_name, table_name, column_name, data_type, ordinal_position) VALUES (?, ?, ?, ?, ?, ?)")
                                    .bind(connection_id)
                                    .bind(db_name)
                                    .bind(&table_name)
                                    .bind(&col_name)
                                    .bind(&col_type)
                                    .bind(0) // SQLite doesn't have ordinal position in PRAGMA
                                    .execute(cache_pool)
                                    .await;
                            }
                        }
                    }
                }
            }
            true
        } else {
            false
        }
    }

    async fn fetch_postgres_data(connection_id: i64, pool: &PgPool, cache_pool: &SqlitePool) -> bool {
        // Fetch databases
        if let Ok(rows) = sqlx::query("SELECT datname FROM pg_database WHERE datistemplate = false")
            .fetch_all(pool)
            .await 
        {
            for row in rows {
                if let Ok(db_name) = row.try_get::<String, _>(0) {
                    // Cache database
                    let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                        .bind(connection_id)
                        .bind(&db_name)
                        .execute(cache_pool)
                        .await;

                    // Fetch tables for this database (PostgreSQL uses schemas, typically 'public')
                    if let Ok(table_rows) = sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
                        .fetch_all(pool)
                        .await 
                    {
                        for table_row in table_rows {
                            if let Ok(table_name) = table_row.try_get::<String, _>(0) {
                                // Cache table
                                let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                                    .bind(connection_id)
                                    .bind(&db_name)
                                    .bind(&table_name)
                                    .bind("table")
                                    .execute(cache_pool)
                                    .await;

                                // Fetch columns for this table
                                if let Ok(col_rows) = sqlx::query("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public' ORDER BY ordinal_position")
                                    .bind(&table_name)
                                    .fetch_all(pool)
                                    .await 
                                {
                                    for col_row in col_rows {
                                        if let (Ok(col_name), Ok(col_type), Ok(ordinal_pos)) = (
                                            col_row.try_get::<String, _>(0),
                                            col_row.try_get::<String, _>(1),
                                            col_row.try_get::<i32, _>(2)
                                        ) {
                                            // Cache column
                                            let _ = sqlx::query("INSERT OR REPLACE INTO column_cache (connection_id, database_name, table_name, column_name, data_type, ordinal_position) VALUES (?, ?, ?, ?, ?, ?)")
                                                .bind(connection_id)
                                                .bind(&db_name)
                                                .bind(&table_name)
                                                .bind(&col_name)
                                                .bind(&col_type)
                                                .bind(ordinal_pos)
                                                .execute(cache_pool)
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn initialize_database(&mut self) {
        // Ensure app directories exist
        if let Err(e) = Self::ensure_app_directories() {
            println!("Failed to create app directories: {}", e);
            return;
        }
        
        // Initialize SQLite database
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool_result = rt.block_on(async {
            // Get the data directory path
            let data_dir = Self::get_data_dir();
            let db_path = data_dir.join("connections.db");
            
            println!("Attempting to connect to database at: {}", db_path.display());
            
            // Convert path to string and use file:// prefix for SQLite
            let db_path_str = db_path.to_string_lossy();
            let connection_string = format!("sqlite://{}?mode=rwc", db_path_str);
            let pool = SqlitePool::connect(&connection_string).await;
            
            match pool {
                Ok(pool) => {
                    println!("Database connection successful");
                    
                    // Create connections table
                    let create_connections_result = sqlx::query(
                        r#"
                        CREATE TABLE IF NOT EXISTS connections (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            host TEXT NOT NULL,
                            port TEXT NOT NULL,
                            username TEXT NOT NULL,
                            password TEXT NOT NULL,
                            database_name TEXT NOT NULL,
                            connection_type TEXT NOT NULL
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    // Create database cache table
                    let create_db_cache_result = sqlx::query(
                        r#"
                        CREATE TABLE IF NOT EXISTS database_cache (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            connection_id INTEGER NOT NULL,
                            database_name TEXT NOT NULL,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (connection_id) REFERENCES connections (id) ON DELETE CASCADE,
                            UNIQUE(connection_id, database_name)
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    // Create table cache table
                    let create_table_cache_result = sqlx::query(
                        r#"
                        CREATE TABLE IF NOT EXISTS table_cache (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            connection_id INTEGER NOT NULL,
                            database_name TEXT NOT NULL,
                            table_name TEXT NOT NULL,
                            table_type TEXT NOT NULL, -- 'table', 'view', 'procedure', etc.
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (connection_id) REFERENCES connections (id) ON DELETE CASCADE,
                            UNIQUE(connection_id, database_name, table_name, table_type)
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    // Create column cache table
                    let create_column_cache_result = sqlx::query(
                        r#"
                        CREATE TABLE IF NOT EXISTS column_cache (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            connection_id INTEGER NOT NULL,
                            database_name TEXT NOT NULL,
                            table_name TEXT NOT NULL,
                            column_name TEXT NOT NULL,
                            data_type TEXT NOT NULL,
                            ordinal_position INTEGER NOT NULL,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (connection_id) REFERENCES connections (id) ON DELETE CASCADE,
                            UNIQUE(connection_id, database_name, table_name, column_name)
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    // Create query history table
                    let create_history_result = sqlx::query(
                        r#"
                        CREATE TABLE IF NOT EXISTS query_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            query_text TEXT NOT NULL,
                            connection_id INTEGER NOT NULL,
                            connection_name TEXT NOT NULL,
                            executed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (connection_id) REFERENCES connections (id) ON DELETE CASCADE
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    match (create_connections_result, create_db_cache_result, create_table_cache_result, create_column_cache_result, create_history_result) {
                        (Ok(_), Ok(_), Ok(_), Ok(_), Ok(_)) => {
                            println!("All database tables created successfully");
                            Some(pool)
                        },
                        _ => {
                            println!("Error creating some tables");
                            None
                        }
                    }
                },
                Err(e) => {
                    println!("Database connection failed: {}", e);
                    None
                }
            }
        });
        
        if let Some(pool) = pool_result {
            self.db_pool = Some(Arc::new(pool));
            // Load existing connections from database
            self.load_connections();
            // Load query history from database
            self.load_query_history();
        }
    }

    fn initialize_sample_data(&mut self) {
        // Initialize with connections as root nodes
        self.refresh_connections_tree();

        // Sample queries
        self.queries_tree = vec![
            TreeNode::new("Get all users".to_string(), NodeType::Query),
            TreeNode::new("Recent orders".to_string(), NodeType::Query),
            TreeNode::new("Product statistics".to_string(), NodeType::Query),
        ];

        // Initialize empty history tree (will be loaded from database)
        self.refresh_history_tree();
    }

    fn refresh_connections_tree(&mut self) {
        println!("=== refresh_connections_tree called with {} connections ===", self.connections.len());
        
        // Debug: List all connections that will be processed
        for (i, conn) in self.connections.iter().enumerate() {
            println!("  [{}] Processing connection: {} (ID: {:?})", i, conn.name, conn.id);
        }
        
        // Clear existing tree
        let old_tree_count = self.items_tree.len();
        self.items_tree.clear();
        println!("Cleared old tree (had {} nodes)", old_tree_count);

        // Convert connections to tree nodes
        self.items_tree = self.connections.iter()
            .filter_map(|conn| {
                println!("Converting connection: {} (ID: {:?})", conn.name, conn.id);
                if let Some(id) = conn.id {
                    let connection_icon = match conn.connection_type {
                        DatabaseType::MySQL => "🐬",
                        DatabaseType::PostgreSQL => "🐘",
                        DatabaseType::SQLite => "📄",
                    };
                    let node = TreeNode::new_connection(
                        format!("{} {}", connection_icon, conn.name),
                        id
                    );
                    println!("Created tree node for connection: {}", node.name);
                    Some(node)
                } else {
                    println!("Skipping connection with no ID: {}", conn.name);
                    None
                }
            })
            .collect();
            
        println!("After refresh: items_tree has {} nodes", self.items_tree.len());
        
        // Debug: List all tree nodes created
        for (i, node) in self.items_tree.iter().enumerate() {
            println!("  Tree node [{}]: {} (connection_id: {:?})", i, node.name, node.connection_id);
        }
        
        println!("=== refresh_connections_tree completed ===");
    }

    // Tab management methods
    fn create_new_tab(&mut self, title: String, content: String) -> usize {
        let tab_id = self.next_tab_id;
        self.next_tab_id += 1;
        
        let new_tab = QueryTab {
            title,
            content: content.clone(),
            file_path: None,
            is_saved: false,
            is_modified: false,
        };
        
        self.query_tabs.push(new_tab);
        let new_index = self.query_tabs.len() - 1;
        self.active_tab_index = new_index;
        
        // Update editor with new tab content
        self.editor_text = content;
        
        tab_id
    }

    fn close_tab(&mut self, tab_index: usize) {
        if self.query_tabs.len() <= 1 {
            // Don't close the last tab, just clear it
            if let Some(tab) = self.query_tabs.get_mut(0) {
                tab.content.clear();
                tab.title = "Untitled Query".to_string();
                tab.file_path = None;
                tab.is_saved = false;
                tab.is_modified = false;
            }
            self.editor_text.clear();
            return;
        }

        if tab_index < self.query_tabs.len() {
            self.query_tabs.remove(tab_index);
            
            // Adjust active tab index
            if self.active_tab_index >= self.query_tabs.len() {
                self.active_tab_index = self.query_tabs.len() - 1;
            } else if self.active_tab_index > tab_index {
                self.active_tab_index -= 1;
            }
            
            // Update editor with active tab content
            if let Some(active_tab) = self.query_tabs.get(self.active_tab_index) {
                self.editor_text = active_tab.content.clone();
            }
        }
    }

    fn switch_to_tab(&mut self, tab_index: usize) {
        if tab_index < self.query_tabs.len() {
            // Save current tab content
            if let Some(current_tab) = self.query_tabs.get_mut(self.active_tab_index) {
                if current_tab.content != self.editor_text {
                    current_tab.content = self.editor_text.clone();
                    current_tab.is_modified = true;
                }
            }
            
            // Switch to new tab
            self.active_tab_index = tab_index;
            if let Some(new_tab) = self.query_tabs.get(tab_index) {
                self.editor_text = new_tab.content.clone();
            }
        }
    }

    fn save_current_tab(&mut self) -> Result<(), String> {
        if let Some(tab) = self.query_tabs.get_mut(self.active_tab_index) {
            tab.content = self.editor_text.clone();
            
            if tab.file_path.is_some() {
                // File already exists, save directly
                let file_path = tab.file_path.as_ref().unwrap().clone();
                std::fs::write(&file_path, &tab.content)
                    .map_err(|e| format!("Failed to save file: {}", e))?;
                
                tab.is_saved = true;
                tab.is_modified = false;
                
                Ok(())
            } else {
                // Show save dialog for new file
                self.save_filename = tab.title.replace("Untitled Query", "").trim().to_string();
                if self.save_filename.is_empty() {
                    self.save_filename = "new_query".to_string();
                }
                if !self.save_filename.ends_with(".sql") {
                    self.save_filename.push_str(".sql");
                }
                self.show_save_dialog = true;
                Ok(())
            }
        } else {
            Err("No active tab".to_string())
        }
    }

    fn save_current_tab_with_name(&mut self, filename: String) -> Result<(), String> {
        if let Some(tab) = self.query_tabs.get_mut(self.active_tab_index) {
            // Get query directory and ensure it exists
            let query_dir = Self::get_query_dir();
            std::fs::create_dir_all(&query_dir).map_err(|e| format!("Failed to create query directory: {}", e))?;
            
            let mut clean_filename = filename.trim().to_string();
            if !clean_filename.ends_with(".sql") {
                clean_filename.push_str(".sql");
            }
            
            let file_path = query_dir.join(&clean_filename);
            
            std::fs::write(&file_path, &tab.content)
                .map_err(|e| format!("Failed to save file: {}", e))?;
            
            tab.file_path = Some(file_path.to_string_lossy().to_string());
            tab.title = clean_filename;
            tab.is_saved = true;
            tab.is_modified = false;
            
            // Refresh queries tree to show the new file
            self.load_queries_from_directory();
            
            Ok(())
        } else {
            Err("No active tab".to_string())
        }
    }

    fn load_queries_from_directory(&mut self) {
        self.queries_tree.clear();
        
        let query_dir = Self::get_query_dir();
        if let Ok(entries) = std::fs::read_dir(&query_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_file() {
                        if let Some(file_name) = entry.file_name().to_str() {
                            if file_name.ends_with(".sql") {
                                let mut node = TreeNode::new(file_name.to_string(), NodeType::Query);
                                node.file_path = Some(entry.path().to_string_lossy().to_string());
                                self.queries_tree.push(node);
                            }
                        }
                    }
                }
            }
        }
        
        // Sort files alphabetically
        self.queries_tree.sort_by(|a, b| a.name.cmp(&b.name));
    }

    fn render_save_dialog(&mut self, ctx: &egui::Context) {
        if self.show_save_dialog {
            egui::Window::new("Save Query")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Enter filename:");
                    ui.text_edit_singleline(&mut self.save_filename);
                    
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() {
                            if !self.save_filename.is_empty() {
                                if let Err(err) = self.save_current_tab_with_name(self.save_filename.clone()) {
                                    println!("Failed to save: {}", err);
                                }
                                self.show_save_dialog = false;
                                self.save_filename.clear();
                            }
                        }
                        
                        if ui.button("Cancel").clicked() {
                            self.show_save_dialog = false;
                            self.save_filename.clear();
                        }
                    });
                });
        }
    }

    fn render_connection_selector(&mut self, ctx: &egui::Context) {
        if self.show_connection_selector {
            egui::Window::new("Select Database Connection")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("🔗 Choose a database connection to execute your query:");
                    ui.add_space(5.0);
                    
                    // Show the query that will be executed
                    ui.horizontal(|ui| {
                        ui.label("Query to execute:");
                        ui.monospace(format!("\"{}...\"", 
                            if self.pending_query.len() > 50 {
                                &self.pending_query[..50]
                            } else {
                                &self.pending_query
                            }
                        ));
                    });
                    ui.separator();
                    
                        egui::ScrollArea::vertical()
                        .max_height(200.0)
                        .show(ui, |ui| {
                            let mut selected_connection = None;
                            
                            for connection in &self.connections {
                                let connection_text = format!("{} ({})", 
                                    connection.name, 
                                    match connection.connection_type {
                                        DatabaseType::MySQL => "MySQL",
                                        DatabaseType::PostgreSQL => "PostgreSQL",
                                        DatabaseType::SQLite => "SQLite",
                                    }
                                );
                                
                                if ui.button(&connection_text).clicked() {
                                    if let Some(connection_id) = connection.id {
                                        selected_connection = Some(connection_id);
                                    }
                                }
                            }
                            
                            // Handle selection outside the loop to avoid borrowing issues
                            if let Some(connection_id) = selected_connection {
                                // Set active connection
                                self.current_connection_id = Some(connection_id);
                                
                                if self.auto_execute_after_connection {
                                    // Execute the query immediately
                                    let query = self.pending_query.clone();
                                    if let Some((headers, data)) = self.execute_query_with_connection(connection_id, query) {
                                        self.current_table_headers = headers;
                                        self.current_table_data = data;
                                        if self.current_table_data.is_empty() {
                                            self.current_table_name = "Query executed successfully (no results)".to_string();
                                        } else {
                                            self.current_table_name = format!("Query Results ({} rows)", self.current_table_data.len());
                                        }
                                    } else {
                                        self.current_table_name = "Query execution failed".to_string();
                                        self.current_table_headers.clear();
                                        self.current_table_data.clear();
                                    }
                                }
                                
                                self.show_connection_selector = false;
                                self.pending_query.clear();
                                self.auto_execute_after_connection = false;
                            }
                        });                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            self.show_connection_selector = false;
                            self.pending_query.clear();
                        }
                    });
                });
        }
    }

    fn render_error_dialog(&mut self, ctx: &egui::Context) {
        if self.show_error_message {
            egui::Window::new("Error")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label(&self.error_message);
                    ui.separator();
                    
                    ui.horizontal(|ui| {
                        if ui.button("OK").clicked() {
                            self.show_error_message = false;
                            self.error_message.clear();
                        }
                    });
                });
        }
    }

    fn open_query_file(&mut self, file_path: &str) -> Result<(), String> {
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| format!("Failed to read file: {}", e))?;
        
        let filename = std::path::Path::new(file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("Unknown")
            .to_string();
        
        // Check if file is already open
        for (index, tab) in self.query_tabs.iter().enumerate() {
            if tab.file_path.as_deref() == Some(file_path) {
                self.switch_to_tab(index);
                return Ok(());
            }
        }
        
        // Create new tab for the file
        let new_tab = QueryTab {
            title: filename,
            content: content.clone(),
            file_path: Some(file_path.to_string()),
            is_saved: true,
            is_modified: false,
        };
        
        self.query_tabs.push(new_tab);
        let new_index = self.query_tabs.len() - 1;
        self.active_tab_index = new_index;
        self.editor_text = content;
        
        Ok(())
    }

    fn get_active_tab_title(&self) -> String {
        if let Some(tab) = self.query_tabs.get(self.active_tab_index) {
            if tab.is_modified {
                format!("● {}", tab.title)
            } else {
                tab.title.clone()
            }
        } else {
            "No Tab".to_string()
        }
    }

    fn render_tree(&mut self, ui: &mut egui::Ui, nodes: &mut [TreeNode]) -> Vec<(String, String, String)> {
        let mut expansion_requests = Vec::new();
        let mut tables_to_expand = Vec::new();
        let mut context_menu_requests = Vec::new();
        let mut table_click_requests = Vec::new();
        let mut connection_click_requests = Vec::new();
        let mut query_files_to_open = Vec::new();
        
        for (index, node) in nodes.iter_mut().enumerate() {
            let (expansion_request, table_expansion, context_menu_request, table_click_request, connection_click_request, query_file_to_open) = Self::render_tree_node_with_table_expansion(ui, node, &mut self.editor_text, index, &self.refreshing_connections);
            if let Some(expansion_req) = expansion_request {
                expansion_requests.push(expansion_req);
            }
            if let Some((table_index, connection_id, table_name)) = table_expansion {
                tables_to_expand.push((table_index, connection_id, table_name));
            }
            if let Some(context_id) = context_menu_request {
                context_menu_requests.push(context_id);
            }
            if let Some((connection_id, table_name)) = table_click_request {
                table_click_requests.push((connection_id, table_name));
            }
            if let Some(connection_id) = connection_click_request {
                connection_click_requests.push(connection_id);
            }
            if let Some((filename, content, file_path)) = query_file_to_open {
                query_files_to_open.push((filename, content, file_path));
            }
        }
        
        // Handle connection clicks (set current active connection)
        for connection_id in connection_click_requests {
            self.current_connection_id = Some(connection_id);
            println!("Set active connection to ID: {}", connection_id);
        }
        
        // Handle expansions after rendering
        for expansion_req in expansion_requests {
            println!("=== EXPANSION REQUEST ===");
            println!("Node Type: {:?}, Connection ID: {}, Database: {:?}", 
                expansion_req.node_type, expansion_req.connection_id, expansion_req.database_name);
            
            match expansion_req.node_type {
                NodeType::Connection => {
                    // Find Connection node and load if not already loaded
                    for node in nodes.iter_mut() {
                        if node.node_type == NodeType::Connection && 
                           node.connection_id == Some(expansion_req.connection_id) && 
                           !node.is_loaded {
                            println!("Loading connection tables for Connection node: {}", node.name);
                            self.load_connection_tables(expansion_req.connection_id, node);
                            break;
                        }
                    }
                },
                NodeType::DatabasesFolder => {
                    // Handle DatabasesFolder expansion - load actual databases from server
                    println!("Loading databases for DatabasesFolder expansion");
                    for node in nodes.iter_mut() {
                        if node.node_type == NodeType::Connection && node.connection_id == Some(expansion_req.connection_id) {
                            // Find the DatabasesFolder within this connection
                            for child in &mut node.children {
                                if child.node_type == NodeType::DatabasesFolder && !child.is_loaded {
                                    println!("Found DatabasesFolder, loading databases...");
                                    self.load_databases_for_folder(expansion_req.connection_id, child);
                                    break;
                                }
                            }
                            break;
                        }
                    }
                },
                NodeType::TablesFolder | NodeType::ViewsFolder | NodeType::StoredProceduresFolder |
                NodeType::UserFunctionsFolder | NodeType::TriggersFolder | NodeType::EventsFolder => {
                    // Find the specific folder node and load if not already loaded
                    println!("Searching for folder node with type: {:?}, connection_id: {}, database: {:?}", 
                        expansion_req.node_type, expansion_req.connection_id, expansion_req.database_name);
                    
                    // We need to find the exact folder node in the tree
                    let connection_id = expansion_req.connection_id;
                    let folder_type = expansion_req.node_type.clone();
                    let database_name = expansion_req.database_name.clone();
                    
                    // Search for folder node using the existing find_folder_node_to_expand logic
                    for node in nodes.iter_mut() {
                        if node.node_type == NodeType::Connection && node.connection_id == Some(connection_id) {
                            if let Some((folder_node, _)) = Self::find_folder_node_to_expand(node, connection_id) {
                                if folder_node.node_type == folder_type && 
                                   folder_node.database_name == database_name &&
                                   !folder_node.is_loaded {
                                    println!("Found and loading folder content for {:?} node: {}", folder_type, folder_node.name);
                                    self.load_folder_content(connection_id, folder_node, folder_type);
                                }
                                break;
                            }
                        }
                    }
                },
                _ => {
                    println!("Unhandled node type: {:?}", expansion_req.node_type);
                }
            }
        }
        
        // Handle table column expansions
        for (table_index, connection_id, table_name) in tables_to_expand {
            self.load_table_columns_for_node(connection_id, &table_name, nodes, table_index);
        }
        
        // Handle table click requests
        for (connection_id, table_name) in table_click_requests {
            self.load_table_data(connection_id, &table_name);
        }
        
        // Handle query file open requests
        let results = query_files_to_open.clone();
        for (_filename, _content, file_path) in query_files_to_open {
            // Use existing open_query_file logic which checks for already open tabs
            if let Err(err) = self.open_query_file(&file_path) {
                println!("Failed to open query file: {}", err);
            }
        }
        
        // Handle context menu requests (deduplicate to avoid multiple calls)
        let mut processed_removals = std::collections::HashSet::new();
        let mut processed_refreshes = std::collections::HashSet::new();
        let mut needs_full_refresh = false;
        
        for context_id in context_menu_requests {
            if context_id > 1000 {
                // ID > 1000 means refresh connection (connection_id = context_id / 1000)
                let connection_id = context_id / 1000;
                if !processed_refreshes.contains(&connection_id) {
                    processed_refreshes.insert(connection_id);
                    println!("Processing refresh request for connection ID: {}", connection_id);
                    self.refresh_connection(connection_id);
                    needs_full_refresh = true;
                }
            } else if context_id > 0 {
                // Positive ID means edit connection
                self.start_edit_connection(context_id);
            } else {
                // Negative ID means remove connection
                let connection_id = -context_id;
                if !processed_removals.contains(&connection_id) {
                    processed_removals.insert(connection_id);
                    println!("🚀 Processing remove request for connection ID: {} (context_id was: {})", connection_id, context_id);
                    self.remove_connection(connection_id);
                    needs_full_refresh = true;
                }
            }
        }
        
        // Force complete UI refresh after any removal
        if needs_full_refresh {
            // Completely clear and rebuild the tree
            self.items_tree.clear();
            println!("Force refresh: clearing items_tree and rebuilding...");
            self.refresh_connections_tree();
            self.needs_refresh = true; // Set flag for next update cycle
            ui.ctx().request_repaint();
            println!("Forced complete UI refresh - items_tree now has {} nodes", self.items_tree.len());
        }
        
        // Return query files that were clicked
        results
    }

    fn render_tree_node_with_table_expansion(ui: &mut egui::Ui, node: &mut TreeNode, editor_text: &mut String, node_index: usize, refreshing_connections: &std::collections::HashSet<i64>) -> (Option<ExpansionRequest>, Option<(usize, i64, String)>, Option<i64>, Option<(i64, String)>, Option<i64>, Option<(String, String, String)>) {
        let has_children = !node.children.is_empty();
        let mut expansion_request = None;
        let mut table_expansion = None;
        let mut context_menu_request = None;
        let mut table_click_request = None;
        let mut connection_click_request = None;
        let mut query_file_to_open = None;
        
        if has_children || node.node_type == NodeType::Connection || node.node_type == NodeType::Table || 
           node.node_type == NodeType::DatabasesFolder || node.node_type == NodeType::TablesFolder ||
           node.node_type == NodeType::ViewsFolder || node.node_type == NodeType::StoredProceduresFolder ||
           node.node_type == NodeType::UserFunctionsFolder || node.node_type == NodeType::TriggersFolder ||
           node.node_type == NodeType::EventsFolder || node.node_type == NodeType::DBAViewsFolder ||
           node.node_type == NodeType::UsersFolder || node.node_type == NodeType::PrivilegesFolder ||
           node.node_type == NodeType::ProcessesFolder || node.node_type == NodeType::StatusFolder ||
           node.node_type == NodeType::Database {
            // Use more unique ID including connection_id for connections
            let unique_id = match node.node_type {
                NodeType::Connection => format!("conn_{}_{}", node_index, node.connection_id.unwrap_or(0)),
                _ => format!("node_{}_{:?}", node_index, node.node_type),
            };
            let id = egui::Id::new(&unique_id);
            ui.horizontal(|ui| {
                let expand_icon = if node.is_expanded { "▼" } else { "▶" };
                if ui.button(expand_icon).clicked() {
                    node.is_expanded = !node.is_expanded;
                    println!("=== NODE EXPANSION CLICKED ===");
                    println!("Node: {} (type: {:?})", node.name, node.node_type);
                    println!("Expanded: {}, Loaded: {}", node.is_expanded, node.is_loaded);
                    println!("Connection ID: {:?}, Database: {:?}", node.connection_id, node.database_name);
                    
                    // If this is a connection node and not loaded, request expansion
                    if node.node_type == NodeType::Connection && !node.is_loaded && node.is_expanded {
                        if let Some(conn_id) = node.connection_id {
                            expansion_request = Some(ExpansionRequest {
                                node_type: NodeType::Connection,
                                connection_id: conn_id,
                                database_name: None,
                            });
                            // Also set as active connection when expanding
                            connection_click_request = Some(conn_id);
                        }
                    }
                    
                    // If this is a table node and not loaded, request table column expansion
                    if node.node_type == NodeType::Table && !node.is_loaded && node.is_expanded {
                        // We need to find the connection ID from parent
                        // For now, we'll mark it for expansion and handle it in the calling code
                        if let Some(conn_id) = node.connection_id {
                            table_expansion = Some((node_index, conn_id, node.name.clone()));
                        }
                    }
                    
                    // If this is a folder node and not loaded, request folder content expansion
                    if (node.node_type == NodeType::DatabasesFolder ||
                        node.node_type == NodeType::TablesFolder || 
                        node.node_type == NodeType::ViewsFolder ||
                        node.node_type == NodeType::StoredProceduresFolder ||
                        node.node_type == NodeType::UserFunctionsFolder ||
                        node.node_type == NodeType::TriggersFolder ||
                        node.node_type == NodeType::EventsFolder) && 
                       !node.is_loaded && node.is_expanded {
                        println!("FOLDER EXPANSION: Requesting expansion for folder");
                        if let Some(conn_id) = node.connection_id {
                            expansion_request = Some(ExpansionRequest {
                                node_type: node.node_type.clone(),
                                connection_id: conn_id,
                                database_name: node.database_name.clone(),
                            });
                        }
                    }
                }
                
                let icon = match node.node_type {
                    NodeType::Database => "🗄",
                    NodeType::Table => "📋",
                    NodeType::Column => "📄",
                    NodeType::Query => "🔍",
                    NodeType::HistoryItem => "📜",
                    NodeType::Connection => "", // Icon already included in name
                    NodeType::DatabasesFolder => "📁",
                    NodeType::TablesFolder => "📋",
                    NodeType::ViewsFolder => "👁",
                    NodeType::StoredProceduresFolder => "⚙️",
                    NodeType::UserFunctionsFolder => "🔧",
                    NodeType::TriggersFolder => "⚡",
                    NodeType::EventsFolder => "📅",
                    NodeType::DBAViewsFolder => "👨‍💼",
                    NodeType::UsersFolder => "👥",
                    NodeType::PrivilegesFolder => "🔒",
                    NodeType::ProcessesFolder => "⚡",
                    NodeType::StatusFolder => "📊",
                    NodeType::View => "👁",
                    NodeType::StoredProcedure => "⚙️",
                    NodeType::UserFunction => "🔧",
                    NodeType::Trigger => "⚡",
                    NodeType::Event => "📅",
                };
                
                let label_text = if icon.is_empty() { 
                    // For connection nodes, add loading indicator if refreshing
                    if node.node_type == NodeType::Connection {
                        if let Some(conn_id) = node.connection_id {
                            if refreshing_connections.contains(&conn_id) {
                                format!("{} 🔄", node.name) // Add refresh spinner
                            } else {
                                node.name.clone()
                            }
                        } else {
                            node.name.clone()
                        }
                    } else {
                        node.name.clone()
                    }
                } else { 
                    format!("{} {}", icon, node.name) 
                };
                let response = ui.label(label_text);
                
                // Handle clicks on connection labels to set active connection
                if node.node_type == NodeType::Connection && response.clicked() {
                    if let Some(conn_id) = node.connection_id {
                        connection_click_request = Some(conn_id);
                        println!("Connection {} clicked, setting as active connection", node.name);
                    }
                }
                
                // Handle clicks on table labels to load table data
                if node.node_type == NodeType::Table && response.clicked() {
                    // Generate a SELECT query for the table
                    *editor_text = format!("SELECT * FROM {} LIMIT 10;", node.name);
                    // Also trigger table data loading
                    if let Some(conn_id) = node.connection_id {
                        table_click_request = Some((conn_id, node.name.clone()));
                        println!("Table {} clicked, connection_id: {}", node.name, conn_id);
                    } else {
                        println!("Table {} clicked, but no connection_id found", node.name);
                    }
                }
                
                // Add context menu for connection nodes
                if node.node_type == NodeType::Connection {
                    response.context_menu(|ui| {
                        if ui.button("Edit Connection").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(conn_id);
                            }
                            ui.close_menu();
                        }
                        if ui.button("Refresh").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(conn_id * 1000); // Use multiplication to indicate refresh
                            }
                            ui.close_menu();
                        }
                        if ui.button("Remove Connection").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                println!("🗑️ Context menu remove clicked for connection: {} (ID: {})", node.name, conn_id);
                                context_menu_request = Some(-conn_id); // Negative ID indicates removal
                            }
                            ui.close_menu();
                        }
                    });
                }
            });

            if node.is_expanded {
                ui.indent(id, |ui| {
                    for (child_index, child) in node.children.iter_mut().enumerate() {
                        let (child_expansion_request, child_table_expansion, _child_context, child_table_click, _child_connection_click, _child_query_file) = Self::render_tree_node_with_table_expansion(ui, child, editor_text, child_index, refreshing_connections);
                        
                        // Handle child expansion requests - propagate to parent
                        if let Some(child_expansion) = child_expansion_request {
                            expansion_request = Some(child_expansion);
                        }
                        
                        // Handle child table expansions with the parent connection ID
                        if let Some((_, _, table_name)) = child_table_expansion {
                            if let Some(conn_id) = node.connection_id {
                                table_expansion = Some((child_index, conn_id, table_name));
                            }
                        }
                        
                        // Handle child table clicks - propagate to parent
                        if let Some((conn_id, table_name)) = child_table_click {
                            table_click_request = Some((conn_id, table_name));
                        }
                    }
                });
            }
        } else {
            ui.horizontal(|ui| {
                ui.add_space(16.0); // Indent for leaf nodes
                
                let icon = match node.node_type {
                    NodeType::Database => "🗄",
                    NodeType::Table => "📋",
                    NodeType::Column => "📄",
                    NodeType::Query => "🔍",
                    NodeType::HistoryItem => "📜",
                    NodeType::Connection => "🔗",
                    NodeType::DatabasesFolder => "📁",
                    NodeType::TablesFolder => "📋",
                    NodeType::ViewsFolder => "👁",
                    NodeType::StoredProceduresFolder => "⚙️",
                    NodeType::UserFunctionsFolder => "🔧",
                    NodeType::TriggersFolder => "⚡",
                    NodeType::EventsFolder => "📅",
                    NodeType::DBAViewsFolder => "👨‍💼",
                    NodeType::UsersFolder => "👥",
                    NodeType::PrivilegesFolder => "🔒",
                    NodeType::ProcessesFolder => "⚡",
                    NodeType::StatusFolder => "📊",
                    NodeType::View => "👁",
                    NodeType::StoredProcedure => "⚙️",
                    NodeType::UserFunction => "🔧",
                    NodeType::Trigger => "⚡",
                    NodeType::Event => "📅",
                };
                
                if ui.button(format!("{} {}", icon, node.name)).clicked() {
                    // Handle node selection
                    match node.node_type {
                        NodeType::Table => {
                            // Generate a SELECT query for the table
                            *editor_text = format!("SELECT * FROM {} LIMIT 10;", node.name);
                            // Also trigger table data loading
                            if let Some(conn_id) = node.connection_id {
                                table_click_request = Some((conn_id, node.name.clone()));
                            }
                        },
                        NodeType::Query => {
                            // Load query file content
                            if let Some(file_path) = &node.file_path {
                                if let Ok(content) = std::fs::read_to_string(file_path) {
                                    *editor_text = content.clone();
                                    query_file_to_open = Some((node.name.clone(), content, file_path.clone()));
                                } else {
                                    *editor_text = format!("-- Failed to load query file: {}", node.name);
                                }
                            } else {
                                *editor_text = format!("-- {}\nSELECT * FROM table_name;", node.name);
                            }
                        },
                        NodeType::HistoryItem => {
                            // Store the display name for processing later
                            *editor_text = node.name.clone();
                        },
                        _ => {}
                    }
                }
            });
        }
        
        (expansion_request, table_expansion, context_menu_request, table_click_request, connection_click_request, query_file_to_open)
    }

    fn render_connection_dialog(&mut self, ctx: &egui::Context, is_edit_mode: bool) {
        let should_show = if is_edit_mode { self.show_edit_connection } else { self.show_add_connection };
        
        if !should_show {
            return;
        }
        
        let mut open = true;
        let title = if is_edit_mode { "Edit Connection" } else { "Add New Connection" };
        
        // Clone the connection data to work with
        let mut connection_data = if is_edit_mode {
            self.edit_connection.clone()
        } else {
            self.new_connection.clone()
        };
        
        egui::Window::new(title)
            .resizable(false)
            .default_width(400.0)
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    egui::Grid::new("connection_form")
                        .num_columns(2)
                        .spacing([10.0, 8.0])
                        .show(ui, |ui| {
                            ui.label("Connection Name:");
                            ui.text_edit_singleline(&mut connection_data.name);
                            ui.end_row();

                            ui.label("Database Type:");
                            egui::ComboBox::from_label("")
                                .selected_text(match connection_data.connection_type {
                                    DatabaseType::MySQL => "MySQL",
                                    DatabaseType::PostgreSQL => "PostgreSQL",
                                    DatabaseType::SQLite => "SQLite",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut connection_data.connection_type, DatabaseType::MySQL, "MySQL");
                                    ui.selectable_value(&mut connection_data.connection_type, DatabaseType::PostgreSQL, "PostgreSQL");
                                    ui.selectable_value(&mut connection_data.connection_type, DatabaseType::SQLite, "SQLite");
                                });
                            ui.end_row();

                            ui.label("Host:");
                            ui.text_edit_singleline(&mut connection_data.host);
                            ui.end_row();

                            ui.label("Port:");
                            ui.text_edit_singleline(&mut connection_data.port);
                            ui.end_row();

                            ui.label("Username:");
                            ui.text_edit_singleline(&mut connection_data.username);
                            ui.end_row();

                            ui.label("Password:");
                            ui.add(egui::TextEdit::singleline(&mut connection_data.password).password(true));
                            ui.end_row();

                            ui.label("Database:");
                            ui.text_edit_singleline(&mut connection_data.database);
                            ui.end_row();
                        });

                    ui.separator();

                    ui.horizontal(|ui| {
                        let save_button_text = if is_edit_mode { "Update" } else { "Save" };
                        if ui.button(save_button_text).clicked() && !connection_data.name.is_empty() {
                            if is_edit_mode {
                                // Update existing connection
                                if let Some(id) = connection_data.id {
                                    if self.update_connection_in_database(&connection_data) {
                                        self.load_connections();
                                        self.refresh_connections_tree();
                                    } else {
                                        // Fallback to in-memory update
                                        if let Some(existing) = self.connections.iter_mut().find(|c| c.id == Some(id)) {
                                            *existing = connection_data.clone();
                                            self.refresh_connections_tree();
                                        }
                                    }
                                }
                                self.show_edit_connection = false;
                            } else {
                                // Add new connection
                                let mut connection_to_add = connection_data.clone();
                                
                                // Try to save to database first
                                if self.save_connection_to_database(&connection_to_add) {
                                    // If database save successful, reload from database to get ID
                                    self.load_connections();
                                    self.refresh_connections_tree();
                                } else {
                                    // Fallback to in-memory storage
                                    let new_id = self.connections.iter()
                                        .filter_map(|c| c.id)
                                        .max()
                                        .unwrap_or(0) + 1;
                                    connection_to_add.id = Some(new_id);
                                    self.connections.push(connection_to_add);
                                    self.refresh_connections_tree();
                                }
                                
                                self.new_connection = ConnectionConfig::default();
                                self.test_connection_status = None;
                                self.test_connection_in_progress = false;
                                self.show_add_connection = false;
                            }
                        }

                        // Push Test Connection button ke kanan
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            // Test Connection button (untuk kedua mode add dan edit)
                            if self.test_connection_in_progress {
                                ui.spinner();
                                ui.label("Testing connection...");
                            } else if ui.button("Test Connection").clicked() {
                                self.test_connection_in_progress = true;
                                self.test_connection_status = None;
                                
                                // Test connection based on database type
                                let result = self.test_database_connection(&connection_data);
                                self.test_connection_in_progress = false;
                                self.test_connection_status = Some(result);
                            }
                        });
                    });
                    
                    // Display test connection status (untuk kedua mode add dan edit)
                    if let Some((success, message)) = &self.test_connection_status {
                        ui.separator();
                        if *success {
                            ui.horizontal(|ui| {
                                ui.colored_label(egui::Color32::GREEN, "✓");
                                ui.colored_label(egui::Color32::GREEN, message);
                            });
                        } else {
                            ui.horizontal(|ui| {
                                ui.colored_label(egui::Color32::RED, "✗");
                                ui.colored_label(egui::Color32::RED, message);
                            });
                        }
                    }
                });
            });
        
        // Update the original data with any changes made in the dialog
        if is_edit_mode {
            self.edit_connection = connection_data;
        } else {
            self.new_connection = connection_data;
        }
        
        // Handle window close via X button
        if !open {
            if is_edit_mode {
                self.show_edit_connection = false;
            } else {
                self.new_connection = ConnectionConfig::default();
                self.test_connection_status = None;
                self.test_connection_in_progress = false;
                self.show_add_connection = false;
            }
        }
    }

    fn render_add_connection_dialog(&mut self, ctx: &egui::Context) {
        self.render_connection_dialog(ctx, false);
    }

    fn render_edit_connection_dialog(&mut self, ctx: &egui::Context) {
        self.render_connection_dialog(ctx, true);
    }

    fn load_connections(&mut self) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let connections_result = rt.block_on(async {
                sqlx::query_as::<_, (i64, String, String, String, String, String, String, String)>(
                    "SELECT id, name, host, port, username, password, database_name, connection_type FROM connections"
                )
                .fetch_all(pool_clone.as_ref())
                .await
            });
            
            if let Ok(rows) = connections_result {

                println!("Loaded {} connections from database", rows.len());

                self.connections = rows.into_iter().map(|(id, name, host, port, username, password, database_name, connection_type)| {
                    ConnectionConfig {
                        id: Some(id),
                        name,
                        host,
                        port,
                        username,
                        password,
                        database: database_name,
                        connection_type: match connection_type.as_str() {
                            "MySQL" => DatabaseType::MySQL,
                            "PostgreSQL" => DatabaseType::PostgreSQL,
                            _ => DatabaseType::SQLite,
                        },
                    }
                }).collect();
            }
        }
        
        // Refresh the tree after loading connections
        self.refresh_connections_tree();
    }


    fn save_connection_to_database(&self, connection: &ConnectionConfig) -> bool {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let connection = connection.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                sqlx::query(
                    "INSERT INTO connections (name, host, port, username, password, database_name, connection_type) VALUES (?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(connection.name)
                .bind(connection.host)
                .bind(connection.port)
                .bind(connection.username)
                .bind(connection.password)
                .bind(connection.database)
                .bind(format!("{:?}", connection.connection_type))
                .execute(pool_clone.as_ref())
                .await
            });
            
            result.is_ok()
        } else {
            false
        }
    }

    fn start_edit_connection(&mut self, connection_id: i64) {
        // Find the connection to edit
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            self.edit_connection = connection.clone();
            // Reset test connection status saat buka edit dialog
            self.test_connection_status = None;
            self.test_connection_in_progress = false;
            self.show_edit_connection = true;
        }
    }

    fn test_database_connection(&self, connection: &ConnectionConfig) -> (bool, String) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        let result = rt.block_on(async {
            match connection.connection_type {
                DatabaseType::MySQL => {
                    let encoded_username = Self::url_encode(&connection.username);
                    let encoded_password = Self::url_encode(&connection.password);
                    let connection_string = format!(
                        "mysql://{}:{}@{}:{}/{}",
                        encoded_username, encoded_password, connection.host, connection.port, connection.database
                    );
                    
                    match MySqlPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            // Test with a simple query
                            match sqlx::query("SELECT 1").execute(&pool).await {
                                Ok(_) => (true, "MySQL connection successful!".to_string()),
                                Err(e) => (false, format!("MySQL query failed: {}", e)),
                            }
                        },
                        Err(e) => (false, format!("MySQL connection failed: {}", e)),
                    }
                },
                DatabaseType::PostgreSQL => {
                    let connection_string = format!(
                        "postgresql://{}:{}@{}:{}/{}",
                        connection.username, connection.password, connection.host, connection.port, connection.database
                    );
                    
                    match PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            // Test with a simple query
                            match sqlx::query("SELECT 1").execute(&pool).await {
                                Ok(_) => (true, "PostgreSQL connection successful!".to_string()),
                                Err(e) => (false, format!("PostgreSQL query failed: {}", e)),
                            }
                        },
                        Err(e) => (false, format!("PostgreSQL connection failed: {}", e)),
                    }
                },
                DatabaseType::SQLite => {
                    let connection_string = format!("sqlite:{}", connection.host);
                    
                    match SqlitePoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            // Test with a simple query
                            match sqlx::query("SELECT 1").execute(&pool).await {
                                Ok(_) => (true, "SQLite connection successful!".to_string()),
                                Err(e) => (false, format!("SQLite query failed: {}", e)),
                            }
                        },
                        Err(e) => (false, format!("SQLite connection failed: {}", e)),
                    }
                }
            }
        });
        
        result
    }

    fn update_connection_in_database(&self, connection: &ConnectionConfig) -> bool {
        if let Some(ref pool) = self.db_pool {
            if let Some(id) = connection.id {
                let pool_clone = pool.clone();
                let connection = connection.clone();
                let rt = tokio::runtime::Runtime::new().unwrap();
                
                let result = rt.block_on(async {
                    sqlx::query(
                        "UPDATE connections SET name = ?, host = ?, port = ?, username = ?, password = ?, database_name = ?, connection_type = ? WHERE id = ?"
                    )
                    .bind(connection.name)
                    .bind(connection.host)
                    .bind(connection.port)
                    .bind(connection.username)
                    .bind(connection.password)
                    .bind(connection.database)
                    .bind(format!("{:?}", connection.connection_type))
                    .bind(id)
                    .execute(pool_clone.as_ref())
                    .await
                });
                
                result.is_ok()
            } else {
                false
            }
        } else {
            false
        }
    }



    fn remove_connection(&mut self, connection_id: i64) {
        println!("=== REMOVE CONNECTION DEBUG START ===");
        println!("Removing connection with ID {}", connection_id);
        
        // Debug: List all connections before removal
        println!("Before removal - All connections:");
        for (i, conn) in self.connections.iter().enumerate() {
            println!("  [{}] {} (ID: {:?})", i, conn.name, conn.id);
        }
        
        // Remove from database first with explicit transaction
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result: Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> = rt.block_on(async {
                // Begin transaction
                let mut tx = pool_clone.begin().await?;
                
                // Delete cache data first (foreign key constraints will handle this automatically due to CASCADE)
                let _ = sqlx::query("DELETE FROM database_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(&mut *tx)
                    .await;
                
                let _ = sqlx::query("DELETE FROM table_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(&mut *tx)
                    .await;
                
                let _ = sqlx::query("DELETE FROM column_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(&mut *tx)
                    .await;
                
                // Delete the connection
                let delete_result = sqlx::query("DELETE FROM connections WHERE id = ?")
                    .bind(connection_id)
                    .execute(&mut *tx)
                    .await?;
                
                // Commit transaction
                tx.commit().await?;
                
                Ok(delete_result)
            });
            
            match result {
                Ok(delete_result) => {
                    println!("Successfully deleted from database, affected rows: {}", delete_result.rows_affected());
                    
                    // Only proceed if we actually deleted something
                    if delete_result.rows_affected() == 0 {
                        println!("Warning: No rows were deleted from database!");
                        return;
                    }
                },
                Err(e) => {
                    println!("Failed to delete from database: {}", e);
                    return; // Don't proceed if database deletion failed
                }
            }
        }
        
        // Remove from memory
        let before_count = self.connections.len();
        println!("Before memory removal: {} connections", before_count);
        
        // Debug: Find which connection will be removed
        for conn in &self.connections {
            if conn.id == Some(connection_id) {
                println!("Found connection to remove: {} (ID: {:?})", conn.name, conn.id);
            }
        }
        
        self.connections.retain(|c| c.id != Some(connection_id));
        let after_count = self.connections.len();
        
        println!("Connections before: {}, after: {}", before_count, after_count);
        
        // Debug: List all connections after removal
        println!("After removal - Remaining connections:");
        for (i, conn) in self.connections.iter().enumerate() {
            println!("  [{}] {} (ID: {:?})", i, conn.name, conn.id);
        }
        
        // Verify connection was actually removed
        if before_count == after_count {
            println!("WARNING: Connection was not removed from memory!");
            println!("This suggests the connection ID {} was not found in memory", connection_id);
        } else {
            println!("SUCCESS: Connection was removed from memory");
        }
        
        // Remove from connection pool cache
        self.connection_pools.remove(&connection_id);
        
        // Clear items tree first
        self.items_tree.clear();
        println!("Cleared items_tree, now rebuilding...");
        
        // Refresh the UI immediately
        self.refresh_connections_tree();
        
        // Set flag to force refresh on next update
        self.needs_refresh = true;
        
        println!("Memory cleanup completed for connection ID: {}", connection_id);
        println!("=== REMOVE CONNECTION DEBUG END ===");
    }

    // Cache functions for database structure
    fn save_databases_to_cache(&self, connection_id: i64, databases: &[String]) {
        println!("Saving {} databases to cache for connection_id: {}", databases.len(), connection_id);
        for db_name in databases {
            println!("  - {}", db_name);
        }
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let databases_clone = databases.to_vec();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let _ = rt.block_on(async {
                // Clear existing cache for this connection
                let _ = sqlx::query("DELETE FROM database_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(pool_clone.as_ref())
                    .await;
                
                // Insert new database names
                for db_name in databases_clone {
                    let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                        .bind(connection_id)
                        .bind(db_name)
                        .execute(pool_clone.as_ref())
                        .await;
                }
            });
        }
    }

    fn get_databases_from_cache(&self, connection_id: i64) -> Option<Vec<String>> {
        println!("Getting databases from cache for connection_id: {}", connection_id);
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                sqlx::query_as::<_, (String,)>("SELECT database_name FROM database_cache WHERE connection_id = ? ORDER BY database_name")
                    .bind(connection_id)
                    .fetch_all(pool_clone.as_ref())
                    .await
            });
            
            match result {
                Ok(rows) => {
                    let databases: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
                    println!("Found {} databases in cache: {:?}", databases.len(), databases);
                    Some(databases)
                },
                Err(e) => {
                    println!("Error reading from cache: {}", e);
                    None
                }
            }
        } else {
            println!("No database pool available for cache lookup");
            None
        }
    }

    fn save_tables_to_cache(&self, connection_id: i64, database_name: &str, tables: &[(String, String)]) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let tables_clone = tables.to_vec();
            let database_name = database_name.to_string();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let _ = rt.block_on(async {
                // Clear existing cache for this database
                let _ = sqlx::query("DELETE FROM table_cache WHERE connection_id = ? AND database_name = ?")
                    .bind(connection_id)
                    .bind(&database_name)
                    .execute(pool_clone.as_ref())
                    .await;
                
                // Insert new table names with types
                for (table_name, table_type) in tables_clone {
                    let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                        .bind(connection_id)
                        .bind(&database_name)
                        .bind(table_name)
                        .bind(table_type)
                        .execute(pool_clone.as_ref())
                        .await;
                }
            });
        }
    }

    fn get_tables_from_cache(&self, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                sqlx::query_as::<_, (String,)>("SELECT table_name FROM table_cache WHERE connection_id = ? AND database_name = ? AND table_type = ? ORDER BY table_name")
                    .bind(connection_id)
                    .bind(database_name)
                    .bind(table_type)
                    .fetch_all(pool_clone.as_ref())
                    .await
            });
            
            match result {
                Ok(rows) => Some(rows.into_iter().map(|(name,)| name).collect()),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn save_columns_to_cache(&self, connection_id: i64, database_name: &str, table_name: &str, columns: &[(String, String)]) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let columns_clone = columns.to_vec();
            let database_name = database_name.to_string();
            let table_name = table_name.to_string();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let _ = rt.block_on(async {
                // Clear existing cache for this table
                let _ = sqlx::query("DELETE FROM column_cache WHERE connection_id = ? AND database_name = ? AND table_name = ?")
                    .bind(connection_id)
                    .bind(&database_name)
                    .bind(&table_name)
                    .execute(pool_clone.as_ref())
                    .await;
                
                // Insert new column names with types
                for (i, (column_name, data_type)) in columns_clone.iter().enumerate() {
                    let _ = sqlx::query("INSERT OR REPLACE INTO column_cache (connection_id, database_name, table_name, column_name, data_type, ordinal_position) VALUES (?, ?, ?, ?, ?, ?)")
                        .bind(connection_id)
                        .bind(&database_name)
                        .bind(&table_name)
                        .bind(column_name)
                        .bind(data_type)
                        .bind(i as i64)
                        .execute(pool_clone.as_ref())
                        .await;
                }
            });
        }
    }

    fn get_columns_from_cache(&self, connection_id: i64, database_name: &str, table_name: &str) -> Option<Vec<(String, String)>> {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                sqlx::query_as::<_, (String, String)>("SELECT column_name, data_type FROM column_cache WHERE connection_id = ? AND database_name = ? AND table_name = ? ORDER BY ordinal_position")
                    .bind(connection_id)
                    .bind(database_name)
                    .bind(table_name)
                    .fetch_all(pool_clone.as_ref())
                    .await
            });
            
            match result {
                Ok(rows) => Some(rows),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn refresh_connection(&mut self, connection_id: i64) {
        println!("Refreshing connection cache for ID: {}", connection_id);
        
        // Clear all cached data for this connection
        self.clear_connection_cache(connection_id);
        
        // Remove from connection pool cache to force reconnection
        self.connection_pools.remove(&connection_id);
        
        // Mark as refreshing
        self.refreshing_connections.insert(connection_id);
        
        // Find the connection node in the tree and reset its loaded state
        for node in &mut self.items_tree {
            if node.node_type == NodeType::Connection && node.connection_id == Some(connection_id) {
                // Reset the connection node
                node.is_loaded = false;
                node.is_expanded = false;
                node.children.clear();
                println!("Reset connection node: {}", node.name);
                break;
            }
        }
        
        // Send background task instead of blocking refresh
        if let Some(sender) = &self.background_sender {
            if let Err(e) = sender.send(BackgroundTask::RefreshConnection { connection_id }) {
                println!("Failed to send background refresh task: {}", e);
                // Fallback to synchronous refresh if background thread is not available
                self.refreshing_connections.remove(&connection_id);
                self.fetch_and_cache_connection_data(connection_id);
            } else {
                println!("Background refresh task sent for connection {}", connection_id);
            }
        } else {
            // Fallback to synchronous refresh if background system is not initialized
            self.refreshing_connections.remove(&connection_id);
            self.fetch_and_cache_connection_data(connection_id);
        }
    }

    fn fetch_and_cache_connection_data(&mut self, connection_id: i64) {
        println!("Fetching fresh data from server for connection_id: {}", connection_id);
        
        // Clone connection info to avoid borrowing issues
        let connection = if let Some(conn) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            conn.clone()
        } else {
            println!("Connection not found for ID: {}", connection_id);
            return;
        };
        
        // Fetch databases from server
        let databases_result = self.fetch_databases_from_connection(connection_id);
        
        if let Some(databases) = databases_result {
            println!("Fetched {} databases from server", databases.len());
            
            // Save databases to cache
            self.save_databases_to_cache(connection_id, &databases);
            
            // For each database, fetch tables and columns
            for database_name in &databases {
                println!("Fetching tables for database: {}", database_name);
                
                // Fetch different types of tables based on database type
                let table_types = match connection.connection_type {
                    DatabaseType::MySQL => vec!["table", "view", "procedure", "function", "trigger", "event"],
                    DatabaseType::PostgreSQL => vec!["table", "view"], // Add PostgreSQL support later
                    DatabaseType::SQLite => vec!["table", "view"],
                };
                
                let mut all_tables = Vec::new();
                
                for table_type in table_types {
                    let tables_result = match connection.connection_type {
                        DatabaseType::MySQL => {
                            self.fetch_tables_from_mysql_connection(connection_id, database_name, table_type)
                        },
                        DatabaseType::SQLite => {
                            self.fetch_tables_from_sqlite_connection(connection_id, table_type)
                        },
                        DatabaseType::PostgreSQL => {
                            // TODO: Add PostgreSQL support
                            None
                        },
                    };
                    
                    if let Some(tables) = tables_result {
                        for table_name in tables {
                            all_tables.push((table_name, table_type.to_string()));
                        }
                    }
                }
                
                if !all_tables.is_empty() {
                    println!("Fetched {} total items from database {}", all_tables.len(), database_name);
                    
                    // Save tables to cache
                    self.save_tables_to_cache(connection_id, database_name, &all_tables);
                    
                    // For each table, fetch columns
                    for (table_name, table_type) in &all_tables {
                        if table_type == "table" { // Only fetch columns for actual tables, not views/procedures
                            println!("Fetching columns for table: {}.{}", database_name, table_name);
                            
                            let columns_result = self.fetch_columns_from_database(connection_id, database_name, table_name, &connection);
                            
                            if let Some(columns) = columns_result {
                                println!("Fetched {} columns from table {}.{}", columns.len(), database_name, table_name);
                                
                                // Save columns to cache
                                self.save_columns_to_cache(connection_id, database_name, table_name, &columns);
                            }
                        }
                    }
                }
            }
            
            println!("Successfully cached all data for connection_id: {}", connection_id);
        } else {
            println!("Failed to fetch databases from server for connection_id: {}", connection_id);
        }
    }

    // Function to clear cache for a connection (useful for refresh)
    fn clear_connection_cache(&self, connection_id: i64) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let _ = rt.block_on(async {
                // Clear all cache tables for this connection
                let _ = sqlx::query("DELETE FROM database_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(pool_clone.as_ref())
                    .await;
                
                let _ = sqlx::query("DELETE FROM table_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(pool_clone.as_ref())
                    .await;
                
                let _ = sqlx::query("DELETE FROM column_cache WHERE connection_id = ?")
                    .bind(connection_id)
                    .execute(pool_clone.as_ref())
                    .await;
            });
            
            println!("Cleared cache for connection ID: {}", connection_id);
        }
    }

    // Helper function untuk mendapatkan atau membuat connection pool
    async fn get_or_create_connection_pool(&mut self, connection_id: i64) -> Option<DatabasePool> {
        // ALWAYS recreate pool to ensure we use the new Arc<Pool> architecture
        // Remove any existing cached pool first
        self.connection_pools.remove(&connection_id);
        println!("Force clearing cached pool for connection_id: {}", connection_id);

        // Jika belum ada, buat connection pool baru
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            match connection.connection_type {
                DatabaseType::MySQL => {
                    let encoded_username = Self::url_encode(&connection.username);
                    let encoded_password = Self::url_encode(&connection.password);
                    let connection_string = format!(
                        "mysql://{}:{}@{}:{}/{}",
                        encoded_username, encoded_password, connection.host, connection.port, connection.database
                    );
                    
                    println!("Creating new MySQL connection pool for: {}", connection.name);
                    
                    // Configure MySQL pool with proper settings
                    let pool_result = MySqlPoolOptions::new()
                        .max_connections(15)  // Increase max connections further
                        .min_connections(3)   // Keep more minimum connections alive
                        .acquire_timeout(std::time::Duration::from_secs(45))  // Even longer timeout
                        .idle_timeout(std::time::Duration::from_secs(1800))   // 30 minute idle timeout
                        .max_lifetime(std::time::Duration::from_secs(7200))   // 2 hour max lifetime
                        .test_before_acquire(true)  // Test connections before use
                        .after_connect(|conn, _meta| {
                            Box::pin(async move {
                                // Set connection timeout and other MySQL specific settings
                                sqlx::query("SET SESSION wait_timeout = 28800, interactive_timeout = 28800").execute(conn).await?;
                                Ok(())
                            })
                        })
                        .connect(&connection_string)
                        .await;
                    
                    match pool_result {
                        Ok(pool) => {
                            let database_pool = DatabasePool::MySQL(Arc::new(pool));
                            self.connection_pools.insert(connection_id, database_pool.clone());
                            Some(database_pool)
                        },
                        Err(e) => {
                            println!("Failed to create MySQL pool: {}", e);
                            None
                        }
                    }
                },
                DatabaseType::PostgreSQL => {
                    let connection_string = format!(
                        "postgresql://{}:{}@{}:{}/{}",
                        connection.username, connection.password, connection.host, connection.port, connection.database
                    );
                    
                    println!("Creating new PostgreSQL connection pool for: {}", connection.name);
                    
                    // Configure PostgreSQL pool with proper settings
                    let pool_result = PgPoolOptions::new()
                        .max_connections(10)
                        .min_connections(2)
                        .acquire_timeout(std::time::Duration::from_secs(30))
                        .idle_timeout(std::time::Duration::from_secs(1200))
                        .max_lifetime(std::time::Duration::from_secs(3600))
                        .test_before_acquire(true)
                        .connect(&connection_string)
                        .await;
                    
                    match pool_result {
                        Ok(pool) => {
                            let database_pool = DatabasePool::PostgreSQL(Arc::new(pool));
                            self.connection_pools.insert(connection_id, database_pool.clone());
                            Some(database_pool)
                        },
                        Err(e) => {
                            println!("Failed to create PostgreSQL pool: {}", e);
                            None
                        }
                    }
                },
                DatabaseType::SQLite => {
                    let connection_string = format!("sqlite:{}", connection.host);
                    
                    println!("Creating new SQLite connection pool for: {}", connection.name);
                    
                    // Configure SQLite pool with proper settings
                    let pool_result = SqlitePoolOptions::new()
                        .max_connections(10)
                        .min_connections(2)
                        .acquire_timeout(std::time::Duration::from_secs(30))
                        .idle_timeout(std::time::Duration::from_secs(1200))
                        .max_lifetime(std::time::Duration::from_secs(3600))
                        .test_before_acquire(true)
                        .connect(&connection_string)
                        .await;
                    
                    match pool_result {
                        Ok(pool) => {
                            let database_pool = DatabasePool::SQLite(Arc::new(pool));
                            self.connection_pools.insert(connection_id, database_pool.clone());
                            Some(database_pool)
                        },
                        Err(e) => {
                            println!("Failed to create SQLite pool: {}", e);
                            None
                        }
                    }
                }
            }
        } else {
            None
        }
    }

    fn load_connection_tables(&mut self, connection_id: i64, node: &mut TreeNode) {
        // First check if we have cached data
        if let Some(databases) = self.get_databases_from_cache(connection_id) {
            if !databases.is_empty() {
                println!("Loading connection structure from cache for connection_id: {}", connection_id);
                self.build_connection_structure_from_cache(connection_id, node, &databases);
                node.is_loaded = true;
                return;
            } else {
                println!("Cache found but empty for connection_id: {}, fetching from server", connection_id);
            }
        } else {
            println!("No cache found for connection_id: {}, fetching from server", connection_id);
        }

        // If no cache or empty cache, create connection and fetch from server
        println!("Fetching from server for connection_id: {}", connection_id);
        
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            // Create the main structure based on database type
            match connection.connection_type {
                DatabaseType::MySQL => {
                    self.load_mysql_structure(connection_id, &connection, node);
                },
                DatabaseType::PostgreSQL => {
                    self.load_postgresql_structure(connection_id, &connection, node);
                },
                DatabaseType::SQLite => {
                    self.load_sqlite_structure(connection_id, &connection, node);
                }
            }
            
            node.is_loaded = true;
        }
    }

    fn build_connection_structure_from_cache(&mut self, connection_id: i64, node: &mut TreeNode, databases: &[String]) {
        // Find the connection to get its type
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let mut main_children = Vec::new();
            
            match connection.connection_type {
                DatabaseType::MySQL => {
                    // 1. Databases folder
                    let mut databases_folder = TreeNode::new("Databases".to_string(), NodeType::DatabasesFolder);
                    databases_folder.connection_id = Some(connection_id);
                    
                    println!("Building from cache: {} databases found", databases.len());
                    // Add each database from cache
                    for db_name in databases {
                        println!("Processing cached database: {}", db_name);
                        // Skip system databases for cleaner view
                        if !["information_schema", "performance_schema", "mysql", "sys"].contains(&db_name.as_str()) {
                            println!("Adding cached database to tree: {}", db_name);
                            let mut db_node = TreeNode::new(db_name.clone(), NodeType::Database);
                            db_node.connection_id = Some(connection_id);
                            db_node.database_name = Some(db_name.clone());
                            db_node.is_loaded = false; // Will be loaded when expanded
                            
                            // Create folder structure but don't load content yet
                            let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                            tables_folder.connection_id = Some(connection_id);
                            tables_folder.database_name = Some(db_name.clone());
                            tables_folder.is_loaded = false;
                            
                            let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                            views_folder.connection_id = Some(connection_id);
                            views_folder.database_name = Some(db_name.clone());
                            views_folder.is_loaded = false;
                            
                            let mut procedures_folder = TreeNode::new("Stored Procedures".to_string(), NodeType::StoredProceduresFolder);
                            procedures_folder.connection_id = Some(connection_id);
                            procedures_folder.database_name = Some(db_name.clone());
                            procedures_folder.is_loaded = false;
                            
                            let mut functions_folder = TreeNode::new("Functions".to_string(), NodeType::UserFunctionsFolder);
                            functions_folder.connection_id = Some(connection_id);
                            functions_folder.database_name = Some(db_name.clone());
                            functions_folder.is_loaded = false;
                            
                            let mut triggers_folder = TreeNode::new("Triggers".to_string(), NodeType::TriggersFolder);
                            triggers_folder.connection_id = Some(connection_id);
                            triggers_folder.database_name = Some(db_name.clone());
                            triggers_folder.is_loaded = false;
                            
                            let mut events_folder = TreeNode::new("Events".to_string(), NodeType::EventsFolder);
                            events_folder.connection_id = Some(connection_id);
                            events_folder.database_name = Some(db_name.clone());
                            events_folder.is_loaded = false;
                            
                            db_node.children = vec![
                                tables_folder,
                                views_folder,
                                procedures_folder,
                                functions_folder,
                                triggers_folder,
                                events_folder,
                            ];
                            
                            databases_folder.children.push(db_node);
                        }
                    }
                    
                    // 2. DBA Views folder
                    let mut dba_folder = TreeNode::new("DBA Views".to_string(), NodeType::DBAViewsFolder);
                    dba_folder.connection_id = Some(connection_id);
                    
                    let mut dba_children = Vec::new();
                    
                    // Users
                    let mut users_folder = TreeNode::new("Users".to_string(), NodeType::UsersFolder);
                    users_folder.connection_id = Some(connection_id);
                    users_folder.is_loaded = false;
                    dba_children.push(users_folder);
                    
                    // Privileges
                    let mut priv_folder = TreeNode::new("Privileges".to_string(), NodeType::PrivilegesFolder);
                    priv_folder.connection_id = Some(connection_id);
                    priv_folder.is_loaded = false;
                    dba_children.push(priv_folder);
                    
                    // Processes
                    let mut proc_folder = TreeNode::new("Processes".to_string(), NodeType::ProcessesFolder);
                    proc_folder.connection_id = Some(connection_id);
                    proc_folder.is_loaded = false;
                    dba_children.push(proc_folder);
                    
                    // Status
                    let mut status_folder = TreeNode::new("Status".to_string(), NodeType::StatusFolder);
                    status_folder.connection_id = Some(connection_id);
                    status_folder.is_loaded = false;
                    dba_children.push(status_folder);
                    
                    dba_folder.children = dba_children;
                    
                    main_children.push(databases_folder);
                    main_children.push(dba_folder);
                },
                DatabaseType::PostgreSQL => {
                    // Similar structure for PostgreSQL
                    let mut databases_folder = TreeNode::new("Databases".to_string(), NodeType::DatabasesFolder);
                    databases_folder.connection_id = Some(connection_id);
                    
                    for db_name in databases {
                        if !["template0", "template1", "postgres"].contains(&db_name.as_str()) {
                            let mut db_node = TreeNode::new(db_name.clone(), NodeType::Database);
                            db_node.connection_id = Some(connection_id);
                            db_node.database_name = Some(db_name.clone());
                            db_node.is_loaded = false;
                            
                            let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                            tables_folder.connection_id = Some(connection_id);
                            tables_folder.database_name = Some(db_name.clone());
                            tables_folder.is_loaded = false;
                            
                            let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                            views_folder.connection_id = Some(connection_id);
                            views_folder.database_name = Some(db_name.clone());
                            views_folder.is_loaded = false;
                            
                            db_node.children = vec![tables_folder, views_folder];
                            databases_folder.children.push(db_node);
                        }
                    }
                    
                    main_children.push(databases_folder);
                },
                DatabaseType::SQLite => {
                    // SQLite structure - single database
                    let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                    tables_folder.connection_id = Some(connection_id);
                    tables_folder.database_name = Some("main".to_string());
                    tables_folder.is_loaded = false;
                    
                    let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                    views_folder.connection_id = Some(connection_id);
                    views_folder.database_name = Some("main".to_string());
                    views_folder.is_loaded = false;
                    
                    main_children = vec![tables_folder, views_folder];
                }
            }
            
            node.children = main_children;
        }
    }

    // Helper function to find folder node that needs expansion
    fn find_folder_node_to_expand(node: &mut TreeNode, connection_id: i64) -> Option<(&mut TreeNode, NodeType)> {
        // Check if this node itself is a folder that needs expansion
        if matches!(node.node_type, 
            NodeType::TablesFolder | NodeType::ViewsFolder | NodeType::StoredProceduresFolder |
            NodeType::UserFunctionsFolder | NodeType::TriggersFolder | NodeType::EventsFolder
        ) && node.connection_id == Some(connection_id) && node.is_expanded && !node.is_loaded {
            println!("Found target folder node: {} (type: {:?})", node.name, node.node_type);
            let node_type = node.node_type.clone();
            return Some((node, node_type));
        }
        
        // Recursively search in children
        for child in &mut node.children {
            if let Some(result) = Self::find_folder_node_to_expand(child, connection_id) {
                return Some(result);
            }
        }
        
        None
    }

    fn load_databases_for_folder(&mut self, connection_id: i64, databases_folder: &mut TreeNode) {
        println!("Loading databases for connection_id: {}", connection_id);
        
        // Clear any loading placeholders
        databases_folder.children.clear();
        
        // First check cache
        if let Some(cached_databases) = self.get_databases_from_cache(connection_id) {
            if !cached_databases.is_empty() {
                println!("Loading {} databases from cache", cached_databases.len());
                
                for db_name in cached_databases {
                    let mut db_node = TreeNode::new(db_name.clone(), NodeType::Database);
                    db_node.connection_id = Some(connection_id);
                    db_node.database_name = Some(db_name.clone());
                    db_node.is_loaded = false;
                    
                    // Add subfolders for each database
                    let mut db_children = Vec::new();
                    
                    // Tables folder
                    let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                    tables_folder.connection_id = Some(connection_id);
                    tables_folder.database_name = Some(db_name.clone());
                    tables_folder.is_loaded = false;
                    db_children.push(tables_folder);
                    
                    // Views folder
                    let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                    views_folder.connection_id = Some(connection_id);
                    views_folder.database_name = Some(db_name.clone());
                    views_folder.is_loaded = false;
                    db_children.push(views_folder);
                    
                    // Stored Procedures folder
                    let mut sp_folder = TreeNode::new("Stored Procedures".to_string(), NodeType::StoredProceduresFolder);
                    sp_folder.connection_id = Some(connection_id);
                    sp_folder.database_name = Some(db_name.clone());
                    sp_folder.is_loaded = false;
                    db_children.push(sp_folder);
                    
                    db_node.children = db_children;
                    databases_folder.children.push(db_node);
                }
                
                databases_folder.is_loaded = true;
                return;
            }
        }
        
        // If cache is empty or doesn't exist, fetch from actual database connection
        println!("Cache is empty, fetching databases from actual connection");
        
        // Try to fetch real databases from the connection
        if let Some(real_databases) = self.fetch_databases_from_connection(connection_id) {
            println!("Successfully fetched {} databases from connection", real_databases.len());
            
            // Save to cache for future use
            self.save_databases_to_cache(connection_id, &real_databases);
            
            // Create tree nodes from fetched data
            for db_name in real_databases {
                let mut db_node = TreeNode::new(db_name.clone(), NodeType::Database);
                db_node.connection_id = Some(connection_id);
                db_node.database_name = Some(db_name.clone());
                db_node.is_loaded = false;
                
                // Add subfolders for each database
                let mut db_children = Vec::new();
                
                // Tables folder
                let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                tables_folder.connection_id = Some(connection_id);
                tables_folder.database_name = Some(db_name.clone());
                tables_folder.is_loaded = false;
                db_children.push(tables_folder);
                
                // Views folder
                let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                views_folder.connection_id = Some(connection_id);
                views_folder.database_name = Some(db_name.clone());
                views_folder.is_loaded = false;
                db_children.push(views_folder);
                
                // Stored Procedures folder
                let mut sp_folder = TreeNode::new("Stored Procedures".to_string(), NodeType::StoredProceduresFolder);
                sp_folder.connection_id = Some(connection_id);
                sp_folder.database_name = Some(db_name.clone());
                sp_folder.is_loaded = false;
                db_children.push(sp_folder);
                
                db_node.children = db_children;
                databases_folder.children.push(db_node);
            }
            
            databases_folder.is_loaded = true;
        } else {
            // If connection fails, show sample data as fallback
            println!("Failed to connect to database, showing sample data as fallback");
            self.populate_sample_databases_for_folder(connection_id, databases_folder);
        }
    }
    
    fn populate_sample_databases_for_folder(&mut self, connection_id: i64, databases_folder: &mut TreeNode) {
        // Find the connection to determine type
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let sample_databases = match connection.connection_type {
                DatabaseType::MySQL => vec!["information_schema".to_string(), "sakila".to_string(), "world".to_string(), "test".to_string()],
                DatabaseType::PostgreSQL => vec!["postgres".to_string(), "template1".to_string(), "dvdrental".to_string()],
                DatabaseType::SQLite => vec!["main".to_string()],
            };
            
            // Clear loading message
            databases_folder.children.clear();
            
            // Add sample databases
            for db_name in sample_databases {
                // Skip system databases for display
                if matches!(connection.connection_type, DatabaseType::MySQL) && 
                   ["information_schema", "performance_schema", "mysql", "sys"].contains(&db_name.as_str()) {
                    continue;
                }
                
                let mut db_node = TreeNode::new(db_name.clone(), NodeType::Database);
                db_node.connection_id = Some(connection_id);
                db_node.database_name = Some(db_name.clone());
                db_node.is_loaded = false;
                
                // Add subfolders for each database
                let mut db_children = Vec::new();
                
                // Tables folder
                let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
                tables_folder.connection_id = Some(connection_id);
                tables_folder.database_name = Some(db_name.clone());
                tables_folder.is_loaded = false;
                db_children.push(tables_folder);
                
                // Views folder  
                let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
                views_folder.connection_id = Some(connection_id);
                views_folder.database_name = Some(db_name.clone());
                views_folder.is_loaded = false;
                db_children.push(views_folder);
                
                if matches!(connection.connection_type, DatabaseType::MySQL) {
                    // Stored Procedures folder
                    let mut sp_folder = TreeNode::new("Stored Procedures".to_string(), NodeType::StoredProceduresFolder);
                    sp_folder.connection_id = Some(connection_id);
                    sp_folder.database_name = Some(db_name.clone());
                    sp_folder.is_loaded = false;
                    db_children.push(sp_folder);
                    
                    // User Functions folder
                    let mut uf_folder = TreeNode::new("User Functions".to_string(), NodeType::UserFunctionsFolder);
                    uf_folder.connection_id = Some(connection_id);
                    uf_folder.database_name = Some(db_name.clone());
                    uf_folder.is_loaded = false;
                    db_children.push(uf_folder);
                    
                    // Triggers folder
                    let mut triggers_folder = TreeNode::new("Triggers".to_string(), NodeType::TriggersFolder);
                    triggers_folder.connection_id = Some(connection_id);
                    triggers_folder.database_name = Some(db_name.clone());
                    triggers_folder.is_loaded = false;
                    db_children.push(triggers_folder);
                    
                    // Events folder
                    let mut events_folder = TreeNode::new("Events".to_string(), NodeType::EventsFolder);
                    events_folder.connection_id = Some(connection_id);
                    events_folder.database_name = Some(db_name.clone());
                    events_folder.is_loaded = false;
                    db_children.push(events_folder);
                }
                
                db_node.children = db_children;
                databases_folder.children.push(db_node);
            }
            
            println!("Populated {} sample databases for connection {}", databases_folder.children.len(), connection.name);
        }
    }
    
    fn fetch_databases_from_connection(&mut self, connection_id: i64) -> Option<Vec<String>> {
        println!("Fetching databases from connection_id: {}", connection_id);
        
        // Find the connection configuration
        let _connection = self.connections.iter().find(|c| c.id == Some(connection_id))?.clone();
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = self.get_or_create_connection_pool(connection_id).await?;
            
            match pool {
                DatabasePool::MySQL(mysql_pool) => {
                    println!("Querying MySQL databases");
                    let result = sqlx::query_as::<_, (String,)>("SHOW DATABASES")
                        .fetch_all(mysql_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let databases: Vec<String> = rows.into_iter()
                                .map(|(db_name,)| db_name)
                                .filter(|db| !["information_schema", "performance_schema", "mysql", "sys"].contains(&db.as_str()))
                                .collect();
                            println!("Found {} MySQL databases: {:?}", databases.len(), databases);
                            Some(databases)
                        },
                        Err(e) => {
                            println!("Error querying MySQL databases: {}", e);
                            None
                        }
                    }
                },
                DatabasePool::PostgreSQL(pg_pool) => {
                    println!("Querying PostgreSQL databases");
                    let result = sqlx::query_as::<_, (String,)>(
                        "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1')"
                    )
                    .fetch_all(pg_pool.as_ref())
                    .await;
                    
                    match result {
                        Ok(rows) => {
                            let databases: Vec<String> = rows.into_iter().map(|(db_name,)| db_name).collect();
                            println!("Found {} PostgreSQL databases: {:?}", databases.len(), databases);
                            Some(databases)
                        },
                        Err(e) => {
                            println!("Error querying PostgreSQL databases: {}", e);
                            None
                        }
                    }
                },
                DatabasePool::SQLite(sqlite_pool) => {
                    println!("SQLite: Checking for database schema and tables");
                    // For SQLite, we'll query the actual database for table information
                    let result = sqlx::query_as::<_, (String,)>("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                        .fetch_all(sqlite_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let table_count = rows.len();
                            if table_count > 0 {
                                println!("Found {} tables in SQLite database, returning 'main' database", table_count);
                                // Since SQLite has tables, return main database
                                Some(vec!["main".to_string()])
                            } else {
                                println!("No tables found in SQLite database, returning 'main' database anyway");
                                Some(vec!["main".to_string()])
                            }
                        },
                        Err(e) => {
                            println!("Error querying SQLite tables: {}", e);
                            Some(vec!["main".to_string()]) // Fallback to main
                        }
                    }
                }
            }
        })
    }
    
    fn fetch_tables_from_mysql_connection(&mut self, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
        println!("Fetching {} from MySQL database: {}", table_type, database_name);
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = self.get_or_create_connection_pool(connection_id).await?;
            
            match pool {
                DatabasePool::MySQL(mysql_pool) => {
                    let query = match table_type {
                        "table" => format!("SHOW TABLES FROM `{}`", database_name),
                        "view" => format!("SELECT table_name FROM information_schema.views WHERE table_schema = '{}'", database_name),
                        "procedure" => format!("SELECT routine_name FROM information_schema.routines WHERE routine_schema = '{}' AND routine_type = 'PROCEDURE'", database_name),
                        "function" => format!("SELECT routine_name FROM information_schema.routines WHERE routine_schema = '{}' AND routine_type = 'FUNCTION'", database_name),
                        "trigger" => format!("SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = '{}'", database_name),
                        "event" => format!("SELECT event_name FROM information_schema.events WHERE event_schema = '{}'", database_name),
                        _ => {
                            println!("Unsupported table type: {}", table_type);
                            return None;
                        }
                    };
                    
                    println!("Executing MySQL query: {}", query);
                    let result = sqlx::query_as::<_, (String,)>(&query)
                        .fetch_all(mysql_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let items: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
                            println!("Found {} {} in MySQL database {}: {:?}", items.len(), table_type, database_name, items);
                            Some(items)
                        },
                        Err(e) => {
                            println!("Error querying MySQL {} from database {}: {}", table_type, database_name, e);
                            None
                        }
                    }
                },
                _ => {
                    println!("Wrong pool type for MySQL connection");
                    None
                }
            }
        })
    }
    
    fn fetch_tables_from_sqlite_connection(&mut self, connection_id: i64, table_type: &str) -> Option<Vec<String>> {
        println!("Fetching {} from SQLite database", table_type);
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = self.get_or_create_connection_pool(connection_id).await?;
            
            match pool {
                DatabasePool::SQLite(sqlite_pool) => {
                    let query = match table_type {
                        "table" => "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        "view" => "SELECT name FROM sqlite_master WHERE type='view'",
                        _ => {
                            println!("Unsupported table type for SQLite: {}", table_type);
                            return None;
                        }
                    };
                    
                    println!("Executing SQLite query: {}", query);
                    let result = sqlx::query_as::<_, (String,)>(query)
                        .fetch_all(sqlite_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let items: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
                            println!("Found {} {} in SQLite database: {:?}", items.len(), table_type, items);
                            Some(items)
                        },
                        Err(e) => {
                            println!("Error querying SQLite {} from database: {}", table_type, e);
                            None
                        }
                    }
                },
                _ => {
                    println!("Wrong pool type for SQLite connection");
                    None
                }
            }
        })
    }
    
    fn fetch_columns_from_database(&self, _connection_id: i64, database_name: &str, table_name: &str, connection: &ConnectionConfig) -> Option<Vec<(String, String)>> {
        println!("Fetching columns from database for table: {}.{}", database_name, table_name);
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        // Clone data to move into async block
        let connection_clone = connection.clone();
        let database_name = database_name.to_string();
        let table_name = table_name.to_string();
        
        rt.block_on(async {
            match connection_clone.connection_type {
                DatabaseType::MySQL => {
                    // Create MySQL connection
                    let encoded_username = MyApp::url_encode(&connection_clone.username);
                    let encoded_password = MyApp::url_encode(&connection_clone.password);
                    let connection_string = format!(
                        "mysql://{}:{}@{}:{}/{}",
                        encoded_username, encoded_password, connection_clone.host, connection_clone.port, database_name
                    );
                    
                    match MySqlPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            let query = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
                            match sqlx::query_as::<_, (String, String)>(query)
                                .bind(&database_name)
                                .bind(&table_name)
                                .fetch_all(&pool)
                                .await
                            {
                                Ok(rows) => {
                                    let columns: Vec<(String, String)> = rows.into_iter().collect();
                                    println!("Found {} columns for MySQL table {}: {:?}", columns.len(), table_name, columns);
                                    Some(columns)
                                },
                                Err(e) => {
                                    println!("Error querying MySQL columns for table {}: {}", table_name, e);
                                    None
                                }
                            }
                        },
                        Err(e) => {
                            println!("Error connecting to MySQL database: {}", e);
                            None
                        }
                    }
                },
                DatabaseType::SQLite => {
                    // Create SQLite connection
                    let connection_string = format!("sqlite:{}", connection_clone.host);
                    
                    match SqlitePoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            let query = format!("PRAGMA table_info({})", table_name);
                            match sqlx::query_as::<_, (i32, String, String, i32, String, i32)>(&query)
                                .fetch_all(&pool)
                                .await
                            {
                                Ok(rows) => {
                                    let columns: Vec<(String, String)> = rows.into_iter()
                                        .map(|(_, name, data_type, _, _, _)| (name, data_type))
                                        .collect();
                                    println!("Found {} columns for SQLite table {}: {:?}", columns.len(), table_name, columns);
                                    Some(columns)
                                },
                                Err(e) => {
                                    println!("Error querying SQLite columns for table {}: {}", table_name, e);
                                    None
                                }
                            }
                        },
                        Err(e) => {
                            println!("Error connecting to SQLite database: {}", e);
                            None
                        }
                    }
                },
                DatabaseType::PostgreSQL => {
                    // Create PostgreSQL connection
                    let connection_string = format!(
                        "postgresql://{}:{}@{}:{}/{}",
                        connection_clone.username, connection_clone.password, connection_clone.host, connection_clone.port, database_name
                    );
                    
                    match PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .connect(&connection_string)
                        .await
                    {
                        Ok(pool) => {
                            let query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? ORDER BY ordinal_position";
                            match sqlx::query_as::<_, (String, String)>(query)
                                .bind(&table_name)
                                .fetch_all(&pool)
                                .await
                            {
                                Ok(rows) => {
                                    let columns: Vec<(String, String)> = rows.into_iter().collect();
                                    println!("Found {} columns for PostgreSQL table {}: {:?}", columns.len(), table_name, columns);
                                    Some(columns)
                                },
                                Err(e) => {
                                    println!("Error querying PostgreSQL columns for table {}: {}", table_name, e);
                                    None
                                }
                            }
                        },
                        Err(e) => {
                            println!("Error connecting to PostgreSQL database: {}", e);
                            None
                        }
                    }
                }
            }
        })
    }

    fn load_mysql_structure(&mut self, connection_id: i64, _connection: &ConnectionConfig, node: &mut TreeNode) {
        println!("Loading MySQL database structure using connection pool");
        
        // Since we can't use block_on in an async context, we'll create a simple structure
        // and populate it with cached data or show a loading message
        
        // Create basic structure immediately
        let mut main_children = Vec::new();
        
        // 1. Databases folder
        let mut databases_folder = TreeNode::new("Databases".to_string(), NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        databases_folder.is_loaded = false; // Will be loaded when expanded
        
        // 2. DBA Views folder
        let mut dba_folder = TreeNode::new("DBA Views".to_string(), NodeType::DBAViewsFolder);
        dba_folder.connection_id = Some(connection_id);
        
        let mut dba_children = Vec::new();
        
        // Users
        let mut users_folder = TreeNode::new("Users".to_string(), NodeType::UsersFolder);
        users_folder.connection_id = Some(connection_id);
        users_folder.is_loaded = false;
        dba_children.push(users_folder);
        
        // Privileges
        let mut priv_folder = TreeNode::new("Privileges".to_string(), NodeType::PrivilegesFolder);
        priv_folder.connection_id = Some(connection_id);
        priv_folder.is_loaded = false;
        dba_children.push(priv_folder);
        
        // Processes
        let mut proc_folder = TreeNode::new("Processes".to_string(), NodeType::ProcessesFolder);
        proc_folder.connection_id = Some(connection_id);
        proc_folder.is_loaded = false;
        dba_children.push(proc_folder);
        
        // Status
        let mut status_folder = TreeNode::new("Status".to_string(), NodeType::StatusFolder);
        status_folder.connection_id = Some(connection_id);
        status_folder.is_loaded = false;
        dba_children.push(status_folder);
        
        dba_folder.children = dba_children;
        
        main_children.push(databases_folder);
        main_children.push(dba_folder);
        
        node.children = main_children;
        
        // Trigger async loading in background (we'll need to implement this differently)
        // For now, we'll rely on the expansion mechanism to load databases when needed
    }

    fn load_postgresql_structure(&mut self, connection_id: i64, _connection: &ConnectionConfig, node: &mut TreeNode) {
        println!("Loading PostgreSQL structure for connection_id: {}", connection_id);
        
        // Create basic structure for PostgreSQL
        let mut main_children = Vec::new();
        
        // Databases folder
        let mut databases_folder = TreeNode::new("Databases".to_string(), NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        
        // Add a loading indicator
        let loading_node = TreeNode::new("Loading databases...".to_string(), NodeType::Database);
        databases_folder.children.push(loading_node);
        
        main_children.push(databases_folder);
        
        node.children = main_children;
    }

    fn load_sqlite_structure(&mut self, connection_id: i64, _connection: &ConnectionConfig, node: &mut TreeNode) {
        println!("Loading SQLite structure for connection_id: {}", connection_id);
        
        // Create basic structure for SQLite
        let mut main_children = Vec::new();
        
        // Tables folder
        let mut tables_folder = TreeNode::new("Tables".to_string(), NodeType::TablesFolder);
        tables_folder.connection_id = Some(connection_id);
        tables_folder.database_name = Some("main".to_string());
        tables_folder.is_loaded = false;
        
        // Add a loading indicator
        let loading_node = TreeNode::new("Loading tables...".to_string(), NodeType::Table);
        tables_folder.children.push(loading_node);
        
        main_children.push(tables_folder);
        
        // Views folder
        let mut views_folder = TreeNode::new("Views".to_string(), NodeType::ViewsFolder);
        views_folder.connection_id = Some(connection_id);
        views_folder.database_name = Some("main".to_string());
        views_folder.is_loaded = false;
        main_children.push(views_folder);
        
        node.children = main_children;
    }

    fn load_folder_content(&mut self, connection_id: i64, node: &mut TreeNode, folder_type: NodeType) {
        println!("=== load_folder_content called ===");
        println!("Connection ID: {}", connection_id);
        println!("Folder type: {:?}", folder_type);
        println!("Node name: {}", node.name);
        
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            println!("Loading {:?} content for connection {}", folder_type, connection.name);
            
            match connection.connection_type {
                DatabaseType::MySQL => {
                    println!("Calling load_mysql_folder_content");
                    self.load_mysql_folder_content(connection_id, &connection, node, folder_type);
                },
                DatabaseType::PostgreSQL => {
                    self.load_postgresql_folder_content(connection_id, &connection, node, folder_type);
                },
                DatabaseType::SQLite => {
                    self.load_sqlite_folder_content(connection_id, &connection, node, folder_type);
                }
            }
            
            node.is_loaded = true;
            println!("Folder content loading completed");
        } else {
            println!("ERROR: Connection with ID {} not found!", connection_id);
        }
    }

    fn load_mysql_folder_content(&mut self, connection_id: i64, connection: &ConnectionConfig, node: &mut TreeNode, folder_type: NodeType) {
        // Get database name from node or connection default
        let database_name = node.database_name.as_ref().unwrap_or(&connection.database);
        
        // Map folder type to cache table type
        let table_type = match folder_type {
            NodeType::TablesFolder => "table",
            NodeType::ViewsFolder => "view",
            NodeType::StoredProceduresFolder => "procedure",
            NodeType::UserFunctionsFolder => "function",
            NodeType::TriggersFolder => "trigger",
            NodeType::EventsFolder => "event",
            _ => {
                println!("Unsupported folder type: {:?}", folder_type);
                return;
            }
        };
        
        // First try to get from cache
        if let Some(cached_items) = self.get_tables_from_cache(connection_id, database_name, table_type) {
            if !cached_items.is_empty() {
                println!("Loading {} {} from cache", cached_items.len(), table_type);
                
                // Create tree nodes from cached data
                let child_nodes: Vec<TreeNode> = cached_items.into_iter().map(|item_name| {
                    let mut child_node = TreeNode::new(item_name.clone(), match folder_type {
                        NodeType::TablesFolder => NodeType::Table,
                        NodeType::ViewsFolder => NodeType::View,
                        NodeType::StoredProceduresFolder => NodeType::StoredProcedure,
                        NodeType::UserFunctionsFolder => NodeType::UserFunction,
                        NodeType::TriggersFolder => NodeType::Trigger,
                        NodeType::EventsFolder => NodeType::Event,
                        _ => NodeType::Table,
                    });
                    child_node.connection_id = Some(connection_id);
                    child_node.database_name = Some(database_name.clone());
                    child_node.is_loaded = false; // Will load columns on expansion if it's a table
                    child_node
                }).collect();
                
                node.children = child_nodes;
                return;
            }
        }
        
        // If cache is empty, fetch from actual database
        println!("Cache miss, fetching {} from actual MySQL database: {}", table_type, database_name);
        
        if let Some(real_items) = self.fetch_tables_from_mysql_connection(connection_id, database_name, table_type) {
            println!("Successfully fetched {} {} from MySQL database", real_items.len(), table_type);
            
            // Save to cache for future use
            let table_data: Vec<(String, String)> = real_items.iter().map(|name| (name.clone(), table_type.to_string())).collect();
            self.save_tables_to_cache(connection_id, database_name, &table_data);
            
            // Create tree nodes from fetched data
            let child_nodes: Vec<TreeNode> = real_items.into_iter().map(|item_name| {
                let mut child_node = TreeNode::new(item_name.clone(), match folder_type {
                    NodeType::TablesFolder => NodeType::Table,
                    NodeType::ViewsFolder => NodeType::View,
                    NodeType::StoredProceduresFolder => NodeType::StoredProcedure,
                    NodeType::UserFunctionsFolder => NodeType::UserFunction,
                    NodeType::TriggersFolder => NodeType::Trigger,
                    NodeType::EventsFolder => NodeType::Event,
                    _ => NodeType::Table,
                });
                child_node.connection_id = Some(connection_id);
                child_node.database_name = Some(database_name.clone());
                child_node.is_loaded = false; // Will load columns on expansion if it's a table
                child_node
            }).collect();
            
            node.children = child_nodes;
        } else {
            // If database fetch fails, add sample data as fallback
            println!("Failed to fetch from MySQL, using sample {} data", table_type);
            
            let sample_items = match folder_type {
                NodeType::TablesFolder => vec!["users".to_string(), "products".to_string(), "orders".to_string()],
                NodeType::ViewsFolder => vec!["user_orders_view".to_string(), "product_summary_view".to_string()],
                NodeType::StoredProceduresFolder => vec!["sp_get_user".to_string(), "sp_create_order".to_string()],
                NodeType::UserFunctionsFolder => vec!["fn_calculate_total".to_string()],
                NodeType::TriggersFolder => vec!["tr_update_timestamp".to_string()],
                NodeType::EventsFolder => vec!["ev_cleanup".to_string()],
                _ => vec![],
            };
            
            // Create tree nodes
            let child_nodes: Vec<TreeNode> = sample_items.into_iter().map(|item_name| {
                let mut child_node = TreeNode::new(item_name.clone(), match folder_type {
                    NodeType::TablesFolder => NodeType::Table,
                    NodeType::ViewsFolder => NodeType::View,
                    NodeType::StoredProceduresFolder => NodeType::StoredProcedure,
                    NodeType::UserFunctionsFolder => NodeType::UserFunction,
                    NodeType::TriggersFolder => NodeType::Trigger,
                    NodeType::EventsFolder => NodeType::Event,
                    _ => NodeType::Table,
                });
                child_node.connection_id = Some(connection_id);
                child_node.database_name = Some(database_name.clone());
                child_node.is_loaded = false; // Will load columns on expansion if it's a table
                child_node
            }).collect();
            
            node.children = child_nodes;
        }
        
        println!("Loaded {} {} items for MySQL", node.children.len(), table_type);
    }

    fn load_postgresql_folder_content(&mut self, _connection_id: i64, _connection: &ConnectionConfig, node: &mut TreeNode, _folder_type: NodeType) {
        // Placeholder for PostgreSQL folder content loading
        node.children = vec![TreeNode::new("PostgreSQL folder content not implemented yet".to_string(), NodeType::Column)];
    }

    fn load_sqlite_folder_content(&mut self, connection_id: i64, _connection: &ConnectionConfig, node: &mut TreeNode, folder_type: NodeType) {
        println!("Loading {:?} content for SQLite", folder_type);
        
        // Try to get from cache first
        let table_type = match folder_type {
            NodeType::TablesFolder => "table",
            NodeType::ViewsFolder => "view",
            _ => {
                // For other folder types, return empty for now
                node.children = vec![TreeNode::new("Not supported for SQLite".to_string(), NodeType::Column)];
                return;
            }
        };
        
        if let Some(cached_items) = self.get_tables_from_cache(connection_id, "main", table_type) {
            if !cached_items.is_empty() {
                println!("Loading {} {} from cache for SQLite", cached_items.len(), table_type);
                
                node.children = cached_items.into_iter().map(|item_name| {
                    let node_type = match folder_type {
                        NodeType::TablesFolder => NodeType::Table,
                        NodeType::ViewsFolder => NodeType::View,
                        _ => NodeType::Table,
                    };
                    
                    let mut item_node = TreeNode::new(item_name, node_type);
                    item_node.connection_id = Some(connection_id);
                    item_node.database_name = Some("main".to_string());
                    item_node.is_loaded = false; // Will load columns on expansion if it's a table
                    item_node
                }).collect();
                
                return;
            }
        }
        
        // If cache is empty, fetch from actual SQLite database
        println!("Cache miss, fetching {} from actual SQLite database", table_type);
        
        if let Some(real_items) = self.fetch_tables_from_sqlite_connection(connection_id, table_type) {
            println!("Successfully fetched {} {} from SQLite database", real_items.len(), table_type);
            
            // Save to cache for future use
            let table_data: Vec<(String, String)> = real_items.iter().map(|name| (name.clone(), table_type.to_string())).collect();
            self.save_tables_to_cache(connection_id, "main", &table_data);
            
            // Create tree nodes from fetched data
            let child_nodes: Vec<TreeNode> = real_items.into_iter().map(|item_name| {
                let node_type = match folder_type {
                    NodeType::TablesFolder => NodeType::Table,
                    NodeType::ViewsFolder => NodeType::View,
                    _ => NodeType::Table,
                };
                
                let mut item_node = TreeNode::new(item_name, node_type);
                item_node.connection_id = Some(connection_id);
                item_node.database_name = Some("main".to_string());
                item_node.is_loaded = false; // Will load columns on expansion if it's a table
                item_node
            }).collect();
            
            node.children = child_nodes;
        } else {
            // If database fetch fails, add sample data as fallback
            println!("Failed to fetch from SQLite, using sample {} data", table_type);
            
            let sample_items = match folder_type {
                NodeType::TablesFolder => vec!["users".to_string(), "products".to_string(), "orders".to_string(), "categories".to_string()],
                NodeType::ViewsFolder => vec!["user_summary".to_string(), "order_details".to_string()],
                _ => vec![],
            };
            
            let item_type = match folder_type {
                NodeType::TablesFolder => NodeType::Table,
                NodeType::ViewsFolder => NodeType::View,
                _ => NodeType::Column, // fallback
            };
            
            node.children = sample_items.into_iter().map(|item_name| {
                let mut item_node = TreeNode::new(item_name.clone(), item_type.clone());
                item_node.connection_id = Some(connection_id);
                item_node.database_name = Some("main".to_string());
                item_node.is_loaded = false;
                item_node
            }).collect();
        }
        
        println!("Loaded {} items into {:?} folder for SQLite", node.children.len(), folder_type);
    }

    fn load_table_columns_sync(&self, connection_id: i64, table_name: &str, connection: &ConnectionConfig, database_name: &str) -> Vec<TreeNode> {
        // First try to get from cache
        if let Some(cached_columns) = self.get_columns_from_cache(connection_id, database_name, table_name) {
            if !cached_columns.is_empty() {
                println!("Loading {} columns from cache for table {}", cached_columns.len(), table_name);
                return cached_columns.into_iter().map(|(column_name, data_type)| {
                    TreeNode::new(format!("{} ({})", column_name, data_type), NodeType::Column)
                }).collect();
            }
        }
        
        // If cache is empty, fetch from actual database
        println!("Column cache miss for table {}, fetching from actual database", table_name);
        
        if let Some(real_columns) = self.fetch_columns_from_database(connection_id, database_name, table_name, connection) {
            println!("Successfully fetched {} columns from database for table {}", real_columns.len(), table_name);
            
            // Save to cache for future use
            self.save_columns_to_cache(connection_id, database_name, table_name, &real_columns);
            
            // Convert to TreeNode
            real_columns.into_iter().map(|(column_name, data_type)| {
                TreeNode::new(format!("{} ({})", column_name, data_type), NodeType::Column)
            }).collect()
        } else {
            // If database fetch fails, return sample columns
            println!("Failed to fetch columns from database for table {}, using sample data", table_name);
            vec![
                TreeNode::new("id (INTEGER)".to_string(), NodeType::Column),
                TreeNode::new("name (VARCHAR)".to_string(), NodeType::Column),
                TreeNode::new("created_at (TIMESTAMP)".to_string(), NodeType::Column),
            ]
        }
    }

    fn load_table_columns_for_node(&mut self, connection_id: i64, table_name: &str, nodes: &mut [TreeNode], _table_index: usize) {
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            // Find the table node to get the correct database_name
            let database_name = self.find_table_database_name(nodes, table_name, connection_id)
                .unwrap_or_else(|| connection.database.clone());
            
            println!("Loading columns for table: {}.{}", database_name, table_name);
            
            // Load columns for this table without creating new runtime
            let columns = self.load_table_columns_sync(connection_id, table_name, &connection, &database_name);
            
            // Find the table node recursively and update it
            self.update_table_node_with_columns_recursive(nodes, table_name, columns, connection_id);
        }
    }

    fn find_table_database_name(&self, nodes: &[TreeNode], table_name: &str, connection_id: i64) -> Option<String> {
        for node in nodes {
            // If this is the table node we're looking for
            if node.node_type == NodeType::Table && 
               node.name == table_name && 
               node.connection_id == Some(connection_id) {
                return node.database_name.clone();
            }
            
            // Recursively search in children
            if let Some(found_db) = self.find_table_database_name(&node.children, table_name, connection_id) {
                return Some(found_db);
            }
        }
        None
    }

    fn update_table_node_with_columns_recursive(&mut self, nodes: &mut [TreeNode], table_name: &str, columns: Vec<TreeNode>, connection_id: i64) -> bool {
        for node in nodes.iter_mut() {
            // If this is the table node we're looking for
            if node.node_type == NodeType::Table && 
               node.name == table_name && 
               node.connection_id == Some(connection_id) {
                node.children = columns;
                node.is_loaded = true;
                println!("Updated table '{}' with {} columns", table_name, node.children.len());
                return true;
            }
            
            // Recursively search in children
            if self.update_table_node_with_columns_recursive(&mut node.children, table_name, columns.clone(), connection_id) {
                return true;
            }
        }
        false
    }



    fn render_tree_for_database_section(&mut self, ui: &mut egui::Ui) {
        // Use slice to avoid borrowing issues
        let mut items_tree = std::mem::take(&mut self.items_tree);
        let _ = self.render_tree(ui, &mut items_tree);
        self.items_tree = items_tree;
    }

    fn load_table_data(&mut self, connection_id: i64, table_name: &str) {
        println!("load_table_data called with connection_id: {}, table_name: {}", connection_id, table_name);
        
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)).cloned() {
            println!("Found connection for table: {}", table_name);
            
            let select_query = format!("SELECT * FROM {} LIMIT 100", table_name);
            
            // Set the query in the editor  
            self.editor_text = select_query.clone();
            self.current_connection_id = Some(connection_id);
            
            // Execute the query with proper database connection
            if let Some((headers, data)) = self.execute_table_query_sync(connection_id, &connection, &select_query) {
                self.current_table_headers = headers;
                self.current_table_data = data;
                if self.current_table_data.is_empty() {
                    self.current_table_name = format!("Table: {} (no results)", table_name);
                } else {
                    self.current_table_name = format!("Table: {} ({} rows)", table_name, self.current_table_data.len());
                }
                println!("Successfully loaded {} rows from table {}", self.current_table_data.len(), table_name);
            } else {
                self.current_table_name = format!("Failed to load table: {}", table_name);
                self.current_table_headers.clear();
                self.current_table_data.clear();
                println!("Failed to execute query for table: {}", table_name);
            }
        }
    }

    fn execute_query(&mut self) {
        let query = self.editor_text.trim().to_string();
        if query.is_empty() {
            self.current_table_name = "No query to execute".to_string();
            self.current_table_headers.clear();
            self.current_table_data.clear();
            return;
        }

        // Check if we have an active connection
        if let Some(connection_id) = self.current_connection_id {
            let result = self.execute_query_with_connection(connection_id, query.clone());
            if let Some((headers, data)) = result {
                self.current_table_headers = headers;
                self.current_table_data = data;
                if self.current_table_data.is_empty() {
                    self.current_table_name = "Query executed successfully (no results)".to_string();
                } else {
                    self.current_table_name = format!("Query Results ({} rows)", self.current_table_data.len());
                }
                // Save query to history after successful execution
                self.save_query_to_history(&query, connection_id);
            } else {
                self.current_table_name = "Query execution failed".to_string();
                self.current_table_headers.clear();
                self.current_table_data.clear();
            }
        } else {
            // No active connection - check if we have any connections available
            if self.connections.is_empty() {
                self.current_table_name = "No connections available. Please add a connection first.".to_string();
                self.current_table_headers.clear();
                self.current_table_data.clear();
            } else {
                // Show connection selector popup
                self.pending_query = query.clone();
                self.show_connection_selector = true;
                self.auto_execute_after_connection = true;
            }
        }
    }

    fn execute_query_with_connection(&mut self, connection_id: i64, query: String) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        println!("Query execution requested for connection {} with query: {}", connection_id, query);
        
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)).cloned() {
            self.execute_table_query_sync(connection_id, &connection, &query)
        } else {
            println!("Connection not found for ID: {}", connection_id);
            None
        }
    }

    fn execute_table_query_sync(&mut self, connection_id: i64, _connection: &ConnectionConfig, query: &str) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        println!("Executing query synchronously: {}", query);
        
        // Create a new runtime specifically for this query execution
        let rt = match tokio::runtime::Runtime::new() {
            Ok(runtime) => runtime,
            Err(e) => {
                println!("Failed to create runtime: {}", e);
                return None;
            }
        };
        
        rt.block_on(async {
            match self.get_or_create_connection_pool(connection_id).await {
                Some(pool) => {
                    match pool {
                        DatabasePool::MySQL(mysql_pool) => {
                            println!("Executing MySQL query: {}", query);
                            match sqlx::query(query).fetch_all(mysql_pool.as_ref()).await {
                                Ok(rows) => {
                                    if rows.is_empty() {
                                        println!("Query returned no rows");
                                        return Some((vec![], vec![]));
                                    }
                                    
                                    // Get column headers
                                    let headers: Vec<String> = rows[0].columns().iter()
                                        .map(|col| col.name().to_string())
                                        .collect();
                                    
                                    // Convert rows to table data
                                    let table_data = Self::convert_mysql_rows_to_table_data(rows);
                                    
                                    println!("Query successful: {} headers, {} rows", headers.len(), table_data.len());
                                    Some((headers, table_data))
                                },
                                Err(e) => {
                                    println!("MySQL query failed: {}", e);
                                    Some((
                                        vec!["Error".to_string()],
                                        vec![vec![format!("Query error: {}", e)]]
                                    ))
                                }
                            }
                        },
                        DatabasePool::PostgreSQL(pg_pool) => {
                            println!("Executing PostgreSQL query: {}", query);
                            match sqlx::query(query).fetch_all(pg_pool.as_ref()).await {
                                Ok(rows) => {
                                    if rows.is_empty() {
                                        return Some((vec![], vec![]));
                                    }
                                    
                                    let headers: Vec<String> = rows[0].columns().iter()
                                        .map(|col| col.name().to_string())
                                        .collect();
                                    
                                    let table_data: Vec<Vec<String>> = rows.iter().map(|row| {
                                        (0..row.len()).map(|i| {
                                            match row.try_get::<Option<String>, _>(i) {
                                                Ok(Some(value)) => value,
                                                Ok(None) => "NULL".to_string(),
                                                Err(_) => "Error".to_string(),
                                            }
                                        }).collect()
                                    }).collect();
                                    
                                    Some((headers, table_data))
                                },
                                Err(e) => {
                                    println!("PostgreSQL query failed: {}", e);
                                    Some((
                                        vec!["Error".to_string()],
                                        vec![vec![format!("Query error: {}", e)]]
                                    ))
                                }
                            }
                        },
                        DatabasePool::SQLite(sqlite_pool) => {
                            println!("Executing SQLite query: {}", query);
                            match sqlx::query(query).fetch_all(sqlite_pool.as_ref()).await {
                                Ok(rows) => {
                                    if rows.is_empty() {
                                        return Some((vec![], vec![]));
                                    }
                                    
                                    let headers: Vec<String> = rows[0].columns().iter()
                                        .map(|col| col.name().to_string())
                                        .collect();
                                    
                                    // Convert SQLite rows to table data with proper type handling
                                    let table_data = Self::convert_sqlite_rows_to_table_data(rows);
                                    
                                    println!("Query successful: {} headers, {} rows", headers.len(), table_data.len());
                                    Some((headers, table_data))
                                },
                                Err(e) => {
                                    println!("SQLite query failed: {}", e);
                                    Some((
                                        vec!["Error".to_string()],
                                        vec![vec![format!("Query error: {}", e)]]
                                    ))
                                }
                            }
                        }
                    }
                },
                None => {
                    println!("Failed to get connection pool for connection_id: {}", connection_id);
                    Some((
                        vec!["Error".to_string()],
                        vec![vec!["Failed to connect to database".to_string()]]
                    ))
                }
            }
        })
    }

    // Helper function to convert MySQL rows to Vec<Vec<String>> with proper type checking
    fn convert_mysql_rows_to_table_data(rows: Vec<sqlx::mysql::MySqlRow>) -> Vec<Vec<String>> {
        use sqlx::{Row, Column, TypeInfo};
        
        let mut table_data = Vec::new();
        
        for row in &rows {
            let mut row_data = Vec::new();
            let columns = row.columns();
            
            for column in columns.iter() {
                let column_name = column.name();
                let type_info = column.type_info();
                let type_name = type_info.name();
                
                // Convert value based on MySQL type
                let value_str = match type_name {
                    // Integer types
                    "TINYINT" => {
                        match row.try_get::<Option<i8>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "SMALLINT" => {
                        match row.try_get::<Option<i16>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "MEDIUMINT" | "INT" => {
                        match row.try_get::<Option<i32>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "BIGINT" => {
                        match row.try_get::<Option<i64>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Unsigned integer types
                    "TINYINT UNSIGNED" => {
                        match row.try_get::<Option<u8>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "SMALLINT UNSIGNED" => {
                        match row.try_get::<Option<u16>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                        match row.try_get::<Option<u32>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "BIGINT UNSIGNED" => {
                        match row.try_get::<Option<u64>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Floating point types
                    "FLOAT" => {
                        match row.try_get::<Option<f32>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    "DOUBLE" => {
                        match row.try_get::<Option<f64>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Decimal types - fallback to string
                    "DECIMAL" | "NUMERIC" => {
                        match row.try_get::<Option<String>, _>(column_name) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // String types
                    "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                        match row.try_get::<Option<String>, _>(column_name) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Binary types
                    "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                        match row.try_get::<Option<Vec<u8>>, _>(column_name) {
                            Ok(Some(val)) => format!("[BINARY:{} bytes]", val.len()),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Date and time types - try proper types first, then fallback to string
                    "DATE" => {
                        // Try chrono::NaiveDate first
                        if let Ok(Some(val)) = row.try_get::<Option<chrono::NaiveDate>, _>(column_name) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<chrono::NaiveDate>, _>(column_name) {
                            "NULL".to_string()
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                            }
                        }
                    },
                    "TIME" => {
                        // Try chrono::NaiveTime first
                        if let Ok(Some(val)) = row.try_get::<Option<chrono::NaiveTime>, _>(column_name) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<chrono::NaiveTime>, _>(column_name) {
                            "NULL".to_string()
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                            }
                        }
                    },
                    "DATETIME" | "TIMESTAMP" => {
                        // Try chrono::NaiveDateTime first
                        if let Ok(Some(val)) = row.try_get::<Option<chrono::NaiveDateTime>, _>(column_name) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<chrono::NaiveDateTime>, _>(column_name) {
                            "NULL".to_string()
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                            }
                        }
                    },
                    // Boolean type
                    "BOOLEAN" | "BOOL" => {
                        match row.try_get::<Option<bool>, _>(column_name) {
                            Ok(Some(val)) => val.to_string(),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => {
                                // Try as tinyint (MySQL stores BOOL as TINYINT)
                                match row.try_get::<Option<i8>, _>(column_name) {
                                    Ok(Some(val)) => (val != 0).to_string(),
                                    Ok(None) => "NULL".to_string(),
                                    Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                                }
                            }
                        }
                    },
                    // JSON type - fallback to string
                    "JSON" => {
                        match row.try_get::<Option<String>, _>(column_name) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("[TYPE_ERROR:{}]", type_name)
                        }
                    },
                    // Default case: try string first, then generic fallback
                    _ => {
                        match row.try_get::<Option<String>, _>(column_name) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => {
                                // Generic fallback - try common types
                                if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(column_name) {
                                    val.to_string()
                                } else if let Ok(Some(val)) = row.try_get::<Option<f64>, _>(column_name) {
                                    val.to_string()
                                } else if let Ok(Some(val)) = row.try_get::<Option<bool>, _>(column_name) {
                                    val.to_string()
                                } else {
                                    format!("[UNKNOWN_TYPE:{}]", type_name)
                                }
                            }
                        }
                    }
                };
                
                row_data.push(value_str);
            }
            table_data.push(row_data);
        }
        
        table_data
    }

    // Helper function to convert SQLite rows to Vec<Vec<String>> with proper type checking
    fn convert_sqlite_rows_to_table_data(rows: Vec<sqlx::sqlite::SqliteRow>) -> Vec<Vec<String>> {
        use sqlx::{Row, Column, TypeInfo};
        
        let mut table_data = Vec::new();
        
        for row in &rows {
            let mut row_data = Vec::new();
            let columns = row.columns();
            
            for (col_idx, column) in columns.iter().enumerate() {
                let column_name = column.name();
                let type_info = column.type_info();
                let type_name = type_info.name();
                
                // Convert value based on SQLite type
                let value_str = match type_name {
                    // SQLite INTEGER type
                    "INTEGER" => {
                        // Try different integer sizes
                        if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<i64>, _>(col_idx) {
                            "NULL".to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<i32>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<i32>, _>(col_idx) {
                            "NULL".to_string()
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(col_idx) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("Error reading INTEGER from column {}", column_name),
                            }
                        }
                    },
                    // SQLite REAL type
                    "REAL" => {
                        if let Ok(Some(val)) = row.try_get::<Option<f64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<f64>, _>(col_idx) {
                            "NULL".to_string()
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(col_idx) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("Error reading REAL from column {}", column_name),
                            }
                        }
                    },
                    // SQLite TEXT type
                    "TEXT" => {
                        match row.try_get::<Option<String>, _>(col_idx) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => format!("Error reading TEXT from column {}", column_name),
                        }
                    },
                    // SQLite BLOB type
                    "BLOB" => {
                        match row.try_get::<Option<Vec<u8>>, _>(col_idx) {
                            Ok(Some(val)) => format!("<BLOB {} bytes>", val.len()),
                            Ok(None) => "NULL".to_string(),
                            Err(_) => {
                                // Try as string fallback
                                match row.try_get::<Option<String>, _>(col_idx) {
                                    Ok(Some(val)) => val,
                                    Ok(None) => "NULL".to_string(),
                                    Err(_) => format!("Error reading BLOB from column {}", column_name),
                                }
                            }
                        }
                    },
                    // SQLite NUMERIC/DECIMAL (stored as TEXT, INTEGER, or REAL)
                    "NUMERIC" | "DECIMAL" => {
                        // Try as number first, then string
                        if let Ok(Some(val)) = row.try_get::<Option<f64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<String>, _>(col_idx) {
                            val
                        } else if let Ok(None) = row.try_get::<Option<String>, _>(col_idx) {
                            "NULL".to_string()
                        } else {
                            format!("Error reading NUMERIC from column {}", column_name)
                        }
                    },
                    // Boolean type (stored as INTEGER 0/1)
                    "BOOLEAN" => {
                        if let Ok(Some(val)) = row.try_get::<Option<bool>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(None) = row.try_get::<Option<bool>, _>(col_idx) {
                            "NULL".to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(col_idx) {
                            // Convert 0/1 to boolean
                            match val {
                                0 => "false".to_string(),
                                1 => "true".to_string(),
                                _ => val.to_string(),
                            }
                        } else {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(col_idx) {
                                Ok(Some(val)) => val,
                                Ok(None) => "NULL".to_string(),
                                Err(_) => format!("Error reading BOOLEAN from column {}", column_name),
                            }
                        }
                    },
                    // Date and time types in SQLite (stored as TEXT, REAL, or INTEGER)
                    "DATE" | "DATETIME" | "TIMESTAMP" => {
                        // SQLite doesn't have native date types, try string first
                        match row.try_get::<Option<String>, _>(col_idx) {
                            Ok(Some(val)) => val,
                            Ok(None) => "NULL".to_string(),
                            Err(_) => {
                                // Try as integer (Unix timestamp)
                                if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(col_idx) {
                                    val.to_string()
                                } else {
                                    format!("Error reading DATE/TIME from column {}", column_name)
                                }
                            }
                        }
                    },
                    // Default case: try different types in order of preference
                    _ => {
                        // Try string first
                        if let Ok(Some(val)) = row.try_get::<Option<String>, _>(col_idx) {
                            val
                        } else if let Ok(None) = row.try_get::<Option<String>, _>(col_idx) {
                            "NULL".to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<i64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<f64>, _>(col_idx) {
                            val.to_string()
                        } else if let Ok(Some(val)) = row.try_get::<Option<bool>, _>(col_idx) {
                            val.to_string()
                        } else {
                            format!("Unsupported type '{}' in column {}", type_name, column_name)
                        }
                    }
                };
                
                row_data.push(value_str);
            }
            table_data.push(row_data);
        }
        
        table_data
    }

    fn highlight_sql_syntax(ui: &egui::Ui, text: &str) -> egui::text::LayoutJob {
        let mut job = egui::text::LayoutJob::default();
        job.text = text.to_owned();
        
        // If text is empty, return empty job
        if text.is_empty() {
            return job;
        }
        
        // SQL keywords for highlighting
        let keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
            "ALTER", "TABLE", "INDEX", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION",
            "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "AS", "AND", "OR", "NOT",
            "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "IF",
            "EXISTS", "IN", "LIKE", "BETWEEN", "GROUP", "BY", "ORDER", "HAVING",
            "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT", "COUNT", "SUM", "AVG",
            "MIN", "MAX", "ASC", "DESC", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
            "CONSTRAINT", "UNIQUE", "DEFAULT", "AUTO_INCREMENT", "SERIAL", "INT",
            "INTEGER", "VARCHAR", "TEXT", "CHAR", "DECIMAL", "FLOAT", "DOUBLE",
            "DATE", "TIME", "DATETIME", "TIMESTAMP", "BOOLEAN", "BOOL",
        ];
        
        // Define colors for different themes
        let text_color = if ui.visuals().dark_mode {
            egui::Color32::from_rgb(220, 220, 220) // Light text for dark mode
        } else {
            egui::Color32::from_rgb(40, 40, 40) // Dark text for light mode
        };
        
        let keyword_color = egui::Color32::from_rgb(86, 156, 214); // Blue - SQL keywords
        let string_color = egui::Color32::from_rgb(206, 145, 120); // Orange - strings
        let comment_color = egui::Color32::from_rgb(106, 153, 85); // Green - comments
        let number_color = egui::Color32::from_rgb(181, 206, 168); // Light green - numbers
        let function_color = egui::Color32::from_rgb(255, 206, 84); // Yellow - functions
        let operator_color = egui::Color32::from_rgb(212, 212, 212); // Light gray - operators
        
        // Process line by line to handle comments properly
        let lines: Vec<&str> = text.lines().collect();
        let mut byte_offset = 0;
        
        for (line_idx, line) in lines.iter().enumerate() {
            let line_start_offset = byte_offset;
            
            // Check if this line is a comment
            if line.trim_start().starts_with("--") {
                // Entire line is a comment
                job.sections.push(egui::text::LayoutSection {
                    leading_space: 0.0,
                    byte_range: line_start_offset..line_start_offset + line.len(),
                    format: egui::TextFormat {
                        color: comment_color,
                        font_id: egui::FontId::monospace(14.0),
                        ..Default::default()
                    },
                });
            } else {
                // Process words in the line
                let words: Vec<&str> = line.split_whitespace().collect();
                let mut line_pos = line_start_offset;
                let mut word_search_start = 0;
                
                for word in words {
                    // Find the word position in the line
                    if let Some(word_start_in_line) = line[word_search_start..].find(word) {
                        let absolute_word_start = line_start_offset + word_search_start + word_start_in_line;
                        let absolute_word_end = absolute_word_start + word.len();
                        
                        // Add whitespace before word if any
                        if absolute_word_start > line_pos {
                            job.sections.push(egui::text::LayoutSection {
                                leading_space: 0.0,
                                byte_range: line_pos..absolute_word_start,
                                format: egui::TextFormat {
                                    color: text_color,
                                    font_id: egui::FontId::monospace(14.0),
                                    ..Default::default()
                                },
                            });
                        }
                        
                        // Determine word color
                        let word_color = if word.starts_with('\'') || word.starts_with('"') {
                            // String
                            string_color
                        } else if word.chars().all(|c| c.is_ascii_digit() || c == '.') && !word.is_empty() {
                            // Number
                            number_color
                        } else if keywords.contains(&word.to_uppercase().as_str()) {
                            // SQL keyword
                            keyword_color
                        } else if word.contains('(') {
                            // Function call
                            function_color
                        } else if "(){}[]<>=!+-*/%,;".contains(word.chars().next().unwrap_or(' ')) {
                            // Operator
                            operator_color
                        } else {
                            // Default text
                            text_color
                        };
                        
                        // Add the word with appropriate color
                        job.sections.push(egui::text::LayoutSection {
                            leading_space: 0.0,
                            byte_range: absolute_word_start..absolute_word_end,
                            format: egui::TextFormat {
                                color: word_color,
                                font_id: egui::FontId::monospace(14.0),
                                ..Default::default()
                            },
                        });
                        
                        // Update positions
                        word_search_start = word_search_start + word_start_in_line + word.len();
                        line_pos = absolute_word_end;
                    }
                }
                
                // Add any remaining text in the line
                if line_pos < line_start_offset + line.len() {
                    job.sections.push(egui::text::LayoutSection {
                        leading_space: 0.0,
                        byte_range: line_pos..line_start_offset + line.len(),
                        format: egui::TextFormat {
                            color: text_color,
                            font_id: egui::FontId::monospace(14.0),
                            ..Default::default()
                        },
                    });
                }
            }
            
            // Add newline character if not the last line
            byte_offset += line.len();
            if line_idx < lines.len() - 1 {
                // Add the newline character
                job.sections.push(egui::text::LayoutSection {
                    leading_space: 0.0,
                    byte_range: byte_offset..byte_offset + 1,
                    format: egui::TextFormat {
                        color: text_color,
                        font_id: egui::FontId::monospace(14.0),
                        ..Default::default()
                    },
                });
                byte_offset += 1; // for the \n character
            }
        }
        
        job
    }

    fn render_table_data(&mut self, ui: &mut egui::Ui) {
        if !self.current_table_headers.is_empty() || !self.current_table_name.is_empty() {            
            if !self.current_table_headers.is_empty() && !self.current_table_data.is_empty() {
                // Store sort state locally to avoid borrowing issues
                let current_sort_column = self.sort_column;
                let current_sort_ascending = self.sort_ascending;
                let headers = self.current_table_headers.clone();
                let mut sort_requests = Vec::new();
                
                // Use available height instead of fixed height for responsive design
                let available_height = ui.available_height();
                egui::ScrollArea::both()
                    .max_height(available_height - 20.0) // Leave small margin for padding
                    .show(ui, |ui| {
                        egui::Grid::new("table_data_grid")
                            .striped(true)
                            .show(ui, |ui| {
                                // Render No column header first (centered)
                                ui.allocate_ui_with_layout(
                                    [60.0, ui.available_height()].into(),
                                    egui::Layout::top_down(egui::Align::Center),
                                    |ui| {
                                        ui.add(egui::Label::new(
                                            egui::RichText::new("No")
                                                .strong()
                                                .size(14.0)
                                                .color(if ui.visuals().dark_mode { 
                                                    egui::Color32::from_rgb(220, 220, 255) // Light blue for dark mode
                                                } else { 
                                                    egui::Color32::from_rgb(60, 60, 120) // Dark blue for light mode
                                                })
                                        ));
                                    }
                                );
                                
                                // Render enhanced headers with sort buttons (centered)
                                for (col_index, header) in headers.iter().enumerate() {
                                    ui.horizontal(|ui| {
                                        // Center the header content with fixed width
                                        ui.allocate_ui_with_layout(
                                            [120.0, ui.available_height()].into(),
                                            egui::Layout::top_down(egui::Align::Center),
                                            |ui| {
                                                // Header text with bold styling and better appearance
                                                ui.add(egui::Label::new(
                                                    egui::RichText::new(header)
                                                        .strong()
                                                        .size(14.0)
                                                        .color(if ui.visuals().dark_mode { 
                                                            egui::Color32::from_rgb(220, 220, 255) // Light blue for dark mode
                                                        } else { 
                                                            egui::Color32::from_rgb(60, 60, 120) // Dark blue for light mode
                                                        })
                                                ));
                                            }
                                        );
                                        
                                        // Sort button with proper icon (right aligned)
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            // Determine sort icon and state - using ASCII characters that work everywhere
                                            let (sort_icon, is_active) = if current_sort_column == Some(col_index) {
                                                if current_sort_ascending {
                                                    ("^", true) // Caret up for ascending
                                                } else {
                                                    ("v", true) // Letter v for descending  
                                                }
                                            } else {
                                                ("=", false) // Equals sign for unsorted
                                            };
                                            
                                            let sort_button = ui.add(
                                                egui::Button::new(
                                                    egui::RichText::new(sort_icon)
                                                        .size(12.0)
                                                        .color(if is_active {
                                                            egui::Color32::from_rgb(100, 150, 255) // Blue when active
                                                        } else {
                                                            egui::Color32::GRAY // Gray when inactive
                                                        })
                                                )
                                                .small()
                                                .fill(if is_active {
                                                    egui::Color32::from_rgba_unmultiplied(100, 150, 255, 50) // Light blue background when active
                                                } else {
                                                    egui::Color32::TRANSPARENT
                                                })
                                            );
                                            
                                            if sort_button.clicked() {
                                                // Toggle logic: if same column, toggle direction; if different column, start with ascending
                                                let new_ascending = if current_sort_column == Some(col_index) {
                                                    !current_sort_ascending // Toggle direction for same column
                                                } else {
                                                    true // Start with ascending for new column
                                                };
                                                sort_requests.push((col_index, new_ascending));
                                            }
                                        });
                                    });
                                }
                                ui.end_row();
                                
                                // Render data rows with row numbers
                                for (row_index, row) in self.current_table_data.iter().enumerate() {
                                    // Add row number as first column (centered with fixed width)
                                    ui.allocate_ui_with_layout(
                                        [60.0, ui.available_height()].into(),
                                        egui::Layout::top_down(egui::Align::Center),
                                        |ui| {
                                            ui.label((row_index + 1).to_string());
                                        }
                                    );
                                    
                                    // Add data cells (left-aligned)
                                    for cell in row {
                                        ui.label(cell);
                                    }
                                    ui.end_row();
                                }
                            });
                    });
                
                // Process sort requests after the UI is rendered
                for (column_index, ascending) in sort_requests {
                    self.sort_table_data(column_index, ascending);
                }
            } else if self.current_table_name.starts_with("Failed") {
                ui.colored_label(egui::Color32::RED, &self.current_table_name);
            } else {
                ui.label("No data available");
            }
        }
    }
    
    fn sort_table_data(&mut self, column_index: usize, ascending: bool) {
        if column_index >= self.current_table_headers.len() || self.current_table_data.is_empty() {
            return;
        }
        
        // Update sort state
        self.sort_column = Some(column_index);
        self.sort_ascending = ascending;
        
        // Sort the data with improved handling
        self.current_table_data.sort_by(|a, b| {
            if column_index >= a.len() || column_index >= b.len() {
                return std::cmp::Ordering::Equal;
            }
            
            let cell_a = &a[column_index];
            let cell_b = &b[column_index];
            
            // Handle NULL or empty values (put them at the end)
            let comparison = match (cell_a.as_str(), cell_b.as_str()) {
                ("NULL", "NULL") | ("", "") => std::cmp::Ordering::Equal,
                ("NULL", _) | ("", _) => std::cmp::Ordering::Greater,
                (_, "NULL") | (_, "") => std::cmp::Ordering::Less,
                (a_val, b_val) => {
                    // Try to parse as numbers first for better numeric sorting
                    match (a_val.parse::<f64>(), b_val.parse::<f64>()) {
                        (Ok(num_a), Ok(num_b)) => {
                            num_a.partial_cmp(&num_b).unwrap_or(std::cmp::Ordering::Equal)
                        },
                        _ => {
                            // Fall back to string comparison (case-insensitive)
                            a_val.to_lowercase().cmp(&b_val.to_lowercase())
                        }
                    }
                }
            };
            
            if ascending {
                comparison
            } else {
                comparison.reverse()
            }
        });
        
        let sort_direction = if ascending { "^ ascending" } else { "v descending" };
        println!("✓ Sorted table by column '{}' in {} order ({} rows)", 
            self.current_table_headers[column_index], 
            sort_direction,
            self.current_table_data.len()
        );
    }

    fn load_query_history(&mut self) {
        if let Some(pool) = &self.db_pool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                match sqlx::query_as::<_, (i64, String, i64, String, String)>(
                    "SELECT id, query_text, connection_id, connection_name, executed_at FROM query_history ORDER BY executed_at DESC LIMIT 100"
                )
                .fetch_all(pool.as_ref())
                .await
                {
                    Ok(rows) => {
                        let mut history_items = Vec::new();
                        for row in rows {
                            history_items.push(HistoryItem {
                                id: Some(row.0),
                                query: row.1,
                                connection_id: row.2,
                                connection_name: row.3,
                                executed_at: row.4,
                            });
                        }
                        history_items
                    }
                    Err(e) => {
                        println!("Failed to load query history: {}", e);
                        Vec::new()
                    }
                }
            });
            
            self.history_items = result;
            self.refresh_history_tree();
        }
    }

    fn save_query_to_history(&mut self, query: &str, connection_id: i64) {
        if let Some(pool) = &self.db_pool {
            if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
                let connection_name = connection.name.clone();
                // Limit query length to prevent memory issues (max 5000 characters)
                let query_text = if query.len() > 5000 {
                    format!("{}...", &query[..5000])
                } else {
                    query.to_string()
                };
                
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let _ = sqlx::query(
                        "INSERT INTO query_history (query_text, connection_id, connection_name) VALUES (?, ?, ?)"
                    )
                    .bind(&query_text)
                    .bind(connection_id)
                    .bind(&connection_name)
                    .execute(pool.as_ref())
                    .await;
                    
                    // Clean up old history entries if we have more than 150 entries
                    let _ = sqlx::query(
                        "DELETE FROM query_history WHERE id NOT IN (
                            SELECT id FROM query_history ORDER BY executed_at DESC LIMIT 150
                        )"
                    )
                    .execute(pool.as_ref())
                    .await;
                });
                
                // Reload history to update UI
                self.load_query_history();
            }
        }
    }

    fn refresh_history_tree(&mut self) {
        self.history_tree.clear();
        
        for item in &self.history_items {
            // Create a display name with only query preview (no connection name)
            let query_preview = if item.query.len() > 50 {
                format!("{}...", &item.query[..50])
            } else {
                item.query.clone()
            };
            
            let mut node = TreeNode::new(query_preview, NodeType::HistoryItem);
            node.connection_id = Some(item.connection_id);
            
            self.history_tree.push(node);
        }
    }
}

impl App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        // Check for background task results
        if let Some(receiver) = &self.background_receiver {
            while let Ok(result) = receiver.try_recv() {
                match result {
                    BackgroundResult::RefreshComplete { connection_id, success } => {
                        // Remove from refreshing set
                        self.refreshing_connections.remove(&connection_id);
                        
                        if success {
                            println!("Background refresh completed successfully for connection {}", connection_id);
                            // Re-expand connection node to show fresh data
                            for node in &mut self.items_tree {
                                if node.node_type == NodeType::Connection && node.connection_id == Some(connection_id) {
                                    node.is_loaded = false; // Force reload from cache
                                    node.is_expanded = true; // Expand to show databases
                                    break;
                                }
                            }
                            // Request UI repaint to show updated data
                            ctx.request_repaint();
                        } else {
                            println!("Background refresh failed for connection {}", connection_id);
                        }
                    }
                }
            }
        }

        // Disable visual indicators for active/focused elements (but keep text selection visible)
        ctx.style_mut(|style| {
            // Keep text selection visible with a subtle highlight
            style.visuals.selection.bg_fill = egui::Color32::from_rgba_premultiplied(100, 150, 255, 80);
            style.visuals.selection.stroke.color = egui::Color32::from_rgba_premultiplied(100, 150, 255, 120);
            
            // Only disable other widget visual indicators
            style.visuals.widgets.active.bg_fill = egui::Color32::TRANSPARENT;
            style.visuals.widgets.active.bg_stroke.color = egui::Color32::TRANSPARENT;
            style.visuals.widgets.hovered.bg_stroke.color = egui::Color32::TRANSPARENT;
        });
        
        // Check if we need to refresh the UI after a connection removal
        if self.needs_refresh {
            self.needs_refresh = false;
            ctx.request_repaint();
        }
        
        self.render_add_connection_dialog(ctx);
        self.render_edit_connection_dialog(ctx);
        self.render_save_dialog(ctx);
        self.render_connection_selector(ctx);
        self.render_error_dialog(ctx);

        egui::SidePanel::left("sidebar")
            .resizable(true)
            .default_width(200.0)
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    // Top section with tabs
                    ui.horizontal(|ui| {
                        // Database tab
                        let database_text = if self.selected_menu == "Database" {
                            egui::RichText::new("Database").color(egui::Color32::from_rgb(255, 130, 0)) // Orange text for active
                        } else {
                            egui::RichText::new("Database")
                        };
                        if ui.button(database_text).clicked() {
                            self.selected_menu = "Database".to_string();
                        }
                        
                        // Queries tab
                        let queries_text = if self.selected_menu == "Queries" {
                            egui::RichText::new("Queries").color(egui::Color32::from_rgb(255, 130, 0)) // Orange text for active
                        } else {
                            egui::RichText::new("Queries")
                        };
                        if ui.button(queries_text).clicked() {
                            self.selected_menu = "Queries".to_string();
                        }
                        
                        // History tab
                        let history_text = if self.selected_menu == "History" {
                            egui::RichText::new("History").color(egui::Color32::from_rgb(255, 130, 0)) // Orange text for active
                        } else {
                            egui::RichText::new("History")
                        };
                        if ui.button(history_text).clicked() {
                            self.selected_menu = "History".to_string();
                        }
                    });

                    ui.separator();

                    // Middle section with scrollable content
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        match self.selected_menu.as_str() {
                            "Database" => {
                                if self.connections.is_empty() {
                                    ui.label("No connections configured");
                                    ui.label("Click ➕ to add a new connection");
                                } else {                                
                                    // Render tree directly without mem::take to avoid race conditions
                                    self.render_tree_for_database_section(ui);
                                }
                            },
                            "Queries" => {
                                ui.label("🔍 Saved Queries");
                                ui.separator();
                                let mut queries_tree = std::mem::take(&mut self.queries_tree);
                                let _ = self.render_tree(ui, &mut queries_tree);
                                self.queries_tree = queries_tree;
                            },
                            "History" => {
                                ui.label("📜 Query History");
                                ui.separator();
                                let mut history_tree = std::mem::take(&mut self.history_tree);
                                let query_files_to_open = self.render_tree(ui, &mut history_tree);
                                self.history_tree = history_tree;
                                
                                // Handle history item clicks
                                for (display_name, _, _) in query_files_to_open {
                                    if let Some(history_item) = self.history_items.iter().find(|item| {
                                        let query_preview = if item.query.len() > 50 {
                                            format!("{}...", &item.query[..50])
                                        } else {
                                            item.query.clone()
                                        };
                                        query_preview == display_name
                                    }) {
                                        // Set the query text in the active tab
                                        if let Some(active_tab) = self.query_tabs.get_mut(self.active_tab_index) {
                                            active_tab.content = history_item.query.clone();
                                            active_tab.is_modified = true;
                                            self.editor_text = history_item.query.clone();
                                        }
                                        // Set the connection and execute automatically
                                        self.current_connection_id = Some(history_item.connection_id);
                                        // Execute the query immediately
                                        if let Some((headers, data)) = self.execute_query_with_connection(history_item.connection_id, history_item.query.clone()) {
                                            self.current_table_headers = headers;
                                            self.current_table_data = data;
                                            if self.current_table_data.is_empty() {
                                                self.current_table_name = "Query executed successfully (no results)".to_string();
                                            } else {
                                                self.current_table_name = format!("Query Results ({} rows)", self.current_table_data.len());
                                            }
                                        }
                                    }
                                }
                            },
                            _ => {}
                        }
                    });

                    // Bottom section with add button - conditional based on active tab
                    ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                        ui.separator();
                        
                        match self.selected_menu.as_str() {
                            "Database" => {
                                if ui.add_sized(
                                    [24.0, 24.0], // Small square button
                                    egui::Button::new("➕")
                                        .fill(egui::Color32::from_rgb(255, 100, 0))
                                ).on_hover_text("Add New Database Connection").clicked() {
                                    // Reset test connection status saat buka add dialog
                                    self.test_connection_status = None;
                                    self.test_connection_in_progress = false;
                                    self.show_add_connection = true;
                                }
                            },
                            "Queries" => {
                                if ui.add_sized(
                                    [24.0, 24.0], // Small square button
                                    egui::Button::new("➕")
                                        .fill(egui::Color32::from_rgb(255, 100, 60))
                                ).on_hover_text("New Query File").clicked() {
                                    // Create new tab instead of clearing editor
                                    self.create_new_tab("Untitled Query".to_string(), String::new());
                                }
                            },
                            _ => {
                                // No button for History tab
                            }
                        }
                    });
                });
            });

        egui::CentralPanel::default()
            .frame(egui::Frame::none().inner_margin(egui::Margin::ZERO)) // Remove all padding
            .show(ctx, |ui| {
            ui.vertical_centered_justified(|ui| {
                ui.spacing_mut().item_spacing.y = 0.0; // Remove vertical spacing
                ui.spacing_mut().indent = 0.0; // Remove indentation
                
                // Use egui's built-in resizable panels for smooth resizing
                let available_height = ui.available_height();
                let editor_height = available_height * self.table_split_ratio;
                
                // SQL Editor in a resizable top panel - completely flush
                egui::TopBottomPanel::top("sql_editor_panel")
                    .resizable(true)
                    .height_range(available_height * 0.1..=available_height * 0.9)
                    .default_height(editor_height)
                    .frame(egui::Frame::none().inner_margin(egui::Margin::ZERO)) // Remove panel margin
                    .show_inside(ui, |ui| {
                        ui.vertical(|ui| {
                            // Tab bar
                            ui.horizontal(|ui| {
                                ui.spacing_mut().item_spacing.x = 0.0; // Remove spacing between tabs
                                
                                // Render tabs
                                let mut tab_to_close = None;
                                let mut tab_to_switch = None;
                                
                                for (index, tab) in self.query_tabs.iter().enumerate() {
                                    let is_active = index == self.active_tab_index;
                                    let tab_color = if is_active {
                                        egui::Color32::from_rgb(255, 130, 0) // Orange for active
                                    } else {
                                        ui.visuals().text_color()
                                    };
                                    
                                    let tab_bg = egui::Color32::from_rgb(0, 0, 0); // Black background for all tabs
                                    
                                    ui.horizontal(|ui| {
                                        // Tab button
                                        let tab_response = ui.add(
                                            egui::Button::new(
                                                egui::RichText::new(&tab.title)
                                                    .color(tab_color)
                                                    .size(12.0)
                                            )
                                            .fill(tab_bg)
                                            .stroke(egui::Stroke::NONE)
                                        );
                                        
                                        if tab_response.clicked() && !is_active {
                                            tab_to_switch = Some(index);
                                        }
                                        
                                        // Close button (only show for non-active tabs or if more than 1 tab)
                                        if self.query_tabs.len() > 1 || !is_active {
                                            let close_response = ui.add_sized(
                                                [16.0, 16.0],
                                                egui::Button::new("×")
                                                    .fill(egui::Color32::TRANSPARENT)
                                                    .stroke(egui::Stroke::NONE)
                                            );
                                            
                                            if close_response.clicked() {
                                                tab_to_close = Some(index);
                                            }
                                        }
                                    });
                                }
                                
                                // New tab button
                                if ui.add_sized(
                                    [24.0, 24.0],
                                    egui::Button::new("+")
                                        .fill(egui::Color32::from_rgb(60, 60, 60))
                                ).clicked() {
                                    self.create_new_tab("Untitled Query".to_string(), String::new());
                                }
                                
                                
                                // Handle tab operations
                                if let Some(index) = tab_to_close {
                                    self.close_tab(index);
                                }
                                if let Some(index) = tab_to_switch {
                                    self.switch_to_tab(index);
                                }
                            });
                            
                            ui.separator();
                            
                            // Update current tab content with editor changes
                            if let Some(current_tab) = self.query_tabs.get_mut(self.active_tab_index) {
                                if current_tab.content != self.editor_text {
                                    current_tab.content = self.editor_text.clone();
                                    current_tab.is_modified = true;
                                }
                            }
                            
                            // SQL Editor area
                            egui::Frame::none()
                                .fill(if ui.visuals().dark_mode { 
                                    egui::Color32::from_rgb(30, 30, 30) // Slightly darker for contrast
                                } else { 
                                    egui::Color32::WHITE // Pure white for light mode
                                })
                                .inner_margin(egui::Margin::ZERO) // No padding for compact design
                                .show(ui, |ui| {
                                    // Use custom layouter with syntax highlighting (always enabled)
                                    let mut layouter = |ui: &egui::Ui, string: &str, wrap_width: f32| {
                                        let mut layout_job = Self::highlight_sql_syntax(ui, string);
                                        layout_job.wrap.max_width = wrap_width;
                                        ui.fonts(|f| f.layout_job(layout_job))
                                    };
                                    
                                    ui.add_sized(
                                        ui.available_size(),
                                        egui::TextEdit::multiline(&mut self.editor_text)
                                            .font(egui::FontId::monospace(14.0))
                                            .code_editor()
                                            .desired_rows(15)
                                            .hint_text("-- Enter your SQL query here\n-- Example: SELECT * FROM table_name;\n-- Press Ctrl+Enter (Cmd+Enter on Mac) to execute")
                                            .layouter(&mut layouter),
                                    );
                                    
                                    // Check for Ctrl+Enter or Cmd+Enter to execute query
                                    if ui.input(|i| {
                                        (i.modifiers.ctrl || i.modifiers.mac_cmd) && i.key_pressed(egui::Key::Enter)
                                    }) && !self.editor_text.trim().is_empty() {
                                        if self.current_connection_id.is_some() {
                                            // Connection is already selected, execute query
                                            self.execute_query();
                                        } else if !self.connections.is_empty() {
                                            // No connection selected but connections exist, show selector
                                            self.pending_query = self.editor_text.clone();
                                            self.auto_execute_after_connection = true;
                                            self.show_connection_selector = true;
                                        } else {
                                            // No connections exist, show error dialog
                                            self.error_message = "No database connections available.\n\nPlease add a connection first by clicking the '+' button in the Database panel.".to_string();
                                            self.show_error_message = true;
                                        }
                                    }
                                    
                                    // Check for Ctrl+S or Cmd+S to save
                                    if ui.input(|i| {
                                        (i.modifiers.ctrl || i.modifiers.mac_cmd) && i.key_pressed(egui::Key::S)
                                    }) {
                                        if let Err(err) = self.save_current_tab() {
                                            println!("Failed to save: {}", err);
                                        }
                                    }
                                });
                        });
                        
                        // Update split ratio when panel is resized
                        let current_height = ui.min_rect().height();
                        self.table_split_ratio = (current_height / available_height).clamp(0.1, 0.9);
                    });
                
                // Table data in the remaining space
                egui::CentralPanel::default()
                    .frame(egui::Frame::none().inner_margin(egui::Margin::ZERO)) // Remove all padding
                    .show_inside(ui, |ui| {
                    self.render_table_data(ui);
                });
            });
        });

        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(format!("Active Tab: {}", self.get_active_tab_title()));
                ui.separator();
                ui.label(format!("Lines: {}", self.editor_text.lines().count()));
                ui.separator();
                ui.label(format!("Tabs: {}", self.query_tabs.len()));
                ui.separator();
                ui.label(format!("Connections: {}", self.connections.len()));
                ui.separator();
                if let Some(connection_id) = self.current_connection_id {
                    if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
                        ui.label(format!("Connected: {}", connection.name));
                        ui.separator();
                    }
                }
                ui.label(format!("Showing {} rows", self.current_table_data.len()));
            });
        });
    }
}
