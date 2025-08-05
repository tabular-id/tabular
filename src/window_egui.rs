
use eframe::{egui, App, Frame};
use sqlx::{SqlitePool, mysql::MySqlPoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use redis::{Client, aio::ConnectionManager};
use egui_code_editor::{CodeEditor, ColorTheme};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};

use crate::{
    cache_data, directory, 
    driver_mysql, driver_postgres, 
    driver_redis, driver_sqlite, 
    export, helpers, models, modules,
    connection
};





pub struct Tabular {
    pub editor_text: String,
    pub selected_menu: String,
    pub items_tree: Vec<models::structs::TreeNode>,
    pub queries_tree: Vec<models::structs::TreeNode>,
    pub history_tree: Vec<models::structs::TreeNode>,
    pub history_items: Vec<models::structs::HistoryItem>, // Actual history data
    pub connections: Vec<models::structs::ConnectionConfig>,
    pub show_add_connection: bool,
    pub new_connection: models::structs::ConnectionConfig,
    pub db_pool: Option<Arc<SqlitePool>>,
    // Global async runtime for all database operations
    pub runtime: Option<Arc<tokio::runtime::Runtime>>,
    // Connection cache untuk menghindari membuat koneksi berulang
    pub connection_pools: HashMap<i64, models::enums::DatabasePool>,
    // Context menu and edit connection fields
    pub show_edit_connection: bool,
    pub edit_connection: models::structs::ConnectionConfig,
    // UI refresh flag
    pub needs_refresh: bool,
    // Table data display
    pub current_table_data: Vec<Vec<String>>,
    pub current_table_headers: Vec<String>,
    pub current_table_name: String,
    pub current_connection_id: Option<i64>,
    // Pagination
    pub current_page: usize,
    pub page_size: usize,
    pub total_rows: usize,
    pub all_table_data: Vec<Vec<String>>, // Store all data for pagination
    // Splitter position for resizable table view (0.0 to 1.0)
    pub table_split_ratio: f32,
    // Table sorting state
    pub sort_column: Option<usize>,
    pub sort_ascending: bool,
    // Test connection status
    pub test_connection_status: Option<(bool, String)>, // (success, message)
    pub test_connection_in_progress: bool,
    // Background processing channels
    pub background_sender: Option<Sender<models::enums::BackgroundTask>>,
    pub background_receiver: Option<Receiver<models::enums::BackgroundResult>>,
    // Background refresh status tracking
    pub refreshing_connections: std::collections::HashSet<i64>,
    // Query tab system
    pub query_tabs: Vec<models::structs::QueryTab>,
    pub active_tab_index: usize,
    pub next_tab_id: usize,
    // Save dialog
    pub show_save_dialog: bool,
    pub save_filename: String,
    // Connection selection dialog
    pub show_connection_selector: bool,
    pub pending_query: String, // Store query to execute after connection is selected
    pub auto_execute_after_connection: bool, // Flag to auto-execute after connection selected
    // Error message display
    pub error_message: String,
    pub show_error_message: bool,
    // Advanced Editor Configuration
    pub advanced_editor: models::structs::AdvancedEditor,
    // Selected text for executing only selected queries
    pub selected_text: String,
    // Command Palette
    pub show_command_palette: bool,
    pub command_palette_input: String,
    pub show_theme_selector: bool,
    pub command_palette_items: Vec<String>,
    pub command_palette_selected_index: usize,
    pub theme_selector_selected_index: usize,
    // Flag to request theme selector on next frame
    pub request_theme_selector: bool,
    // Database search functionality
    pub database_search_text: String,
    pub filtered_items_tree: Vec<models::structs::TreeNode>,
    pub show_search_results: bool,
    // Query folder management
    pub show_create_folder_dialog: bool,
    pub new_folder_name: String,
    pub selected_query_for_move: Option<String>,
    pub show_move_to_folder_dialog: bool,
    pub target_folder_name: String,
    pub parent_folder_for_creation: Option<String>,
    pub selected_folder_for_removal: Option<String>,
    pub folder_removal_map: std::collections::HashMap<i64, String>, // Map hash to folder path
    // Connection pool cleanup tracking
    pub last_cleanup_time: std::time::Instant,
    // Table selection tracking
    pub selected_row: Option<usize>,
    pub selected_cell: Option<(usize, usize)>, // (row_index, column_index)
    // Column width management for resizable columns
    pub column_widths: Vec<f32>, // Store individual column widths
    pub min_column_width: f32,
    pub max_column_width: f32,
}



impl Tabular {


    pub fn new() -> Self {
        // Create background processing channels
        let (background_sender, background_receiver) = mpsc::channel::<models::enums::BackgroundTask>();
        let (result_sender, result_receiver) = mpsc::channel::<models::enums::BackgroundResult>();

        // Create shared runtime for all database operations
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => Some(Arc::new(rt)),
            Err(e) => {
                println!("Failed to create runtime: {}", e);
                None
            }
        };

        let mut app = Self {
            editor_text: String::new(),
            selected_menu: "Database".to_string(),
            items_tree: Vec::new(),
            queries_tree: Vec::new(),
            history_tree: Vec::new(),
            history_items: Vec::new(),
            connections: Vec::new(),
            show_add_connection: false,
            new_connection: models::structs::ConnectionConfig::default(),
            db_pool: None,
            runtime,
            connection_pools: HashMap::new(), // Start with empty cache
            show_edit_connection: false,
            edit_connection: models::structs::ConnectionConfig::default(),
            needs_refresh: false,
            current_table_data: Vec::new(),
            current_table_headers: Vec::new(),
            current_table_name: String::new(),
            current_connection_id: None,
            current_page: 0,
            page_size: 100, // Default 100 rows per page
            total_rows: 0,
            all_table_data: Vec::new(),
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
            advanced_editor: models::structs::AdvancedEditor::default(),
            selected_text: String::new(),
            show_command_palette: false,
            command_palette_input: String::new(),
            show_theme_selector: false,
            command_palette_items: Vec::new(),
            command_palette_selected_index: 0,
            theme_selector_selected_index: 0,
            request_theme_selector: false,
            // Database search functionality
            database_search_text: String::new(),
            filtered_items_tree: Vec::new(),
            show_search_results: false,
            // Query folder management
            show_create_folder_dialog: false,
            new_folder_name: String::new(),
            selected_query_for_move: None,
            show_move_to_folder_dialog: false,
            target_folder_name: String::new(),
            parent_folder_for_creation: None,
            selected_folder_for_removal: None,
            folder_removal_map: std::collections::HashMap::new(),
            last_cleanup_time: std::time::Instant::now(),
            selected_row: None,
            selected_cell: None,
            // Column width management
            column_widths: Vec::new(),
            min_column_width: 50.0,
            max_column_width: 600.0,
        };
        
        // Clear any old cached pools
        app.connection_pools.clear();
        
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

    fn start_background_worker(&self, task_receiver: Receiver<models::enums::BackgroundTask>, result_sender: Sender<models::enums::BackgroundResult>) {
        // Get the current db_pool for cache operations
        let db_pool = self.db_pool.clone();
        
        std::thread::spawn(move || {
            // Create a single-threaded Tokio runtime for this background thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            
            while let Ok(task) = task_receiver.recv() {
                match task {
                    models::enums::BackgroundTask::RefreshConnection { connection_id } => {
                        let success = rt.block_on(async {
                            Self::refresh_connection_background_async(
                                connection_id,
                                &db_pool
                            ).await
                        });
                        
                        let _ = result_sender.send(models::enums::BackgroundResult::RefreshComplete {
                            connection_id,
                            success,
                        });
                    }
                }
            }
        });
    }

    async fn refresh_connection_background_async(
        connection_id: i64,
        db_pool: &Option<Arc<SqlitePool>>,
    ) -> bool {

        println!("Refreshing connection with ID: {}", connection_id);

        // Get connection from database
        if let Some(cache_pool_arc) = db_pool {
            let connection_result = sqlx::query_as::<_, (i64, String, String, String, String, String, String, String)>(
                "SELECT id, name, host, port, username, password, database_name, connection_type FROM connections WHERE id = ?"
            )
            .bind(connection_id)
            .fetch_optional(cache_pool_arc.as_ref())
            .await;
            
            if let Ok(Some((id, name, host, port, username, password, database_name, connection_type))) = connection_result {
                let connection = models::structs::ConnectionConfig {
                    id: Some(id),
                    name,
                    host,
                    port,
                    username,
                    password,
                    database: database_name,
                    connection_type: match connection_type.as_str() {
                        "MySQL" => models::enums::DatabaseType::MySQL,
                        "PostgreSQL" => models::enums::DatabaseType::PostgreSQL,
                        "Redis" => models::enums::DatabaseType::Redis,
                        _ => models::enums::DatabaseType::SQLite,
                    },
                    folder: None, // Will be loaded from database later
                };
                
                // Clear cache
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
                match tokio::time::timeout(
                    std::time::Duration::from_secs(30), // 30 second timeout
                    Self::create_database_pool(&connection)
                ).await {
                    Ok(Some(new_pool)) => {
                        Self::fetch_and_cache_all_data(connection_id, &connection, &new_pool, cache_pool_arc.as_ref()).await
                    }
                    Ok(None) => {
                        false
                    }
                    Err(_) => {
                        false
                    }
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    async fn create_database_pool(connection: &models::structs::ConnectionConfig) -> Option<models::enums::DatabasePool> {
        match connection.connection_type {
            models::enums::DatabaseType::MySQL => {
                let encoded_username = modules::url_encode(&connection.username);
                let encoded_password = modules::url_encode(&connection.password);
                let connection_string = format!(
                    "mysql://{}:{}@{}:{}/{}",
                    encoded_username, encoded_password, connection.host, connection.port, connection.database
                );
                
                match MySqlPoolOptions::new()
                    .max_connections(3) // Reduced from 5 to 3
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        Some(models::enums::DatabasePool::MySQL(Arc::new(pool)))
                    }
                    Err(_e) => {
                        None
                    }
                }
            }
            models::enums::DatabaseType::PostgreSQL => {
                let connection_string = format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    connection.username, connection.password, connection.host, connection.port, connection.database
                );
                
                match PgPoolOptions::new()
                    .max_connections(3)
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        Some(models::enums::DatabasePool::PostgreSQL(Arc::new(pool)))
                    }
                    Err(_e) => {
                        None
                    }
                }
            }
            models::enums::DatabaseType::SQLite => {
                let connection_string = format!("sqlite:{}", connection.host);
                
                
                match SqlitePoolOptions::new()
                    .max_connections(3)
                    .min_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(300))
                    .connect(&connection_string)
                    .await
                {
                    Ok(pool) => {
                        Some(models::enums::DatabasePool::SQLite(Arc::new(pool)))
                    }
                    Err(_e) => {
                        None
                    }
                }
            }
            models::enums::DatabaseType::Redis => {
                let connection_string = if connection.password.is_empty() {
                    format!("redis://{}:{}", connection.host, connection.port)
                } else {
                    format!("redis://{}:{}@{}:{}", connection.username, connection.password, connection.host, connection.port)
                };
                
                match Client::open(connection_string) {
                    Ok(client) => {
                        match ConnectionManager::new(client).await {
                            Ok(manager) => Some(models::enums::DatabasePool::Redis(Arc::new(manager))),
                            Err(_e) => None,
                        }
                    }
                    Err(_e) => None,
                }
            }
        }
    }

    async fn fetch_and_cache_all_data(
        connection_id: i64,
        connection: &models::structs::ConnectionConfig,
        pool: &models::enums::DatabasePool,
        cache_pool: &SqlitePool,
    ) -> bool {
        match &connection.connection_type {
            models::enums::DatabaseType::MySQL => {
                if let models::enums::DatabasePool::MySQL(mysql_pool) = pool {
                    driver_mysql::fetch_mysql_data(connection_id, mysql_pool, cache_pool).await
                } else {
                    false
                }
            }
            models::enums::DatabaseType::SQLite => {
                if let models::enums::DatabasePool::SQLite(sqlite_pool) = pool {
                    driver_sqlite::fetch_data(connection_id, sqlite_pool, cache_pool).await
                } else {
                    false
                }
            }
            models::enums::DatabaseType::PostgreSQL => {
                if let models::enums::DatabasePool::PostgreSQL(postgres_pool) = pool {
                    driver_postgres::fetch_postgres_data(connection_id, postgres_pool, cache_pool).await
                } else {
                    false
                }
            }
            models::enums::DatabaseType::Redis => {
                if let models::enums::DatabasePool::Redis(redis_manager) = pool {
                    driver_redis::fetch_redis_data(connection_id, redis_manager, cache_pool).await
                } else {
                    false
                }
            }
        }
    }



    fn initialize_database(&mut self) {
        // Ensure app directories exist
        if let Err(e) = directory::ensure_app_directories() {
            println!("Failed to create app directories: {}", e);
            return;
        }
        
        // Initialize SQLite database
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool_result = rt.block_on(async {
            // Get the data directory path
            let data_dir = directory::get_data_dir();
            let db_path = data_dir.join("connections.db");
                        
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
                            connection_type TEXT NOT NULL,
                            folder TEXT DEFAULT NULL
                        )
                        "#
                    )
                    .execute(&pool)
                    .await;
                    
                    // Add folder column if it doesn't exist (for existing databases)
                    let _ = sqlx::query(
                        "ALTER TABLE connections ADD COLUMN folder TEXT DEFAULT NULL"
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

        // Don't add sample queries - let load_queries_from_directory handle the real structure
        // self.queries_tree will be populated by load_queries_from_directory()

        // Initialize empty history tree (will be loaded from database)
        self.refresh_history_tree();
    }

    fn refresh_connections_tree(&mut self) {
                
        // Clear existing tree
        self.items_tree.clear();

        // Create folder structure for connections
        self.items_tree = self.create_connections_folder_structure();
            
        
    }

    fn create_connections_folder_structure(&self) -> Vec<models::structs::TreeNode> {
        // Group connections by custom folder first, then by database type
        let mut folder_groups: std::collections::HashMap<String, Vec<&models::structs::ConnectionConfig>> = std::collections::HashMap::new();
        
        // Group connections by custom folder
        for conn in &self.connections {
            let folder_name = conn.folder.as_ref().unwrap_or(&"Default".to_string()).clone();
            folder_groups.entry(folder_name).or_insert_with(Vec::new).push(conn);
        }
        
        let mut result = Vec::new();
        
        // Create folder structure for each custom folder
        for (folder_name, connections) in folder_groups {
            if connections.is_empty() {
                continue;
            }
            
            // Create custom folder node
            let mut custom_folder = models::structs::TreeNode::new(folder_name.clone(), models::enums::NodeType::CustomFolder);
            custom_folder.is_expanded = false; // Start collapsed
            
            // Within each custom folder, group by database type
            let mut mysql_connections = Vec::new();
            let mut postgresql_connections = Vec::new();
            let mut sqlite_connections = Vec::new();
            let mut redis_connections = Vec::new();
            
            for conn in connections {
                if let Some(id) = conn.id {
                    let node = models::structs::TreeNode::new_connection(conn.name.clone(), id);
                    match conn.connection_type {
                        models::enums::DatabaseType::MySQL => {
                            mysql_connections.push(node);
                        },
                        models::enums::DatabaseType::PostgreSQL => {
                            postgresql_connections.push(node);
                        },
                        models::enums::DatabaseType::SQLite => {
                            sqlite_connections.push(node);
                        },
                        models::enums::DatabaseType::Redis => {
                            redis_connections.push(node);
                        },
                    }
                } else {
                    println!("  -> Skipping connection with no ID");
                }
            }
            
            // Create database type folders within custom folder
            let mut db_type_folders = Vec::new();
                        
            if !mysql_connections.is_empty() {
                let _ = mysql_connections.len();
                let mut mysql_folder = models::structs::TreeNode::new("MySQL".to_string(), models::enums::NodeType::MySQLFolder);
                mysql_folder.children = mysql_connections;
                mysql_folder.is_expanded = false;
                db_type_folders.push(mysql_folder);
            }
            
            if !postgresql_connections.is_empty() {
                let _ = postgresql_connections.len();
                let mut postgresql_folder = models::structs::TreeNode::new("PostgreSQL".to_string(), models::enums::NodeType::PostgreSQLFolder);
                postgresql_folder.children = postgresql_connections;
                postgresql_folder.is_expanded = false;
                db_type_folders.push(postgresql_folder);
            }
            
            if !sqlite_connections.is_empty() {
                let _ = sqlite_connections.len();
                let mut sqlite_folder = models::structs::TreeNode::new("SQLite".to_string(), models::enums::NodeType::SQLiteFolder);
                sqlite_folder.children = sqlite_connections;
                sqlite_folder.is_expanded = false;
                db_type_folders.push(sqlite_folder);
            }
            
            if !redis_connections.is_empty() {
                let _ = redis_connections.len();
                let mut redis_folder = models::structs::TreeNode::new("Redis".to_string(), models::enums::NodeType::RedisFolder);
                redis_folder.children = redis_connections;
                redis_folder.is_expanded = false;
                db_type_folders.push(redis_folder);
            }
            
            custom_folder.children = db_type_folders;
            result.push(custom_folder);
        }
        
        // Sort folders alphabetically, but put "Default" first
        result.sort_by(|a, b| {
            if a.name == "Default" {
                std::cmp::Ordering::Less
            } else if b.name == "Default" {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });
        
        if result.is_empty() {
            println!("No connections found, returning empty tree");
        }
        
        result
    }

    // Tab management methods
    fn create_new_tab(&mut self, title: String, content: String) -> usize {
        let tab_id = self.next_tab_id;
        self.next_tab_id += 1;
        
        let new_tab = models::structs::QueryTab {
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
            let query_dir = directory::get_query_dir();
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

        let query_dir = directory::get_query_dir();
        self.queries_tree = directory::load_directory_recursive(&query_dir);
        
        // Sort folders and files alphabetically
        self.queries_tree.sort_by(|a, b| {
            match (&a.node_type, &b.node_type) {
                (models::enums::NodeType::QueryFolder, models::enums::NodeType::Query) => std::cmp::Ordering::Less, // Folders first
                (models::enums::NodeType::Query, models::enums::NodeType::QueryFolder) => std::cmp::Ordering::Greater, // Files after folders
                _ => a.name.cmp(&b.name), // Alphabetical within same type
            }
        });
    }

    fn create_query_folder(&mut self, folder_name: &str) -> Result<(), String> {
        if folder_name.trim().is_empty() {
            return Err("Folder name cannot be empty".to_string());
        }

        let query_dir = directory::get_query_dir();
        let folder_path = query_dir.join(folder_name);
        
        if folder_path.exists() {
            return Err("Folder already exists".to_string());
        }
        
        std::fs::create_dir_all(&folder_path)
            .map_err(|e| format!("Failed to create folder: {}", e))?;
            
        // Refresh the queries tree
        self.load_queries_from_directory();
        
        Ok(())
    }

    fn create_query_folder_in_parent(&mut self, folder_name: &str, parent_folder: &str) -> Result<(), String> {
        if folder_name.trim().is_empty() {
            return Err("Folder name cannot be empty".to_string());
        }

        let query_dir = directory::get_query_dir();
        let parent_path = query_dir.join(parent_folder);
        
        if !parent_path.exists() || !parent_path.is_dir() {
            return Err(format!("Parent folder '{}' does not exist", parent_folder));
        }
        
        let folder_path = parent_path.join(folder_name);
        
        if folder_path.exists() {
            return Err(format!("Folder '{}' already exists in '{}'", folder_name, parent_folder));
        }
        
        std::fs::create_dir_all(&folder_path)
            .map_err(|e| format!("Failed to create folder: {}", e))?;
            
        // Refresh the queries tree
        self.load_queries_from_directory();
        
        Ok(())
    }

    fn move_query_to_folder(&mut self, query_file_path: &str, target_folder: &str) -> Result<(), String> {
        let source_path = std::path::Path::new(query_file_path);
        let file_name = source_path.file_name()
            .ok_or("Invalid file path")?;
            
        let query_dir = directory::get_query_dir();
        let target_folder_path = query_dir.join(target_folder);
        let target_file_path = target_folder_path.join(file_name);
        
        // Create target folder if it doesn't exist
        std::fs::create_dir_all(&target_folder_path)
            .map_err(|e| format!("Failed to create target folder: {}", e))?;
            
        // Move the file
        std::fs::rename(source_path, &target_file_path)
            .map_err(|e| format!("Failed to move file: {}", e))?;
            
        // Close any open tabs for this file and update with new path
        self.close_tabs_for_file(query_file_path);
        
        // Refresh the queries tree
        self.load_queries_from_directory();
        
        Ok(())
    }

    fn move_query_to_root(&mut self, query_file_path: &str) -> Result<(), String> {
        let source_path = std::path::Path::new(query_file_path);
        let file_name = source_path.file_name()
            .ok_or("Invalid file path")?;
            
        let query_dir = directory::get_query_dir();
        let target_file_path = query_dir.join(file_name);
        
        // Move the file to root
        std::fs::rename(source_path, &target_file_path)
            .map_err(|e| format!("Failed to move file: {}", e))?;
            
        // Close any open tabs for this file and update with new path
        self.close_tabs_for_file(query_file_path);
        
        // Refresh the queries tree
        self.load_queries_from_directory();
        
        Ok(())
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
                        if ui.button("Save").clicked() && !self.save_filename.is_empty() {
                            if let Err(err) = self.save_current_tab_with_name(self.save_filename.clone()) {
                                println!("Failed to save: {}", err);
                            }
                            self.show_save_dialog = false;
                            self.save_filename.clear();
                        }
                        
                        if ui.button("Cancel").clicked() {
                            self.show_save_dialog = false;
                            self.save_filename.clear();
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

    fn render_create_folder_dialog(&mut self, ctx: &egui::Context) {
        if self.show_create_folder_dialog {
            let window_title = if let Some(ref parent) = self.parent_folder_for_creation {
                format!("Create Folder in '{}'", parent)
            } else {
                "Create Query Folder".to_string()
            };
            
            egui::Window::new(window_title)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    if let Some(ref parent) = self.parent_folder_for_creation {
                        ui.label(format!("Creating folder inside: {}", parent));
                        ui.separator();
                    }
                    
                    ui.label("Folder name:");
                    ui.text_edit_singleline(&mut self.new_folder_name);
                    ui.separator();
                    
                    ui.horizontal(|ui| {
                        if ui.button("Create").clicked() {
                            let folder_name = self.new_folder_name.clone();
                            let parent_folder = self.parent_folder_for_creation.clone();
                            
                            let result = if let Some(parent) = parent_folder {
                                self.create_query_folder_in_parent(&folder_name, &parent)
                            } else {
                                self.create_query_folder(&folder_name)
                            };
                            
                            if let Err(err) = result {
                                self.error_message = err;
                                self.show_error_message = true;
                            } else {
                                // Force immediate UI repaint after successful folder creation
                                ui.ctx().request_repaint();
                            }
                            
                            self.show_create_folder_dialog = false;
                            self.new_folder_name.clear();
                            self.parent_folder_for_creation = None;
                        }
                        
                        if ui.button("Cancel").clicked() {
                            self.show_create_folder_dialog = false;
                            self.new_folder_name.clear();
                            self.parent_folder_for_creation = None;
                        }
                    });
                });
        }
    }

    fn render_move_to_folder_dialog(&mut self, ctx: &egui::Context) {
        if self.show_move_to_folder_dialog {
            egui::Window::new("Move Query to Folder")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    if let Some(query_path) = &self.selected_query_for_move {
                        let file_name = std::path::Path::new(query_path)
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("Unknown");
                        ui.label(format!("Moving: {}", file_name));
                        ui.separator();
                    }
                    
                    ui.label("Target folder:");
                    ui.text_edit_singleline(&mut self.target_folder_name);
                    ui.small("(Leave empty to move to root, or enter folder name)");
                    ui.separator();
                    
                    ui.horizontal(|ui| {
                        if ui.button("Move").clicked() {
                            if let Some(query_path) = self.selected_query_for_move.clone() {
                                if self.target_folder_name.trim().is_empty() {
                                    // Move to root
                                    if let Err(err) = self.move_query_to_root(&query_path) {
                                        self.error_message = err;
                                        self.show_error_message = true;
                                    }
                                } else if let Err(err) = self.move_query_to_folder(&query_path, &self.target_folder_name.clone()) {
                                    self.error_message = err;
                                    self.show_error_message = true;
                                }
                            }
                            self.show_move_to_folder_dialog = false;
                            self.selected_query_for_move = None;
                            self.target_folder_name.clear();
                        }
                        
                        if ui.button("Cancel").clicked() {
                            self.show_move_to_folder_dialog = false;
                            self.selected_query_for_move = None;
                            self.target_folder_name.clear();
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
        let new_tab = models::structs::QueryTab {
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

    fn render_tree(&mut self, ui: &mut egui::Ui, nodes: &mut [models::structs::TreeNode]) -> Vec<(String, String, String)> {
        let mut expansion_requests = Vec::new();
        let mut tables_to_expand = Vec::new();
        let mut context_menu_requests = Vec::new();
        let mut table_click_requests = Vec::new();
        let mut connection_click_requests = Vec::new();
        let mut query_files_to_open = Vec::new();
        
        for (index, node) in nodes.iter_mut().enumerate() {
            let (expansion_request, table_expansion, context_menu_request, table_click_request, connection_click_request, query_file_to_open, folder_for_removal, parent_for_creation, folder_removal_mapping) = Self::render_tree_node_with_table_expansion(ui, node, &mut self.editor_text, index, &self.refreshing_connections);
            if let Some(expansion_req) = expansion_request {
                expansion_requests.push(expansion_req);
            }
            if let Some((table_index, connection_id, table_name)) = table_expansion {
                tables_to_expand.push((table_index, connection_id, table_name));
            }
            if let Some(folder_name) = folder_for_removal {
                self.selected_folder_for_removal = Some(folder_name.clone());
            }
            if let Some((hash, folder_path)) = folder_removal_mapping {
                self.folder_removal_map.insert(hash, folder_path);
                println!("📁 Stored folder removal mapping: hash={} -> path={}", hash, self.folder_removal_map.get(&hash).unwrap_or(&"NONE".to_string()));
            }
            if let Some(parent_folder) = parent_for_creation {
                self.parent_folder_for_creation = Some(parent_folder);
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
                println!("📋 Collected query file to open: {} (path: {})", filename, file_path);
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
            
            match expansion_req.node_type {
                models::enums::NodeType::Connection => {
                    // Find Connection node recursively and load if not already loaded
                    if let Some(connection_node) = Self::find_connection_node_recursive(nodes, expansion_req.connection_id) {
                        if !connection_node.is_loaded {
                            self.load_connection_tables(expansion_req.connection_id, connection_node);
                        }
                    } else {
                        println!("Connection node not found for ID: {}", expansion_req.connection_id);
                    }
                },
                models::enums::NodeType::DatabasesFolder => {
                    // Handle DatabasesFolder expansion - load actual databases from server
                    for node in nodes.iter_mut() {
                        if node.node_type == models::enums::NodeType::Connection && node.connection_id == Some(expansion_req.connection_id) {
                            // Find the DatabasesFolder within this connection
                            for child in &mut node.children {
                                if child.node_type == models::enums::NodeType::DatabasesFolder && !child.is_loaded {
                                    self.load_databases_for_folder(expansion_req.connection_id, child);
                                    break;
                                }
                            }
                            break;
                        }
                    }
                },
                models::enums::NodeType::Database => {
                    println!("🔍 Database expansion request received for connection_id: {}, database_name: {:?}", 
                             expansion_req.connection_id, expansion_req.database_name);
                    
                    // Handle Database expansion for Redis - load keys for the database
                    if let Some(connection) = self.connections.iter().find(|c| c.id == Some(expansion_req.connection_id)) {
                        println!("✅ Found connection: {} (type: {:?})", connection.name, connection.connection_type);
                        
                        if connection.connection_type == models::enums::DatabaseType::Redis {
                            println!("🔑 Processing Redis database expansion");
                            
                            // Find the database node and load its keys
                            let mut node_found = false;
                            for (node_idx, node) in nodes.iter_mut().enumerate() {
                                println!("🌳 Checking tree node [{}]: '{}' (type: {:?}, connection_id: {:?})", 
                                         node_idx, node.name, node.node_type, node.connection_id);
                                
                                if let Some(db_node) = Self::find_redis_database_node(node, expansion_req.connection_id, &expansion_req.database_name) {
                                    println!("📁 Found database node: {}, is_loaded: {}", db_node.name, db_node.is_loaded);
                                    node_found = true;
                                    
                                    if !db_node.is_loaded {
                                        println!("⏳ Loading keys for database: {}", expansion_req.database_name.clone().unwrap_or_default());
                                        self.load_redis_keys_for_database(expansion_req.connection_id, &expansion_req.database_name.clone().unwrap_or_default(), db_node);
                                    } else {
                                        println!("✅ Database already loaded with {} children", db_node.children.len());
                                    }
                                    break;
                                }
                            }
                            
                            if !node_found {
                                println!("❌ Database node not found in any tree branch for database: {:?}", expansion_req.database_name);
                            }
                        } else {
                            println!("❌ Connection is not Redis type: {:?}", connection.connection_type);
                        }
                    } else {
                        println!("❌ Connection not found for ID: {}", expansion_req.connection_id);
                    }
                },
                models::enums::NodeType::TablesFolder | models::enums::NodeType::ViewsFolder | models::enums::NodeType::StoredProceduresFolder |
                models::enums::NodeType::UserFunctionsFolder | models::enums::NodeType::TriggersFolder | models::enums::NodeType::EventsFolder => {
                    // Find the specific folder node and load if not already loaded
                    
                    // We need to find the exact folder node in the tree
                    let connection_id = expansion_req.connection_id;
                    let folder_type = expansion_req.node_type.clone();
                    let database_name = expansion_req.database_name.clone();
                    
                    // Search for folder node by traversing the tree recursively
                    let mut found = false;
                    for node in nodes.iter_mut() {
                        // Search recursively through all nodes, not just top level
                        if let Some(folder_node) = Self::find_specific_folder_node(node, connection_id, &folder_type, &database_name) {
                            if !folder_node.is_loaded {
                                self.load_folder_content(connection_id, folder_node, folder_type.clone());
                                found = true;
                            }
                            break;
                        }
                    }
                    if !found {
                        println!("Could not find folder node with type {:?} and database {:?} in any of the nodes", folder_type, database_name);
                    }
                },
                _ => {
                    println!("Unhandled node type: {:?}", expansion_req.node_type);
                }
            }
        }
        
        // Handle table column expansions
        // Handle table expansions
        for (table_index, connection_id, table_name) in tables_to_expand {
            self.load_table_columns_for_node(connection_id, &table_name, nodes, table_index);
        }
        
        // Handle table click requests - create new tab for each table
        for (connection_id, table_name) in table_click_requests {
            // Find the connection to determine the database type
            let connection_type = self.connections.iter()
                .find(|conn| conn.id == Some(connection_id))
                .map(|conn| &conn.connection_type);
            
            match connection_type {
                Some(models::enums::DatabaseType::Redis) => {
                    
                    // Check if this is a Redis key (has specific Redis data types in the tree structure)
                    // For Redis keys, we need to find which database they belong to
                    let mut is_redis_key = false;
                    let mut key_type: Option<String> = None;
                    
                    for node in nodes.iter() {
                        if let Some((_, k_type)) = self.find_redis_key_info(node, &table_name) {
                            key_type = Some(k_type.clone());
                            is_redis_key = true;
                            break;
                        }
                    }
                    
                    if is_redis_key {
                        if let Some(k_type) = key_type {
                            // This is a Redis key - create a query tab with appropriate Redis command
                            let redis_command = match k_type.to_lowercase().as_str() {
                                "string" => format!("GET {}", table_name),
                                "hash" => format!("HGETALL {}", table_name),
                                "list" => format!("LRANGE {} 0 -1", table_name),
                                "set" => format!("SMEMBERS {}", table_name),
                                "zset" | "sorted_set" => format!("ZRANGE {} 0 -1 WITHSCORES", table_name),
                                "stream" => format!("XRANGE {} - +", table_name),
                                _ => format!("TYPE {}", table_name), // Fallback to show type
                            };
                            
                            let tab_title = format!("Redis Key: {} ({})", table_name, k_type);
                            self.create_new_tab(tab_title, redis_command.clone());
                            
                            // Set current connection ID for Redis query execution
                            self.current_connection_id = Some(connection_id);
                            
                            // Auto-execute the Redis query
                            if let Some((headers, data)) = connection::execute_query_with_connection(self, connection_id, redis_command) {
                                self.current_table_headers = headers;
                                self.current_table_data = data.clone();
                                self.all_table_data = data;
                                self.current_table_name = format!("Redis Key: {}", table_name);
                                self.total_rows = self.all_table_data.len();
                                self.current_page = 0;
                            }
                        }
                    } else {
                        // This is a Redis folder/type - create a query tab for scanning keys
                        let redis_command = match table_name.as_str() {
                            "hashes" => "SCAN 0 MATCH *:* TYPE hash COUNT 100".to_string(),
                            "strings" => "SCAN 0 MATCH *:* TYPE string COUNT 100".to_string(),
                            "lists" => "SCAN 0 MATCH *:* TYPE list COUNT 100".to_string(),
                            "sets" => "SCAN 0 MATCH *:* TYPE set COUNT 100".to_string(),
                            "sorted_sets" => "SCAN 0 MATCH *:* TYPE zset COUNT 100".to_string(),
                            "streams" => "SCAN 0 MATCH *:* TYPE stream COUNT 100".to_string(),
                            _ => {
                                // Extract folder name from display format like "Strings (5)"
                                let clean_name = table_name.split('(').next().unwrap_or(&table_name).trim();
                                format!("SCAN 0 MATCH *:* COUNT 100 # Browse {}", clean_name)
                            }
                        };
                        let tab_title = format!("Redis {}", table_name);
                        self.create_new_tab(tab_title, redis_command);
                    }
                }
                _ => {
                    // SQL databases - use regular SELECT query
                    let query_content = format!("SELECT * FROM {} LIMIT 100;", table_name);
                    let tab_title = format!("Table: {}", table_name);
                    self.create_new_tab(tab_title, query_content);
                    
                    // Also load the table data
                    self.load_table_data(connection_id, &table_name);
                }
            };
        }
        
        // Handle query file open requests
        let results = query_files_to_open.clone();
        for (filename, content, file_path) in query_files_to_open {
            println!("📂 Processing file: {} (path: {})", filename, file_path);
            if file_path.is_empty() {
                // This is a placeholder query without a file path - create a new unsaved tab
                println!("📝 Creating new tab for placeholder query: {}", filename);
                self.create_new_tab(filename, content);
            } else {
                // Use existing open_query_file logic which checks for already open tabs
                println!("📁 Opening query file: {}", file_path);
                if let Err(err) = self.open_query_file(&file_path) {
                    println!("❌ Failed to open query file: {}", err);
                } else {
                    println!("✅ Successfully opened query file: {}", file_path);
                }
            }
        }
        
        // Handle context menu requests (deduplicate to avoid multiple calls)
        let mut processed_removals = std::collections::HashSet::new();
        let mut processed_refreshes = std::collections::HashSet::new();
        let mut needs_full_refresh = false;
                
        for context_id in context_menu_requests {
            println!("🔍 Processing context_id: {}", context_id);
            
            if context_id >= 50000 {
                // ID >= 50000 means create folder in folder operation
                let hash = context_id - 50000;
                println!("📁 Create folder operation with hash: {}", hash);
                self.handle_create_folder_in_folder_request(hash);
                // Force immediate UI repaint after create folder request
                ui.ctx().request_repaint();
            } else if context_id >= 40000 {
                // ID >= 40000 means move query to folder operation
                let hash = context_id - 40000;
                println!("📦 Move query operation with hash: {}", hash);
                self.handle_query_move_request(hash);
            } else if context_id >= 30000 {
                // ID >= 30000 means alter table operation
                let connection_id = context_id - 30000;
                println!("🔧 Alter table operation for connection: {}", connection_id);
                self.handle_alter_table_request(connection_id);
            } else if context_id >= 20000 {
                // ID >= 20000 means query edit operation
                let hash = context_id - 20000;
                println!("✏️ Query edit operation with hash: {}", hash);
                self.handle_query_edit_request(hash);
            } else if context_id <= -50000 {
                // ID <= -50000 means remove folder operation
                let hash = (-context_id) - 50000;
                println!("🗑️ Remove folder operation with hash: {}", hash);
                self.handle_remove_folder_request(hash);
                // Force immediate UI repaint after folder removal
                ui.ctx().request_repaint();
            } else if context_id <= -20000 {
                // ID <= -20000 means query removal operation  
                let hash = (-context_id) - 20000;
                println!("🗑️ Remove query operation with hash: {}", hash);
                if self.handle_query_remove_request_by_hash(hash) {
                    // Force refresh of queries tree if removal was successful
                    self.load_queries_from_directory();
                    
                    // Force immediate UI repaint - this is crucial!
                    ui.ctx().request_repaint();
                    
                    // Set needs_refresh flag to ensure UI updates
                    self.needs_refresh = true;
                    
                }
            } else if context_id > 10000 {
                // ID > 10000 means copy connection (connection_id = context_id - 10000)
                let connection_id = context_id - 10000;
                println!("📋 Copy connection operation for connection: {}", connection_id);
                self.copy_connection(connection_id);
                
                // Force immediate tree refresh and UI update
                self.items_tree.clear();
                self.refresh_connections_tree();
                needs_full_refresh = true;
                ui.ctx().request_repaint();
                
                // Break early to prevent further processing
                break;
            } else if (1000..10000).contains(&context_id) {
                // ID 1000-9999 means refresh connection (connection_id = context_id - 1000)
                let connection_id = context_id - 1000;
                println!("🔄 Refresh connection operation for connection: {}", connection_id);
                if !processed_refreshes.contains(&connection_id) {
                    processed_refreshes.insert(connection_id);
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
                    self.remove_connection(connection_id);
                    
                    // Force immediate tree refresh and UI update
                    self.items_tree.clear();
                    self.refresh_connections_tree();
                    needs_full_refresh = true;
                    ui.ctx().request_repaint();
                    
                    // Break early to prevent further processing
                    break;
                }
            }
        }
        
        // Force complete UI refresh after any removal
        if needs_full_refresh {
            // Completely clear and rebuild the tree
            self.items_tree.clear();
            self.refresh_connections_tree();
            self.needs_refresh = true; // Set flag for next update cycle
            ui.ctx().request_repaint();
            
            // Return early to prevent any further processing of the old tree
            return Vec::new();
        }
        
        // Clean up processed folder removal mappings (optional - only if we want to prevent memory buildup)
        // We could also keep them for potential retry scenarios
        
        // Return query files that were clicked
        results
    }

    fn render_tree_node_with_table_expansion(
            ui: &mut egui::Ui, node: &mut models::structs::TreeNode, 
            editor_text: &mut String, node_index: usize, 
            refreshing_connections: &std::collections::HashSet<i64>
        ) -> (
            Option<models::structs::ExpansionRequest>, Option<(usize, i64, String)>, 
            Option<i64>, Option<(i64, String)>, Option<i64>, 
            Option<(String, String, String)>, Option<String>, Option<String>, // Add parent folder for creation
            Option<(i64, String)> // Add mapping for folder removal: (hash, folder_path)
        ) {
        let has_children = !node.children.is_empty();
        let mut expansion_request = None;
        let mut table_expansion = None;
        let mut context_menu_request = None;
        let mut table_click_request = None;
        let mut folder_removal_mapping: Option<(i64, String)> = None;
        let mut connection_click_request = None;
        let mut query_file_to_open = None;
        let mut folder_name_for_removal = None;
        let mut parent_folder_for_creation = None;
        
        if has_children || node.node_type == models::enums::NodeType::Connection || node.node_type == models::enums::NodeType::Table || 
           node.node_type == models::enums::NodeType::DatabasesFolder || node.node_type == models::enums::NodeType::TablesFolder ||
           node.node_type == models::enums::NodeType::ViewsFolder || node.node_type == models::enums::NodeType::StoredProceduresFolder ||
           node.node_type == models::enums::NodeType::UserFunctionsFolder || node.node_type == models::enums::NodeType::TriggersFolder ||
           node.node_type == models::enums::NodeType::EventsFolder || node.node_type == models::enums::NodeType::DBAViewsFolder ||
           node.node_type == models::enums::NodeType::UsersFolder || node.node_type == models::enums::NodeType::PrivilegesFolder ||
           node.node_type == models::enums::NodeType::ProcessesFolder || node.node_type == models::enums::NodeType::StatusFolder ||
           node.node_type == models::enums::NodeType::Database || node.node_type == models::enums::NodeType::QueryFolder {
            // Use more unique ID including connection_id for connections
            let unique_id = match node.node_type {
                models::enums::NodeType::Connection => format!("conn_{}_{}", node_index, node.connection_id.unwrap_or(0)),
                _ => format!("node_{}_{:?}", node_index, node.node_type),
            };
            let id = egui::Id::new(&unique_id);
            ui.horizontal(|ui| {
                let expand_icon = if node.is_expanded { "▼" } else { "▶" };
                if ui.button(expand_icon).clicked() {
                    node.is_expanded = !node.is_expanded;
                    
                    // If this is a connection node and not loaded, request expansion
                    if node.node_type == models::enums::NodeType::Connection && !node.is_loaded && node.is_expanded {
                        if let Some(conn_id) = node.connection_id {
                            expansion_request = Some(models::structs::ExpansionRequest {
                                node_type: models::enums::NodeType::Connection,
                                connection_id: conn_id,
                                database_name: None,
                            });
                            // Also set as active connection when expanding
                            connection_click_request = Some(conn_id);
                        }
                    }
                    
                    // If this is a table node and not loaded, request table column expansion
                    if node.node_type == models::enums::NodeType::Table && !node.is_loaded && node.is_expanded {
                        if let Some(conn_id) = node.connection_id {
                            table_expansion = Some((node_index, conn_id, node.name.clone()));
                        }
                    }
                    
                    // If this is a folder node and not loaded, request folder content expansion
                    if (node.node_type == models::enums::NodeType::DatabasesFolder ||
                        node.node_type == models::enums::NodeType::TablesFolder || 
                        node.node_type == models::enums::NodeType::ViewsFolder ||
                        node.node_type == models::enums::NodeType::StoredProceduresFolder ||
                        node.node_type == models::enums::NodeType::UserFunctionsFolder ||
                        node.node_type == models::enums::NodeType::TriggersFolder ||
                        node.node_type == models::enums::NodeType::EventsFolder) && 
                       !node.is_loaded && node.is_expanded {
                        if let Some(conn_id) = node.connection_id {
                            expansion_request = Some(models::structs::ExpansionRequest {
                                node_type: node.node_type.clone(),
                                connection_id: conn_id,
                                database_name: node.database_name.clone(),
                            });
                        }
                    }
                    
                    // If this is a Database node and not loaded, request database expansion (for Redis keys)
                    if node.node_type == models::enums::NodeType::Database && !node.is_loaded && node.is_expanded {
                        if let Some(conn_id) = node.connection_id {
                            expansion_request = Some(models::structs::ExpansionRequest {
                                node_type: models::enums::NodeType::Database,
                                connection_id: conn_id,
                                database_name: node.database_name.clone(),
                            });
                        }
                    }
                }
                
                let icon = match node.node_type {
                    models::enums::NodeType::Database => "🗄",
                    models::enums::NodeType::Table => "📋",
                    models::enums::NodeType::Column => "📄",
                    models::enums::NodeType::Query => "🔍",
                    models::enums::NodeType::QueryHistItem => "📜",
                    models::enums::NodeType::Connection => "", // Icon already included in name
                    models::enums::NodeType::DatabasesFolder => "📁",
                    models::enums::NodeType::TablesFolder => "📋",
                    models::enums::NodeType::ViewsFolder => "👁",
                    models::enums::NodeType::StoredProceduresFolder => "⚙️",
                    models::enums::NodeType::UserFunctionsFolder => "🔧",
                    models::enums::NodeType::TriggersFolder => "⚡",
                    models::enums::NodeType::EventsFolder => "📅",
                    models::enums::NodeType::DBAViewsFolder => "👨‍💼",
                    models::enums::NodeType::UsersFolder => "👥",
                    models::enums::NodeType::PrivilegesFolder => "🔒",
                    models::enums::NodeType::ProcessesFolder => "⚡",
                    models::enums::NodeType::StatusFolder => "📊",
                    models::enums::NodeType::View => "👁",
                    models::enums::NodeType::StoredProcedure => "⚙️",
                    models::enums::NodeType::UserFunction => "🔧",
                    models::enums::NodeType::Trigger => "⚡",
                    models::enums::NodeType::Event => "📅",
                    models::enums::NodeType::MySQLFolder => "🐬",
                    models::enums::NodeType::PostgreSQLFolder => "🐘",
                    models::enums::NodeType::SQLiteFolder => "📄",
                    models::enums::NodeType::RedisFolder => "🔴",
                    models::enums::NodeType::CustomFolder => "📁",
                    models::enums::NodeType::QueryFolder => "📂",
                };
                
                let label_text = if icon.is_empty() { 
                    // For connection nodes, add loading indicator if refreshing
                    if node.node_type == models::enums::NodeType::Connection {
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
                let response = if node.node_type == models::enums::NodeType::Connection {
                    // Use button for connections to make them more clickable
                    ui.button(&label_text)
                } else {
                    ui.label(label_text)
                };
                
                // Handle clicks on connection labels to set active connection
                if node.node_type == models::enums::NodeType::Connection && response.clicked() {
                    if let Some(conn_id) = node.connection_id {
                        connection_click_request = Some(conn_id);
                    }
                }
                
                // Handle clicks on table labels to load table data - open in new tab
                if node.node_type == models::enums::NodeType::Table && response.clicked() {
                    // Don't modify current editor_text, we'll create a new tab instead
                    // Still trigger table data loading
                    if let Some(conn_id) = node.connection_id {
                        table_click_request = Some((conn_id, node.name.clone()));
                    }
                }
                
                // Add context menu for connection nodes
                if node.node_type == models::enums::NodeType::Connection {
                    response.context_menu(|ui| {
                        if ui.button("Copy Connection").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(conn_id + 10000); // Use +10000 to indicate copy
                            }
                            ui.close_menu();
                        }
                        if ui.button("Edit Connection").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(conn_id);
                            }
                            ui.close_menu();
                        }
                        if ui.button("Remove Connection").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(-conn_id); // Negative ID indicates removal
                            }
                            ui.close_menu();
                        }
                        if ui.button("Refresh").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                context_menu_request = Some(1000 + conn_id); // Add to 1000 base for refresh (range 1001-9999)
                            }
                            ui.close_menu();
                        }
                    });
                }
                
                // Add context menu for folder nodes
                if node.node_type == models::enums::NodeType::QueryFolder {
                    response.context_menu(|ui| {
                        if ui.button("📁 Create New Folder").clicked() {
                            // Store the parent folder name for creation
                            parent_folder_for_creation = Some(node.name.clone());
                            // Use ID range 50000+ for create folder in folder operations
                            let create_in_folder_id = 50000 + (node.name.len() as i64 % 1000);
                            context_menu_request = Some(create_in_folder_id);
                            ui.close_menu();
                        }
                        
                        if ui.button("🗑️ Remove Folder").clicked() {
                            // Store the full folder path for removal (relative to query dir)
                            if let Some(full_path) = &node.file_path {
                                let query_dir = directory::get_query_dir();
                                // Get relative path from query directory
                                let relative_path = std::path::Path::new(full_path)
                                    .strip_prefix(&query_dir)
                                    .unwrap_or(std::path::Path::new(&node.name))
                                    .to_string_lossy()
                                    .to_string();
                                folder_name_for_removal = Some(relative_path.clone());
                                
                                // Use ID range -50000 for remove folder operations
                                let remove_folder_id = -50000 - (node.name.len() as i64 % 1000);
                                let hash = (-remove_folder_id) - 50000;
                                folder_removal_mapping = Some((hash, relative_path));
                                context_menu_request = Some(remove_folder_id);
                            } else {
                                // Fallback to just folder name for root folders
                                let folder_name = node.name.clone();
                                folder_name_for_removal = Some(folder_name.clone());
                                
                                // Use ID range -50000 for remove folder operations
                                let remove_folder_id = -50000 - (node.name.len() as i64 % 1000);
                                let hash = (-remove_folder_id) - 50000;
                                folder_removal_mapping = Some((hash, folder_name));
                                context_menu_request = Some(remove_folder_id);
                            }
                            ui.close_menu();
                        }
                    });
                }
                
                // Add context menu for table nodes
                if node.node_type == models::enums::NodeType::Table {
                    response.context_menu(|ui| {
                        if ui.button("📊 View Data").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                table_click_request = Some((conn_id, node.name.clone()));
                            }
                            ui.close_menu();
                        }
                        if ui.button("📋 SELECT Query (New Tab)").clicked() {
                            // We'll create a new tab instead of modifying current editor
                            // Store the request and handle it in render_tree
                            ui.close_menu();
                        }
                        if ui.button("🔍 COUNT Query (Current Tab)").clicked() {
                            *editor_text = format!("SELECT COUNT(*) FROM {};", node.name);
                            ui.close_menu();
                        }
                        if ui.button("📝 DESCRIBE Query (Current Tab)").clicked() {
                            // Different DESCRIBE syntax for different database types
                            if node.database_name.is_some() {
                                *editor_text = format!("DESCRIBE {};", node.name);
                            } else {
                                *editor_text = format!("PRAGMA table_info({});", node.name); // SQLite syntax
                            }
                            ui.close_menu();
                        }
                        ui.separator();
                        if ui.button("🔧 Alter Table").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                // Use connection_id + 30000 to indicate alter table request
                                context_menu_request = Some(conn_id + 30000);
                            }
                            ui.close_menu();
                        }
                    });
                }
                
                // Add context menu for view nodes
                if node.node_type == models::enums::NodeType::View {
                    response.context_menu(|ui| {
                        if ui.button("📊 View Data").clicked() {
                            if let Some(conn_id) = node.connection_id {
                                table_click_request = Some((conn_id, node.name.clone()));
                            }
                            ui.close_menu();
                        }
                        if ui.button("📋 SELECT Query (New Tab)").clicked() {
                            // We'll create a new tab instead of modifying current editor  
                            // Store the request and handle it in render_tree
                            ui.close_menu();
                        }
                        if ui.button("🔍 COUNT Query (Current Tab)").clicked() {
                            *editor_text = format!("SELECT COUNT(*) FROM {};", node.name);
                            ui.close_menu();
                        }
                        if ui.button("📝 DESCRIBE View (Current Tab)").clicked() {
                            // Different DESCRIBE syntax for different database types
                            if node.database_name.is_some() {
                                *editor_text = format!("DESCRIBE {};", node.name);
                            } else {
                                *editor_text = format!("PRAGMA table_info({});", node.name); // SQLite syntax
                            }
                            ui.close_menu();
                        }
                        ui.separator();
                        if ui.button("🗂️ Show Columns").clicked() {
                            // Trigger table expansion to show columns
                            if let Some(conn_id) = node.connection_id {
                                table_expansion = Some((0, conn_id, node.name.clone()));
                            }
                            ui.close_menu();
                        }
                    });
                }
            });

            if node.is_expanded {
                ui.indent(id, |ui| {
                    for (child_index, child) in node.children.iter_mut().enumerate() {
                        let (child_expansion_request, child_table_expansion, child_context, child_table_click, _child_connection_click, _child_query_file, _child_folder_removal, _child_parent_creation, _child_folder_removal_mapping) = Self::render_tree_node_with_table_expansion(ui, child, editor_text, child_index, refreshing_connections);
                        
                        // Handle child expansion requests - propagate to parent
                        if let Some(child_expansion) = child_expansion_request {
                            expansion_request = Some(child_expansion);
                        }
                        
                        // Handle child table expansions with the parent connection ID
                        // Only set if we don't already have a table expansion from this node
                        if table_expansion.is_none() {
                            if let Some((child_index, child_conn_id, table_name)) = child_table_expansion {
                                if let Some(conn_id) = node.connection_id {
                                    table_expansion = Some((child_index, conn_id, table_name));
                                } else {
                                    table_expansion = Some((child_index, child_conn_id, table_name));
                                }
                            }
                        }
                        
                        // Handle child table clicks - propagate to parent
                        if let Some((conn_id, table_name)) = child_table_click {
                            table_click_request = Some((conn_id, table_name));
                        }
                        
                        // Handle child folder removal - propagate to parent
                        if let Some(child_folder_name) = _child_folder_removal {
                            folder_name_for_removal = Some(child_folder_name);
                        }
                        
                        // Handle child parent folder creation - propagate to parent
                        if let Some(child_parent) = _child_parent_creation {
                            parent_folder_for_creation = Some(child_parent);
                        }
                        
                        // Handle child folder removal mapping - propagate to parent
                        if let Some(child_mapping) = _child_folder_removal_mapping {
                            folder_removal_mapping = Some(child_mapping);
                        }
                        
                        // Handle child query file open requests - propagate to parent
                        if let Some(child_query_file) = _child_query_file {
                            query_file_to_open = Some(child_query_file);
                        }
                        
                        // Handle child context menu requests - propagate to parent
                        if let Some(child_context_id) = child_context {
                            context_menu_request = Some(child_context_id);
                        }
                    }
                });
            }
        } else {
            ui.horizontal(|ui| {
                ui.add_space(16.0); // Indent for leaf nodes
                
                let icon = match node.node_type {
                    models::enums::NodeType::Database => "🗄",
                    models::enums::NodeType::Table => "📋",
                    models::enums::NodeType::Column => "📄",
                    models::enums::NodeType::Query => "🔍",
                    models::enums::NodeType::QueryHistItem => "📜",
                    models::enums::NodeType::Connection => "🔗",
                    models::enums::NodeType::DatabasesFolder => "📁",
                    models::enums::NodeType::TablesFolder => "📋",
                    models::enums::NodeType::ViewsFolder => "👁",
                    models::enums::NodeType::StoredProceduresFolder => "⚙️",
                    models::enums::NodeType::UserFunctionsFolder => "🔧",
                    models::enums::NodeType::TriggersFolder => "⚡",
                    models::enums::NodeType::EventsFolder => "📅",
                    models::enums::NodeType::DBAViewsFolder => "👨‍💼",
                    models::enums::NodeType::UsersFolder => "👥",
                    models::enums::NodeType::PrivilegesFolder => "🔒",
                    models::enums::NodeType::ProcessesFolder => "⚡",
                    models::enums::NodeType::StatusFolder => "📊",
                    models::enums::NodeType::View => "👁",
                    models::enums::NodeType::StoredProcedure => "⚙️",
                    models::enums::NodeType::UserFunction => "🔧",
                    models::enums::NodeType::Trigger => "⚡",
                    models::enums::NodeType::Event => "📅",
                    models::enums::NodeType::MySQLFolder => "🐬",
                    models::enums::NodeType::PostgreSQLFolder => "🐘",
                    models::enums::NodeType::SQLiteFolder => "📄",
                    models::enums::NodeType::RedisFolder => "🔴",
                    models::enums::NodeType::CustomFolder => "📁",
                    models::enums::NodeType::QueryFolder => "📂",
                };
                
                let response = ui.button(format!("{} {}", icon, node.name));
                
                if response.clicked() {
                    // Handle node selection
                    match node.node_type {
                        models::enums::NodeType::Table => {
                            // Don't modify current editor_text, we'll create a new tab
                            // Just trigger table data loading 
                            if let Some(conn_id) = node.connection_id {
                                table_click_request = Some((conn_id, node.name.clone()));
                            }
                        },
                        models::enums::NodeType::Query => {
                            // Load query file content
                            println!("🔍 Query node clicked: {}", node.name);
                            if let Some(file_path) = &node.file_path {
                                println!("📁 File path: {}", file_path);
                                if let Ok(content) = std::fs::read_to_string(file_path) {
                                    println!("✅ File read successfully, content length: {}", content.len());
                                    // Don't modify editor_text directly, let open_query_file handle it
                                    query_file_to_open = Some((node.name.clone(), content, file_path.clone()));
                                } else {
                                    println!("❌ Failed to read file: {}", file_path);
                                    // Handle read error case
                                    query_file_to_open = Some((node.name.clone(), format!("-- Failed to load query file: {}", node.name), file_path.clone()));
                                }
                            } else {
                                println!("❌ No file path for query node: {}", node.name);
                                // Handle missing file path case - create a placeholder query
                                let placeholder_content = format!("-- {}\nSELECT * FROM table_name;", node.name);
                                // For files without path, we'll create a new unsaved tab
                                query_file_to_open = Some((node.name.clone(), placeholder_content, String::new()));
                            }
                        },
                        models::enums::NodeType::QueryHistItem => {
                            // Store the display name for processing later
                            *editor_text = node.name.clone();
                        },
                        _ => {}
                    }
                }
                
                // Add context menu for query nodes
                if node.node_type == models::enums::NodeType::Query {
                    response.context_menu(|ui| {
                        if ui.button("Edit Query").clicked() {
                            if let Some(file_path) = &node.file_path {
                                // Use the file path directly as context identifier
                                // Format: 20000 + simple index to differentiate from connections
                                let edit_id = 20000 + (file_path.len() as i64 % 1000); // Simple deterministic ID
                                context_menu_request = Some(edit_id);
                            }
                            ui.close_menu();
                        }
                        
                        if ui.button("Move to Folder").clicked() {
                            if let Some(file_path) = &node.file_path {
                                // Use a different ID range for move operations
                                let move_id = 40000 + (file_path.len() as i64 % 1000);
                                context_menu_request = Some(move_id);
                            }
                            ui.close_menu();
                        }
                        
                        if ui.button("Remove Query").clicked() {
                            if let Some(file_path) = &node.file_path {
                                // Use the file path directly as context identifier
                                // Format: -20000 - simple index to differentiate from connections
                                let remove_id = -20000 - (file_path.len() as i64 % 1000); // Simple deterministic ID
                                context_menu_request = Some(remove_id);
                            }
                            ui.close_menu();
                        }
                    });
                }
            });
        }
        
        (expansion_request, table_expansion, context_menu_request, table_click_request, connection_click_request, query_file_to_open, folder_name_for_removal, parent_folder_for_creation, folder_removal_mapping)
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
            .default_width(600.0)
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
                                    models::enums::DatabaseType::MySQL => "MySQL",
                                    models::enums::DatabaseType::PostgreSQL => "PostgreSQL",
                                    models::enums::DatabaseType::SQLite => "SQLite",
                                    models::enums::DatabaseType::Redis => "Redis",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut connection_data.connection_type, models::enums::DatabaseType::MySQL, "MySQL");
                                    ui.selectable_value(&mut connection_data.connection_type, models::enums::DatabaseType::PostgreSQL, "PostgreSQL");
                                    ui.selectable_value(&mut connection_data.connection_type, models::enums::DatabaseType::SQLite, "SQLite");
                                    ui.selectable_value(&mut connection_data.connection_type, models::enums::DatabaseType::Redis, "Redis");
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

                            ui.label("Folder (Optional):");
                            let mut folder_text = connection_data.folder.as_ref().unwrap_or(&String::new()).clone();
                            ui.text_edit_singleline(&mut folder_text);
                            connection_data.folder = if folder_text.trim().is_empty() { None } else { Some(folder_text.trim().to_string()) };
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
                                        } else {
                                            println!("ERROR: Could not find connection {} in memory", id);
                                        }
                                    }
                                } else {
                                    println!("ERROR: Connection has no ID, cannot update");
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
                                
                                self.new_connection = models::structs::ConnectionConfig::default();
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
                self.new_connection = models::structs::ConnectionConfig::default();
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
                sqlx::query_as::<_, (i64, String, String, String, String, String, String, String, Option<String>)>(
                    "SELECT id, name, host, port, username, password, database_name, connection_type, folder FROM connections"
                )
                .fetch_all(pool_clone.as_ref())
                .await
            });
            
            if let Ok(rows) = connections_result {

                self.connections = rows.into_iter().map(|(id, name, host, port, username, password, database_name, connection_type, folder)| {
                    models::structs::ConnectionConfig {
                        id: Some(id),
                        name,
                        host,
                        port,
                        username,
                        password,
                        database: database_name,
                        connection_type: match connection_type.as_str() {
                            "MySQL" => models::enums::DatabaseType::MySQL,
                            "PostgreSQL" => models::enums::DatabaseType::PostgreSQL,
                            "Redis" => models::enums::DatabaseType::Redis,
                            _ => models::enums::DatabaseType::SQLite,
                        },
                        folder,
                    }
                }).collect();
            }
        }
        
        // Refresh the tree after loading connections
        self.refresh_connections_tree();
    }


    fn save_connection_to_database(&self, connection: &models::structs::ConnectionConfig) -> bool {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let connection = connection.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let result = rt.block_on(async {
                sqlx::query(
                    "INSERT INTO connections (name, host, port, username, password, database_name, connection_type, folder) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(connection.name)
                .bind(connection.host)
                .bind(connection.port)
                .bind(connection.username)
                .bind(connection.password)
                .bind(connection.database)
                .bind(format!("{:?}", connection.connection_type))
                .bind(connection.folder)
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
        } else {
            for conn in &self.connections {
                println!("  - {} (ID: {:?})", conn.name, conn.id);
            }
        }
    }

    fn copy_connection(&mut self, connection_id: i64) {
        // Find the connection to copy
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)).cloned() {
            let mut copied_connection = connection.clone();
            
            // Reset ID and modify name to indicate it's a copy
            copied_connection.id = None;
            copied_connection.name = format!("{} - Copy", copied_connection.name);
            
            
            // Try to save to database first
            if self.save_connection_to_database(&copied_connection) {
                // If database save successful, reload from database to get ID
                self.load_connections();
            } else {
                // Fallback to in-memory storage
                let new_id = self.connections.iter()
                    .filter_map(|c| c.id)
                    .max()
                    .unwrap_or(0) + 1;
                copied_connection.id = Some(new_id);
                self.connections.push(copied_connection);
            }
            
        } else {
            println!("❌ Connection with ID {} not found for copying", connection_id);
        }
    }

    fn handle_query_edit_request(&mut self, hash: i64) {
        
        // Find the query file by hash
        if let Some(query_file_path) = self.find_query_file_by_hash(hash) {
            
            // Open the query file in a new tab for editing
            if let Err(err) = self.open_query_file(&query_file_path) {
                println!("Failed to open query file for editing: {}", err);
            }
        } else {
            println!("Query file not found for hash: {}", hash);
        }
    }

    fn handle_query_move_request(&mut self, hash: i64) {
        
        // Find the query file by hash
        if let Some(query_file_path) = self.find_query_file_by_hash(hash) {
            
            // Set up the move dialog
            self.selected_query_for_move = Some(query_file_path);
            self.show_move_to_folder_dialog = true;
        } else {
            println!("Query file not found for hash: {}", hash);
        }
    }

    fn handle_query_remove_request_by_hash(&mut self, hash: i64) -> bool {
        
        // Find the query file by hash using recursive search
        if let Some(file_path) = self.find_query_file_by_hash(hash) {
            
            // Close any open tabs for this file first
            self.close_tabs_for_file(&file_path);
            
            // Remove the file from filesystem
            match std::fs::remove_file(&file_path) {
                Ok(()) => {
                    
                    // Set needs_refresh flag for next update cycle
                    self.needs_refresh = true;
                    
                    return true;
                },
                Err(e) => {
                    println!("❌ Failed to remove query file: {}", e);
                    return false;
                }
            }
        }
        
        println!("❌ Query file not found for hash: {}", hash);
        false
    }


    fn find_query_file_by_hash(&self, hash: i64) -> Option<String> {
        let query_dir = directory::get_query_dir();
        
        // Function to search recursively in directories
        fn search_in_dir(dir: &std::path::Path, target_hash: i64) -> Option<String> {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_file() {
                            if let Some(filename) = entry.file_name().to_str() {
                                if filename.ends_with(".sql") {
                                    let file_path = entry.path().to_string_lossy().to_string();
                                    
                                    // Use same hash calculation as in context menu: file_path.len() % 1000
                                    let file_hash = (file_path.len() as i64) % 1000;
                                    
                                    if file_hash == target_hash {
                                        return Some(file_path);
                                    }
                                }
                            }
                        } else if metadata.is_dir() {
                            // Recursively search in subdirectories
                            if let Some(found) = search_in_dir(&entry.path(), target_hash) {
                                return Some(found);
                            }
                        }
                    }
                }
            }
            None
        }
        
        search_in_dir(&query_dir, hash)
    }

    fn close_tabs_for_file(&mut self, file_path: &str) {
        // Find all tabs that have this file open and close them
        let mut indices_to_close = Vec::new();
        
        for (index, tab) in self.query_tabs.iter().enumerate() {
            if tab.file_path.as_deref() == Some(file_path) {
                indices_to_close.push(index);
            }
        }
        
        // Close tabs in reverse order to maintain correct indices
        for &index in indices_to_close.iter().rev() {
            self.close_tab(index);
        }
    }

    fn handle_alter_table_request(&mut self, connection_id: i64) {
        println!("🔍 handle_alter_table_request called with connection_id: {}", connection_id);
        
        // Find the connection by ID to determine database type
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            // Find the currently selected table in the tree
            if let Some(table_name) = self.find_selected_table_name(connection_id) {
                // Generate ALTER TABLE template based on database type
                let alter_template = match connection.connection_type {
                    models::enums::DatabaseType::MySQL => self.generate_mysql_alter_table_template(&table_name),
                    models::enums::DatabaseType::PostgreSQL => self.generate_postgresql_alter_table_template(&table_name),
                    models::enums::DatabaseType::SQLite => self.generate_sqlite_alter_table_template(&table_name),
                    models::enums::DatabaseType::Redis => "-- Redis does not support ALTER TABLE operations\n-- Redis is a key-value store, not a relational database".to_string(),
                };
                
                // Set the ALTER TABLE template in the editor
                self.editor_text = alter_template;
                self.current_connection_id = Some(connection_id);
                
            } else {
                // If no specific table is selected, show a generic template
                let alter_template = match connection.connection_type {
                    models::enums::DatabaseType::MySQL => "-- MySQL ALTER TABLE template\nALTER TABLE your_table_name\n  ADD COLUMN new_column VARCHAR(255),\n  MODIFY COLUMN existing_column INT,\n  DROP COLUMN old_column;".to_string(),
                    models::enums::DatabaseType::PostgreSQL => "-- PostgreSQL ALTER TABLE template\nALTER TABLE your_table_name\n  ADD COLUMN new_column VARCHAR(255),\n  ALTER COLUMN existing_column TYPE INTEGER,\n  DROP COLUMN old_column;".to_string(),
                    models::enums::DatabaseType::SQLite => "-- SQLite ALTER TABLE template\n-- Note: SQLite has limited ALTER TABLE support\nALTER TABLE your_table_name\n  ADD COLUMN new_column TEXT;".to_string(),
                    models::enums::DatabaseType::Redis => "-- Redis does not support ALTER TABLE operations\n-- Redis is a key-value store, not a relational database\n-- Use Redis commands like SET, GET, HSET, etc.".to_string(),
                };
                
                self.editor_text = alter_template;
                self.current_connection_id = Some(connection_id);
                
            }
        } else {
            println!("❌ Connection with ID {} not found", connection_id);
        }
    }

    fn find_selected_table_name(&self, _connection_id: i64) -> Option<String> {
        // This is a simplified approach - in a more sophisticated implementation,
        // you might track which table was right-clicked
        // For now, we'll return None to show the generic template
        None
    }

    fn generate_mysql_alter_table_template(&self, table_name: &str) -> String {
        format!(
            "-- MySQL ALTER TABLE for {}\nALTER TABLE {}\n  ADD COLUMN new_column VARCHAR(255) DEFAULT NULL COMMENT 'New column description',\n  MODIFY COLUMN existing_column INT NOT NULL,\n  DROP COLUMN old_column,\n  ADD INDEX idx_new_column (new_column);",
            table_name, table_name
        )
    }

    fn generate_postgresql_alter_table_template(&self, table_name: &str) -> String {
        format!(
            "-- PostgreSQL ALTER TABLE for {}\nALTER TABLE {}\n  ADD COLUMN new_column VARCHAR(255) DEFAULT NULL,\n  ALTER COLUMN existing_column TYPE INTEGER,\n  DROP COLUMN old_column;\n\n-- Add constraint example\n-- ALTER TABLE {} ADD CONSTRAINT chk_constraint CHECK (new_column IS NOT NULL);",
            table_name, table_name, table_name
        )
    }

    fn generate_sqlite_alter_table_template(&self, table_name: &str) -> String {
        format!(
            "-- SQLite ALTER TABLE for {}\n-- Note: SQLite has limited ALTER TABLE support\n-- Only ADD COLUMN and RENAME operations are supported\n\nALTER TABLE {} ADD COLUMN new_column TEXT DEFAULT NULL;\n\n-- To modify or drop columns, you need to recreate the table:\n-- CREATE TABLE {}_new AS SELECT existing_columns FROM {};\n-- DROP TABLE {};\n-- ALTER TABLE {}_new RENAME TO {};",
            table_name, table_name, table_name, table_name, table_name, table_name, table_name
        )
    }

    fn handle_create_folder_in_folder_request(&mut self, _hash: i64) {
        println!("🔍 handle_create_folder_in_folder_request called with hash: {}", _hash);
        // Parent folder should already be set when context menu was clicked
        if self.parent_folder_for_creation.is_some() {
            // Show the create folder dialog
            self.show_create_folder_dialog = true;
        } else {
            println!("❌ No parent folder set for creation! This should not happen.");
            self.error_message = "No parent folder selected for creation".to_string();
            self.show_error_message = true;
        }
    }

    fn handle_remove_folder_request(&mut self, hash: i64) {
        
        // Look up the folder path using the hash
        if let Some(folder_relative_path) = self.folder_removal_map.get(&hash).cloned() {
            let query_dir = directory::get_query_dir();
            let folder_path = query_dir.join(&folder_relative_path);
            
            
            if folder_path.exists() && folder_path.is_dir() {
                // Check if folder is empty (recursively)
                let is_empty = Self::is_directory_empty(&folder_path);
                
                if is_empty {
                    // Remove empty folder
                    match std::fs::remove_dir(&folder_path) {
                        Ok(()) => {
                            // Refresh the queries tree
                            self.load_queries_from_directory();
                            // Force UI refresh
                            self.needs_refresh = true;
                        }
                        Err(e) => {
                            println!("❌ Failed to remove folder: {}", e);
                            self.error_message = format!("Failed to remove folder '{}': {}", folder_relative_path, e);
                            self.show_error_message = true;
                        }
                    }
                } else {
                    // Offer option to remove folder and all contents
                    self.error_message = format!("Folder '{}' is not empty.\n\nWould you like to remove it and all its contents?", folder_relative_path);
                    self.show_error_message = true;
                    println!("❌ Cannot remove non-empty folder: {}", folder_relative_path);
                }
            } else {
                self.error_message = format!("Folder '{}' does not exist", folder_relative_path);
                self.show_error_message = true;
                println!("❌ Folder does not exist: {}", folder_relative_path);
            }
            
            // Remove the mapping after processing
            self.folder_removal_map.remove(&hash);
        } else {
            println!("❌ No folder path found for hash: {}", hash);
            println!("❌ Available mappings: {:?}", self.folder_removal_map);
            // Fallback to the old method
            if let Some(folder_relative_path) = &self.selected_folder_for_removal {
                let query_dir = directory::get_query_dir();
                let folder_path = query_dir.join(folder_relative_path);
                
                
                if folder_path.exists() && folder_path.is_dir() {
                    let is_empty = Self::is_directory_empty(&folder_path);
                    
                    if is_empty {
                        match std::fs::remove_dir(&folder_path) {
                            Ok(()) => {
                                self.load_queries_from_directory();
                                self.needs_refresh = true;
                            }
                            Err(e) => {
                                println!("❌ Failed to remove folder: {}", e);
                                self.error_message = format!("Failed to remove folder '{}': {}", folder_relative_path, e);
                                self.show_error_message = true;
                            }
                        }
                    } else {
                        self.error_message = format!("Folder '{}' is not empty.\n\nWould you like to remove it and all its contents?", folder_relative_path);
                        self.show_error_message = true;
                        println!("❌ Cannot remove non-empty folder: {}", folder_relative_path);
                    }
                } else {
                    self.error_message = format!("Folder '{}' does not exist", folder_relative_path);
                    self.show_error_message = true;
                    println!("❌ Folder does not exist: {}", folder_relative_path);
                }
                
                self.selected_folder_for_removal = None;
            } else {
                println!("❌ No folder selected for removal in fallback either");
            }
        }
    }

    fn is_directory_empty(dir_path: &std::path::Path) -> bool {
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            entries.count() == 0
        } else {
            false
        }
    }

    fn test_database_connection(&self, connection: &models::structs::ConnectionConfig) -> (bool, String) {

        // ping the host first
        if !helpers::ping_host(&connection.host) {
            return (false, format!("Failed to ping host: {}", connection.host));
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        
        rt.block_on(async {
            match connection.connection_type {
                models::enums::DatabaseType::MySQL => {
                    let encoded_username = modules::url_encode(&connection.username);
                    let encoded_password = modules::url_encode(&connection.password);
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
                models::enums::DatabaseType::PostgreSQL => {
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
                models::enums::DatabaseType::SQLite => {
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
                },
                models::enums::DatabaseType::Redis => {
                    let connection_string = if connection.password.is_empty() {
                        format!("redis://{}:{}", connection.host, connection.port)
                    } else {
                        format!("redis://{}:{}@{}:{}", connection.username, connection.password, connection.host, connection.port)
                    };
                    
                    match Client::open(connection_string) {
                        Ok(client) => {
                            match client.get_connection() {
                                Ok(mut conn) => {
                                    // Test with a simple PING command
                                    match redis::cmd("PING").query::<String>(&mut conn) {
                                        Ok(response) => {
                                            if response == "PONG" {
                                                (true, "Redis connection successful!".to_string())
                                            } else {
                                                (false, "Redis PING returned unexpected response".to_string())
                                            }
                                        },
                                        Err(e) => (false, format!("Redis PING failed: {}", e)),
                                    }
                                },
                                Err(e) => (false, format!("Redis connection failed: {}", e)),
                            }
                        },
                        Err(e) => (false, format!("Redis client creation failed: {}", e)),
                    }
                }
            }
        })
    }

    fn update_connection_in_database(&self, connection: &models::structs::ConnectionConfig) -> bool {
        if let Some(ref pool) = self.db_pool {
            if let Some(id) = connection.id {
                let pool_clone = pool.clone();
                let connection = connection.clone();
                let rt = tokio::runtime::Runtime::new().unwrap();
                
                
                let result = rt.block_on(async {
                    sqlx::query(
                        "UPDATE connections SET name = ?, host = ?, port = ?, username = ?, password = ?, database_name = ?, connection_type = ?, folder = ? WHERE id = ?"
                    )
                    .bind(connection.name)
                    .bind(connection.host)
                    .bind(connection.port)
                    .bind(connection.username)
                    .bind(connection.password)
                    .bind(connection.database)
                    .bind(format!("{:?}", connection.connection_type))
                    .bind(connection.folder)
                    .bind(id)
                    .execute(pool_clone.as_ref())
                    .await
                });
                
                match &result {
                    Ok(query_result) => {
                        println!("Update successful: {} rows affected", query_result.rows_affected());
                    }
                    Err(e) => {
                        println!("Update failed: {}", e);
                    }
                }
                
                result.is_ok()
            } else {
                println!("Cannot update connection: no ID found");
                false
            }
        } else {
            println!("Cannot update connection: no database pool available");
            false
        }
    }



    fn remove_connection(&mut self, connection_id: i64) {
        
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
        
        self.connections.retain(|c| c.id != Some(connection_id));
        // Remove from connection pool cache
        self.connection_pools.remove(&connection_id);
                
        // Set flag to force refresh on next update
        self.needs_refresh = true;
        
    }

    // Cache functions for database structure
    fn save_databases_to_cache(&self, connection_id: i64, databases: &[String]) {
        for db_name in databases {
            println!("  - {}", db_name);
        }
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let databases_clone = databases.to_vec();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            rt.block_on(async {
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
            
            rt.block_on(async {
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

    fn save_columns_to_cache(&self, connection_id: i64, database_name: &str, table_name: &str, columns: &[(String, String)]) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let columns_clone = columns.to_vec();
            let database_name = database_name.to_string();
            let table_name = table_name.to_string();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            rt.block_on(async {
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

    fn find_connection_node_recursive(nodes: &mut [models::structs::TreeNode], connection_id: i64) -> Option<&mut models::structs::TreeNode> {
        for node in nodes.iter_mut() {
            // Check if this is the connection node we're looking for
            if node.node_type == models::enums::NodeType::Connection && 
               node.connection_id == Some(connection_id) {
                return Some(node);
            }
            
            // Recursively search in children
            if !node.children.is_empty() {
                if let Some(found) = Self::find_connection_node_recursive(&mut node.children, connection_id) {
                    return Some(found);
                }
            }
        }
        None
    }

    fn refresh_connection(&mut self, connection_id: i64) {
        
        // Clear all cached data for this connection
        self.clear_connection_cache(connection_id);
        
        // Remove from connection pool cache to force reconnection
        self.connection_pools.remove(&connection_id);
        
        // Mark as refreshing
        self.refreshing_connections.insert(connection_id);
        
        // Find the connection node in the tree and reset its loaded state
        for node in &mut self.items_tree {
            if node.node_type == models::enums::NodeType::Connection && node.connection_id == Some(connection_id) {
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
            if let Err(e) = sender.send(models::enums::BackgroundTask::RefreshConnection { connection_id }) {
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
            
            // Save databases to cache
            self.save_databases_to_cache(connection_id, &databases);
            
            // For each database, fetch tables and columns
            for database_name in &databases {
                
                // Fetch different types of tables based on database type
                let table_types = match connection.connection_type {
                    models::enums::DatabaseType::MySQL => vec!["table", "view", "procedure", "function", "trigger", "event"],
                    models::enums::DatabaseType::PostgreSQL => vec!["table", "view"], // Add PostgreSQL support later
                    models::enums::DatabaseType::SQLite => vec!["table", "view"],
                    models::enums::DatabaseType::Redis => vec!["info_section", "redis_keys"], // Redis specific types
                };
                
                let mut all_tables = Vec::new();
                
                for table_type in table_types {
                    let tables_result = match connection.connection_type {
                        models::enums::DatabaseType::MySQL => {
                            self.fetch_tables_from_mysql_connection(connection_id, database_name, table_type)
                        },
                        models::enums::DatabaseType::SQLite => {
                            self.fetch_tables_from_sqlite_connection(connection_id, table_type)
                        },
                        models::enums::DatabaseType::PostgreSQL => {
                            // TODO: Add PostgreSQL support
                            None
                        },
                        models::enums::DatabaseType::Redis => {
                            self.fetch_tables_from_redis_connection(connection_id, database_name, table_type)
                        },
                    };
                    
                    if let Some(tables) = tables_result {
                        for table_name in tables {
                            all_tables.push((table_name, table_type.to_string()));
                        }
                    }
                }
                
                if !all_tables.is_empty() {
                    
                    // Save tables to cache
                    self.save_tables_to_cache(connection_id, database_name, &all_tables);
                    
                    // For each table, fetch columns
                    for (table_name, table_type) in &all_tables {
                        if table_type == "table" { // Only fetch columns for actual tables, not views/procedures

                            let columns_result = self.fetch_columns_from_database(connection_id, database_name, table_name, &connection);
                            
                            if let Some(columns) = columns_result {                                
                                // Save columns to cache
                                self.save_columns_to_cache(connection_id, database_name, table_name, &columns);
                            }
                        }
                    }
                }
            }
            
        } else {
            println!("Failed to fetch databases from server for connection_id: {}", connection_id);
        }
    }

    // Function to clear cache for a connection (useful for refresh)
    fn clear_connection_cache(&self, connection_id: i64) {
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            rt.block_on(async {
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
            
        }
    }

    fn load_connection_tables(&mut self, connection_id: i64, node: &mut models::structs::TreeNode) {

        println!("Loading connection tables for ID: {}", connection_id);

        // First check if we have cached data
        if let Some(databases) = self.get_databases_from_cache(connection_id) {
            println!("Found cached databases for connection {}: {:?}", connection_id, databases);
            if !databases.is_empty() {
                self.build_connection_structure_from_cache(connection_id, node, &databases);
                node.is_loaded = true;
                return;
            }
        }

        println!("🔄 Cache empty or not found, fetching databases from server for connection {}", connection_id);
        
        // Try to fetch from actual database server
        if let Some(fresh_databases) = self.fetch_databases_from_connection(connection_id) {
            println!("✅ Successfully fetched {} databases from server", fresh_databases.len());
            // Save to cache for future use
            self.save_databases_to_cache(connection_id, &fresh_databases);
            // Build structure from fresh data
            self.build_connection_structure_from_cache(connection_id, node, &fresh_databases);
            node.is_loaded = true;
            return;
        } else {
            println!("❌ Failed to fetch databases from server, creating default structure");
        }

        
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            // Create the main structure based on database type
            match connection.connection_type {
                models::enums::DatabaseType::MySQL => {
                    self.load_mysql_structure(connection_id, &connection, node);
                },
                models::enums::DatabaseType::PostgreSQL => {
                    self.load_postgresql_structure(connection_id, &connection, node);
                },
                models::enums::DatabaseType::SQLite => {
                    self.load_sqlite_structure(connection_id, &connection, node);
                },
                models::enums::DatabaseType::Redis => {
                    self.load_redis_structure(connection_id, &connection, node);
                }
            }
            
            node.is_loaded = true;
        }
    }

    fn build_connection_structure_from_cache(&mut self, connection_id: i64, node: &mut models::structs::TreeNode, databases: &[String]) {
        // Find the connection to get its type
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let mut main_children = Vec::new();
            
            match connection.connection_type {
                models::enums::DatabaseType::MySQL => {
                    // 1. Databases folder
                    let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
                    databases_folder.connection_id = Some(connection_id);
                    
                    // Add each database from cache
                    for db_name in databases {
                        // Skip system databases for cleaner view
                        if !["information_schema", "performance_schema", "mysql", "sys"].contains(&db_name.as_str()) {
                            let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                            db_node.connection_id = Some(connection_id);
                            db_node.database_name = Some(db_name.clone());
                            db_node.is_loaded = false; // Will be loaded when expanded
                            
                            // Create folder structure but don't load content yet
                            let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                            tables_folder.connection_id = Some(connection_id);
                            tables_folder.database_name = Some(db_name.clone());
                            tables_folder.is_loaded = false;
                            
                            let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                            views_folder.connection_id = Some(connection_id);
                            views_folder.database_name = Some(db_name.clone());
                            views_folder.is_loaded = false;
                            
                            let mut procedures_folder = models::structs::TreeNode::new("Stored Procedures".to_string(), models::enums::NodeType::StoredProceduresFolder);
                            procedures_folder.connection_id = Some(connection_id);
                            procedures_folder.database_name = Some(db_name.clone());
                            procedures_folder.is_loaded = false;
                            
                            let mut functions_folder = models::structs::TreeNode::new("Functions".to_string(), models::enums::NodeType::UserFunctionsFolder);
                            functions_folder.connection_id = Some(connection_id);
                            functions_folder.database_name = Some(db_name.clone());
                            functions_folder.is_loaded = false;
                            
                            let mut triggers_folder = models::structs::TreeNode::new("Triggers".to_string(), models::enums::NodeType::TriggersFolder);
                            triggers_folder.connection_id = Some(connection_id);
                            triggers_folder.database_name = Some(db_name.clone());
                            triggers_folder.is_loaded = false;
                            
                            let mut events_folder = models::structs::TreeNode::new("Events".to_string(), models::enums::NodeType::EventsFolder);
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
                    let mut dba_folder = models::structs::TreeNode::new("DBA Views".to_string(), models::enums::NodeType::DBAViewsFolder);
                    dba_folder.connection_id = Some(connection_id);
                    
                    let mut dba_children = Vec::new();
                    
                    // Users
                    let mut users_folder = models::structs::TreeNode::new("Users".to_string(), models::enums::NodeType::UsersFolder);
                    users_folder.connection_id = Some(connection_id);
                    users_folder.is_loaded = false;
                    dba_children.push(users_folder);
                    
                    // Privileges
                    let mut priv_folder = models::structs::TreeNode::new("Privileges".to_string(), models::enums::NodeType::PrivilegesFolder);
                    priv_folder.connection_id = Some(connection_id);
                    priv_folder.is_loaded = false;
                    dba_children.push(priv_folder);
                    
                    // Processes
                    let mut proc_folder = models::structs::TreeNode::new("Processes".to_string(), models::enums::NodeType::ProcessesFolder);
                    proc_folder.connection_id = Some(connection_id);
                    proc_folder.is_loaded = false;
                    dba_children.push(proc_folder);
                    
                    // Status
                    let mut status_folder = models::structs::TreeNode::new("Status".to_string(), models::enums::NodeType::StatusFolder);
                    status_folder.connection_id = Some(connection_id);
                    status_folder.is_loaded = false;
                    dba_children.push(status_folder);
                    
                    dba_folder.children = dba_children;
                    
                    main_children.push(databases_folder);
                    main_children.push(dba_folder);
                },
                models::enums::DatabaseType::PostgreSQL => {
                    // Similar structure for PostgreSQL
                    let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
                    databases_folder.connection_id = Some(connection_id);
                    
                    for db_name in databases {
                        if !["template0", "template1", "postgres"].contains(&db_name.as_str()) {
                            let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                            db_node.connection_id = Some(connection_id);
                            db_node.database_name = Some(db_name.clone());
                            db_node.is_loaded = false;
                            
                            let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                            tables_folder.connection_id = Some(connection_id);
                            tables_folder.database_name = Some(db_name.clone());
                            tables_folder.is_loaded = false;
                            
                            let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                            views_folder.connection_id = Some(connection_id);
                            views_folder.database_name = Some(db_name.clone());
                            views_folder.is_loaded = false;
                            
                            db_node.children = vec![tables_folder, views_folder];
                            databases_folder.children.push(db_node);
                        }
                    }
                    
                    main_children.push(databases_folder);
                },
                models::enums::DatabaseType::SQLite => {
                    // SQLite structure - single database
                    let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                    tables_folder.connection_id = Some(connection_id);
                    tables_folder.database_name = Some("main".to_string());
                    tables_folder.is_loaded = false;
                    
                    let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                    views_folder.connection_id = Some(connection_id);
                    views_folder.database_name = Some("main".to_string());
                    views_folder.is_loaded = false;
                    
                    main_children = vec![tables_folder, views_folder];
                },
                models::enums::DatabaseType::Redis => {
                    // Redis structure with databases
                    self.build_redis_structure_from_cache(connection_id, node, databases);
                    return;
                }
            }
            
            node.children = main_children;
        }
    }

    fn build_redis_structure_from_cache(&mut self, connection_id: i64, node: &mut models::structs::TreeNode, databases: &[String]) {
        let mut main_children = Vec::new();
        
        // Create databases folder for Redis
        let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        databases_folder.is_expanded = false;
        databases_folder.is_loaded = true;
        
        // Add each Redis database from cache (db0, db1, etc.)
        for db_name in databases {
            if db_name.starts_with("db") {
                let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                db_node.connection_id = Some(connection_id);
                db_node.database_name = Some(db_name.clone());
                db_node.is_loaded = false; // Keys will be loaded when clicked
                
                // Check if this database has keys by looking for the marker
                let has_keys = self.check_redis_database_has_keys(connection_id, db_name);
                if has_keys {
                    // Add a placeholder for keys that will be loaded on expansion
                    let loading_node = models::structs::TreeNode::new("Loading keys...".to_string(), models::enums::NodeType::Table);
                    db_node.children.push(loading_node);
                }
                
                databases_folder.children.push(db_node);
            }
        }
        
        main_children.push(databases_folder);
        node.children = main_children;
    }

    fn check_redis_database_has_keys(&self, connection_id: i64, database_name: &str) -> bool {
        if let Some(ref pool) = self.db_pool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let pool_clone = pool.clone();
            let database_name = database_name.to_string();
            
            let result = rt.block_on(async move {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM table_cache WHERE connection_id = ? AND database_name = ? AND table_name = '_has_keys'"
                )
                .bind(connection_id)
                .bind(database_name)
                .fetch_one(pool_clone.as_ref())
                .await
                .unwrap_or(0)
            });
            
            result > 0
        } else {
            false
        }
    }

    // More specific function to find folder node with exact type and database name
    fn find_specific_folder_node<'a>(node: &'a mut models::structs::TreeNode, connection_id: i64, folder_type: &models::enums::NodeType, database_name: &Option<String>) -> Option<&'a mut models::structs::TreeNode> {
        // Check if this node is the folder we're looking for
        if node.node_type == *folder_type && 
           node.connection_id == Some(connection_id) && 
           node.database_name == *database_name &&
           node.is_expanded && 
           !node.is_loaded {
            return Some(node);
        }
        
        // Recursively search in children
        for child in &mut node.children {
            if let Some(result) = Self::find_specific_folder_node(child, connection_id, folder_type, database_name) {
                return Some(result);
            }
        }
        
        None
    }

    fn load_databases_for_folder(&mut self, connection_id: i64, databases_folder: &mut models::structs::TreeNode) {
        // Check connection type to handle Redis differently
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            if connection.connection_type == models::enums::DatabaseType::Redis {
                self.load_redis_databases_for_folder(connection_id, databases_folder);
                return;
            }
        }
        
        // Clear any loading placeholders
        databases_folder.children.clear();
        
        // First check cache
        if let Some(cached_databases) = self.get_databases_from_cache(connection_id) {
            if !cached_databases.is_empty() {
                
                for db_name in cached_databases {
                    let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                    db_node.connection_id = Some(connection_id);
                    db_node.database_name = Some(db_name.clone());
                    db_node.is_loaded = false;
                    
                    // Add subfolders for each database
                    let mut db_children = Vec::new();
                    
                    // Tables folder
                    let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                    tables_folder.connection_id = Some(connection_id);
                    tables_folder.database_name = Some(db_name.clone());
                    tables_folder.is_loaded = false;
                    db_children.push(tables_folder);
                    
                    // Views folder
                    let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                    views_folder.connection_id = Some(connection_id);
                    views_folder.database_name = Some(db_name.clone());
                    views_folder.is_loaded = false;
                    db_children.push(views_folder);
                    
                    // Stored Procedures folder
                    let mut sp_folder = models::structs::TreeNode::new("Stored Procedures".to_string(), models::enums::NodeType::StoredProceduresFolder);
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
                
        // Try to fetch real databases from the connection
        if let Some(real_databases) = self.fetch_databases_from_connection(connection_id) {
            
            // Save to cache for future use
            self.save_databases_to_cache(connection_id, &real_databases);
            
            // Create tree nodes from fetched data
            for db_name in real_databases {
                let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                db_node.connection_id = Some(connection_id);
                db_node.database_name = Some(db_name.clone());
                db_node.is_loaded = false;
                
                // Add subfolders for each database
                let mut db_children = Vec::new();
                
                // Tables folder
                let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                tables_folder.connection_id = Some(connection_id);
                tables_folder.database_name = Some(db_name.clone());
                tables_folder.is_loaded = false;
                db_children.push(tables_folder);
                
                // Views folder
                let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                views_folder.connection_id = Some(connection_id);
                views_folder.database_name = Some(db_name.clone());
                views_folder.is_loaded = false;
                db_children.push(views_folder);
                
                // Stored Procedures folder
                let mut sp_folder = models::structs::TreeNode::new("Stored Procedures".to_string(), models::enums::NodeType::StoredProceduresFolder);
                sp_folder.connection_id = Some(connection_id);
                sp_folder.database_name = Some(db_name.clone());
                sp_folder.is_loaded = false;
                db_children.push(sp_folder);
                
                db_node.children = db_children;
                databases_folder.children.push(db_node);
            }
            
            databases_folder.is_loaded = true;
        } else {
            self.populate_sample_databases_for_folder(connection_id, databases_folder);
        }
    }
    
    fn populate_sample_databases_for_folder(&mut self, connection_id: i64, databases_folder: &mut models::structs::TreeNode) {
        // Find the connection to determine type
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let sample_databases = match connection.connection_type {
                models::enums::DatabaseType::MySQL => vec!["information_schema".to_string(), "sakila".to_string(), "world".to_string(), "test".to_string()],
                models::enums::DatabaseType::PostgreSQL => vec!["postgres".to_string(), "template1".to_string(), "dvdrental".to_string()],
                models::enums::DatabaseType::SQLite => vec!["main".to_string()],
                models::enums::DatabaseType::Redis => vec!["redis".to_string(), "info".to_string()],
            };
            
            // Clear loading message
            databases_folder.children.clear();
            
            // Add sample databases
            for db_name in sample_databases {
                // Skip system databases for display
                if matches!(connection.connection_type, models::enums::DatabaseType::MySQL) && 
                   ["information_schema", "performance_schema", "mysql", "sys"].contains(&db_name.as_str()) {
                    continue;
                }
                
                let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                db_node.connection_id = Some(connection_id);
                db_node.database_name = Some(db_name.clone());
                db_node.is_loaded = false;
                
                // Add subfolders for each database
                let mut db_children = Vec::new();
                
                // Tables folder
                let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
                tables_folder.connection_id = Some(connection_id);
                tables_folder.database_name = Some(db_name.clone());
                tables_folder.is_loaded = false;
                db_children.push(tables_folder);
                
                // Views folder  
                let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
                views_folder.connection_id = Some(connection_id);
                views_folder.database_name = Some(db_name.clone());
                views_folder.is_loaded = false;
                db_children.push(views_folder);
                
                if matches!(connection.connection_type, models::enums::DatabaseType::MySQL) {
                    // Stored Procedures folder
                    let mut sp_folder = models::structs::TreeNode::new("Stored Procedures".to_string(), models::enums::NodeType::StoredProceduresFolder);
                    sp_folder.connection_id = Some(connection_id);
                    sp_folder.database_name = Some(db_name.clone());
                    sp_folder.is_loaded = false;
                    db_children.push(sp_folder);
                    
                    // User Functions folder
                    let mut uf_folder = models::structs::TreeNode::new("User Functions".to_string(), models::enums::NodeType::UserFunctionsFolder);
                    uf_folder.connection_id = Some(connection_id);
                    uf_folder.database_name = Some(db_name.clone());
                    uf_folder.is_loaded = false;
                    db_children.push(uf_folder);
                    
                    // Triggers folder
                    let mut triggers_folder = models::structs::TreeNode::new("Triggers".to_string(), models::enums::NodeType::TriggersFolder);
                    triggers_folder.connection_id = Some(connection_id);
                    triggers_folder.database_name = Some(db_name.clone());
                    triggers_folder.is_loaded = false;
                    db_children.push(triggers_folder);
                    
                    // Events folder
                    let mut events_folder = models::structs::TreeNode::new("Events".to_string(), models::enums::NodeType::EventsFolder);
                    events_folder.connection_id = Some(connection_id);
                    events_folder.database_name = Some(db_name.clone());
                    events_folder.is_loaded = false;
                    db_children.push(events_folder);
                }
                
                db_node.children = db_children;
                databases_folder.children.push(db_node);
            }
            
        }
    }
    
    fn load_redis_databases_for_folder(&mut self, connection_id: i64, databases_folder: &mut models::structs::TreeNode) {
        // Clear loading placeholders
        databases_folder.children.clear();

        // Ambil daftar database Redis dari cache
        if let Some(cached_databases) = self.get_databases_from_cache(connection_id) {
            for db_name in cached_databases {
                if db_name.starts_with("db") {
                    let mut db_node = models::structs::TreeNode::new(db_name.clone(), models::enums::NodeType::Database);
                    db_node.connection_id = Some(connection_id);
                    db_node.database_name = Some(db_name.clone());
                    db_node.is_loaded = false;

                    // Tambahkan node child untuk key, akan di-load saat node db di-expand
                    let loading_keys_node = models::structs::TreeNode::new("Loading keys...".to_string(), models::enums::NodeType::Table);
                    db_node.children.push(loading_keys_node);

                    databases_folder.children.push(db_node);
                }
            }
            databases_folder.is_loaded = true;
        }
    }

    fn find_redis_database_node<'a>(node: &'a mut models::structs::TreeNode, connection_id: i64, database_name: &Option<String>) -> Option<&'a mut models::structs::TreeNode> {
        // Check if this is the database node we're looking for
        if node.connection_id == Some(connection_id) && 
           node.node_type == models::enums::NodeType::Database && 
           node.database_name == *database_name {
            return Some(node);
        }
        
        // Recursively search in children
        for child in &mut node.children {
            if let Some(found) = Self::find_redis_database_node(child, connection_id, database_name) {
                return Some(found);
            }
        }
        
        None
    }

    fn load_redis_keys_for_database(&mut self, connection_id: i64, database_name: &str, db_node: &mut models::structs::TreeNode) {
        
        // Clear existing children and mark as loading
        db_node.children.clear();
        
        // Extract database number from database_name (e.g., "db0" -> 0)
        let db_number = if let Some(suffix) = database_name.strip_prefix("db") {
            suffix.parse::<u8>().unwrap_or(0)
        } else {
            0
        };
        
        // Get connection pool and fetch keys
        let rt = tokio::runtime::Runtime::new().unwrap();
        let keys_result = rt.block_on(async {
            if let Some(pool) = connection::get_or_create_connection_pool(self, connection_id).await {
                if let models::enums::DatabasePool::Redis(redis_manager) = pool {
                    let mut conn = redis_manager.as_ref().clone();
                    
                    // Select the specific database
                    if let Err(e) = redis::cmd("SELECT").arg(db_number).query_async::<_, ()>(&mut conn).await {
                        println!("❌ Failed to select database {}: {}", db_number, e);
                        return Vec::new();
                    }
                    
                    // Use SCAN for safe key enumeration (better than KEYS * in production)
                    let mut cursor = 0u64;
                    let mut all_keys = Vec::new();
                    let max_keys = 100; // Limit to first 100 keys to avoid overwhelming UI
                    
                    loop {
                        match redis::cmd("SCAN")
                            .arg(cursor)
                            .arg("COUNT")
                            .arg(10)
                            .query_async::<_, (u64, Vec<String>)>(&mut conn)
                            .await 
                        {
                            Ok((next_cursor, keys)) => {
                                for key in keys {
                                    if all_keys.len() >= max_keys {
                                        break;
                                    }
                                    
                                    // Get the type of each key
                                    if let Ok(key_type) = redis::cmd("TYPE").arg(&key).query_async::<_, String>(&mut conn).await {
                                        all_keys.push((key, key_type));
                                    }
                                }
                                
                                cursor = next_cursor;
                                if cursor == 0 || all_keys.len() >= max_keys {
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("❌ SCAN command failed: {}", e);
                                break;
                            }
                        }
                    }
                    
                    println!("✅ Found {} keys in database {}", all_keys.len(), database_name);
                    all_keys
                } else {
                    println!("❌ Connection pool is not Redis type");
                    Vec::new()
                }
            } else {
                println!("❌ Failed to get Redis connection pool");
                Vec::new()
            }
        });
        
        // Group keys by type
        let mut keys_by_type: std::collections::HashMap<String, Vec<(String, String)>> = std::collections::HashMap::new();
        for (key, key_type) in keys_result {
            keys_by_type.entry(key_type.clone()).or_insert_with(Vec::new).push((key, key_type));
        }
        
        // Create folder structure for each Redis data type
        for (data_type, keys) in keys_by_type {
            let folder_name = match data_type.as_str() {
                "string" => "Strings",
                "hash" => "Hashes", 
                "list" => "Lists",
                "set" => "Sets",
                "zset" => "Sorted Sets",
                "stream" => "Streams",
                _ => &data_type,
            };
            
            let mut type_folder = models::structs::TreeNode::new(format!("{} ({})", folder_name, keys.len()), models::enums::NodeType::TablesFolder);
            type_folder.connection_id = Some(connection_id);
            type_folder.database_name = Some(database_name.to_string());
            type_folder.is_expanded = false;
            type_folder.is_loaded = true;
            
            // Add keys of this type to the folder
            for (key, _key_type) in keys {
                let mut key_node = models::structs::TreeNode::new(key.clone(), models::enums::NodeType::Table);
                key_node.connection_id = Some(connection_id);
                key_node.database_name = Some(database_name.to_string());
                type_folder.children.push(key_node);
            }
            
            db_node.children.push(type_folder);
        }
        
        db_node.is_loaded = true;
        println!("✅ Database node loaded with {} type folders", db_node.children.len());
    }
    
    fn fetch_databases_from_connection(&mut self, connection_id: i64) -> Option<Vec<String>> {
        
        // Find the connection configuration
        let _connection = self.connections.iter().find(|c| c.id == Some(connection_id))?.clone();
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = connection::get_or_create_connection_pool(self, connection_id).await?;
            
            match pool {
                models::enums::DatabasePool::MySQL(mysql_pool) => {
                    let result = sqlx::query_as::<_, (String,)>("SHOW DATABASES")
                        .fetch_all(mysql_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let databases: Vec<String> = rows.into_iter()
                                .map(|(db_name,)| db_name)
                                .filter(|db| !["information_schema", "performance_schema", "mysql", "sys"].contains(&db.as_str()))
                                .collect();
                            Some(databases)
                        },
                        Err(e) => {
                            println!("Error querying MySQL databases: {}", e);
                            None
                        }
                    }
                },
                models::enums::DatabasePool::PostgreSQL(pg_pool) => {
                    let result = sqlx::query_as::<_, (String,)>(
                        "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres', 'template0', 'template1')"
                    )
                    .fetch_all(pg_pool.as_ref())
                    .await;
                    
                    match result {
                        Ok(rows) => {
                            let databases: Vec<String> = rows.into_iter().map(|(db_name,)| db_name).collect();
                            Some(databases)
                        },
                        Err(e) => {
                            println!("Error querying PostgreSQL databases: {}", e);
                            None
                        }
                    }
                },
                models::enums::DatabasePool::SQLite(sqlite_pool) => {
                    // For SQLite, we'll query the actual database for table information
                    let result = sqlx::query_as::<_, (String,)>("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                        .fetch_all(sqlite_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let table_count = rows.len();
                            if table_count > 0 {
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
                },
                models::enums::DatabasePool::Redis(redis_manager) => {
                    // For Redis, get actual databases (db0, db1, etc.)
                    let mut conn = redis_manager.as_ref().clone();
                    
                    // Get CONFIG GET databases to determine max database count
                    let max_databases = match redis::cmd("CONFIG").arg("GET").arg("databases").query_async::<_, Vec<String>>(&mut conn).await {
                        Ok(config_result) if config_result.len() >= 2 => {
                            config_result[1].parse::<i32>().unwrap_or(16)
                        }
                        _ => 16 // Default fallback
                    };
                    
                    println!("Redis max databases: {}", max_databases);
                    
                    // Create list of all Redis databases (db0 to db15 by default)
                    let mut databases = Vec::new();
                    for db_num in 0..max_databases {
                        let db_name = format!("db{}", db_num);
                        databases.push(db_name);
                    }
                    
                    println!("Generated Redis databases: {:?}", databases);
                    Some(databases)
                }
            }
        })
    }
    
    fn fetch_tables_from_mysql_connection(&mut self, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = connection::get_or_create_connection_pool(self, connection_id).await?;
            
            match pool {
                models::enums::DatabasePool::MySQL(mysql_pool) => {
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
                    
                    let result = sqlx::query_as::<_, (String,)>(&query)
                        .fetch_all(mysql_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let items: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
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
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = connection::get_or_create_connection_pool(self, connection_id).await?;
            
            match pool {
                models::enums::DatabasePool::SQLite(sqlite_pool) => {
                    let query = match table_type {
                        "table" => "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        "view" => "SELECT name FROM sqlite_master WHERE type='view'",
                        _ => {
                            println!("Unsupported table type for SQLite: {}", table_type);
                            return None;
                        }
                    };
                    
                    let result = sqlx::query_as::<_, (String,)>(query)
                        .fetch_all(sqlite_pool.as_ref())
                        .await;
                        
                    match result {
                        Ok(rows) => {
                            let items: Vec<String> = rows.into_iter().map(|(name,)| name).collect();
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
    
    fn fetch_tables_from_redis_connection(&mut self, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        rt.block_on(async {
            // Get or create connection pool
            let pool = connection::get_or_create_connection_pool(self, connection_id).await?;
            
            match pool {
                models::enums::DatabasePool::Redis(redis_manager) => {
                    let mut conn = redis_manager.as_ref().clone();
                    match table_type {
                                "info_section" => {
                                    // Return the info sections we cached
                                    if database_name == "info" {
                                        // Get Redis INFO sections
                                        match redis::cmd("INFO").query_async::<_, String>(&mut conn).await {
                                            Ok(info_result) => {
                                                let sections: Vec<String> = info_result
                                                    .lines()
                                                    .filter(|line| line.starts_with('#') && !line.is_empty())
                                                    .map(|line| line.trim_start_matches('#').trim().to_string())
                                                    .filter(|section| !section.is_empty())
                                                    .collect();
                                                Some(sections)
                                            },
                                            Err(e) => {
                                                println!("Error getting Redis INFO: {}", e);
                                                None
                                            }
                                        }
                                    } else {
                                        None
                                    }
                                },
                                "redis_keys" => {
                                    // Get sample keys from Redis
                                    if database_name.starts_with("db") {
                                        // Select the specific database
                                        if let Ok(db_num) = database_name.trim_start_matches("db").parse::<i32>() {
                                            if let Ok(_) = redis::cmd("SELECT").arg(db_num).query_async::<_, String>(&mut conn).await {
                                                // Get a sample of keys (limit to first 100)
                                                match redis::cmd("SCAN").arg(0).arg("COUNT").arg(100).query_async::<_, Vec<String>>(&mut conn).await {
                                                    Ok(keys) => Some(keys),
                                                    Err(e) => {
                                                        println!("Error scanning Redis keys: {}", e);
                                                        Some(vec!["keys".to_string()]) // Return generic "keys" entry
                                                    }
                                                }
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                },
                                _ => {
                                    println!("Unsupported Redis table type: {}", table_type);
                                    None
                                }
                            }
                },
                _ => {
                    println!("Wrong pool type for Redis connection");
                    None
                }
            }
        })
    }

    fn fetch_columns_from_database(&self, _connection_id: i64, database_name: &str, table_name: &str, connection: &models::structs::ConnectionConfig) -> Option<Vec<(String, String)>> {
        
        // Create a new runtime for the database query
        let rt = tokio::runtime::Runtime::new().ok()?;
        
        // Clone data to move into async block
        let connection_clone = connection.clone();
        let database_name = database_name.to_string();
        let table_name = table_name.to_string();
        
        rt.block_on(async {
            match connection_clone.connection_type {
                models::enums::DatabaseType::MySQL => {
                    // Create MySQL connection
                    let encoded_username = modules::url_encode(&connection_clone.username);
                    let encoded_password = modules::url_encode(&connection_clone.password);
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
                models::enums::DatabaseType::SQLite => {
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
                models::enums::DatabaseType::PostgreSQL => {
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
                },
                models::enums::DatabaseType::Redis => {
                    // Redis doesn't have traditional tables/columns
                    // Return some generic "columns" for Redis key-value structure
                    Some(vec![
                        ("key".to_string(), "String".to_string()),
                        ("value".to_string(), "Any".to_string()),
                        ("type".to_string(), "String".to_string()),
                        ("ttl".to_string(), "Integer".to_string()),
                    ])
                }
            }
        })
    }

    fn load_mysql_structure(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {

        println!("Loading MySQL structure for connection ID: {}", connection_id);
        
        // Since we can't use block_on in an async context, we'll create a simple structure
        // and populate it with cached data or show a loading message
        
        // Create basic structure immediately
        let mut main_children = Vec::new();
        
        // 1. Databases folder
        let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        databases_folder.is_loaded = false; // Will be loaded when expanded
        
        // 2. DBA Views folder
        let mut dba_folder = models::structs::TreeNode::new("DBA Views".to_string(), models::enums::NodeType::DBAViewsFolder);
        dba_folder.connection_id = Some(connection_id);
        
        let mut dba_children = Vec::new();
        
        // Users
        let mut users_folder = models::structs::TreeNode::new("Users".to_string(), models::enums::NodeType::UsersFolder);
        users_folder.connection_id = Some(connection_id);
        users_folder.is_loaded = false;
        dba_children.push(users_folder);
        
        // Privileges
        let mut priv_folder = models::structs::TreeNode::new("Privileges".to_string(), models::enums::NodeType::PrivilegesFolder);
        priv_folder.connection_id = Some(connection_id);
        priv_folder.is_loaded = false;
        dba_children.push(priv_folder);
        
        // Processes
        let mut proc_folder = models::structs::TreeNode::new("Processes".to_string(), models::enums::NodeType::ProcessesFolder);
        proc_folder.connection_id = Some(connection_id);
        proc_folder.is_loaded = false;
        dba_children.push(proc_folder);
        
        // Status
        let mut status_folder = models::structs::TreeNode::new("Status".to_string(), models::enums::NodeType::StatusFolder);
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

    fn load_postgresql_structure(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {
        
        // Create basic structure for PostgreSQL
        let mut main_children = Vec::new();
        
        // Databases folder
        let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        
        // Add a loading indicator
        let loading_node = models::structs::TreeNode::new("Loading databases...".to_string(), models::enums::NodeType::Database);
        databases_folder.children.push(loading_node);
        
        main_children.push(databases_folder);
        
        node.children = main_children;
    }

    fn load_sqlite_structure(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {
        
        // Create basic structure for SQLite
        let mut main_children = Vec::new();
        
        // Tables folder
        let mut tables_folder = models::structs::TreeNode::new("Tables".to_string(), models::enums::NodeType::TablesFolder);
        tables_folder.connection_id = Some(connection_id);
        tables_folder.database_name = Some("main".to_string());
        tables_folder.is_loaded = false;
        
        // Add a loading indicator
        let loading_node = models::structs::TreeNode::new("Loading tables...".to_string(), models::enums::NodeType::Table);
        tables_folder.children.push(loading_node);
        
        main_children.push(tables_folder);
        
        // Views folder
        let mut views_folder = models::structs::TreeNode::new("Views".to_string(), models::enums::NodeType::ViewsFolder);
        views_folder.connection_id = Some(connection_id);
        views_folder.database_name = Some("main".to_string());
        views_folder.is_loaded = false;
        main_children.push(views_folder);
        
        node.children = main_children;
    }

    fn load_redis_structure(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {
        // Check if we have cached databases
        if let Some(databases) = self.get_databases_from_cache(connection_id) {
            println!("🔍 Found cached Redis databases: {:?}", databases);
            if !databases.is_empty() {
                self.build_redis_structure_from_cache(connection_id, node, &databases);
                node.is_loaded = true;
                return;
            }
        }
        
        println!("🔄 No cached Redis databases found, fetching from server...");
        
        // Fetch fresh data from Redis server
        self.fetch_and_cache_connection_data(connection_id);
        
        // Try again to get from cache after fetching
        if let Some(databases) = self.get_databases_from_cache(connection_id) {
            println!("✅ Successfully loaded Redis databases from server: {:?}", databases);
            if !databases.is_empty() {
                self.build_redis_structure_from_cache(connection_id, node, &databases);
                node.is_loaded = true;
                return;
            }
        }
        
        // Create basic structure for Redis with databases as fallback
        let mut main_children = Vec::new();
        
        // Add databases folder for Redis
        let mut databases_folder = models::structs::TreeNode::new("Databases".to_string(), models::enums::NodeType::DatabasesFolder);
        databases_folder.connection_id = Some(connection_id);
        databases_folder.is_loaded = false;
        
        // Add a loading indicator
        let loading_node = models::structs::TreeNode::new("Loading databases...".to_string(), models::enums::NodeType::Database);
        databases_folder.children.push(loading_node);
        
        main_children.push(databases_folder);
        
        node.children = main_children;
    }

    fn load_folder_content(&mut self, connection_id: i64, node: &mut models::structs::TreeNode, folder_type: models::enums::NodeType) {        
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            
            match connection.connection_type {
                models::enums::DatabaseType::MySQL => {
                    self.load_mysql_folder_content(connection_id, &connection, node, folder_type);
                },
                models::enums::DatabaseType::PostgreSQL => {
                    self.load_postgresql_folder_content(connection_id, &connection, node, folder_type);
                },
                models::enums::DatabaseType::SQLite => {
                    self.load_sqlite_folder_content(connection_id, &connection, node, folder_type);
                },
                models::enums::DatabaseType::Redis => {
                    self.load_redis_folder_content(connection_id, &connection, node, folder_type);
                }
            }
            
            node.is_loaded = true;
        } else {
            println!("ERROR: Connection with ID {} not found!", connection_id);
        }
    }

    fn load_mysql_folder_content(&mut self, connection_id: i64, connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode, folder_type: models::enums::NodeType) {
        // Get database name from node or connection default
        let database_name = node.database_name.as_ref().unwrap_or(&connection.database);
        
        // Map folder type to cache table type
        let table_type = match folder_type {
            models::enums::NodeType::TablesFolder => "table",
            models::enums::NodeType::ViewsFolder => "view",
            models::enums::NodeType::StoredProceduresFolder => "procedure",
            models::enums::NodeType::UserFunctionsFolder => "function",
            models::enums::NodeType::TriggersFolder => "trigger",
            models::enums::NodeType::EventsFolder => "event",
            _ => {
                println!("Unsupported folder type: {:?}", folder_type);
                return;
            }
        };
        
        // First try to get from cache
        if let Some(cached_items) = cache_data::get_tables_from_cache(self, connection_id, database_name, table_type) {
            if !cached_items.is_empty() {                
                // Create tree nodes from cached data
                let child_nodes: Vec<models::structs::TreeNode> = cached_items.into_iter().map(|item_name| {
                    let mut child_node = models::structs::TreeNode::new(item_name.clone(), match folder_type {
                        models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                        models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                        models::enums::NodeType::StoredProceduresFolder => models::enums::NodeType::StoredProcedure,
                        models::enums::NodeType::UserFunctionsFolder => models::enums::NodeType::UserFunction,
                        models::enums::NodeType::TriggersFolder => models::enums::NodeType::Trigger,
                        models::enums::NodeType::EventsFolder => models::enums::NodeType::Event,
                        _ => models::enums::NodeType::Table,
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
        if let Some(real_items) = self.fetch_tables_from_mysql_connection(connection_id, database_name, table_type) {
            println!("Successfully fetched {} {} from MySQL database", real_items.len(), table_type);
            
            // Save to cache for future use
            let table_data: Vec<(String, String)> = real_items.iter().map(|name| (name.clone(), table_type.to_string())).collect();
            self.save_tables_to_cache(connection_id, database_name, &table_data);
            
            // Create tree nodes from fetched data
            let child_nodes: Vec<models::structs::TreeNode> = real_items.into_iter().map(|item_name| {
                let mut child_node = models::structs::TreeNode::new(item_name.clone(), match folder_type {
                    models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                    models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                    models::enums::NodeType::StoredProceduresFolder => models::enums::NodeType::StoredProcedure,
                    models::enums::NodeType::UserFunctionsFolder => models::enums::NodeType::UserFunction,
                    models::enums::NodeType::TriggersFolder => models::enums::NodeType::Trigger,
                    models::enums::NodeType::EventsFolder => models::enums::NodeType::Event,
                    _ => models::enums::NodeType::Table,
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
                models::enums::NodeType::TablesFolder => vec!["users".to_string(), "products".to_string(), "orders".to_string()],
                models::enums::NodeType::ViewsFolder => vec!["user_orders_view".to_string(), "product_summary_view".to_string()],
                models::enums::NodeType::StoredProceduresFolder => vec!["sp_get_user".to_string(), "sp_create_order".to_string()],
                models::enums::NodeType::UserFunctionsFolder => vec!["fn_calculate_total".to_string()],
                models::enums::NodeType::TriggersFolder => vec!["tr_update_timestamp".to_string()],
                models::enums::NodeType::EventsFolder => vec!["ev_cleanup".to_string()],
                _ => vec![],
            };
            
            // Create tree nodes
            let child_nodes: Vec<models::structs::TreeNode> = sample_items.into_iter().map(|item_name| {
                let mut child_node = models::structs::TreeNode::new(item_name.clone(), match folder_type {
                    models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                    models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                    models::enums::NodeType::StoredProceduresFolder => models::enums::NodeType::StoredProcedure,
                    models::enums::NodeType::UserFunctionsFolder => models::enums::NodeType::UserFunction,
                    models::enums::NodeType::TriggersFolder => models::enums::NodeType::Trigger,
                    models::enums::NodeType::EventsFolder => models::enums::NodeType::Event,
                    _ => models::enums::NodeType::Table,
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

    fn load_postgresql_folder_content(&mut self, _connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode, _folder_type: models::enums::NodeType) {
        // Placeholder for PostgreSQL folder content loading
        node.children = vec![models::structs::TreeNode::new("PostgreSQL folder content not implemented yet".to_string(), models::enums::NodeType::Column)];
    }

    fn load_sqlite_folder_content(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode, folder_type: models::enums::NodeType) {
        println!("Loading {:?} content for SQLite", folder_type);
        
        // Try to get from cache first
        let table_type = match folder_type {
            models::enums::NodeType::TablesFolder => "table",
            models::enums::NodeType::ViewsFolder => "view",
            _ => {
                // For other folder types, return empty for now
                node.children = vec![models::structs::TreeNode::new("Not supported for SQLite".to_string(), models::enums::NodeType::Column)];
                return;
            }
        };
        
        if let Some(cached_items) = cache_data::get_tables_from_cache(self, connection_id, "main", table_type) {
            if !cached_items.is_empty() {
                println!("Loading {} {} from cache for SQLite", cached_items.len(), table_type);
                
                node.children = cached_items.into_iter().map(|item_name| {
                    let node_type = match folder_type {
                        models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                        models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                        _ => models::enums::NodeType::Table,
                    };
                    
                    let mut item_node = models::structs::TreeNode::new(item_name, node_type);
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
            let child_nodes: Vec<models::structs::TreeNode> = real_items.into_iter().map(|item_name| {
                let node_type = match folder_type {
                    models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                    models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                    _ => models::enums::NodeType::Table,
                };
                
                let mut item_node = models::structs::TreeNode::new(item_name, node_type);
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
                models::enums::NodeType::TablesFolder => vec!["users".to_string(), "products".to_string(), "orders".to_string(), "categories".to_string()],
                models::enums::NodeType::ViewsFolder => vec!["user_summary".to_string(), "order_details".to_string()],
                _ => vec![],
            };
            
            let item_type = match folder_type {
                models::enums::NodeType::TablesFolder => models::enums::NodeType::Table,
                models::enums::NodeType::ViewsFolder => models::enums::NodeType::View,
                _ => models::enums::NodeType::Column, // fallback
            };
            
            node.children = sample_items.into_iter().map(|item_name| {
                let mut item_node = models::structs::TreeNode::new(item_name.clone(), item_type.clone());
                item_node.connection_id = Some(connection_id);
                item_node.database_name = Some("main".to_string());
                item_node.is_loaded = false;
                item_node
            }).collect();
        }
        
        println!("Loaded {} items into {:?} folder for SQLite", node.children.len(), folder_type);
    }

    fn load_redis_folder_content(&mut self, connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode, folder_type: models::enums::NodeType) {
        println!("Loading {:?} content for Redis", folder_type);
        
        // Redis doesn't have traditional folder structures like SQL databases
        // We'll create a simplified structure based on Redis concepts
        match folder_type {
            models::enums::NodeType::TablesFolder => {
                // For Redis, "tables" could be key patterns or data structures
                let redis_structures = vec![
                    "strings".to_string(),
                    "hashes".to_string(), 
                    "lists".to_string(),
                    "sets".to_string(),
                    "sorted_sets".to_string(),
                    "streams".to_string(),
                ];
                
                node.children = redis_structures.into_iter().map(|structure_name| {
                    let mut structure_node = models::structs::TreeNode::new(structure_name, models::enums::NodeType::Table);
                    structure_node.connection_id = Some(connection_id);
                    structure_node.database_name = Some("redis".to_string());
                    structure_node.is_loaded = false;
                    structure_node
                }).collect();
            },
            models::enums::NodeType::ViewsFolder => {
                // For Redis, "views" could be info sections
                let info_sections = vec![
                    "server".to_string(),
                    "clients".to_string(),
                    "memory".to_string(),
                    "persistence".to_string(),
                    "stats".to_string(),
                    "replication".to_string(),
                    "cpu".to_string(),
                    "keyspace".to_string(),
                ];
                
                node.children = info_sections.into_iter().map(|section_name| {
                    let mut section_node = models::structs::TreeNode::new(section_name, models::enums::NodeType::View);
                    section_node.connection_id = Some(connection_id);
                    section_node.database_name = Some("info".to_string());
                    section_node.is_loaded = false;
                    section_node
                }).collect();
            },
            _ => {
                // Other folder types not supported for Redis
                node.children = vec![models::structs::TreeNode::new("Not supported for Redis".to_string(), models::enums::NodeType::Column)];
            }
        }
        
        println!("Loaded {} items into {:?} folder for Redis", node.children.len(), folder_type);
    }

    fn load_table_columns_sync(&self, connection_id: i64, table_name: &str, connection: &models::structs::ConnectionConfig, database_name: &str) -> Vec<models::structs::TreeNode> {
        // First try to get from cache
        if let Some(cached_columns) = self.get_columns_from_cache(connection_id, database_name, table_name) {
            if !cached_columns.is_empty() {
                return cached_columns.into_iter().map(|(column_name, data_type)| {
                    models::structs::TreeNode::new(format!("{} ({})", column_name, data_type), models::enums::NodeType::Column)
                }).collect();
            }
        }
        
        // If cache is empty, fetch from actual database
        if let Some(real_columns) = self.fetch_columns_from_database(connection_id, database_name, table_name, connection) {
            // Save to cache for future use
            self.save_columns_to_cache(connection_id, database_name, table_name, &real_columns);
            
            // Convert to models::structs::TreeNode
            real_columns.into_iter().map(|(column_name, data_type)| {
                models::structs::TreeNode::new(format!("{} ({})", column_name, data_type), models::enums::NodeType::Column)
            }).collect()
        } else {
            // If database fetch fails, return sample columns
            vec![
                models::structs::TreeNode::new("id (INTEGER)".to_string(), models::enums::NodeType::Column),
                models::structs::TreeNode::new("name (VARCHAR)".to_string(), models::enums::NodeType::Column),
                models::structs::TreeNode::new("created_at (TIMESTAMP)".to_string(), models::enums::NodeType::Column),
            ]
        }
    }

    fn load_table_columns_for_node(&mut self, connection_id: i64, table_name: &str, nodes: &mut [models::structs::TreeNode], _table_index: usize) {
        // Find the connection by ID
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)) {
            let connection = connection.clone();
            
            // Find the table node to get the correct database_name
            let database_name = self.find_table_database_name(nodes, table_name, connection_id)
                .unwrap_or_else(|| connection.database.clone());
            
            // Load columns for this table without creating new runtime
            let columns = self.load_table_columns_sync(connection_id, table_name, &connection, &database_name);
            
            // Find the table node recursively and update it
            let updated = self.update_table_node_with_columns_recursive(nodes, table_name, columns, connection_id);
            
            if !updated {
                // Log only if update failed
            }
        }
    }

    fn find_table_database_name(&self, nodes: &[models::structs::TreeNode], table_name: &str, connection_id: i64) -> Option<String> {
        for node in nodes {
            // If this is the table node we're looking for
            if node.node_type == models::enums::NodeType::Table && 
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

    fn update_table_node_with_columns_recursive(&mut self, nodes: &mut [models::structs::TreeNode], table_name: &str, columns: Vec<models::structs::TreeNode>, connection_id: i64) -> bool {
        for node in nodes.iter_mut() {
            // If this is the table node we're looking for
            if node.node_type == models::enums::NodeType::Table && 
               node.name == table_name && 
               node.connection_id == Some(connection_id) {
                node.children = columns;
                node.is_loaded = true;
                return true;
            }
            
            // Recursively search in children
            if self.update_table_node_with_columns_recursive(&mut node.children, table_name, columns.clone(), connection_id) {
                return true;
            }
        }
        false
    }

    // Pagination methods
    fn update_pagination_data(&mut self, all_data: Vec<Vec<String>>) {
        println!("=== UPDATE_PAGINATION_DATA DEBUG ===");
        println!("Received data rows: {}", all_data.len());
        if !all_data.is_empty() {
            println!("First row sample: {:?}", &all_data[0]);
        }
        
        self.all_table_data = all_data;
        self.total_rows = self.all_table_data.len();
        self.current_page = 0; // Reset to first page
        
        println!("After assignment - all_table_data.len(): {}", self.all_table_data.len());
        println!("After assignment - total_rows: {}", self.total_rows);
        println!("====================================");
        
        self.update_current_page_data();
        
        // Initialize column widths when new data is loaded
        self.initialize_column_widths();
    }

    // Column width management methods
    fn initialize_column_widths(&mut self) {
        let num_columns = self.current_table_headers.len();
        if num_columns > 0 {
            // Calculate initial column width based on available space
            let base_width = 180.0; // Base width per column
            self.column_widths = vec![base_width; num_columns];
        } else {
            self.column_widths.clear();
        }
    }

    fn get_column_width(&self, column_index: usize) -> f32 {
        self.column_widths.get(column_index).copied().unwrap_or(180.0).max(self.min_column_width)
    }

    fn set_column_width(&mut self, column_index: usize, width: f32) {
        if column_index < self.column_widths.len() {
            // Clamp width between min and max values with extra safety checks
            let safe_width = width.max(self.min_column_width).min(self.max_column_width);
            // Ensure we never have invalid floating point values
            let final_width = if safe_width.is_finite() && safe_width > 0.0 {
                safe_width
            } else {
                self.min_column_width
            };
            self.column_widths[column_index] = final_width;
        }
    }

    fn update_current_page_data(&mut self) {
        let start_index = self.current_page * self.page_size;
        let end_index = ((self.current_page + 1) * self.page_size).min(self.all_table_data.len());
        
        if start_index < self.all_table_data.len() {
            self.current_table_data = self.all_table_data[start_index..end_index].to_vec();
        } else {
            self.current_table_data.clear();
        }
    }

    fn next_page(&mut self) {
        let max_page = (self.total_rows.saturating_sub(1)) / self.page_size;
        if self.current_page < max_page {
            self.current_page += 1;
            self.update_current_page_data();
            self.clear_table_selection();
        }
    }

    fn previous_page(&mut self) {
        if self.current_page > 0 {
            self.current_page -= 1;
            self.update_current_page_data();
            self.clear_table_selection();
        }
    }

    fn go_to_page(&mut self, page: usize) {
        let max_page = (self.total_rows.saturating_sub(1)) / self.page_size;
        if page <= max_page {
            self.current_page = page;
            self.update_current_page_data();
            self.clear_table_selection();
        }
    }

    fn set_page_size(&mut self, new_size: usize) {
        if new_size > 0 {
            self.page_size = new_size;
            self.current_page = 0; // Reset to first page
            self.update_current_page_data();
            self.clear_table_selection();
        }
    }

    fn get_total_pages(&self) -> usize {
        if self.total_rows == 0 {
            0
        } else {
            (self.total_rows + self.page_size - 1) / self.page_size
        }
    }



    fn render_tree_for_database_section(&mut self, ui: &mut egui::Ui) {
        // Add responsive search box
        ui.horizontal(|ui| {            
            // Make search box responsive to sidebar width
            let available_width = ui.available_width() - 5.0; // Leave space for clear button and padding
            let search_response = ui.add_sized(
                [available_width, 20.0], 
                egui::TextEdit::singleline(&mut self.database_search_text)
                    .hint_text("Search databases, tables, keys...")
            );
            
            if search_response.changed() {
                self.update_search_results();
            }
        });
        
        ui.separator();
        
        // Use search results if search is active, otherwise use normal tree
        if self.show_search_results && !self.database_search_text.trim().is_empty() {
            // Show search results
            let mut filtered_tree = std::mem::take(&mut self.filtered_items_tree);
            let _ = self.render_tree(ui, &mut filtered_tree);
            self.filtered_items_tree = filtered_tree;
        } else {
            // Show normal tree
            // Use slice to avoid borrowing issues
            let mut items_tree = std::mem::take(&mut self.items_tree);
            
            let _ = self.render_tree(ui, &mut items_tree);
            
            // Check if tree was refreshed inside render_tree
            if self.items_tree.is_empty() {
                // Tree was not refreshed, restore the modified tree
                self.items_tree = items_tree;
            } else {
                // Tree was refreshed inside render_tree, keep the new tree
                println!("Tree was refreshed inside render_tree, keeping the new tree");
            }
        }
    }

    fn update_search_results(&mut self) {
        // Clone search text to avoid borrowing issues
        let search_text = self.database_search_text.trim().to_string();
        
        if search_text.is_empty() {
            self.show_search_results = false;
            self.filtered_items_tree.clear();
            return;
        }
        
        self.show_search_results = true;
        self.filtered_items_tree.clear();
        
        // Search through the main items_tree with LIKE functionality
        for node in &self.items_tree {
            if let Some(filtered_node) = self.filter_node_with_like_search(node, &search_text) {
                self.filtered_items_tree.push(filtered_node);
            }
        }
        
        // If we have an active connection, also search in its database/table cache
        if let Some(connection_id) = self.current_connection_id {
            self.search_in_connection_data(connection_id, &search_text);
        }
        // Note: We don't search all connections to avoid borrowing issues
        // Users can select a specific connection to search within it
    }
    
    fn filter_node_with_like_search(&self, node: &models::structs::TreeNode, search_text: &str) -> Option<models::structs::TreeNode> {
        let mut matches = false;
        let mut filtered_children = Vec::new();
        
        // Check if current node matches using case-insensitive LIKE search
        let node_name_lower = node.name.to_lowercase();
        let search_lower = search_text.to_lowercase();
        
        // LIKE search: if search text is contained anywhere in the node name
        if node_name_lower.contains(&search_lower) {
            matches = true;
        }
        
        // Check children recursively
        for child in &node.children {
            if let Some(filtered_child) = self.filter_node_with_like_search(child, search_text) {
                filtered_children.push(filtered_child);
                matches = true;
            }
        }
        
        if matches {
            let mut filtered_node = node.clone();
            filtered_node.children = filtered_children;
            filtered_node.is_expanded = true; // Auto-expand search results
            Some(filtered_node)
        } else {
            None
        }
    }
    
    fn search_in_connection_data(&mut self, connection_id: i64, search_text: &str) {
        // Find the connection to determine its type
        let connection_type = self.connections.iter()
            .find(|c| c.id == Some(connection_id))
            .map(|c| c.connection_type.clone());
            
        if let Some(conn_type) = connection_type {
            match conn_type {
                models::enums::DatabaseType::Redis => {
                    self.search_redis_keys(connection_id, search_text);
                }
                models::enums::DatabaseType::MySQL | models::enums::DatabaseType::PostgreSQL | models::enums::DatabaseType::SQLite => {
                    self.search_sql_tables(connection_id, search_text, &conn_type);
                }
            }
        }
    }
    
    fn search_redis_keys(&mut self, connection_id: i64, search_text: &str) {
        // Search through Redis keys using SCAN with flexible pattern
        let rt = tokio::runtime::Runtime::new().unwrap();
        
        let search_results = rt.block_on(async {
            if let Some(pool) = connection::get_or_create_connection_pool(self, connection_id).await {
                if let models::enums::DatabasePool::Redis(redis_manager) = pool {
                    let mut conn = redis_manager.as_ref().clone();
                    
                    // Use flexible pattern for LIKE search - search text can appear anywhere
                    let pattern = format!("*{}*", search_text.to_lowercase());
                    let mut cursor = 0u64;
                    let mut found_keys = Vec::new();
                    
                    // First try exact pattern match
                    for _iteration in 0..20 { // Increase iterations for more comprehensive search
                        let scan_result: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                            .arg(cursor)
                            .arg("MATCH")
                            .arg(&pattern)
                            .arg("COUNT")
                            .arg(100) // Increase count for better performance
                            .query_async(&mut conn)
                            .await;
                            
                        if let Ok((new_cursor, keys)) = scan_result {
                            // Additional filtering for case-insensitive LIKE search
                            let search_lower = search_text.to_lowercase();
                            for key in keys {
                                let key_lower = key.to_lowercase();
                                if key_lower.contains(&search_lower) {
                                    found_keys.push(key);
                                }
                            }
                            cursor = new_cursor;
                            if cursor == 0 {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    
                    // Also try case-insensitive pattern if not found enough results
                    if found_keys.len() < 10 {
                        let upper_pattern = format!("*{}*", search_text.to_uppercase());
                        cursor = 0u64;
                        
                        for _iteration in 0..10 {
                            let scan_result: Result<(u64, Vec<String>), _> = redis::cmd("SCAN")
                                .arg(cursor)
                                .arg("MATCH")
                                .arg(&upper_pattern)
                                .arg("COUNT")
                                .arg(100)
                                .query_async(&mut conn)
                                .await;
                                
                            if let Ok((new_cursor, keys)) = scan_result {
                                let search_lower = search_text.to_lowercase();
                                for key in keys {
                                    let key_lower = key.to_lowercase();
                                    if key_lower.contains(&search_lower) && !found_keys.contains(&key) {
                                        found_keys.push(key);
                                    }
                                }
                                cursor = new_cursor;
                                if cursor == 0 {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    
                    found_keys
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        });
        
        // Add search results to filtered tree
        if !search_results.is_empty() {
            // Find or create the connection node in filtered results
            let connection_name = self.connections.iter()
                .find(|c| c.id == Some(connection_id))
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "Unknown Connection".to_string());
                
            let mut search_result_node = models::structs::TreeNode::new(
                format!("🔍 Search Results in {} ({} keys)", connection_name, search_results.len()), 
                models::enums::NodeType::CustomFolder
            );
            search_result_node.connection_id = Some(connection_id);
            search_result_node.is_expanded = true;
            
            // Add found keys as children
            for key in search_results {
                let mut key_node = models::structs::TreeNode::new(key.clone(), models::enums::NodeType::Table);
                key_node.connection_id = Some(connection_id);
                search_result_node.children.push(key_node);
            }
            
            self.filtered_items_tree.push(search_result_node);
        }
    }
    
    fn search_sql_tables(&mut self, connection_id: i64, search_text: &str, db_type: &models::enums::DatabaseType) {
        // Search through cached table data first
        if let Some(ref pool) = self.db_pool {
            let pool_clone = pool.clone();
            let search_pattern = format!("%{}%", search_text);
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let search_results = rt.block_on(async {
                let query = match db_type {
                    models::enums::DatabaseType::SQLite => {
                        "SELECT table_name, database_name, table_type FROM table_cache WHERE connection_id = ? AND table_name LIKE ? ORDER BY table_name"
                    }
                    _ => {
                        "SELECT table_name, database_name, table_type FROM table_cache WHERE connection_id = ? AND table_name LIKE ? ORDER BY database_name, table_name"
                    }
                };
                
                sqlx::query_as::<_, (String, String, String)>(query)
                    .bind(connection_id)
                    .bind(&search_pattern)
                    .fetch_all(pool_clone.as_ref())
                    .await
                    .unwrap_or_default()
            });
            
            // Group results by database
            let mut results_by_db: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
            for (table_name, database_name, _table_type) in search_results {
                results_by_db.entry(database_name).or_insert_with(Vec::new).push(table_name);
            }
            
            // Add search results to filtered tree
            if !results_by_db.is_empty() {
                let connection_name = self.connections.iter()
                    .find(|c| c.id == Some(connection_id))
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "Unknown Connection".to_string());
                
                let total_tables: usize = results_by_db.values().map(|v| v.len()).sum();
                let mut search_result_node = models::structs::TreeNode::new(
                    format!("🔍 Search Results in {} ({} tables)", connection_name, total_tables), 
                    models::enums::NodeType::CustomFolder
                );
                search_result_node.connection_id = Some(connection_id);
                search_result_node.is_expanded = true;
                
                // Add databases and their tables
                for (database_name, tables) in results_by_db {
                    let mut db_node = models::structs::TreeNode::new(
                        format!("📁 {} ({} tables)", database_name, tables.len()),
                        models::enums::NodeType::Database
                    );
                    db_node.connection_id = Some(connection_id);
                    db_node.database_name = Some(database_name.clone());
                    db_node.is_expanded = true;
                    
                    for table_name in tables {
                        let mut table_node = models::structs::TreeNode::new(table_name.clone(), models::enums::NodeType::Table);
                        table_node.connection_id = Some(connection_id);
                        table_node.database_name = Some(database_name.clone());
                        db_node.children.push(table_node);
                    }
                    
                    search_result_node.children.push(db_node);
                }
                
                self.filtered_items_tree.push(search_result_node);
            }
        }
    }

    fn clear_table_selection(&mut self) {
        self.selected_row = None;
        self.selected_cell = None;
    }

    fn load_table_data(&mut self, connection_id: i64, table_name: &str) {
        println!("load_table_data called with connection_id: {}, table_name: {}", connection_id, table_name);
        
        // Clear any previous table selection
        self.clear_table_selection();
        
        if let Some(connection) = self.connections.iter().find(|c| c.id == Some(connection_id)).cloned() {
            println!("Found connection for table: {}", table_name);
            
            // Generate appropriate query based on database type
            let query = match connection.connection_type {
                models::enums::DatabaseType::Redis => {
                    // Create safe Redis commands using SCAN instead of KEYS for production safety
                    match table_name {
                        "hashes" => "SCAN 0 MATCH hash:* COUNT 100".to_string(),  // Scan hash keys safely
                        "strings" => "SCAN 0 MATCH string:* COUNT 100".to_string(), // Scan string keys safely
                        "lists" => "SCAN 0 MATCH list:* COUNT 100".to_string(),     // Scan list keys safely
                        "sets" => "SCAN 0 MATCH set:* COUNT 100".to_string(),       // Scan set keys safely
                        "sorted_sets" => "SCAN 0 MATCH zset:* COUNT 100".to_string(), // Scan sorted set keys safely
                        "streams" => "SCAN 0 MATCH stream:* COUNT 100".to_string(),   // Scan stream keys safely
                        "keys" => "SCAN 0 COUNT 100".to_string(),                     // Scan all keys safely
                        _ => {
                            // For info sections or other types, show INFO command
                            format!("INFO {}", table_name)
                        }
                    }
                }
                _ => {
                    // SQL databases - use regular SELECT query
                    format!("SELECT * FROM {} LIMIT 10000", table_name)
                }
            };
            
            // Set the query in the editor  
            self.editor_text = query.clone();
            self.current_connection_id = Some(connection_id);
            
            // Execute the query with proper database connection
            if let Some((headers, data)) = connection::execute_table_query_sync(self, connection_id, &connection, &query) {
                self.current_table_headers = headers;
                
                // Use pagination for table data
                self.update_pagination_data(data);
                
                if self.total_rows == 0 {
                    self.current_table_name = format!("Table: {} (no results)", table_name);
                } else {
                    self.current_table_name = format!("Table: {} ({} total rows, showing page {} of {})", 
                        table_name, self.total_rows, self.current_page + 1, self.get_total_pages());
                }
                println!("Successfully loaded {} total rows from table {}", self.total_rows, table_name);
            } else {
                self.current_table_name = format!("Failed to load table: {}", table_name);
                self.current_table_headers.clear();
                self.current_table_data.clear();
                println!("Failed to execute query for table: {}", table_name);
            }
        }
    }

    fn find_redis_key_info(&self, node: &models::structs::TreeNode, key_name: &str) -> Option<(String, String)> {
        // Check if this node is a type folder (like "Strings (5)")
        if node.node_type == models::enums::NodeType::TablesFolder {            
            // Extract the type from folder name
            let folder_type = if node.name.starts_with("Strings") {
                "string"
            } else if node.name.starts_with("Hashes") {
                "hash"
            } else if node.name.starts_with("Lists") {
                "list"
            } else if node.name.starts_with("Sets") {
                "set"
            } else if node.name.starts_with("Sorted Sets") {
                "zset"
            } else if node.name.starts_with("Streams") {
                "stream"
            } else {
                // Continue searching instead of returning None
                "unknown"
            };
            
            
            // Search for the key in this folder's children
            for child in &node.children {
                println!("🔍 Checking child: '{}' (type: {:?})", child.name, child.node_type);
                if child.node_type == models::enums::NodeType::Table && child.name == key_name {
                    if let Some(db_name) = &child.database_name {
                        return Some((db_name.clone(), folder_type.to_string()));
                    }
                }
            }
        }
        
        // Recursively search in children
        for child in &node.children {
            if let Some((db_name, key_type)) = self.find_redis_key_info(child, key_name) {
                return Some((db_name, key_type));
            }
        }
        
        None
    }

    fn execute_query(&mut self) {
        // Use selected text if available, otherwise use full editor text
        let query = if !self.selected_text.trim().is_empty() {
            self.selected_text.trim().to_string()
        } else {
            self.editor_text.trim().to_string()
        };
        
        if query.is_empty() {
            self.current_table_name = "No query to execute".to_string();
            self.current_table_headers.clear();
            self.current_table_data.clear();
            return;
        }

        // Check if we have an active connection
        if let Some(connection_id) = self.current_connection_id {
            println!("=== EXECUTING QUERY ===");
            println!("Connection ID: {}", connection_id);
            println!("Query: {}", query);
            
            let result = connection::execute_query_with_connection(self, connection_id, query.clone());
            
            println!("Query execution result: {:?}", result.is_some());
            
            if let Some((headers, data)) = result {
                println!("=== QUERY RESULT SUCCESS ===");
                println!("Headers received: {} - {:?}", headers.len(), headers);
                println!("Data rows received: {}", data.len());
                if !data.is_empty() {
                    println!("First row sample: {:?}", &data[0]);
                }
                
                self.current_table_headers = headers;
                
                // Use pagination for query results
                self.update_pagination_data(data);
                
                if self.total_rows == 0 {
                    self.current_table_name = "Query executed successfully (no results)".to_string();
                } else {
                    self.current_table_name = format!("Query Results ({} total rows, showing page {} of {})", 
                        self.total_rows, self.current_page + 1, self.get_total_pages());
                }
                println!("After update_pagination_data - total_rows: {}, all_table_data.len(): {}", 
                         self.total_rows, self.all_table_data.len());
                println!("============================");
                
                // Save query to history after successful execution
                self.save_query_to_history(&query, connection_id);
            } else {
                self.current_table_name = "Query execution failed".to_string();
                self.current_table_headers.clear();
                self.current_table_data.clear();
                self.all_table_data.clear();
                self.total_rows = 0;
            }
        } else {
            // No active connection - check if we have any connections available
            if self.connections.is_empty() {
                self.current_table_name = "No connections available. Please add a connection first.".to_string();
                self.current_table_headers.clear();
                self.current_table_data.clear();
                self.all_table_data.clear();
                self.total_rows = 0;
            } else {
                // Show connection selector popup
                self.pending_query = query.clone();
                self.show_connection_selector = true;
                self.auto_execute_after_connection = true;
            }
        }
    }

    #[allow(dead_code)]
    fn highlight_sql_syntax(ui: &egui::Ui, text: &str) -> egui::text::LayoutJob {
        let mut job = egui::text::LayoutJob {
            text: text.to_owned(),
            ..Default::default()
        };
        
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
        let string_color = egui::Color32::from_rgb(255, 60, 0); // Orange - strings
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
            // Render pagination controls at the top
            if self.total_rows > 0 {
                ui.horizontal(|ui| {
                    ui.label(format!("Total rows: {}", self.total_rows));
                    ui.separator();
                    
                    // Page size selector
                    ui.label("Rows per page:");
                    let mut page_size_str = self.page_size.to_string();
                    if ui.text_edit_singleline(&mut page_size_str).changed() {
                        if let Ok(new_size) = page_size_str.parse::<usize>() {
                            if new_size > 0 && new_size <= 10000 {
                                self.set_page_size(new_size);
                            }
                        }
                    }
                    
                    ui.separator();
                    
                    // Navigation buttons
                    if ui.button("⏮ First").clicked() {
                        self.go_to_page(0);
                    }
                    
                    ui.add_enabled(self.current_page > 0, egui::Button::new("◀ Prev"))
                        .clicked()
                        .then(|| self.previous_page());
                    
                    ui.label(format!("Page {} of {}", self.current_page + 1, self.get_total_pages()));
                    
                    ui.add_enabled(self.current_page < self.get_total_pages().saturating_sub(1), egui::Button::new("Next ▶"))
                        .clicked()
                        .then(|| self.next_page());
                    
                    if ui.button("Last ⏭").clicked() {
                        let last_page = self.get_total_pages().saturating_sub(1);
                        self.go_to_page(last_page);
                    }
                    
                    ui.separator();
                    
                    // Quick page jump
                    ui.label("Go to page:");
                    let mut page_input = (self.current_page + 1).to_string();
                    if ui.text_edit_singleline(&mut page_input).changed() {
                        if let Ok(page_num) = page_input.parse::<usize>() {
                            if page_num > 0 {
                                self.go_to_page(page_num - 1);
                            }
                        }
                    }
                });
                ui.separator();
            }
            
            if !self.current_table_headers.is_empty() && !self.current_table_data.is_empty() {
                // Store sort state locally to avoid borrowing issues
                let current_sort_column = self.sort_column;
                let current_sort_ascending = self.sort_ascending;
                let headers = self.current_table_headers.clone();
                let mut sort_requests = Vec::new();
                
                // Ensure column widths are initialized
                if self.column_widths.len() != headers.len() {
                    self.initialize_column_widths();
                }
                
                // Use available height for full responsive design
                egui::ScrollArea::both()
                    .auto_shrink([false, false]) // Don't auto-shrink to content
                    .show(ui, |ui| {
                        let grid_response = egui::Grid::new("table_data_grid")
                            .striped(true)
                            .spacing([0.0, 0.0]) // Remove spacing between columns and rows
                            .min_col_width(0.0) // No minimum column width spacing
                            .max_col_width(f32::INFINITY) // Allow any column width
                            .show(ui, |ui| {
                                // Render No column header first (centered)
                                ui.allocate_ui_with_layout(
                                    [60.0, ui.available_height().max(30.0)].into(), // Ensure minimum height
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        let rect = ui.available_rect_before_wrap();
                                        
                                        // Draw thin border for header cell
                                        let border_color = if ui.visuals().dark_mode {
                                            egui::Color32::from_gray(60) // Dark gray for dark mode
                                        } else {
                                            egui::Color32::from_gray(200) // Light gray for light mode
                                        };
                                        let thin_stroke = egui::Stroke::new(0.5, border_color);
                                        
                                        // Draw cell borders
                                        ui.painter().line_segment([rect.left_top(), rect.right_top()], thin_stroke);
                                        ui.painter().line_segment([rect.right_top(), rect.right_bottom()], thin_stroke);
                                        ui.painter().line_segment([rect.right_bottom(), rect.left_bottom()], thin_stroke);
                                        ui.painter().line_segment([rect.left_bottom(), rect.left_top()], thin_stroke);
                                        
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
                                
                                // Render enhanced headers with sort buttons and resize handles
                                for (col_index, header) in headers.iter().enumerate() {
                                    let column_width = self.get_column_width(col_index).max(30.0); // Ensure minimum width of 30px
                                    let available_height = ui.available_height().max(30.0); // Ensure minimum height
                                    
                                    ui.allocate_ui_with_layout(
                                        [column_width, available_height].into(), // Use safe values
                                        egui::Layout::left_to_right(egui::Align::Center),
                                        |ui| {
                                            let rect = ui.available_rect_before_wrap();
                                            
                                            // Draw thin border for header cell
                                            let border_color = if ui.visuals().dark_mode {
                                                egui::Color32::from_gray(60) // Dark gray for dark mode
                                            } else {
                                                egui::Color32::from_gray(200) // Light gray for light mode
                                            };
                                            let thin_stroke = egui::Stroke::new(0.5, border_color);
                                            
                                            // Draw cell borders
                                            ui.painter().line_segment([rect.left_top(), rect.right_top()], thin_stroke);
                                            ui.painter().line_segment([rect.right_top(), rect.right_bottom()], thin_stroke);
                                            ui.painter().line_segment([rect.right_bottom(), rect.left_bottom()], thin_stroke);
                                            ui.painter().line_segment([rect.left_bottom(), rect.left_top()], thin_stroke);
                                            
                                            // Use horizontal layout to position header text centered and sort button on right
                                            ui.horizontal(|ui| {
                                                // Calculate available width for centering (total width minus sort button space)
                                                let sort_button_width = 25.0;
                                                let text_area_width = ui.available_width() - sort_button_width;
                                                
                                                // Header text - centered in available space
                                                ui.allocate_ui_with_layout(
                                                    [text_area_width, ui.available_height()].into(),
                                                    egui::Layout::top_down(egui::Align::Center),
                                                    |ui| {
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
                                                
                                                // Sort button at the right
                                                let (sort_icon, is_active) = if current_sort_column == Some(col_index) {
                                                    if current_sort_ascending {
                                                        ("^", true) // Caret up for ascending
                                                    } else {
                                                        ("v", true) // Letter v for descending  
                                                    }
                                                } else {
                                                    ("-", false) // Dash for unsorted
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
                                                    let new_ascending = if current_sort_column == Some(col_index) {
                                                        !current_sort_ascending // Toggle direction for same column
                                                    } else {
                                                        true // Start with ascending for new column
                                                    };
                                                    sort_requests.push((col_index, new_ascending));
                                                }
                                            });
                                            
                                            // Add resize handle for all but the last column
                                            if col_index < headers.len() - 1 {
                                                // Position resize handle exactly at the right edge of the column
                                                let handle_x = ui.max_rect().max.x; // Position exactly at right edge
                                                let handle_y = ui.max_rect().min.y;
                                                let handle_height = available_height;
                                                
                                                let resize_handle_rect = egui::Rect::from_min_size(
                                                    egui::pos2(handle_x - 2.0, handle_y), // Just 2 pixels wide, starting 2 pixels before edge
                                                    egui::vec2(4.0, handle_height) // Make it much thinner (4 pixels)
                                                );
                                                
                                                let resize_response = ui.allocate_rect(resize_handle_rect, egui::Sense::drag());
                                                
                                                if resize_response.hovered() {
                                                    ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeColumn);
                                                }
                                                
                                                if resize_response.dragged() {
                                                    let delta_x = resize_response.drag_delta().x;
                                                    let new_width = column_width + delta_x;
                                                    self.set_column_width(col_index, new_width);
                                                }
                                                
                                                // Visual indicator for resize handle - only show when hovered
                                                if resize_response.hovered() || resize_response.dragged() {
                                                    ui.painter().rect_filled(
                                                        resize_handle_rect,
                                                        0.0,
                                                        egui::Color32::from_rgba_unmultiplied(100, 150, 255, 150) // More visible when hovered
                                                    );
                                                }
                                                // Remove the "else" clause that shows subtle line - no visual indicator when not hovered
                                            }
                                        }
                                    );
                                }
                                ui.end_row();
                                
                                // Render data rows with row numbers
                                for (row_index, row) in self.current_table_data.iter().enumerate() {
                                    let is_selected_row = self.selected_row == Some(row_index);
                                    
                                    // Set row background color if selected
                                    let row_color = if is_selected_row {
                                        if ui.visuals().dark_mode {
                                            egui::Color32::from_rgba_unmultiplied(100, 150, 255, 30) // Light blue for dark mode
                                        } else {
                                            egui::Color32::from_rgba_unmultiplied(200, 220, 255, 80) // Light blue for light mode
                                        }
                                    } else {
                                        egui::Color32::TRANSPARENT
                                    };
                                    
                                    // Add row number as first column (centered with fixed width)
                                    ui.allocate_ui_with_layout(
                                        [60.0, ui.available_height().max(25.0)].into(), // Ensure minimum height
                                        egui::Layout::top_down(egui::Align::Center),
                                        |ui| {
                                            let rect = ui.available_rect_before_wrap();
                                            if row_color != egui::Color32::TRANSPARENT {
                                                ui.painter().rect_filled(rect, 3.0, row_color);
                                            }
                                            
                                            // Draw thin border for row number cell
                                            let border_color = if ui.visuals().dark_mode {
                                                egui::Color32::from_gray(60) // Dark gray for dark mode
                                            } else {
                                                egui::Color32::from_gray(200) // Light gray for light mode
                                            };
                                            let thin_stroke = egui::Stroke::new(0.5, border_color);
                                            
                                            // Draw cell borders
                                            ui.painter().line_segment([rect.left_top(), rect.right_top()], thin_stroke);
                                            ui.painter().line_segment([rect.right_top(), rect.right_bottom()], thin_stroke);
                                            ui.painter().line_segment([rect.right_bottom(), rect.left_bottom()], thin_stroke);
                                            ui.painter().line_segment([rect.left_bottom(), rect.left_top()], thin_stroke);
                                            
                                            let label_response = ui.label((row_index + 1).to_string());
                                            
                                            // Handle row number click to select entire row
                                            if label_response.clicked() {
                                                self.selected_row = Some(row_index);
                                                self.selected_cell = None; // Clear cell selection when row is selected
                                            }
                                            
                                            label_response
                                        }
                                    );
                                    
                                    // Add data cells (left-aligned with individual column width)
                                    for (col_index, cell) in row.iter().enumerate() {
                                        let is_selected_cell = self.selected_cell == Some((row_index, col_index));
                                        let column_width = self.get_column_width(col_index).max(50.0); // Ensure minimum width
                                        let cell_height = ui.available_height().max(25.0); // Ensure minimum height
                                        
                                        ui.allocate_ui_with_layout(
                                            [column_width, cell_height].into(), // Use safe values
                                            egui::Layout::left_to_right(egui::Align::Center),
                                            |ui| {
                                                let rect = ui.available_rect_before_wrap();
                                                
                                                // Draw row background if row is selected
                                                if row_color != egui::Color32::TRANSPARENT {
                                                    ui.painter().rect_filled(rect, 3.0, row_color);
                                                }
                                                
                                                // Draw thin border for all cells
                                                let border_color = if ui.visuals().dark_mode {
                                                    egui::Color32::from_gray(60) // Dark gray for dark mode
                                                } else {
                                                    egui::Color32::from_gray(200) // Light gray for light mode
                                                };
                                                let thin_stroke = egui::Stroke::new(0.5, border_color);
                                                
                                                // Draw cell borders
                                                ui.painter().line_segment([rect.left_top(), rect.right_top()], thin_stroke);
                                                ui.painter().line_segment([rect.right_top(), rect.right_bottom()], thin_stroke);
                                                ui.painter().line_segment([rect.right_bottom(), rect.left_bottom()], thin_stroke);
                                                ui.painter().line_segment([rect.left_bottom(), rect.left_top()], thin_stroke);
                                                
                                                // Draw red border if this cell is selected (on top of thin border)
                                                if is_selected_cell {
                                                    let stroke = egui::Stroke::new(2.0, egui::Color32::from_rgb(255, 60, 0)); // Red stroke for selected cell
                                                    ui.painter().rect_filled(rect, 0.0, egui::Color32::from_rgba_unmultiplied(255, 60, 10, 20));
                                                    // Draw border lines manually
                                                    ui.painter().line_segment([rect.left_top(), rect.right_top()], stroke);
                                                    ui.painter().line_segment([rect.right_top(), rect.right_bottom()], stroke);
                                                    ui.painter().line_segment([rect.right_bottom(), rect.left_bottom()], stroke);
                                                    ui.painter().line_segment([rect.left_bottom(), rect.left_top()], stroke);
                                                }
                                                
                                                // Truncate text if it's too long and add tooltip
                                                let max_chars = ((column_width / 8.0) as usize).max(10); // Dynamic max chars based on column width, minimum 10
                                                let display_text = if cell.chars().count() > max_chars {
                                                    format!("{}...", cell.chars().take(max_chars.saturating_sub(3)).collect::<String>())
                                                } else {
                                                    cell.clone()
                                                };
                                                
                                                // Create invisible button that covers the entire cell area for click detection
                                                let cell_response = ui.allocate_response(rect.size(), egui::Sense::click());
                                                
                                                // Handle cell click on the entire area
                                                if cell_response.clicked() {
                                                    self.selected_row = Some(row_index);
                                                    self.selected_cell = Some((row_index, col_index));
                                                }
                                                
                                                // Show full text in tooltip if truncated or if cell has content
                                                let hover_response = if cell.chars().count() > max_chars || !cell.is_empty() {
                                                    cell_response.on_hover_text(cell)
                                                } else {
                                                    cell_response
                                                };
                                                
                                                // Draw the text on top of the button
                                                let text_pos = rect.left_top() + egui::vec2(5.0, rect.height() * 0.5);
                                                ui.painter().text(
                                                    text_pos,
                                                    egui::Align2::LEFT_CENTER,
                                                    &display_text,
                                                    egui::FontId::default(),
                                                    if is_selected_cell {
                                                        if ui.visuals().dark_mode {
                                                            egui::Color32::WHITE
                                                        } else {
                                                            egui::Color32::BLACK
                                                        }
                                                    } else {
                                                        ui.visuals().text_color()
                                                    }
                                                );
                                                
                                                // Add context menu to the cell response (entire area)
                                                hover_response.context_menu(|ui| {
                                                    ui.set_min_width(150.0);
                                                    ui.vertical(|ui| {
                                                        if ui.button("📋 Copy Cell Value").clicked() {
                                                            ui.ctx().copy_text(cell.clone());
                                                            ui.close_menu();
                                                        }
                                                        
                                                        if let Some(selected_row_idx) = self.selected_row {
                                                            if ui.button("📄 Copy Row as CSV").clicked() {
                                                                if let Some(row_data) = self.current_table_data.get(selected_row_idx) {
                                                                    let csv_row = row_data.iter()
                                                                        .map(|cell| {
                                                                            if cell.contains(',') || cell.contains('"') || cell.contains('\n') {
                                                                                format!("\"{}\"", cell.replace('"', "\"\""))
                                                                            } else {
                                                                                cell.clone()
                                                                            }
                                                                        })
                                                                        .collect::<Vec<_>>()
                                                                        .join(",");
                                                                    ui.ctx().copy_text(csv_row);
                                                                }
                                                                ui.close_menu();
                                                            }
                                                        }
                                                        
                                                        ui.separator();
                                                        
                                                        if ui.button("📄 Export to CSV").clicked() {
                                                            export::export_to_csv(&self.all_table_data, &self.current_table_headers, &self.current_table_name);
                                                            ui.close_menu();
                                                        }
                                                        if ui.button("📊 Export to XLSX").clicked() {
                                                            export::export_to_xlsx(&self.all_table_data, &self.current_table_headers, &self.current_table_name);
                                                            ui.close_menu();
                                                        }
                                                    });
                                                });
                                            }
                                        );
                                    }
                                    ui.end_row();
                                }
                            });
                        
                        // Add context menu detection for export
                        grid_response.response.context_menu(|ui| {
                            ui.set_min_width(150.0);
                            ui.vertical(|ui| {
                                if ui.button("📄 Export to CSV").clicked() {
                                    export::export_to_csv(&self.all_table_data, &self.current_table_headers, &self.current_table_name);
                                    ui.close_menu();
                                }
                                if ui.button("📊 Export to XLSX").clicked() {
                                    export::export_to_xlsx(&self.all_table_data, &self.current_table_headers, &self.current_table_name);
                                    ui.close_menu();
                                }
                            });
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
        if column_index >= self.current_table_headers.len() || self.all_table_data.is_empty() {
            return;
        }
        
        // Update sort state
        self.sort_column = Some(column_index);
        self.sort_ascending = ascending;
        
        // Sort ALL the data (not just current page)
        self.all_table_data.sort_by(|a, b| {
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
        
        // Update current page data after sorting
        self.update_current_page_data();
        
        let sort_direction = if ascending { "^ ascending" } else { "v descending" };
        println!("✓ Sorted table by column '{}' in {} order ({} total rows)", 
            self.current_table_headers[column_index], 
            sort_direction,
            self.all_table_data.len()
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
                            history_items.push(models::structs::HistoryItem {
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

                
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let _ = sqlx::query(
                        "INSERT INTO query_history (query_text, connection_id, connection_name) VALUES (?, ?, ?)"
                    )
                    .bind(query.to_string())
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
            let mut node = models::structs::TreeNode::new(item.query.clone(), models::enums::NodeType::QueryHistItem);
            node.connection_id = Some(item.connection_id);
            
            self.history_tree.push(node);
        }
    }

    fn render_advanced_editor(&mut self, ui: &mut egui::Ui) {
        // Find & Replace panel
        if self.advanced_editor.show_find_replace {
            ui.horizontal(|ui| {
                ui.label("Find:");
                ui.add_sized([200.0, 20.0], egui::TextEdit::singleline(&mut self.advanced_editor.find_text));
                
                ui.label("Replace:");
                ui.add_sized([200.0, 20.0], egui::TextEdit::singleline(&mut self.advanced_editor.replace_text));
                
                ui.checkbox(&mut self.advanced_editor.case_sensitive, "Case Sensitive");
                ui.checkbox(&mut self.advanced_editor.use_regex, "Regex");
                
                if ui.button("Replace All").clicked() {
                    self.perform_replace_all();
                }
                
                if ui.button("Find Next").clicked() {
                    self.find_next();
                }
                
                if ui.button("✕").clicked() {
                    self.advanced_editor.show_find_replace = false;
                }
            });
        }

        // Main code editor using egui_code_editor
        let mut editor = CodeEditor::default()
            .id_source("sql_editor")
            .with_rows(25)
            .with_fontsize(self.advanced_editor.font_size)
            .with_theme(self.advanced_editor.theme)
            .with_syntax(egui_code_editor::Syntax::sql())
            .with_numlines(self.advanced_editor.show_line_numbers);

        let response = editor.show(ui, &mut self.editor_text);
        
        // Try to capture selected text from the response
        // Note: This is a simplified approach. The actual implementation may vary depending on the CodeEditor version
        if let Some(text_cursor_range) = response.cursor_range {
            let start = text_cursor_range.primary.ccursor.index.min(text_cursor_range.secondary.ccursor.index);
            let end = text_cursor_range.primary.ccursor.index.max(text_cursor_range.secondary.ccursor.index);
            
            if start != end {
                // There is a selection
                if let Some(selected) = self.editor_text.get(start..end) {
                    self.selected_text = selected.to_string();
                } else {
                    self.selected_text.clear();
                }
            } else {
                // No selection
                self.selected_text.clear();
            }
        } else {
            // No cursor range available, clear selection
            self.selected_text.clear();
        }
        
        // If you get a type error here, try:
        // let mut buffer = egui_code_editor::SimpleTextBuffer::from(&self.editor_text);
        // let response = editor.show(ui, &mut buffer);
        // self.editor_text = buffer.text().to_string();
        
        // Update tab content when editor changes
        if response.response.changed() {
            if let Some(tab) = self.query_tabs.get_mut(self.active_tab_index) {
                tab.content = self.editor_text.clone();
                tab.is_modified = true;
            }
        }
    }

    fn perform_replace_all(&mut self) {
        if self.advanced_editor.find_text.is_empty() {
            return;
        }

        let find_text = &self.advanced_editor.find_text;
        let replace_text = &self.advanced_editor.replace_text;

        if self.advanced_editor.use_regex {
            // Use regex replacement
            if let Ok(re) = regex::Regex::new(find_text) {
                self.editor_text = re.replace_all(&self.editor_text, replace_text).into_owned();
            }
        } else {
            // Simple string replacement
            if self.advanced_editor.case_sensitive {
                self.editor_text = self.editor_text.replace(find_text, replace_text);
            } else {
                // Case insensitive replacement
                let find_lower = find_text.to_lowercase();
                let mut result = String::new();
                let mut last_end = 0;
                
                for (start, part) in self.editor_text.match_indices(&find_lower) {
                    result.push_str(&self.editor_text[last_end..start]);
                    result.push_str(replace_text);
                    last_end = start + part.len();
                }
                result.push_str(&self.editor_text[last_end..]);
                self.editor_text = result;
            }
        }

        // Update current tab content
        if let Some(tab) = self.query_tabs.get_mut(self.active_tab_index) {
            tab.content = self.editor_text.clone();
            tab.is_modified = true;
        }
    }

    fn find_next(&mut self) {
        // This is a simplified find implementation
        // In a real implementation, you'd want to track cursor position and highlight matches
        if !self.advanced_editor.find_text.is_empty() {
            if let Some(_pos) = self.editor_text.find(&self.advanced_editor.find_text) {
                // In a full implementation, you would scroll to and highlight the match
                println!("Found match for: {}", self.advanced_editor.find_text);
            }
        }
    }

    fn open_command_palette(&mut self) {
        self.show_command_palette = true;
        self.command_palette_input.clear();
        self.show_theme_selector = false;
        self.command_palette_selected_index = 0;
        
        // Initialize command palette items
        self.command_palette_items = vec![
            "Preferences: Color Theme".to_string(),
            "View: Toggle Word Wrap".to_string(),
            "View: Toggle Line Numbers".to_string(),
            "View: Toggle Find and Replace".to_string(),
        ];
    }

    fn navigate_command_palette(&mut self, direction: i32) {
        // Filter commands based on current input
        let filtered_commands: Vec<String> = if self.command_palette_input.is_empty() {
            self.command_palette_items.clone()
        } else {
            self.command_palette_items
                .iter()
                .filter(|cmd| cmd.to_lowercase().contains(&self.command_palette_input.to_lowercase()))
                .cloned()
                .collect()
        };

        if filtered_commands.is_empty() {
            return;
        }

        // Update selected index with wrapping
        if direction > 0 {
            // Down arrow
            self.command_palette_selected_index = (self.command_palette_selected_index + 1) % filtered_commands.len();
        } else {
            // Up arrow
            if self.command_palette_selected_index == 0 {
                self.command_palette_selected_index = filtered_commands.len() - 1;
            } else {
                self.command_palette_selected_index -= 1;
            }
        }
    }

    fn execute_selected_command(&mut self) {
        // Filter commands based on current input
        let filtered_commands: Vec<String> = if self.command_palette_input.is_empty() {
            self.command_palette_items.clone()
        } else {
            self.command_palette_items
                .iter()
                .filter(|cmd| cmd.to_lowercase().contains(&self.command_palette_input.to_lowercase()))
                .cloned()
                .collect()
        };

        if self.command_palette_selected_index < filtered_commands.len() {
            let selected_command = filtered_commands[self.command_palette_selected_index].clone();
            self.execute_command(&selected_command);
        }
    }

    fn navigate_theme_selector(&mut self, direction: i32) {
        // There are 3 themes available
        let theme_count = 3;

        // Update selected index with wrapping
        if direction > 0 {
            // Down arrow
            self.theme_selector_selected_index = (self.theme_selector_selected_index + 1) % theme_count;
        } else {
            // Up arrow
            if self.theme_selector_selected_index == 0 {
                self.theme_selector_selected_index = theme_count - 1;
            } else {
                self.theme_selector_selected_index -= 1;
            }
        }
    }

    fn select_current_theme(&mut self) {
        // Map index to theme
        let theme = match self.theme_selector_selected_index {
            0 => ColorTheme::GITHUB_DARK,
            1 => ColorTheme::GITHUB_LIGHT,
            2 => ColorTheme::GRUVBOX,
            _ => ColorTheme::GITHUB_DARK, // fallback
        };

        self.advanced_editor.theme = theme;
        self.show_theme_selector = false;
    }

    fn render_command_palette(&mut self, ctx: &egui::Context) {
        // Create a centered modal dialog
        egui::Area::new(egui::Id::new("command_palette"))
            .fixed_pos(egui::pos2(
                ctx.screen_rect().center().x - 300.0,
                ctx.screen_rect().center().y - 200.0,
            ))
            .show(ctx, |ui| {
                egui::Frame::default()
                    .fill(ui.style().visuals.window_fill)
                    .stroke(ui.style().visuals.window_stroke)
                    .shadow(egui::epaint::Shadow::default())
                    .inner_margin(egui::Margin::same(10))
                    .show(ui, |ui| {
                        ui.set_min_size(egui::vec2(600.0, 400.0));
                        
                        ui.vertical(|ui| {
                            // Title and input field
                            ui.label(egui::RichText::new("Command Palette").heading());
                            ui.separator();
                            
                            // Search input
                            let response = ui.add_sized(
                                [580.0, 25.0],
                                egui::TextEdit::singleline(&mut self.command_palette_input)
                                    .hint_text("Type command name...")
                            );
                            
                            // Reset selection when text changes
                            if response.changed() {
                                self.command_palette_selected_index = 0;
                            }
                            
                            // Auto-focus the input when palette opens
                            if self.command_palette_input.is_empty() {
                                response.request_focus();
                            }
                            
                            ui.separator();
                            
                            // Filter commands based on input
                            let filtered_commands: Vec<String> = if self.command_palette_input.is_empty() {
                                self.command_palette_items.clone()
                            } else {
                                self.command_palette_items
                                    .iter()
                                    .filter(|cmd| cmd.to_lowercase().contains(&self.command_palette_input.to_lowercase()))
                                    .cloned()
                                    .collect()
                            };

                            // Ensure selected index is within bounds when filtering
                            if self.command_palette_selected_index >= filtered_commands.len() && !filtered_commands.is_empty() {
                                self.command_palette_selected_index = 0;
                            }
                            
                            // Command list
                            egui::ScrollArea::vertical()
                                .max_height(300.0)
                                .show(ui, |ui| {
                                    for (index, command) in filtered_commands.iter().enumerate() {
                                        let is_selected = index == self.command_palette_selected_index;
                                        
                                        // Highlight selected item
                                        let text = if is_selected {
                                            egui::RichText::new(command)
                                                .background_color(ui.style().visuals.selection.bg_fill)
                                                .color(ui.style().visuals.selection.stroke.color)
                                        } else {
                                            egui::RichText::new(command)
                                        };
                                        
                                        if ui.selectable_label(is_selected, text).clicked() {
                                            self.execute_command(command);
                                        }
                                    }
                                });
                            
                            ui.separator();
                            ui.horizontal(|ui| {
                                ui.label("Use");
                                ui.code("↑↓");
                                ui.label("to navigate,");
                                ui.code("Enter");
                                ui.label("to select,");
                                ui.code("Escape");
                                ui.label("to close");
                            });
                        });
                    });
            });
    }

    fn execute_command(&mut self, command: &str) {
        match command {
            "Preferences: Color Theme" => {
                self.show_command_palette = false;
                // Instead of directly setting show_theme_selector, use a flag
                self.request_theme_selector = true;
                self.theme_selector_selected_index = 0; // Reset to first theme
            }
            "View: Toggle Word Wrap" => {
                self.advanced_editor.word_wrap = !self.advanced_editor.word_wrap;
                self.show_command_palette = false;
            }
            "View: Toggle Line Numbers" => {
                self.advanced_editor.show_line_numbers = !self.advanced_editor.show_line_numbers;
                self.show_command_palette = false;
            }
            "View: Toggle Find and Replace" => {
                self.advanced_editor.show_find_replace = !self.advanced_editor.show_find_replace;
                self.show_command_palette = false;
            }
            _ => {
                println!("Unknown command: {}", command);
                self.show_command_palette = false;
            }
        }
    }

    fn render_theme_selector(&mut self, ctx: &egui::Context) {
        // Create a centered modal dialog for theme selection
        egui::Area::new(egui::Id::new("theme_selector"))
            .fixed_pos(egui::pos2(
                ctx.screen_rect().center().x - 200.0,
                ctx.screen_rect().center().y - 150.0,
            ))
            .show(ctx, |ui| {
                egui::Frame::default()
                    .fill(ui.style().visuals.window_fill)
                    .stroke(ui.style().visuals.window_stroke)
                    .shadow(egui::epaint::Shadow::default())
                    .inner_margin(egui::Margin::same(15))
                    .show(ui, |ui| {
                        ui.set_min_size(egui::vec2(400.0, 300.0));
                        
                        ui.vertical(|ui| {
                            ui.label(egui::RichText::new("Select Color Theme").heading());
                            ui.separator();
                            
                            ui.spacing_mut().item_spacing.y = 8.0;
                            
                            // Available themes with descriptions
                            let themes = vec![
                                (ColorTheme::GITHUB_DARK, "GitHub Dark", "Dark theme with blue accents"),
                                (ColorTheme::GITHUB_LIGHT, "GitHub Light", "Light theme with subtle colors"),
                                (ColorTheme::GRUVBOX, "Gruvbox", "Retro warm theme with earthy colors"),
                            ];
                            
                            for (index, (theme, name, description)) in themes.iter().enumerate() {
                                let is_current = self.advanced_editor.theme == *theme;
                                let is_selected = index == self.theme_selector_selected_index;
                                
                                // Create horizontal layout for theme item
                                ui.horizontal(|ui| {
                                    // Current theme indicator (checkmark)
                                    if is_current {
                                        ui.label("✓");
                                    } else {
                                        ui.label(" "); // Space for alignment
                                    }
                                    
                                    // Theme name with different styling based on selection
                                    let text = if is_selected {
                                        // Highlight the selected item for keyboard navigation
                                        egui::RichText::new(*name)
                                            .size(16.0)
                                            .background_color(ui.style().visuals.selection.bg_fill)
                                            .color(ui.style().visuals.selection.stroke.color)
                                    } else if is_current {
                                        // Bold text for current theme
                                        egui::RichText::new(*name)
                                            .size(16.0)
                                            .strong()
                                            .color(egui::Color32::from_rgb(0, 150, 255)) // Blue for current
                                    } else {
                                        // Normal text for other themes
                                        egui::RichText::new(*name).size(16.0)
                                    };
                                    
                                    let response = ui.label(text);
                                    
                                    // Handle click to select theme
                                    if response.clicked() && !is_current {
                                        self.advanced_editor.theme = *theme;
                                        self.show_theme_selector = false;
                                    }
                                });
                                
                                // Show description with indentation
                                ui.horizontal(|ui| {
                                    ui.add_space(20.0); // Indent description
                                    ui.label(egui::RichText::new(*description).size(12.0).weak());
                                });
                                ui.add_space(5.0);
                            }
                            
                            ui.separator();
                            ui.horizontal(|ui| {
                                ui.label("Use");
                                ui.code("↑↓");
                                ui.label("to navigate,");
                                ui.code("Enter");
                                ui.label("to select,");
                                ui.code("Escape");
                                ui.label("to close");
                            });
                        });
                    });
            });
    }
}

impl App for Tabular {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        // Handle forced refresh flag
        if self.needs_refresh {
            self.needs_refresh = false;
            
            // Force refresh of query tree
            self.load_queries_from_directory();
            
            // Request UI repaint
            ctx.request_repaint();
        }
        
        // Periodic cleanup of stale connection pools (every 5 minutes)
        if self.last_cleanup_time.elapsed().as_secs() > 300 { // 5 minutes
            println!("🧹 Performing periodic connection pool cleanup");
            
            // Clean up connections that might be stale
            let mut connections_to_refresh: Vec<i64> = self.connection_pools.keys().copied().collect();
            
            // Limit cleanup to avoid blocking UI
            if connections_to_refresh.len() > 5 {
                connections_to_refresh.truncate(5);
            }
            
            for connection_id in connections_to_refresh {
                connection::cleanup_connection_pool(self, connection_id);
            }
            
            self.last_cleanup_time = std::time::Instant::now();
        }
        
        // Handle deferred theme selector request
        if self.request_theme_selector {
            self.request_theme_selector = false;
            self.show_theme_selector = true;
        }
        
        // Handle keyboard shortcuts
        ctx.input(|i| {
            // CMD+W or CTRL+W to close current tab
            if (i.modifiers.mac_cmd || i.modifiers.ctrl) && i.key_pressed(egui::Key::W) && !self.query_tabs.is_empty() {
                self.close_tab(self.active_tab_index);
            }
            
            // CMD+Q or CTRL+Q to quit application
            if (i.modifiers.mac_cmd || i.modifiers.ctrl) && i.key_pressed(egui::Key::Q) {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            
            // CMD+SHIFT+P to open command palette (on macOS)
            if i.modifiers.mac_cmd && i.modifiers.shift && i.key_pressed(egui::Key::P) {
                self.open_command_palette();
            }
            
            // Handle command palette navigation
            if self.show_command_palette {
                // Arrow key navigation
                if i.key_pressed(egui::Key::ArrowDown) {
                    self.navigate_command_palette(1);
                } else if i.key_pressed(egui::Key::ArrowUp) {
                    self.navigate_command_palette(-1);
                }
                // Enter to execute selected command
                else if i.key_pressed(egui::Key::Enter) {
                    self.execute_selected_command();
                }
            }
            
            // Handle theme selector navigation
            if self.show_theme_selector {
                // Arrow key navigation
                if i.key_pressed(egui::Key::ArrowDown) {
                    self.navigate_theme_selector(1);
                } else if i.key_pressed(egui::Key::ArrowUp) {
                    self.navigate_theme_selector(-1);
                }
                // Enter to select theme
                else if i.key_pressed(egui::Key::Enter) {
                    self.select_current_theme();
                }
            }
            
            // Escape to close command palette or theme selector  
            if i.key_pressed(egui::Key::Escape) {
                if self.show_theme_selector {
                    self.show_theme_selector = false;
                } else if self.show_command_palette {
                    self.show_command_palette = false;
                    self.command_palette_input.clear();
                    self.command_palette_selected_index = 0;
                }
            }
        });

        // Render command palette if open
        if self.show_command_palette {
            self.render_command_palette(ctx);
        }

        // Render theme selector if open
        if self.show_theme_selector {
            self.render_theme_selector(ctx);
        }

        // Check for background task results
        if let Some(receiver) = &self.background_receiver {
            while let Ok(result) = receiver.try_recv() {
                match result {
                    models::enums::BackgroundResult::RefreshComplete { connection_id, success } => {
                        // Remove from refreshing set
                        self.refreshing_connections.remove(&connection_id);
                        
                        if success {
                            println!("Background refresh completed successfully for connection {}", connection_id);
                            // Re-expand connection node to show fresh data
                            for node in &mut self.items_tree {
                                if node.node_type == models::enums::NodeType::Connection && node.connection_id == Some(connection_id) {
                                    node.is_loaded = false; // Force reload from cache
                                    // Don't auto-expand after refresh, let user manually expand
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
            style.visuals.selection.bg_fill = egui::Color32::from_rgb(255, 60, 0);
            style.visuals.selection.stroke.color = egui::Color32::from_rgb(0, 0, 0);
            
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
        connection::render_connection_selector(self, ctx);
        self.render_error_dialog(ctx);
        self.render_create_folder_dialog(ctx);
        self.render_move_to_folder_dialog(ctx);

        egui::SidePanel::left("sidebar")
            .resizable(true)
            .default_width(250.0)
            .min_width(150.0)
            .max_width(500.0)
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    // Top section with tabs
                    ui.horizontal(|ui| {
                        // Calculate equal width for three buttons with responsive design
                        let available_width = ui.available_width();
                        let button_spacing = ui.spacing().item_spacing.x;
                        let button_width = (available_width - (button_spacing * 2.0)) / 3.0;
                        let button_height = 24.0;
                        
                        // Database tab
                        let database_button = if self.selected_menu == "Database" {
                            egui::Button::new(
                                egui::RichText::new("Database").color(egui::Color32::WHITE).text_style(egui::TextStyle::Body)
                            ).fill(egui::Color32::from_rgb(255, 60, 0))
                        } else {
                            egui::Button::new("Database").fill(egui::Color32::TRANSPARENT)
                        };
                        if ui.add_sized([button_width, button_height], database_button).clicked() {
                            self.selected_menu = "Database".to_string();
                        }
                        
                        // Queries tab
                        let queries_button = if self.selected_menu == "Queries" {
                            egui::Button::new(egui::RichText::new("Queries").color(egui::Color32::WHITE).text_style(egui::TextStyle::Body)).fill(egui::Color32::from_rgb(255, 60, 0)) // Orange fill for active
                        } else {
                            egui::Button::new("Queries").fill(egui::Color32::TRANSPARENT)
                        };
                        if ui.add_sized([button_width, button_height], queries_button).clicked() {
                            self.selected_menu = "Queries".to_string();
                        }
                        
                        // History tab
                        let history_button = if self.selected_menu == "History" {
                            egui::Button::new(egui::RichText::new("History").color(egui::Color32::WHITE).text_style(egui::TextStyle::Body)).fill(egui::Color32::from_rgb(255, 60, 0)) // Orange fill for active
                        } else {
                            egui::Button::new("History").fill(egui::Color32::TRANSPARENT)
                        };
                        if ui.add_sized([button_width, button_height], history_button).clicked() {
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
                                // Add right-click context menu support to the UI area itself
                                let queries_response = ui.interact(ui.available_rect_before_wrap(), egui::Id::new("queries_area"), egui::Sense::click());
                                queries_response.context_menu(|ui| {
                                    if ui.button("📂 Create Folder").clicked() {
                                        self.show_create_folder_dialog = true;
                                        ui.close_menu();
                                    }
                                });
                                
                                // Render the queries tree normally
                                let mut queries_tree = std::mem::take(&mut self.queries_tree);
                                let _ = self.render_tree(ui, &mut queries_tree);
                                self.queries_tree = queries_tree;
                            },
                            "History" => {
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
                                        if let Some((headers, data)) = connection::execute_query_with_connection(self, history_item.connection_id, history_item.query.clone()) {
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
                                        .fill(egui::Color32::RED)
                                ).on_hover_text("Add New Database Connection").clicked() {
                                    // Reset test connection status saat buka add dialog
                                    self.test_connection_status = None;
                                    self.test_connection_in_progress = false;
                                    self.show_add_connection = true;
                                }
                            },
                            // "Queries" => {
                            //     if ui.add_sized(
                            //         [24.0, 24.0], // Small square button
                            //         egui::Button::new("➕")
                            //             .fill(egui::Color32::RED)
                            //     ).on_hover_text("New Query File").clicked() {
                            //         // Create new tab instead of clearing editor
                            //         self.create_new_tab("Untitled Query".to_string(), String::new());
                            //     }
                            // },
                            _ => {
                                // No button for History tab
                            }
                        }
                    });
                });
            });

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.inner_margin(egui::Margin::ZERO)) // Remove all padding
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
                    .frame(egui::Frame::NONE.inner_margin(egui::Margin::ZERO)) // Remove panel margin
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
                                        egui::Color32::from_rgb(255, 60, 0) // Orange for active
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
                                        .fill(egui::Color32::BLACK)
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
                            egui::Frame::NONE
                                .fill(if ui.visuals().dark_mode { 
                                    egui::Color32::from_rgb(30, 30, 30) // Slightly darker for contrast
                                } else { 
                                    egui::Color32::WHITE // Pure white for light mode
                                })
                                .inner_margin(egui::Margin::ZERO) // No padding for compact design
                                .show(ui, |ui| {
                                    self.render_advanced_editor(ui);
                                    
                                    // Check for Ctrl+Enter or Cmd+Enter to execute query
                                    if ui.input(|i| {
                                        (i.modifiers.ctrl || i.modifiers.mac_cmd) && i.key_pressed(egui::Key::Enter)
                                    }) && (!self.selected_text.trim().is_empty() || !self.editor_text.trim().is_empty()) {
                                        if self.current_connection_id.is_some() {
                                            // Connection is already selected, execute query
                                            self.execute_query();
                                        } else if !self.connections.is_empty() {
                                            // No connection selected but connections exist, show selector
                                            self.pending_query = if !self.selected_text.trim().is_empty() {
                                                self.selected_text.clone()
                                            } else {
                                                self.editor_text.clone()
                                            };
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
                    .frame(egui::Frame::NONE.inner_margin(egui::Margin::ZERO)) // Remove all padding
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
                
                // Show selection status
                if !self.selected_text.trim().is_empty() {
                    ui.colored_label(egui::Color32::from_rgb(0, 150, 255), 
                        format!("Selected: {} chars", self.selected_text.len()));
                    ui.separator();
                }
                
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
                // Show pagination info
                if self.total_rows > 0 {
                    ui.label(format!("Showing {} of {} rows (page {}/{})", 
                        self.current_table_data.len(), 
                        self.total_rows,
                        self.current_page + 1,
                        self.get_total_pages()));
                } else {
                    ui.label(format!("Showing {} rows", self.current_table_data.len()));
                }
                ui.separator();
                
                // Show execution hint
                if !self.selected_text.trim().is_empty() {
                    ui.colored_label(egui::Color32::from_rgb(100, 200, 100), "CMD+Enter: Execute selection");
                } else {
                    ui.label("CMD+Enter: Execute all");
                }
            });
        });
    }
}
