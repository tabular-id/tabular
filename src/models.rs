#[derive(Clone, PartialEq, Debug)]
pub enum NodeType {
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
    MySQLFolder,       // Folder untuk koneksi MySQL
    PostgreSQLFolder,  // Folder untuk koneksi PostgreSQL
    SQLiteFolder,      // Folder untuk koneksi SQLite
    RedisFolder,       // Folder untuk koneksi Redis
    CustomFolder,      // Folder custom yang bisa dinamai user
    QueryFolder,       // Folder untuk mengelompokkan query files
}

#[derive(Clone)]
pub struct TreeNode {
    pub name: String,
    pub children: Vec<TreeNode>,
    pub is_expanded: bool,
    pub(crate) node_type: NodeType,
    pub connection_id: Option<i64>, // For connection nodes
    pub is_loaded: bool, // For tracking if tables/columns are loaded
    pub database_name: Option<String>, // For storing database context
    pub file_path: Option<String>, // For query files
}

impl TreeNode {
    pub fn new(name: String, node_type: NodeType) -> Self {
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
    pub fn with_children(name: String, node_type: NodeType, children: Vec<TreeNode>) -> Self {
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

    pub fn new_connection(name: String, connection_id: i64) -> Self {
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