use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row, SqlitePool};

use crate::{models, window_egui};

#[allow(dead_code)]
pub(crate) async fn fetch_postgres_data(
    connection_id: i64,
    pool: &PgPool,
    cache_pool: &SqlitePool,
) -> bool {
    // 1) Cache database names
    let db_rows = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sqlx::query("SELECT datname FROM pg_database WHERE datistemplate = false").fetch_all(pool),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)
    .and_then(|r| r)
    {
        Ok(r) => r,
        Err(_) => return false,
    };

    for row in db_rows {
        if let Ok(db_name) = row.try_get::<String, _>(0) {
            let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                            .bind(connection_id)
                            .bind(&db_name)
                            .execute(cache_pool)
                            .await;
        }
    }

    // 2) Cache tables/views for the CURRENT database only
    let current_db: Option<String> = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sqlx::query_scalar("SELECT current_database()").fetch_one(pool),
    )
    .await
    .ok()
    .and_then(|r| r.ok());

    if let Some(db_name) = current_db {
        // Tables (public)
        if let Ok(table_rows) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sqlx::query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'").fetch_all(pool),
        )
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)
        .and_then(|r| r)
              {
                     for table_row in table_rows {
                            if let Ok(table_name) = table_row.try_get::<String, _>(0) {
                                   let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                                          .bind(connection_id)
                                          .bind(&db_name)
                                          .bind(&table_name)
                                          .bind("table")
                                          .execute(cache_pool)
                                          .await;

                                   // Columns
                    if let Ok(col_rows) = tokio::time::timeout(
                         std::time::Duration::from_secs(5),
                         sqlx::query("SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position")
                        .bind(&table_name)
                        .fetch_all(pool),
                    )
                    .await
                    .map_err(|_| sqlx::Error::PoolTimedOut)
                    .and_then(|r| r)
                                   {
                                          for col_row in col_rows {
                                                 if let (Ok(col_name), Ok(col_type), Ok(ordinal_pos)) = (
                                                        col_row.try_get::<String, _>(0),
                                                        col_row.try_get::<String, _>(1),
                                                        col_row.try_get::<i32, _>(2),
                                                 ) {
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

        // Views (public)
        if let Ok(view_rows) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sqlx::query("SELECT table_name FROM information_schema.views WHERE table_schema = 'public'")
                .fetch_all(pool),
        )
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)
        .and_then(|r| r)
        {
            for view_row in view_rows {
                if let Ok(view_name) = view_row.try_get::<String, _>(0) {
                    let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                                          .bind(connection_id)
                                          .bind(&db_name)
                                          .bind(&view_name)
                                          .bind("view")
                                          .execute(cache_pool)
                                          .await;
                }
            }
        }
    }

    true
}

pub(crate) fn load_postgresql_structure(
    connection_id: i64,
    _connection: &models::structs::ConnectionConfig,
    node: &mut models::structs::TreeNode,
) {
    // Create basic structure for PostgreSQL
    let mut main_children = Vec::new();

    // Databases folder
    let mut databases_folder = models::structs::TreeNode::new(
        "Databases".to_string(),
        models::enums::NodeType::DatabasesFolder,
    );
    databases_folder.connection_id = Some(connection_id);

    // Add a loading indicator
    let loading_node = models::structs::TreeNode::new(
        "Loading databases...".to_string(),
        models::enums::NodeType::Database,
    );
    databases_folder.children.push(loading_node);

    main_children.push(databases_folder);

    node.children = main_children;
}

// Fetch tables/views from a PostgreSQL database (schema: public)
pub(crate) fn fetch_tables_from_postgres_connection(
    tabular: &mut window_egui::Tabular,
    connection_id: i64,
    database_name: &str,
    table_type: &str,
) -> Option<Vec<String>> {
    let rt = tokio::runtime::Runtime::new().ok()?;
    let db = database_name.to_string();

    rt.block_on(async {
              let conn = tabular.connections.iter().find(|c| c.id == Some(connection_id))?.clone();
              let conn_str = format!(
                     "postgresql://{}:{}@{}:{}/{}",
                     conn.username, conn.password, conn.host, conn.port, db
              );

        let pool = match PgPoolOptions::new()
                     .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
                     .connect(&conn_str)
                     .await
              {
                     Ok(p) => p,
                     Err(_) => return None,
              };

              let sql = match table_type {
                     "table" => "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name",
                     "view" => "SELECT table_name FROM information_schema.views WHERE table_schema = 'public' ORDER BY table_name",
                     _ => return None,
              };

        match tokio::time::timeout(
              std::time::Duration::from_secs(5),
              sqlx::query_as::<_, (String,)>(sql).fetch_all(&pool),
        )
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)
        .and_then(|r| r)
        {
                     Ok(rows) => Some(rows.into_iter().map(|(n,)| n).collect()),
                     Err(_) => None,
              }
       })
}
