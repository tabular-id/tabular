use sqlx::{SqlitePool, PgPool, Row};

use crate::{models};

pub(crate) async fn fetch_postgres_data(connection_id: i64, pool: &PgPool, cache_pool: &SqlitePool) -> bool {
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



pub(crate) fn load_postgresql_structure(connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {
       
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
