use sqlx::{SqlitePool, Row};

pub async fn fetch_data(connection_id: i64, pool: &SqlitePool, cache_pool: &SqlitePool) -> bool {
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
