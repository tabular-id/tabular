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


// Helper function to convert SQLite rows to Vec<Vec<String>> with proper type checking
pub(crate) fn convert_sqlite_rows_to_table_data(rows: Vec<sqlx::sqlite::SqliteRow>) -> Vec<Vec<String>> {
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

