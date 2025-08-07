use sqlx::{MySqlPool};
use sqlx::{SqlitePool, Row, Column};
use log::{debug};

use crate::{connection, models, window_egui};



// Helper function for final fallback when all type-specific conversions fail
fn get_value_as_string_fallback(row: &sqlx::mysql::MySqlRow, column_name: &str, type_name: &str) -> String {
       
       debug!("Fallback for column '{}' with type '{}'", column_name, type_name);
       
       // Simple fallback: try string conversion with both column name and index
       // Try column index instead of name (more reliable)
       let column_index = match row.columns().iter().position(|col| col.name() == column_name) {
       Some(idx) => idx,
       None => {
              debug!("Column '{}' not found in row", column_name);
              return format!("[COLUMN_NOT_FOUND:{}]", column_name);
       }
       };
       
       // Try with column index first
       if let Ok(Some(val)) = row.try_get::<Option<String>, _>(column_index) {
       return val;
       }
       if let Ok(val) = row.try_get::<String, _>(column_index) {
       return val;
       }
       
       // Try with column name as fallback
       match row.try_get::<Option<String>, _>(column_name) {
       Ok(Some(val)) => val,
       Ok(None) => "NULL".to_string(),
       Err(_) => format!("[CONVERSION_ERROR:{}]", type_name)
       }
}



// Helper function to convert MySQL rows to Vec<Vec<String>> with proper type checking
pub(crate) fn convert_mysql_rows_to_table_data(rows: Vec<sqlx::mysql::MySqlRow>) -> Vec<Vec<String>> {
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
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "SMALLINT" => {
                     match row.try_get::<Option<i16>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "MEDIUMINT" | "INT" => {
                     match row.try_get::<Option<i32>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "BIGINT" => {
                     match row.try_get::<Option<i64>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Unsigned integer types
              "TINYINT UNSIGNED" => {
                     match row.try_get::<Option<u8>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "SMALLINT UNSIGNED" => {
                     match row.try_get::<Option<u16>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                     match row.try_get::<Option<u32>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "BIGINT UNSIGNED" => {
                     match row.try_get::<Option<u64>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Floating point types
              "FLOAT" => {
                     match row.try_get::<Option<f32>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              "DOUBLE" => {
                     match row.try_get::<Option<f64>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Decimal types - use rust_decimal for proper handling
              "DECIMAL" | "NUMERIC" => {
                     // Method 1: Try rust_decimal::Decimal first (best for precision)
                     if let Ok(Some(val)) = row.try_get::<Option<rust_decimal::Decimal>, _>(column_name) {
                     val.to_string()
                     } else if let Ok(val) = row.try_get::<rust_decimal::Decimal, _>(column_name) {
                     val.to_string()
                     }
                     // Method 2: Try as string (fallback)
                     else if let Ok(Some(val)) = row.try_get::<Option<String>, _>(column_name) {
                     val
                     } else if let Ok(val) = row.try_get::<String, _>(column_name) {
                     val
                     }
                     // Method 3: Try as f64 (last resort)
                     else if let Ok(Some(val)) = row.try_get::<Option<f64>, _>(column_name) {
                     val.to_string()
                     } else if let Ok(val) = row.try_get::<f64, _>(column_name) {
                     val.to_string()
                     }
                     // Method 4: Final fallback
                     else {
                     get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // String types
              "VARCHAR" | "CHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                     match row.try_get::<Option<String>, _>(column_name) {
                     Ok(Some(val)) => val,
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Binary types
              "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                     match row.try_get::<Option<Vec<u8>>, _>(column_name) {
                     Ok(Some(val)) => format!("[BINARY:{} bytes]", val.len()),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Date and time types - try proper types first, then fallback to string
              "DATE" => {
                     // Try chrono::NaiveDate first - single read
                     match row.try_get::<Option<chrono::NaiveDate>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                   Ok(Some(val)) => val,
                                   Ok(None) => "NULL".to_string(),
                                   Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                            }
                     }
                     }
              },
              "TIME" => {
                     // Try chrono::NaiveTime first - single read
                     match row.try_get::<Option<chrono::NaiveTime>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                   Ok(Some(val)) => val,
                                   Ok(None) => "NULL".to_string(),
                                   Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                            }
                     }
                     }
              },
              "DATETIME" | "TIMESTAMP" => {
                     // Try chrono::NaiveDateTime first - single read
                     match row.try_get::<Option<chrono::NaiveDateTime>, _>(column_name) {
                     Ok(Some(val)) => val.to_string(),
                     Ok(None) => "NULL".to_string(),
                     Err(_) => {
                            // Fallback to string
                            match row.try_get::<Option<String>, _>(column_name) {
                                   Ok(Some(val)) => val,
                                   Ok(None) => "NULL".to_string(),
                                   Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                            }
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
                                   Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                            }
                     }
                     }
              },
              // JSON type - fallback to string
              "JSON" => {
                     match row.try_get::<Option<String>, _>(column_name) {
                     Ok(Some(val)) => val,
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              },
              // Default case: try string first, then generic fallback
              _ => {
                     match row.try_get::<Option<String>, _>(column_name) {
                     Ok(Some(val)) => val,
                     Ok(None) => "NULL".to_string(),
                     Err(_) => get_value_as_string_fallback(row, column_name, type_name)
                     }
              }
              };
              
              row_data.push(value_str);
       }
       table_data.push(row_data);
       }
       
       table_data
}



pub(crate) async fn fetch_mysql_data(connection_id: i64, pool: &MySqlPool, cache_pool: &SqlitePool) -> bool {

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


pub(crate) fn fetch_tables_from_mysql_connection(tabular: &mut window_egui::Tabular, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
       
       // Create a new runtime for the database query
       let rt = tokio::runtime::Runtime::new().ok()?;
       
       rt.block_on(async {
       // Get or create connection pool
       let pool = connection::get_or_create_connection_pool(tabular, connection_id).await?;
       
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
                     debug!("Unsupported table type: {}", table_type);
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
                     debug!("Error querying MySQL {} from database {}: {}", table_type, database_name, e);
                     None
                     }
              }
              },
              _ => {
              debug!("Wrong pool type for MySQL connection");
              None
              }
       }
       })
}


pub(crate) fn load_mysql_structure(connection_id: i64, _connection: &models::structs::ConnectionConfig, node: &mut models::structs::TreeNode) {

       debug!("Loading MySQL structure for connection ID: {}", connection_id);
       
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
