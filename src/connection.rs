use crate::{
              modules, models, window_egui::{Tabular}, 
              driver_mysql, driver_sqlite, helpers
       };
use eframe::{egui, App, Frame};
use sqlx::{SqlitePool, Row, Column, mysql::MySqlPoolOptions, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use redis::{Client, aio::ConnectionManager};


pub(crate) fn render_connection_selector(tabular: &mut Tabular, ctx: &egui::Context) {
       if tabular.show_connection_selector {
       egui::Window::new("Select Connection to Execute Query")
              .collapsible(false)
              .resizable(false)
              .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
              .show(ctx, |ui| {
              ui.add_space(5.0);
              
              // Show the query that will be executed
              ui.horizontal(|ui| {
                     ui.monospace(format!("\"{}...\"", 
                     if tabular.pending_query.len() > 50 {
                            &tabular.pending_query[..50]
                     } else {
                            &tabular.pending_query
                     }
                     ));
              });
              ui.separator();
              
                     egui::ScrollArea::vertical()
                     .max_height(200.0)
                     .show(ui, |ui| {
                     let mut selected_connection = None;

                     // sort tabular.connections by folder and name
                     tabular.connections.sort_by(|a, b| {
                            a.folder.cmp(&b.folder).then_with(|| a.name.cmp(&b.name))
                     });
                     
                     for connection in &tabular.connections {
                            let connection_text = format!("{} / {} / {}", 
                                   connection.folder.as_deref().unwrap_or(""),
                                   match connection.connection_type {
                                   models::enums::DatabaseType::MySQL => "MySQL",
                                   models::enums::DatabaseType::PostgreSQL => "PostgreSQL",
                                   models::enums::DatabaseType::SQLite => "SQLite",
                                   models::enums::DatabaseType::Redis => "Redis",
                                   },
                                   connection.name 
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
                            tabular.current_connection_id = Some(connection_id);
                            
                            if tabular.auto_execute_after_connection {
                                   // Execute the query immediately
                                   let query = tabular.pending_query.clone();
                                   if let Some((headers, data)) = execute_query_with_connection(tabular, connection_id, query) {
                                   tabular.current_table_headers = headers;
                                   tabular.current_table_data = data;
                                   if tabular.current_table_data.is_empty() {
                                          tabular.current_table_name = "Query executed successfully (no results)".to_string();
                                   } else {
                                          tabular.current_table_name = format!("Query Results ({} rows)", tabular.current_table_data.len());
                                   }
                                   } else {
                                   tabular.current_table_name = "Query execution failed".to_string();
                                   tabular.current_table_headers.clear();
                                   tabular.current_table_data.clear();
                                   }
                            }
                            
                            tabular.show_connection_selector = false;
                            tabular.pending_query.clear();
                            tabular.auto_execute_after_connection = false;
                     }
                     });                    ui.separator();
              ui.horizontal(|ui| {
                     if ui.button("Cancel").clicked() {
                     tabular.show_connection_selector = false;
                     tabular.pending_query.clear();
                     }
              });
              });
       }
}


pub(crate) fn execute_query_with_connection(tabular: &mut Tabular, connection_id: i64, query: String) -> Option<(Vec<String>, Vec<Vec<String>>)> {
       println!("Query execution requested for connection {} with query: {}", connection_id, query);
       
       if let Some(connection) = tabular.connections.iter().find(|c| c.id == Some(connection_id)).cloned() {
       execute_table_query_sync(tabular, connection_id, &connection, &query)
       } else {
       println!("Connection not found for ID: {}", connection_id);
       None
       }
}

pub(crate) fn execute_table_query_sync(tabular: &mut Tabular, connection_id: i64, _connection: &models::structs::ConnectionConfig, query: &str) -> Option<(Vec<String>, Vec<Vec<String>>)> {
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
       match get_or_create_connection_pool(tabular, connection_id).await {
              Some(pool) => {
              match pool {
                     models::enums::DatabasePool::MySQL(mysql_pool) => {
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
                                   let table_data = driver_mysql::convert_mysql_rows_to_table_data(rows);
                                   
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
                     models::enums::DatabasePool::PostgreSQL(pg_pool) => {
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
                     models::enums::DatabasePool::SQLite(sqlite_pool) => {
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
                                   let table_data = driver_sqlite::convert_sqlite_rows_to_table_data(rows);
                                   
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
                     },
                     models::enums::DatabasePool::Redis(redis_manager) => {
                     println!("Executing Redis command: {}", query);
                     
                     // For Redis, we need to handle commands differently
                     // Redis doesn't have SQL queries, so we'll treat the query as a Redis command
                     let mut connection = redis_manager.as_ref().clone();
                     use redis::AsyncCommands;
                     
                     // Parse simple Redis commands
                     let parts: Vec<&str> = query.trim().split_whitespace().collect();
                     if parts.is_empty() {
                            return Some((
                                   vec!["Error".to_string()],
                                   vec![vec!["Empty command".to_string()]]
                            ));
                     }
                     
                     match parts[0].to_uppercase().as_str() {
                            "GET" => {
                                   if parts.len() != 2 {
                                   return Some((
                                          vec!["Error".to_string()],
                                          vec![vec!["GET requires exactly one key".to_string()]]
                                   ));
                                   }
                                   match connection.get::<&str, Option<String>>(parts[1]).await {
                                   Ok(Some(value)) => {
                                          Some((
                                          vec!["Key".to_string(), "Value".to_string()],
                                          vec![vec![parts[1].to_string(), value]]
                                          ))
                                   },
                                   Ok(None) => {
                                          Some((
                                          vec!["Key".to_string(), "Value".to_string()],
                                          vec![vec![parts[1].to_string(), "NULL".to_string()]]
                                          ))
                                   },
                                   Err(e) => {
                                          Some((
                                          vec!["Error".to_string()],
                                          vec![vec![format!("Redis GET error: {}", e)]]
                                          ))
                                   }
                                   }
                            },
                            "KEYS" => {
                                   if parts.len() != 2 {
                                   return Some((
                                          vec!["Error".to_string()],
                                          vec![vec!["KEYS requires exactly one pattern".to_string()]]
                                   ));
                                   }
                                   match connection.keys::<&str, Vec<String>>(parts[1]).await {
                                   Ok(keys) => {
                                          let table_data: Vec<Vec<String>> = keys.into_iter()
                                          .map(|key| vec![key])
                                          .collect();
                                          Some((
                                          vec!["Key".to_string()],
                                          table_data
                                          ))
                                   },
                                   Err(e) => {
                                          Some((
                                          vec!["Error".to_string()],
                                          vec![vec![format!("Redis KEYS error: {}", e)]]
                                          ))
                                   }
                                   }
                            },
                            "SCAN" => {
                                   // SCAN cursor [MATCH pattern] [COUNT count]
                                   // Parse SCAN command arguments
                                   if parts.len() < 2 {
                                   return Some((
                                          vec!["Error".to_string()],
                                          vec![vec!["SCAN requires cursor parameter".to_string()]]
                                   ));
                                   }
                                   
                                   let cursor = parts[1];
                                   let mut match_pattern = "*"; // default pattern
                                   let mut count = 10; // default count
                                   
                                   // Parse optional MATCH and COUNT parameters
                                   let mut i = 2;
                                   while i < parts.len() {
                                   match parts[i].to_uppercase().as_str() {
                                          "MATCH" => {
                                          if i + 1 < parts.len() {
                                                 match_pattern = parts[i + 1];
                                                 i += 2;
                                          } else {
                                                 return Some((
                                                 vec!["Error".to_string()],
                                                 vec![vec!["MATCH requires a pattern".to_string()]]
                                                 ));
                                          }
                                          },
                                          "COUNT" => {
                                          if i + 1 < parts.len() {
                                                 if let Ok(c) = parts[i + 1].parse::<i64>() {
                                                 count = c;
                                                 i += 2;
                                                 } else {
                                                 return Some((
                                                        vec!["Error".to_string()],
                                                        vec![vec!["COUNT must be a number".to_string()]]
                                                 ));
                                                 }
                                          } else {
                                                 return Some((
                                                 vec!["Error".to_string()],
                                                 vec![vec!["COUNT requires a number".to_string()]]
                                                 ));
                                          }
                                          },
                                          _ => {
                                          return Some((
                                                 vec!["Error".to_string()],
                                                 vec![vec![format!("Unknown SCAN parameter: {}", parts[i])]]
                                          ));
                                          }
                                   }
                                   }
                                   
                                   // Execute SCAN command using redis::cmd
                                   let mut cmd = redis::cmd("SCAN");
                                   cmd.arg(cursor);
                                   if match_pattern != "*" {
                                   cmd.arg("MATCH").arg(match_pattern);
                                   }
                                   cmd.arg("COUNT").arg(count);
                                   
                                   match cmd.query_async::<_, (String, Vec<String>)>(&mut connection).await {
                                   Ok((next_cursor, keys)) => {
                                          let mut table_data = Vec::new();
                                          
                                          if keys.is_empty() {
                                          // No keys found, provide helpful information
                                          table_data.push(vec!["Info".to_string(), format!("No keys found matching pattern: {}", match_pattern)]);
                                          table_data.push(vec!["Cursor".to_string(), next_cursor.clone()]);
                                          table_data.push(vec!["Suggestion".to_string(), "Try different pattern or use 'SCAN 0 COUNT 100' to see all keys".to_string()]);
                                          
                                          // If this was a pattern search and found nothing, try a general scan as fallback
                                          if match_pattern != "*" {
                                                 match redis::cmd("SCAN").arg("0").arg("COUNT").arg("10").query_async::<_, (String, Vec<String>)>(&mut connection).await {
                                                 Ok((_, sample_keys)) => {
                                                        if !sample_keys.is_empty() {
                                                               table_data.push(vec!["Sample Keys Found".to_string(), "".to_string()]);
                                                               for (i, key) in sample_keys.iter().take(5).enumerate() {
                                                               table_data.push(vec![format!("Sample {}", i+1), key.clone()]);
                                                               }
                                                        }
                                                 },
                                                 Err(_) => {
                                                        table_data.push(vec!["Note".to_string(), "Could not retrieve sample keys".to_string()]);
                                                 }
                                                 }
                                          }
                                          } else {
                                          // Add cursor info as first row
                                          table_data.push(vec!["CURSOR".to_string(), next_cursor]);
                                          
                                          // Add keys as subsequent rows
                                          for key in keys {
                                                 table_data.push(vec!["KEY".to_string(), key]);
                                          }
                                          }
                                          
                                          Some((
                                          vec!["Type".to_string(), "Value".to_string()],
                                          table_data
                                          ))
                                   },
                                   Err(e) => {
                                          Some((
                                          vec!["Error".to_string()],
                                          vec![vec![format!("Redis SCAN error: {}", e)]]
                                          ))
                                   }
                                   }
                            },
                            "INFO" => {
                                   // INFO command can have optional section parameter
                                   let section = if parts.len() > 1 { parts[1] } else { "default" };
                                   
                                   // Use Redis cmd for INFO command
                                   match redis::cmd("INFO").arg(section).query_async::<_, String>(&mut connection).await {
                                   Ok(info_result) => {
                                          // Parse INFO result into key-value pairs
                                          let mut table_data = Vec::new();
                                          
                                          for line in info_result.lines() {
                                          if line.trim().is_empty() || line.starts_with('#') {
                                                 continue;
                                          }
                                          
                                          if let Some((key, value)) = line.split_once(':') {
                                                 table_data.push(vec![key.to_string(), value.to_string()]);
                                          }
                                          }
                                          
                                          Some((
                                          vec!["Property".to_string(), "Value".to_string()],
                                          table_data
                                          ))
                                   },
                                   Err(e) => {
                                          Some((
                                          vec!["Error".to_string()],
                                          vec![vec![format!("Redis INFO error: {}", e)]]
                                          ))
                                   }
                                   }
                            },
                            "HGETALL" => {
                                   // HGETALL key - get all fields and values from a hash
                                   if parts.len() != 2 {
                                   return Some((
                                          vec!["Error".to_string()],
                                          vec![vec!["HGETALL requires exactly one key".to_string()]]
                                   ));
                                   }
                                   
                                   match redis::cmd("HGETALL").arg(parts[1]).query_async::<_, Vec<String>>(&mut connection).await {
                                   Ok(hash_data) => {
                                          let mut table_data = Vec::new();
                                          
                                          // HGETALL returns a flat list: [field1, value1, field2, value2, ...]
                                          for chunk in hash_data.chunks(2) {
                                          if chunk.len() == 2 {
                                                 table_data.push(vec![chunk[0].clone(), chunk[1].clone()]);
                                          }
                                          }
                                          
                                          if table_data.is_empty() {
                                          table_data.push(vec!["No data".to_string(), "Hash is empty or key does not exist".to_string()]);
                                          }
                                          
                                          Some((
                                          vec!["Field".to_string(), "Value".to_string()],
                                          table_data
                                          ))
                                   },
                                   Err(e) => {
                                          Some((
                                          vec!["Error".to_string()],
                                          vec![vec![format!("Redis HGETALL error: {}", e)]]
                                          ))
                                   }
                                   }
                            },
                            _ => {
                                   Some((
                                   vec!["Error".to_string()],
                                   vec![vec![format!("Unsupported Redis command: {}", parts[0])]]
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


// Helper function untuk mendapatkan atau membuat connection pool
pub(crate) async fn get_or_create_connection_pool(tabular: &mut Tabular, connection_id: i64) -> Option<models::enums::DatabasePool> {
       // First check if we already have a cached connection pool for this connection
       if let Some(cached_pool) = tabular.connection_pools.get(&connection_id) {
       println!("✅ Using cached connection pool for connection {}", connection_id);
       return Some(cached_pool.clone());
       }

       println!("🔄 Creating new connection pool for connection {}", connection_id);

       // If not cached, create a new connection pool
       if let Some(connection) = tabular.connections.iter().find(|c| c.id == Some(connection_id)) {
       match connection.connection_type {
              models::enums::DatabaseType::MySQL => {
              let encoded_username = modules::url_encode(&connection.username);
              let encoded_password = modules::url_encode(&connection.password);
              let connection_string = format!(
                     "mysql://{}:{}@{}:{}/{}",
                     encoded_username, encoded_password, connection.host, connection.port, connection.database
              );

              // ping the host first
              if !helpers::ping_host(&connection.host) {
                     println!("❌ Cannot ping host: {}", connection.host);
                     return None;
              }
              
              // Configure MySQL pool with proper settings
              let pool_result = MySqlPoolOptions::new()
                     .max_connections(10)  // Reduce connections for better stability
                     .min_connections(2)   // Fewer minimum connections
                     .acquire_timeout(std::time::Duration::from_secs(30))  // Reasonable timeout
                     .idle_timeout(std::time::Duration::from_secs(600))    // 10 minute idle timeout
                     .max_lifetime(std::time::Duration::from_secs(3600))   // 1 hour max lifetime
                     .test_before_acquire(true)  // Test connections before use
                     .connect(&connection_string)
                     .await;
              
              match pool_result {
                     Ok(pool) => {
                     let database_pool = models::enums::DatabasePool::MySQL(Arc::new(pool));
                     tabular.connection_pools.insert(connection_id, database_pool.clone());
                     println!("✅ Created MySQL connection pool for connection {}", connection_id);
                     Some(database_pool)
                     },
                     Err(e) => {
                     println!("❌ Failed to create MySQL pool for connection {}: {}", connection_id, e);
                     None
                     }
              }
              },
              models::enums::DatabaseType::PostgreSQL => {
              let connection_string = format!(
                     "postgresql://{}:{}@{}:{}/{}",
                     connection.username, connection.password, connection.host, connection.port, connection.database
              );
                                   
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
                     let database_pool = models::enums::DatabasePool::PostgreSQL(Arc::new(pool));
                     tabular.connection_pools.insert(connection_id, database_pool.clone());
                     Some(database_pool)
                     },
                     Err(e) => {
                     println!("Failed to create PostgreSQL pool: {}", e);
                     None
                     }
              }
              },
              models::enums::DatabaseType::SQLite => {
              let connection_string = format!("sqlite:{}", connection.host);
              
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
                     let database_pool = models::enums::DatabasePool::SQLite(Arc::new(pool));
                     tabular.connection_pools.insert(connection_id, database_pool.clone());
                     Some(database_pool)
                     },
                     Err(e) => {
                     println!("Failed to create SQLite pool: {}", e);
                     None
                     }
              }
              },
              models::enums::DatabaseType::Redis => {
              let connection_string = if connection.password.is_empty() {
                     format!("redis://{}:{}", connection.host, connection.port)
              } else {
                     format!("redis://{}:{}@{}:{}", connection.username, connection.password, connection.host, connection.port)
              };
              
              println!("Creating new Redis connection manager for: {}", connection.name);
              match Client::open(connection_string) {
                     Ok(client) => {
                     match ConnectionManager::new(client).await {
                            Ok(manager) => {
                                   let database_pool = models::enums::DatabasePool::Redis(Arc::new(manager));
                                   tabular.connection_pools.insert(connection_id, database_pool.clone());
                                   Some(database_pool)
                            },
                            Err(e) => {
                                   println!("Failed to create Redis connection manager: {}", e);
                                   None
                            }
                     }
                     }
                     Err(e) => {
                     println!("Failed to create Redis client: {}", e);
                     None
                     }
              }
              }
       }
       } else {
       None
       }
}

