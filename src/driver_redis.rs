use sqlx::SqlitePool;
use redis::aio::ConnectionManager;

pub(crate) async fn fetch_redis_data(connection_id: i64, redis_manager: &ConnectionManager, cache_pool: &SqlitePool) -> bool {
       // Try to get a Redis connection
       let mut conn = redis_manager.clone();
       match redis::cmd("PING").query_async::<_, String>(&mut conn).await {
       Ok(_) => {
              // Get CONFIG GET databases to determine max database count
              let max_databases = if let Ok(config_result) = redis::cmd("CONFIG").arg("GET").arg("databases").query_async::<_, Vec<String>>(&mut conn).await {
              if config_result.len() >= 2 {
                     config_result[1].parse::<i32>().unwrap_or(16)
              } else {
                     16 // Default Redis databases count
              }
              } else {
              16 // Default fallback
              };
              
              // Cache all potential databases (db0 to db15 by default)
              for db_num in 0..max_databases {
              let db_name = format!("db{}", db_num);
              let _ = sqlx::query("INSERT OR REPLACE INTO database_cache (connection_id, database_name) VALUES (?, ?)")
                     .bind(connection_id)
                     .bind(&db_name)
                     .execute(cache_pool)
                     .await;
              }
              
              // Get keyspace info to identify which databases actually have keys
              if let Ok(keyspace_result) = redis::cmd("INFO").arg("keyspace").query_async::<_, String>(&mut conn).await {
              for line in keyspace_result.lines() {
                     if line.starts_with("db") {
                     if let Some(db_part) = line.split(':').next() {
                            // Mark this database as having keys by adding a special marker
                            let _ = sqlx::query("INSERT OR REPLACE INTO table_cache (connection_id, database_name, table_name, table_type) VALUES (?, ?, ?, ?)")
                                   .bind(connection_id)
                                   .bind(db_part)
                                   .bind("_has_keys")
                                   .bind("redis_marker")
                                   .execute(cache_pool)
                                   .await;
                     }
                     }
              }
              }
              
              true
       }
       Err(_e) => {
              false
       }
       }
}

