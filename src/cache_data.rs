use crate::window_egui::Tabular;


pub(crate) fn get_tables_from_cache(tabular: &Tabular, connection_id: i64, database_name: &str, table_type: &str) -> Option<Vec<String>> {
       if let Some(ref pool) = tabular.db_pool {
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
