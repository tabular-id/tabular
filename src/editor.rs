use eframe::egui;
use log::error;

use crate::{directory, editor, models, sidebar_query, window_egui};


    // Tab management methods
 pub(crate) fn create_new_tab(tabular: &mut window_egui::Tabular, title: String, content: String) -> usize {
        let tab_id = tabular.next_tab_id;
        tabular.next_tab_id += 1;
        
        let new_tab = models::structs::QueryTab {
            title,
            content: content.clone(),
            file_path: None,
            is_saved: false,
            is_modified: false,
            connection_id: None, // No connection assigned by default
            database_name: None, // No database assigned by default
            has_executed_query: false, // New tab hasn't executed any query yet
        };
        
        tabular.query_tabs.push(new_tab);
        let new_index = tabular.query_tabs.len() - 1;
        tabular.active_tab_index = new_index;
        
        // Update editor with new tab content
        tabular.editor_text = content;
        
        tab_id
    }

 pub(crate) fn create_new_tab_with_connection(tabular: &mut window_egui::Tabular, title: String, content: String, connection_id: Option<i64>) -> usize {
        create_new_tab_with_connection_and_database(tabular, title, content, connection_id, None)
    }
    
 pub(crate) fn create_new_tab_with_connection_and_database(tabular: &mut window_egui::Tabular, title: String, content: String, connection_id: Option<i64>, database_name: Option<String>) -> usize {
        let tab_id = tabular.next_tab_id;
        tabular.next_tab_id += 1;
        
        let new_tab = models::structs::QueryTab {
            title,
            content: content.clone(),
            file_path: None,
            is_saved: false,
            is_modified: false,
            connection_id,
            database_name,
            has_executed_query: false, // New tab hasn't executed any query yet
        };
        
        tabular.query_tabs.push(new_tab);
        let new_index = tabular.query_tabs.len() - 1;
        tabular.active_tab_index = new_index;
        
        // Update editor with new tab content
        tabular.editor_text = content;
        
        tab_id
    }

 pub(crate) fn close_tab(tabular: &mut window_egui::Tabular, tab_index: usize) {
        if tabular.query_tabs.len() <= 1 {
            // Don't close the last tab, just clear it
            if let Some(tab) = tabular.query_tabs.get_mut(0) {
                tab.content.clear();
                tab.title = "Untitled Query".to_string();
                tab.file_path = None;
                tab.is_saved = false;
                tab.is_modified = false;
                tab.connection_id = None; // Clear connection as well
                tab.database_name = None; // Clear database as well
            }
            tabular.editor_text.clear();
            return;
        }

        if tab_index < tabular.query_tabs.len() {
            tabular.query_tabs.remove(tab_index);
            
            // Adjust active tab index
            if tabular.active_tab_index >= tabular.query_tabs.len() {
                tabular.active_tab_index = tabular.query_tabs.len() - 1;
            } else if tabular.active_tab_index > tab_index {
                tabular.active_tab_index -= 1;
            }
            
            // Update editor with active tab content
            if let Some(active_tab) = tabular.query_tabs.get(tabular.active_tab_index) {
                tabular.editor_text = active_tab.content.clone();
            }
        }
    }

 pub(crate) fn switch_to_tab(tabular: &mut window_egui::Tabular, tab_index: usize) {
        if tab_index < tabular.query_tabs.len() {
            // Save current tab content
            if let Some(current_tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
                if current_tab.content != tabular.editor_text {
                    current_tab.content = tabular.editor_text.clone();
                    current_tab.is_modified = true;
                }
            }
            
            // Switch to new tab
            tabular.active_tab_index = tab_index;
            if let Some(new_tab) = tabular.query_tabs.get(tab_index) {
                tabular.editor_text = new_tab.content.clone();
            }
        }
    }

 pub(crate) fn save_current_tab(tabular: &mut window_egui::Tabular) -> Result<(), String> {
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
            tab.content = tabular.editor_text.clone();
            
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
                tabular.save_filename = tab.title.replace("Untitled Query", "").trim().to_string();
                if tabular.save_filename.is_empty() {
                    tabular.save_filename = "new_query".to_string();
                }
                if !tabular.save_filename.ends_with(".sql") {
                    tabular.save_filename.push_str(".sql");
                }
                tabular.show_save_dialog = true;
                Ok(())
            }
        } else {
            Err("No active tab".to_string())
        }
    }

 pub(crate) fn save_current_tab_with_name(tabular: &mut window_egui::Tabular, filename: String) -> Result<(), String> {
        if let Some(tab) = tabular.query_tabs.get_mut(tabular.active_tab_index) {
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
            sidebar_query::load_queries_from_directory(tabular);
            
            Ok(())
        } else {
            Err("No active tab".to_string())
        }
    }


pub(crate) fn render_save_dialog(tabular: &mut window_egui::Tabular, ctx: &egui::Context) {
        if tabular.show_save_dialog {
            egui::Window::new("Save Query")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Enter filename:");
                    ui.text_edit_singleline(&mut tabular.save_filename);
                    
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() && !tabular.save_filename.is_empty() {
                            if let Err(err) = editor::save_current_tab_with_name(tabular,tabular.save_filename.clone()) {
                                error!("Failed to save: {}", err);
                            }
                            tabular.show_save_dialog = false;
                            tabular.save_filename.clear();
                        }
                        
                        if ui.button("Cancel").clicked() {
                            tabular.show_save_dialog = false;
                            tabular.save_filename.clear();
                        }
                    });
                });
        }
    }
