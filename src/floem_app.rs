//! Tabular App - Floem UI
//! 
//! Main application structure using Floem UI framework with lapce-core editor

use floem::{
    Application, View,
    window::WindowConfig,
    peniko::Color,
    reactive::{RwSignal, create_rw_signal, SignalGet, SignalUpdate},
    views::{
        Decorators,
        empty, label, button,
        h_stack, v_stack, text_editor,
        editor::text::{default_dark_color, SimpleStyling},
    },
    style::CursorStyle,
};

use crate::floem_connection::{ConnectionManager, ConnectionInfo};
use crate::floem_query::{QueryResult, execute_query};

/// Run the Floem-based Tabular application
pub fn run_floem_app() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize config and logging
    crate::config::init_data_dir();
    dotenv::dotenv().ok();
    let _ = env_logger::Builder::from_default_env()
        .filter_module("tabular", log::LevelFilter::Debug)
        .is_test(false)
        .try_init();
    
    log::info!("Starting Tabular with Floem UI + Lapce Editor");
    
    let window_config = WindowConfig::default()
        .title("Tabular - SQL & NoSQL Database Client (Floem + Lapce)")
        .size((1200.0, 800.0));
    
    let app_view = build_app_ui();
    
    Application::new()
        .window(move |_| app_view, Some(window_config))
        .run();
    
    Ok(())
}

/// Build the main application UI
fn build_app_ui() -> impl View {
    // Create reactive state
    let query_count = create_rw_signal(0);
    let status_text = create_rw_signal("Ready".to_string());
    let editor_text = create_rw_signal("SELECT * FROM users LIMIT 10;".to_string());
    let query_result = create_rw_signal(None::<QueryResult>);
    
    // Connection manager
    let connection_manager = create_rw_signal(ConnectionManager::new());
    let selected_connection = create_rw_signal(None::<ConnectionInfo>);
    
    // Add sample SQLite connection for testing
    {
        let mut manager = connection_manager.get_untracked();
        manager.add_connection(ConnectionInfo::new_sqlite("test_db".to_string(), "./test.db".to_string()));
        let conns = manager.get_connections();
        if let Some(first) = conns.first() {
            selected_connection.set(Some(first.clone()));
        }
        connection_manager.set(manager);
    }
    
    v_stack((
        // Top toolbar
        build_toolbar(query_count, status_text, editor_text, selected_connection, query_result),
        
        // Main content area
        h_stack((
            // Left sidebar (connections & queries)
            build_sidebar(connection_manager, selected_connection),
            
            // Center editor area
            build_editor_area(query_count, status_text, editor_text),
            
            // Right panel (results)
            build_results_panel(query_result),
        ))
        .style(|s| s.flex_grow(1.0)),
        
        // Bottom status bar
        build_status_bar(status_text, query_result),
    ))
    .style(|s| {
        s.width_full()
            .height_full()
            .background(Color::from_rgb8(30, 30, 30))
    })
}

/// Build toolbar with actions
fn build_toolbar(
    query_count: RwSignal<i32>,
    status_text: RwSignal<String>,
    editor_text: RwSignal<String>,
    selected_connection: RwSignal<Option<ConnectionInfo>>,
    query_result: RwSignal<Option<QueryResult>>,
) -> impl View {
    h_stack((
        label(|| "Tabular 🗄️").style(|s| {
            s.font_size(18.0)
                .font_weight(floem::text::Weight::BOLD)
                .padding(10.0)
                .color(Color::from_rgb8(100, 181, 246))
        }),
        
        // Spacer
        empty().style(|s| s.flex_grow(1.0)),
        
        // Action buttons
        button("+ New Query")
            .on_click_stop(move |_| {
                query_count.update(|v| *v += 1);
                status_text.set(format!("Created query #{}", query_count.get()));
            })
            .style(|s| {
                s.padding(8.0)
                    .margin_right(10.0)
                    .background(Color::from_rgb8(56, 142, 60))
                    .border_radius(4.0)
                    .cursor(CursorStyle::Pointer)
                    .hover(|s| s.background(Color::from_rgb8(67, 160, 71)))
            }),
            
        button("▶ Execute (F5)")
            .on_click_stop(move |_| {
                let query = editor_text.get();
                let conn = selected_connection.get();
                
                if let Some(conn_info) = conn {
                    status_text.set("Executing query...".to_string());
                    log::info!("Executing query: {}", query);
                    
                    // For now, just update status
                    // TODO: Execute query in background thread and update UI
                    status_text.set("Query execution not yet implemented - UI ready!".to_string());
                } else {
                    status_text.set("No connection selected!".to_string());
                }
            })
            .style(|s| {
                s.padding(8.0)
                    .margin_right(10.0)
                    .background(Color::from_rgb8(25, 118, 210))
                    .border_radius(4.0)
                    .cursor(CursorStyle::Pointer)
                    .hover(|s| s.background(Color::from_rgb8(30, 136, 229)))
            }),
    ))
    .style(|s| {
        s.width_full()
            .height(40.0)
            .background(Color::from_rgb8(40, 40, 40))
            .border_bottom(1.0)
            .border_color(Color::from_rgb8(60, 60, 60))
    })
}

/// Build sidebar
fn build_sidebar(
    connection_manager: RwSignal<ConnectionManager>,
    selected_connection: RwSignal<Option<ConnectionInfo>>,
) -> impl View {
    v_stack((
        label(|| "Connections").style(|s| {
            s.font_size(14.0)
                .font_weight(floem::text::Weight::BOLD)
                .padding(10.0)
                .color(Color::from_rgb8(255, 235, 59))
        }),
        
        // Connection list placeholder - will be replaced with dynamic list
        v_stack((
            label(|| "📄 test_db").style(|s| s.padding(5.0).cursor(CursorStyle::Pointer)),
        ))
        .style(|s| s.padding_left(10.0).color(Color::from_rgb8(189, 189, 189))),
        
        empty().style(|s| s.flex_grow(1.0)),
        
        button("+ Add Connection")
            .on_click_stop(|_| {
                log::info!("Add connection clicked");
            })
            .style(|s| {
                s.width_full()
                    .padding(10.0)
                    .background(Color::from_rgb8(50, 50, 50))
                    .cursor(CursorStyle::Pointer)
                    .hover(|s| s.background(Color::from_rgb8(60, 60, 60)))
            }),
    ))
    .style(|s| {
        s.width(250.0)
            .height_full()
            .background(Color::from_rgb8(35, 35, 35))
            .border_right(1.0)
            .border_color(Color::from_rgb8(60, 60, 60))
    })
}

/// Build editor area with real Lapce editor
fn build_editor_area(
    query_count: RwSignal<i32>,
    status_text: RwSignal<String>,
    editor_text: RwSignal<String>,
) -> impl View {
    v_stack((
        // Tab bar
        h_stack((
            label(move || format!("Query {} ", query_count.get().max(1))).style(|s| {
                s.padding(10.0)
                    .background(Color::from_rgb8(50, 50, 50))
            }),
        ))
        .style(|s| {
            s.width_full()
                .height(35.0)
                .background(Color::from_rgb8(40, 40, 40))
        }),
        
        // Real Lapce editor widget with syntax highlighting
        text_editor("-- Write your SQL query here\n-- Press F5 or click Execute to run\n\nSELECT id, name, email FROM users LIMIT 10;")
            .placeholder("Enter your SQL query...")
            .styling(SimpleStyling::new())
            .editor_style(default_dark_color)
            .style(|s| {
                s.flex_grow(1.0)
                    .width_full()
                    .font_size(14.0)
                    .font_family("Monaco".to_string())
            }),
    ))
    .style(|s| s.flex_grow(1.0))
}

/// Build results panel
fn build_results_panel(query_result: RwSignal<Option<QueryResult>>) -> impl View {
    v_stack((
        label(|| "Results").style(|s| {
            s.font_size(14.0)
                .font_weight(floem::text::Weight::BOLD)
                .padding(10.0)
                .color(Color::from_rgb8(129, 212, 250))
        }),
        
        // Results display
        v_stack((
            h_stack((
                label(|| "Rows: ").style(|s| s.font_weight(floem::text::Weight::BOLD)),
                label(move || {
                    if let Some(result) = query_result.get() {
                        result.row_count.to_string()
                    } else {
                        "0".to_string()
                    }
                }),
            ))
            .style(|s| s.padding(5.0)),
            
            h_stack((
                label(|| "Time: ").style(|s| s.font_weight(floem::text::Weight::BOLD)),
                label(move || {
                    if let Some(result) = query_result.get() {
                        format!("{}ms", result.execution_time_ms)
                    } else {
                        "0ms".to_string()
                    }
                }),
            ))
            .style(|s| s.padding(5.0)),
            
            label(move || {
                if let Some(result) = query_result.get() {
                    if let Some(err) = &result.error {
                        format!("❌ Error: {}", err)
                    } else if !result.columns.is_empty() {
                        format!("Columns: {}", result.columns.join(", "))
                    } else {
                        "No results".to_string()
                    }
                } else {
                    "Execute a query to see results".to_string()
                }
            })
            .style(move |s| {
                let color = if let Some(result) = query_result.get() {
                    if result.error.is_some() {
                        Color::from_rgb8(244, 67, 54)
                    } else {
                        Color::from_rgb8(156, 204, 101)
                    }
                } else {
                    Color::from_rgb8(158, 158, 158)
                };
                s.padding(5.0).color(color)
            }),
            
            label(move || {
                if let Some(result) = query_result.get() {
                    if result.error.is_none() && !result.rows.is_empty() {
                        format!("First row: {:?}", result.rows.first().unwrap())
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            })
            .style(|s| s.padding(5.0).color(Color::from_rgb8(189, 189, 189))),
        )),
    ))
    .style(|s| {
        s.width(400.0)
            .height_full()
            .background(Color::from_rgb8(35, 35, 35))
            .border_left(1.0)
            .border_color(Color::from_rgb8(60, 60, 60))
    })
}

/// Build status bar
fn build_status_bar(
    status_text: RwSignal<String>,
    query_result: RwSignal<Option<QueryResult>>,
) -> impl View {
    h_stack((
        label(move || status_text.get())
            .style(|s| s.padding(5.0).color(Color::from_rgb8(156, 204, 101))),
        empty().style(|s| s.flex_grow(1.0)),
        label(move || {
            if let Some(result) = query_result.get() {
                if result.error.is_some() {
                    "Error".to_string()
                } else {
                    format!("{} rows", result.row_count)
                }
            } else {
                "Ready".to_string()
            }
        })
        .style(|s| s.padding(5.0).color(Color::from_rgb8(189, 189, 189))),
        label(|| "Floem + Lapce Core")
            .style(|s| s.padding(5.0).color(Color::from_rgb8(189, 189, 189))),
    ))
    .style(|s| {
        s.width_full()
            .height(25.0)
            .background(Color::from_rgb8(40, 40, 40))
            .border_top(1.0)
            .border_color(Color::from_rgb8(60, 60, 60))
    })
}
