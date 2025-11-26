#[cfg(feature = "egui_ui")]
use eframe::egui;

pub mod auto_updater;

#[cfg(feature = "egui_ui")]
pub mod cache_data;

pub mod config;

#[cfg(feature = "egui_ui")]
pub mod connection;
#[cfg(feature = "egui_ui")]
pub mod data_table;
#[cfg(feature = "egui_ui")]
pub mod dialog;

pub mod directory;

#[cfg(feature = "egui_ui")]
pub mod driver_mongodb;
#[cfg(feature = "egui_ui")]
pub mod driver_mssql;
#[cfg(feature = "egui_ui")]
pub mod driver_mysql;
#[cfg(feature = "egui_ui")]
pub mod driver_postgres;
#[cfg(feature = "egui_ui")]
pub mod driver_redis;
#[cfg(feature = "egui_ui")]
pub mod driver_sqlite;

#[cfg(feature = "egui_ui")]
pub mod editor;
#[cfg(feature = "egui_ui")]
pub mod editor_autocomplete;
#[cfg(feature = "egui_ui")]
pub mod editor_autocomplete_new; // temporary clean implementation backing the shim

pub mod editor_buffer;

#[cfg(feature = "egui_ui")]
pub mod editor_rope_widget; // Custom Rope-based editor widget

pub mod editor_selection;

#[cfg(feature = "egui_ui")]
pub mod editor_state_adapter;

pub mod export;

// Floem UI (new)
#[cfg(feature = "floem_ui")]
pub mod floem_app;
#[cfg(feature = "floem_ui")]
pub mod floem_connection;
#[cfg(feature = "floem_ui")]
pub mod floem_query;

pub mod models;

#[cfg(feature = "egui_ui")]
pub mod modules;

pub mod query_tools;
pub mod self_update;

#[cfg(feature = "egui_ui")]
pub mod sidebar_database;

#[cfg(feature = "egui_ui")]
pub mod sidebar_history;

#[cfg(feature = "egui_ui")]
pub mod sidebar_query;

#[cfg(feature = "egui_ui")]
pub mod spreadsheet;
pub mod ssh_tunnel;
// Unified syntax / parsing module (legacy highlighter + optional tree-sitter parsing)
#[cfg(feature = "query_ast")]
pub mod query_ast;

#[cfg(feature = "egui_ui")]
pub mod syntax_ts;

#[cfg(feature = "egui_ui")]
pub mod window_egui; // re-enabled syntax highlighting helpers

/// Reusable entrypoint so other launchers (e.g., iOS) can run the UI.
#[cfg(all(feature = "egui_ui", not(feature = "floem_ui")))]
pub fn run() -> Result<(), eframe::Error> {
    run_egui()
}

/// Floem UI entrypoint
#[cfg(feature = "floem_ui")]
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    floem_app::run_floem_app()
}

/// Legacy egui entrypoint
#[cfg(feature = "egui_ui")]
pub fn run_egui() -> Result<(), eframe::Error> {
    dotenv::dotenv().ok();
    let _ = env_logger::Builder::from_default_env()
        // Enable info-level logs for our crate so users can see data source messages
        .filter_module("tabular", log::LevelFilter::Debug)
        .is_test(false)
        .try_init();
    config::init_data_dir();
    log::info!(
        "Application starting with data directory: {}",
        config::get_data_dir().display()
    );
    let mut options = eframe::NativeOptions::default();
    options.viewport.inner_size = Some(egui::vec2(1600.0, 1000.0));
    options.viewport.min_inner_size = Some(egui::vec2(800.0, 600.0));
    if let Some(icon) = modules::load_icon() {
        options.viewport.icon = Some(std::sync::Arc::new(icon));
    }
    eframe::run_native(
        "Tabular",
        options,
        Box::new(|_cc| Ok(Box::new(window_egui::Tabular::new()))),
    )
}

// ----------------- FFI (iOS) -----------------
// Basic exported C ABI helpers so Swift can query version and (optionally) launch.
use std::os::raw::c_char;

#[unsafe(no_mangle)]
pub extern "C" fn tabular_version() -> *const c_char {
    // Compile-time string; ends with NUL for C.
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

#[unsafe(no_mangle)]
pub extern "C" fn tabular_run() -> i32 {
    match run() {
        Ok(_) => 0,
        Err(_) => 1,
    }
}
