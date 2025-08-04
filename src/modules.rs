use eframe::egui;

use crate::modules;

pub(crate) fn url_encode(input: &str) -> String {
       input
       .replace("%", "%25")  // Must be first
       .replace("#", "%23")
       .replace("&", "%26")
       .replace("@", "%40")
       .replace("?", "%3F")
       .replace("=", "%3D")
       .replace("+", "%2B")
       .replace(" ", "%20")
       .replace(":", "%3A")
       .replace("/", "%2F")
}


pub(crate) fn load_icon() -> Option<egui::IconData> {
    let icon_bytes = include_bytes!("../assets/logo.png");
    
    match image::load_from_memory(icon_bytes) {
        Ok(image) => {
            let rgba = image.to_rgba8();
            let (width, height) = rgba.dimensions();
            Some(egui::IconData {
                rgba: rgba.into_raw(),
                width,
                height,
            })
        }
        Err(e) => {
            eprintln!("Failed to load icon: {}", e);
            None
        }
    }
}



pub(crate) fn get_app_data_dir() -> std::path::PathBuf {
    let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    home_dir.join(".tabular")
}

pub(crate) fn get_data_dir() -> std::path::PathBuf {
    modules::get_app_data_dir().join("data")
}

pub(crate) fn get_query_dir() -> std::path::PathBuf {
    modules::get_app_data_dir().join("query")
}

pub(crate) fn ensure_app_directories() -> Result<(), std::io::Error> {
    let app_dir = modules::get_app_data_dir();
    let data_dir = modules::get_data_dir();
    let query_dir = modules::get_query_dir();

    // Create directories if they don't exist
    std::fs::create_dir_all(&app_dir)?;
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&query_dir)?;
    
    Ok(())
}
