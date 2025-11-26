#[cfg(feature = "floem_ui")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tabular::run()
}

#[cfg(feature = "egui_ui")]
fn main() -> Result<(), eframe::Error> {
    tabular::run()
}

#[cfg(not(any(feature = "floem_ui", feature = "egui_ui")))]
fn main() {
    eprintln!("Error: No UI feature enabled. Use --features floem_ui or --features egui_ui");
    std::process::exit(1);
}
