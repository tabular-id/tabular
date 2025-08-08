use eframe::egui;

use crate::window_egui;


fn load_logo_texture(tabular: &mut window_egui::Tabular, ctx: &egui::Context) {
       if tabular.logo_texture.is_none() {
       // Try to load the logo from assets/logo.png
       if let Ok(image_bytes) = std::fs::read("assets/logo.png") {
              if let Ok(image) = image::load_from_memory(&image_bytes) {
              let rgba_image = image.to_rgba8();
              let size = [image.width() as usize, image.height() as usize];
              let pixels = rgba_image.as_flat_samples();
              let color_image = egui::ColorImage::from_rgba_unmultiplied(size, pixels.as_slice());
              tabular.logo_texture = Some(ctx.load_texture("logo", color_image, Default::default()));
              }
       }
       }
}
pub(crate) fn render_about_dialog(tabular: &mut window_egui::Tabular, ctx: &egui::Context) {
        if tabular.show_about_dialog {
            // Load logo texture if not already loaded
            load_logo_texture(tabular, ctx);
            
            egui::Window::new("About Tabular")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .default_width(400.0)
                .open(&mut tabular.show_about_dialog)
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(10.0);
                        
                        // App icon/logo - use actual logo if loaded, fallback to emoji
                        if let Some(logo_texture) = &tabular.logo_texture {
                            ui.add(egui::Image::from_texture(logo_texture).max_size(egui::vec2(64.0, 64.0)));
                        } else {
                            ui.label(egui::RichText::new("📊").size(48.0));
                        }
                        ui.add_space(10.0);
                        
                        // App name and version
                        ui.label(egui::RichText::new("Tabular").size(24.0).strong());
                        ui.label(egui::RichText::new(format!("Version {}", env!("CARGO_PKG_VERSION"))).size(14.0).color(egui::Color32::GRAY));
                        ui.add_space(15.0);
                        
                        // Description
                        ui.label("Your SQL Editor, Forged with Rust: Fast, Safe, Efficient.");
                        ui.label("Jayuda");
                        ui.add_space(10.0);
                        
                       
                        ui.label(egui::RichText::new("© 2025 PT. Vneu Teknologi Indonesia ").size(12.0).color(egui::Color32::GRAY));
                        ui.hyperlink_to("https://github.com/tabular-id/tabular", "https://github.com/tabular-id/tabular");
                        ui.label(egui::RichText::new("Built with ❤️ using Rust").size(12.0).color(egui::Color32::GRAY));
                        ui.add_space(15.0);
                    });
                });
        }
    }
