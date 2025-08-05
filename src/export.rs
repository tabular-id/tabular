use std::path::Path;

pub fn export_to_csv(
    all_table_data: &[Vec<String>],
    current_table_headers: &[String],
    current_table_name: &str,
) {
    println!("=== CSV EXPORT DEBUG INFO ===");
    println!("Table name: {}", current_table_name);
    println!("Headers count: {}", current_table_headers.len());
    println!("Headers: {:?}", current_table_headers);
    println!("Data rows: {}", all_table_data.len());
    if !all_table_data.is_empty() {
        println!("First row sample: {:?}", &all_table_data[0]);
    }
    println!("============================");
    
    if all_table_data.is_empty() || current_table_headers.is_empty() {
        eprintln!("No data to export - empty data: {}, empty headers: {}", 
                 all_table_data.is_empty(), current_table_headers.is_empty());
        return;
    }

    // Use rfd to open save dialog
    let file_dialog = rfd::FileDialog::new()
        .add_filter("CSV files", &["csv"])
        .set_file_name(format!("{}.csv", current_table_name.replace(' ', "_")));

    println!("Opening CSV file dialog...");
    
    if let Some(path) = file_dialog.save_file() {
        println!("CSV file path selected: {:?}", path);
        println!("About to write CSV with {} rows and {} headers", all_table_data.len(), current_table_headers.len());
        
        match write_csv_file(&path, all_table_data, current_table_headers) {
            Ok(_) => println!("✓ Successfully exported {} rows to CSV: {:?}", all_table_data.len(), path),
            Err(e) => eprintln!("❌ Failed to export CSV: {}", e),
        }
    } else {
        println!("CSV file dialog was cancelled");
    }
}

fn write_csv_file(
    path: &Path,
    all_table_data: &[Vec<String>],
    current_table_headers: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WRITE_CSV_FILE DEBUG ===");
    println!("Path: {:?}", path);
    println!("Headers to write: {:?}", current_table_headers);
    println!("Data rows to write: {}", all_table_data.len());
    
    let mut writer = csv::Writer::from_path(path)?;
    println!("CSV writer created successfully");
    
    // Write headers
    writer.write_record(current_table_headers)?;
    println!("Headers written successfully");
    
    // Write data rows
    for (idx, row) in all_table_data.iter().enumerate() {
        writer.write_record(row)?;
        if idx < 3 {  // Log first 3 rows
            println!("Row {} written: {:?}", idx, row);
        }
    }
    println!("All {} data rows written successfully", all_table_data.len());
    
    writer.flush()?;
    println!("CSV file flushed and closed successfully");
    println!("===========================");
    
    Ok(())
}

pub fn export_to_xlsx(
    all_table_data: &[Vec<String>],
    current_table_headers: &[String],
    current_table_name: &str,
) {
    println!("=== EXPORT DEBUG INFO ===");
    println!("Table name: {}", current_table_name);
    println!("Headers count: {}", current_table_headers.len());
    println!("Headers: {:?}", current_table_headers);
    println!("Data rows: {}", all_table_data.len());
    if !all_table_data.is_empty() {
        println!("First row sample: {:?}", &all_table_data[0]);
    }
    println!("========================");
    
    println!("Exporting {} rows to XLSX", all_table_data.len());
    if all_table_data.is_empty() || current_table_headers.is_empty() {
        eprintln!("No data to export - empty data: {}, empty headers: {}", 
                 all_table_data.is_empty(), current_table_headers.is_empty());
        return;
    }

    // Use rfd to open save dialog
    let file_dialog = rfd::FileDialog::new()
        .add_filter("Excel files", &["xlsx"])
        .set_file_name(format!("{}.xlsx", current_table_name.replace(' ', "_")));

    if let Some(path) = file_dialog.save_file() {
        match write_xlsx_file(&path, all_table_data, current_table_headers) {
            Ok(_) => println!("✓ Successfully exported {} rows to XLSX: {:?}", all_table_data.len(), path),
            Err(e) => eprintln!("❌ Failed to export XLSX: {}", e),
        }
    }
}

fn write_xlsx_file(
    path: &Path,
    all_table_data: &[Vec<String>],
    current_table_headers: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let workbook = xlsxwriter::Workbook::new(path.to_str().unwrap())?;
    let mut worksheet = workbook.add_worksheet(Some("Data"))?;

    // Create header format (bold)
    let mut header_format = xlsxwriter::Format::new();
    header_format.set_bold();

    // Write headers
    for (col, header) in current_table_headers.iter().enumerate() {
        worksheet.write_string(0, col as u16, header, Some(&header_format))?;
    }

    // Write data rows
    for (row_idx, row) in all_table_data.iter().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            // Try to parse as number first, otherwise write as string
            if let Ok(number) = cell.parse::<f64>() {
                worksheet.write_number((row_idx + 1) as u32, col_idx as u16, number, None)?;
            } else {
                worksheet.write_string((row_idx + 1) as u32, col_idx as u16, cell, None)?;
            }
        }
    }

    // Auto-fit columns
    for col in 0..current_table_headers.len() {
        worksheet.set_column(col as u16, col as u16, 15.0, None)?;
    }

    workbook.close()?;
    Ok(())
}
