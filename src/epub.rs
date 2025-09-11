use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};

use crate::calibre::BookMetadata;

/// Extracts full metadata from the EPUB file.
pub fn get_epub_metadata(path: &Path) -> Result<BookMetadata> {
    let doc = epub::doc::EpubDoc::new(path)?;
    let title = doc
        .mdata("title")
        .context("EPUB has no title metadata")?;
    let author = doc
        .mdata("creator")
        .context("EPUB has no author (creator) metadata")?;
    let description = doc.mdata("description");
    let rights = doc.mdata("rights");
    let subtitle = doc.mdata("subtitle");

    // Handle language codes with proper normalization
    let language = doc.mdata("language").map(|lang| {
        let lang = lang.trim().to_lowercase();
        
        // Helper closure to normalize language codes
        let normalize_language = |code: &str| -> String {
            match code {
                // Common ISO 639-1 to ISO 639-2 mappings
                "en" => "eng".to_string(),
                "fr" => "fre".to_string(),
                "es" => "spa".to_string(),
                "de" => "ger".to_string(),
                "it" => "ita".to_string(),
                "ja" => "jpn".to_string(),
                "zh" => "chi".to_string(),
                "ru" => "rus".to_string(),
                "ar" => "ara".to_string(),
                "hi" => "hin".to_string(),
                "pt" => "por".to_string(),
                "nl" => "dut".to_string(),
                "pl" => "pol".to_string(),
                "ko" => "kor".to_string(),
                // Add more mappings as needed
                _ => code.to_string(),
            }
        };

        // Split on hyphens to handle extended tags (e.g., "en-US" -> "en")
        let base_lang = lang.split(['-', '_']).next().unwrap_or(&lang);

        // Normalize the language code
        let normalized = if base_lang.len() == 2 {
            normalize_language(base_lang)
        } else if base_lang.len() == 3 {
            // Assume it's already ISO 639-2
            base_lang.to_string()
        } else {
            // Unknown format, keep as is
            base_lang.to_string()
        };

        // Verify it's a known ISO 639-2 code and convert unknown codes to "und"
        match normalized.as_str() {
            "eng" | "fre" | "ger" | "spa" | "ita" | "jpn" | "chi" | "rus" | "ara" |
            "hin" | "por" | "ben" | "urd" | "dut" | "tur" | "vie" | "tel" | "mar" |
            "tam" | "kor" | "fra" | "deu" | "nld" | "fas" | "tha" | "pol" | "ukr" |
            "ron" | "mal" | "hun" | "ces" | "gre" | "swe" | "bul" | "dan" | "fin" |
            "nor" | "slo" | "cat" | "hrv" | "heb" | "lit" | "slv" | "est" |
            "lav" | "fil" | "per" | "rum" | "cze" | "ell" | "srp" | "bel" | "kan" |
            "alb" | "afr" | "swa" | "glg" | "mkd" | "gle" | "arm" | "lat" | "wel" |
            "baq" | "geo" | "aze" | "kat" | "hye" | "eus" | "zho" | "yue" | "cmn" => normalized,
            _ => "und".to_string()
        }
    });

    let isbn = doc.metadata.get("identifier").and_then(|identifiers| {
        identifiers.iter().find_map(|id| {
            let id = id.trim();
            if id.starts_with("urn:isbn:") {
                return Some(id.trim_start_matches("urn:isbn:").to_string());
            }
            let digits: String = id.chars().filter(|c| c.is_digit(10)).collect();
            if digits.len() == 10 || digits.len() == 13 {
                return Some(digits);
            }
            None
        })
    });

    // Get publisher
    let publisher = doc.mdata("publisher");

    // Get publication date
    let pubdate = doc.mdata("date")
        .and_then(|date_str| {
            // Try various date formats
            let date_str = date_str.trim();
            
            // Try ISO8601/RFC3339 with time (YYYY-MM-DDThh:mm:ssZ)
            if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
                return Some(dt.with_timezone(&Utc));
            }
            
            // Try ISO format (YYYY-MM-DD)
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    dt.and_hms_opt(0, 0, 0).unwrap(),
                    Utc,
                ));
            }
            
            // Try format with month name (DD MMMM YYYY)
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%d %B %Y")
                .or_else(|_| chrono::NaiveDate::parse_from_str(date_str, "%d %b %Y")) {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    dt.and_hms_opt(0, 0, 0).unwrap(),
                    Utc,
                ));
            }
            
            // Try year-month format (YYYY-MM)
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(&format!("{}-01", date_str), "%Y-%m-%d") {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    dt.and_hms_opt(0, 0, 0).unwrap(),
                    Utc,
                ));
            }
            
            // Try year only
            if let Ok(year) = date_str.parse::<i32>() {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(
                    chrono::NaiveDate::from_ymd_opt(year, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                    Utc,
                ));
            }
            
            None
        });

    // Extract series information from metadata
    // Look for calibre:series and calibre:series_index first
    let series = doc.mdata("calibre:series")
        .or_else(|| {
            // Fallback to looking for series information in the title
            // Common format: Series Name #X - Book Title
            let title = title.trim();
            if let Some(hash_idx) = title.find('#') {
                if let Some(_dash_idx) = title[hash_idx..].find('-') {
                    // Extract everything before the # as the series name
                    let series_part = title[..hash_idx].trim();
                    if !series_part.is_empty() {
                        Some(series_part.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        });

    let series_index = doc.mdata("calibre:series_index")
        .and_then(|idx| idx.parse::<f64>().ok())
        .or_else(|| {
            // Try to extract series index from title if in #X format
            title.find('#')
                .and_then(|i| {
                    let rest = &title[i + 1..];
                    let num_str: String = rest.chars()
                        .take_while(|c| c.is_digit(10) || *c == '.')
                        .collect();
                    num_str.parse::<f64>().ok()
                })
        });

    // Get the original filename
    let original_filename = path.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .context("Could not get filename from EPUB path")?;

    // Get the file size
    let file_size = std::fs::metadata(path)
        .with_context(|| format!("Failed to get file size for {:?}", path))?
        .len();

    Ok(BookMetadata {
        title,
        author,
        description,
        language,
        isbn,
        rights,
        subtitle,
        series,
        series_index,
        publisher,
        pubdate,
        original_filename,
        file_size,
    })
}

/// Copies or updates the EPUB file in the Calibre library structure.
/// If updating, it first clears the destination directory of old files.
/// Returns true if a cover was saved.
pub fn update_book_files(library_dir: &Path, epub_file: &Path, book_path: &str, is_update: bool) -> Result<bool> {
    let dest_dir = library_dir.join(book_path);
    let mut cover_saved = false;

    if is_update && dest_dir.exists() {
        println!(" -> Removing old book file(s)...");
        for entry in fs::read_dir(&dest_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                fs::remove_file(&path)
                    .with_context(|| format!("Failed to remove old file: {:?}", path))?;
            }
        }
    }

    fs::create_dir_all(&dest_dir)
        .with_context(|| format!("Failed to create directory: {:?}", dest_dir))?;

    let epub_filename = epub_file
        .file_name()
        .context("Could not get filename from EPUB path")?;
    let dest_file = dest_dir.join(epub_filename);
    fs::copy(epub_file, &dest_file)
        .with_context(|| format!("Failed to copy EPUB to {:?}", dest_file))?;

    // Handle cover image: extract from EPUB if present, else fallback to external cover.jpg
    let cover_dest = dest_dir.join("cover.jpg");
    if let Ok(mut doc) = epub::doc::EpubDoc::new(epub_file) {
        match doc.get_cover() {
            Some((cover_data, _mime)) => {
                std::fs::write(&cover_dest, &cover_data)
                    .with_context(|| format!("Failed to write cover image to {:?}", cover_dest))?;
                println!(" -> Cover image extracted from EPUB and saved.");
                cover_saved = true;
            }
            None => {
                // Fallback: copy external cover.jpg if it exists
                let cover_src = epub_file.parent().map(|p| p.join("cover.jpg")).unwrap_or_else(|| PathBuf::from("cover.jpg"));
                if cover_src.exists() {
                    fs::copy(&cover_src, &cover_dest)
                        .with_context(|| format!("Failed to copy cover image to {:?}", cover_dest))?;
                    println!(" -> Cover image copied from external file.");
                    cover_saved = true;
                }
            }
        }
    } else {
        println!("Warning: Could not open EPUB for cover extraction.");
    }

    Ok(cover_saved)
}