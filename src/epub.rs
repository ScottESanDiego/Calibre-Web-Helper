use anyhow::{Context, Result};
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

    let language = doc.mdata("language");

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

    Ok(BookMetadata {
        title,
        author,
        description,
        language,
        isbn,
        rights,
        subtitle,
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
