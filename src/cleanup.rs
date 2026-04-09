use anyhow::Result;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use crate::utils::{now_utc_micro, get_valid_filename};

/// Cleans up orphaned data in both Calibre and Calibre-Web databases
pub(crate) fn cleanup_databases(metadata_conn: &mut Connection, appdb_conn: Option<&mut Connection>, calibre_library_path: &PathBuf) -> Result<()> {
    println!("🧹 Starting database cleanup...");
    
    // Get list of actual files in the Calibre library
    let mut existing_files = std::collections::HashSet::new();
    let mut book_paths = std::collections::HashSet::new();
    
    // Walk the library directory
    for entry in walkdir::WalkDir::new(calibre_library_path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_file()
                && let Ok(relative_path) = path.strip_prefix(calibre_library_path) {
                    existing_files.insert(relative_path.to_path_buf());
                    // Store the immediate parent directory if it contains a book file
                    if let Some(parent) = relative_path.parent()
                        && let Some(ext) = relative_path.extension() {
                            let ext_lower = ext.to_ascii_lowercase();
                            if ext_lower != "jpg" && ext_lower != "opf" {
                                book_paths.insert(parent.to_path_buf());
                            }
                        }
                }
    }

    // Start transaction for metadata DB cleanup
    let tx = metadata_conn.transaction()?;

    // Get all books and their paths from the database
    let mut stmt = tx.prepare("SELECT id, path FROM books")?;
    let book_iter = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut orphaned_books = Vec::new();
    for book_result in book_iter {
        let (book_id, db_path) = book_result?;
        let path = PathBuf::from(&db_path);
        
        // Check if the book's directory exists and contains files
        if !book_paths.contains(&path) {
            orphaned_books.push(book_id);
        }
    }

    // Clean up orphaned books and their related data
    if !orphaned_books.is_empty() {
        println!("\n📚 Cleaning up orphaned books...");
        for book_id in &orphaned_books {
            // Delete from related tables
            for table in &[
                "books_authors_link",
                "books_languages_link",
                "books_publishers_link",
                "books_ratings_link",
                "books_series_link",
                "books_tags_link",
                "comments",
                "data",
                "identifiers",
                "metadata_dirtied",
                "annotations_dirtied",
            ] {
                let query = format!("DELETE FROM {} WHERE book = ?1", table);
                tx.execute(&query, params![book_id])?;
            }
            
            // Delete the book itself
            tx.execute("DELETE FROM books WHERE id = ?1", params![book_id])?;
            println!(" -> Removed orphaned book (ID: {})", book_id);
        }
    }

    // Drop the statement before committing
    drop(stmt);

    // Clean up authors with no books
    let deleted = tx.execute(
        "DELETE FROM authors WHERE NOT EXISTS (SELECT 1 FROM books_authors_link WHERE author = authors.id)",
        [],
    )?;
    if deleted > 0 {
        println!(" -> Removed {} orphaned author entries", deleted);
    }

    // Clean up publishers with no books
    let deleted = tx.execute(
        "DELETE FROM publishers WHERE NOT EXISTS (SELECT 1 FROM books_publishers_link WHERE publisher = publishers.id)",
        [],
    )?;
    if deleted > 0 {
        println!(" -> Removed {} orphaned publisher entries", deleted);
    }

    // Clean up series with no books
    let deleted = tx.execute(
        "DELETE FROM series WHERE NOT EXISTS (SELECT 1 FROM books_series_link WHERE series = series.id)",
        [],
    )?;
    if deleted > 0 {
        println!(" -> Removed {} orphaned series entries", deleted);
    }

    // Clean up tags with no books
    let deleted = tx.execute(
        "DELETE FROM tags WHERE NOT EXISTS (SELECT 1 FROM books_tags_link WHERE tag = tags.id)",
        [],
    )?;
    if deleted > 0 {
        println!(" -> Removed {} orphaned tag entries", deleted);
    }

    // --- Integrity checks ---

    check_duplicate_books(&tx)?;
    check_missing_data_entries(&tx)?;
    check_data_name_mismatches(&tx, calibre_library_path)?;
    check_missing_covers(&tx, calibre_library_path)?;

    // Commit metadata DB changes
    tx.commit()?;

        // Clean up Calibre-Web database if provided
    if let Some(conn) = appdb_conn {
        println!("
🌐 Cleaning up Calibre-Web database...");
        let tx = conn.transaction()?;

        // Fix NULL datetime values that can cause TypeError
        // Update shelf records where created is NULL but last_modified exists
        let fixed = tx.execute(
            "UPDATE shelf SET created = last_modified WHERE created IS NULL AND last_modified IS NOT NULL",
            [],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelf records with missing created timestamp", fixed);
        }

        // Fix NULL last_modified values in shelf records
        let fixed = tx.execute(
            "UPDATE shelf SET last_modified = created WHERE last_modified IS NULL AND created IS NOT NULL",
            [],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelf records with missing last_modified timestamp", fixed);
        }

        // Set both timestamps to current time if both are NULL
        let now_micro = now_utc_micro();
        let fixed = tx.execute(
            "UPDATE shelf SET created = ?, last_modified = ? WHERE created IS NULL AND last_modified IS NULL",
            params![now_micro, now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelf records with no timestamps", fixed);
        }

        // Fix NULL timestamps in book_shelf_link
        let fixed = tx.execute(
            "UPDATE book_shelf_link SET date_added = ? WHERE date_added IS NULL",
            params![now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} book shelf links with missing timestamp", fixed);
        }

        // Get valid book IDs from Calibre database
        let mut valid_books = std::collections::HashSet::new();
        {
            let mut books_query = metadata_conn.prepare("SELECT id FROM books")?;
            let book_iter = books_query.query_map([], |row| {
                row.get::<_, i64>(0)
            })?;

            for book_id in book_iter {
                valid_books.insert(book_id?);
            }
        }

        // Build the valid book IDs list for SQLite IN clause
        let valid_book_ids: String = valid_books.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // If there are no valid books, use a dummy value to prevent SQL syntax error
        let valid_book_ids = if valid_book_ids.is_empty() {
            "-1".to_string()
        } else {
            valid_book_ids
        };

        // First level: Clean up leaf tables that don't have dependencies
        
        // Clean up downloads
        let deleted = tx.execute(
            &format!("DELETE FROM downloads WHERE book_id NOT IN ({})", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned download entries", deleted);
        }

        // Clean up archived books
        let deleted = tx.execute(
            &format!("DELETE FROM archived_book WHERE book_id NOT IN ({})", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned archived book entries", deleted);
        }

        // Clean up Kobo bookmarks before reading state
        let deleted = tx.execute(
            &format!("DELETE FROM kobo_bookmark WHERE kobo_reading_state_id IN (
                SELECT id FROM kobo_reading_state WHERE book_id NOT IN ({})
            )", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned Kobo bookmark entries", deleted);
        }

        // Clean up Kobo statistics before reading state
        let deleted = tx.execute(
            &format!("DELETE FROM kobo_statistics WHERE kobo_reading_state_id IN (
                SELECT id FROM kobo_reading_state WHERE book_id NOT IN ({})
            )", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned Kobo statistics entries", deleted);
        }

        // Clean up Kobo reading state after its dependents
        let deleted = tx.execute(
            &format!("DELETE FROM kobo_reading_state WHERE book_id NOT IN ({})", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned Kobo reading state entries", deleted);
        }

        // Clean up Kobo synced books
        let deleted = tx.execute(
            &format!("DELETE FROM kobo_synced_books WHERE book_id NOT IN ({})", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned Kobo sync entries", deleted);
        }

        // Finally book shelf links and empty shelves
        let deleted = tx.execute(
            &format!("DELETE FROM book_shelf_link WHERE book_id NOT IN ({})", valid_book_ids),
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned shelf links", deleted);
        }

        // Clean up empty shelves last
        let deleted = tx.execute(
            "DELETE FROM shelf WHERE NOT EXISTS (SELECT 1 FROM book_shelf_link WHERE shelf = shelf.id)",
            [],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} empty shelves", deleted);
        }

        // Commit app DB changes
        tx.commit()?;
    }

    println!("\n✨ Database cleanup complete!");
    Ok(())
}

/// Reports duplicate books (same title + author_sort) with different IDs.
fn check_duplicate_books(tx: &rusqlite::Transaction) -> Result<()> {
    println!("\n🔍 Checking for duplicate books...");

    let mut stmt = tx.prepare(
        "SELECT title, author_sort, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
         FROM books
         GROUP BY title, author_sort
         HAVING cnt > 1
         ORDER BY title"
    )?;

    let dupes: Vec<(String, String, String, i64)> = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?.collect::<Result<Vec<_>, _>>()?;

    if dupes.is_empty() {
        println!(" -> No duplicate books found.");
    } else {
        println!(" ⚠️  Found {} sets of duplicate books:", dupes.len());
        for (title, author_sort, ids, count) in &dupes {
            println!("    '{}' by {} — {} copies (IDs: {})", title, author_sort, count, ids);
        }
        println!("    These are not automatically removed; review and delete manually with the 'delete' command.");
    }

    Ok(())
}

/// Reports books that have no entry in the `data` table (no format/file record).
fn check_missing_data_entries(tx: &rusqlite::Transaction) -> Result<()> {
    println!("\n🔍 Checking for books with missing format data...");

    let mut stmt = tx.prepare(
        "SELECT b.id, b.title, b.author_sort, b.path
         FROM books b
         LEFT JOIN data d ON b.id = d.book
         WHERE d.id IS NULL
         ORDER BY b.title"
    )?;

    let missing: Vec<(i64, String, String, String)> = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?.collect::<Result<Vec<_>, _>>()?;

    if missing.is_empty() {
        println!(" -> All books have format data entries.");
    } else {
        println!(" ⚠️  Found {} book(s) with no format data:", missing.len());
        for (id, title, author, path) in &missing {
            println!("    ID {} — '{}' by {} (path: {})", id, title, author, path);
        }
        println!("    These books exist in the database but have no associated file format.");
        println!("    Consider deleting them with the 'delete' command or re-adding the EPUB.");
    }

    Ok(())
}

/// Reports mismatches between `data.name` and the actual filename on disk.
fn check_data_name_mismatches(tx: &rusqlite::Transaction, library_dir: &Path) -> Result<()> {
    println!("\n🔍 Checking for data.name vs filename mismatches...");

    let mut stmt = tx.prepare(
        "SELECT d.id, d.book, d.name, d.format, b.path, b.title, b.author_sort
         FROM data d
         JOIN books b ON d.book = b.id
         ORDER BY b.title"
    )?;

    let rows: Vec<(i64, i64, String, String, String, String, String)> = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?.collect::<Result<Vec<_>, _>>()?;

    let mut mismatch_count = 0;
    let mut missing_file_count = 0;

    for (data_id, book_id, data_name, format, book_path, title, author) in &rows {
        let extension = match format.as_str() {
            "KEPUB" => "kepub",
            "EPUB" => "epub",
            _ => &format.to_lowercase(),
        };
        let expected_filename = format!("{}.{}", data_name, extension);
        let book_dir = library_dir.join(book_path);
        let expected_path = book_dir.join(&expected_filename);

        if !book_dir.exists() {
            continue; // already handled by orphan check
        }

        if !expected_path.exists() {
            // The expected file doesn't exist — look for what's actually there
            let actual_files: Vec<String> = std::fs::read_dir(&book_dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let p = e.path();
                    p.is_file() && {
                        let name = p.to_string_lossy().to_lowercase();
                        name.ends_with(".epub") || name.ends_with(".kepub")
                    }
                })
                .filter_map(|e| e.file_name().into_string().ok())
                .collect();

            if actual_files.is_empty() {
                missing_file_count += 1;
                println!("    ⚠️  ID {} — '{}' by {}: no book file found in {}", book_id, title, author, book_path);
            } else {
                mismatch_count += 1;
                println!("    ⚠️  ID {} — '{}' by {} (data.id {}):", book_id, title, author, data_id);
                println!("       Expected: {}", expected_filename);
                println!("       Found:    {}", actual_files.join(", "));

                // Auto-fix: update data.name to match the actual file on disk
                if actual_files.len() == 1 {
                    let actual = &actual_files[0];
                    // Strip the extension(s) to get the stem
                    let stem = actual
                        .strip_suffix(".kepub.epub")
                        .or_else(|| actual.strip_suffix(".epub"))
                        .or_else(|| actual.strip_suffix(".kepub"))
                        .unwrap_or(actual);
                    tx.execute("UPDATE data SET name = ?1 WHERE id = ?2", params![stem, data_id])?;
                    println!("       ✅ Fixed: updated data.name to '{}'", stem);
                }
            }
        } else {
            // File exists — also verify data.name matches Calibre-Web naming convention
            let expected_name = format!("{} - {}",
                get_valid_filename(title, 42),
                get_valid_filename(author, 42));
            if *data_name != expected_name {
                // Only report if the file itself also doesn't match (avoid noise for legacy names)
                let convention_path = book_dir.join(format!("{}.{}", expected_name, extension));
                if !convention_path.exists() && expected_path.exists() {
                    // data.name matches the file but not the convention — just informational
                }
            }
        }
    }

    if mismatch_count == 0 && missing_file_count == 0 {
        println!(" -> All data.name entries match their files on disk.");
    } else {
        if mismatch_count > 0 {
            println!(" -> Fixed {} filename mismatch(es).", mismatch_count);
        }
        if missing_file_count > 0 {
            println!(" -> {} book(s) have a data record but no file on disk.", missing_file_count);
        }
    }

    Ok(())
}

/// Reports books where has_cover=1 but cover.jpg is missing, and fixes the flag.
fn check_missing_covers(tx: &rusqlite::Transaction, library_dir: &Path) -> Result<()> {
    println!("\n🔍 Checking for missing cover images...");

    let mut stmt = tx.prepare(
        "SELECT id, title, author_sort, path FROM books WHERE has_cover = 1 ORDER BY title"
    )?;

    let books: Vec<(i64, String, String, String)> = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?.collect::<Result<Vec<_>, _>>()?;

    let mut missing_count = 0;
    for (book_id, title, author, book_path) in &books {
        let cover_path = library_dir.join(book_path).join("cover.jpg");
        if !cover_path.exists() {
            missing_count += 1;
            println!("    ⚠️  ID {} — '{}' by {}: has_cover=1 but cover.jpg missing", book_id, title, author);
            tx.execute("UPDATE books SET has_cover = 0 WHERE id = ?1", params![book_id])?;
        }
    }

    if missing_count == 0 {
        println!(" -> All books with has_cover=1 have their cover.jpg file.");
    } else {
        println!(" -> Fixed {} book(s): set has_cover=0 where cover.jpg was missing.", missing_count);
    }

    // Also check the reverse: has_cover=0 but cover.jpg exists
    let mut stmt2 = tx.prepare(
        "SELECT id, title, author_sort, path FROM books WHERE has_cover = 0 ORDER BY title"
    )?;

    let books_no_cover: Vec<(i64, String, String, String)> = stmt2.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?.collect::<Result<Vec<_>, _>>()?;

    let mut found_count = 0;
    for (book_id, title, author, book_path) in &books_no_cover {
        let cover_path = library_dir.join(book_path).join("cover.jpg");
        if cover_path.exists() {
            found_count += 1;
            println!("    ✅ ID {} — '{}' by {}: has_cover=0 but cover.jpg exists, fixing", book_id, title, author);
            tx.execute("UPDATE books SET has_cover = 1 WHERE id = ?1", params![book_id])?;
        }
    }

    if found_count > 0 {
        println!(" -> Fixed {} book(s): set has_cover=1 where cover.jpg was found.", found_count);
    }

    Ok(())
}