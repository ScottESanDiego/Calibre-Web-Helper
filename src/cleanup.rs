use anyhow::Result;
use rusqlite::{Connection, params};
use std::path::PathBuf;

/// Cleans up orphaned data in both Calibre and Calibre-Web databases
pub fn cleanup_databases(metadata_conn: &mut Connection, appdb_conn: Option<&mut Connection>, calibre_library_path: &PathBuf) -> Result<()> {
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
            if path.is_file() {
                if let Some(relative_path) = path.strip_prefix(calibre_library_path).ok() {
                    existing_files.insert(relative_path.to_path_buf());
                    // Store the immediate parent directory if it contains a book file
                    if let Some(parent) = relative_path.parent() {
                        if let Some(ext) = relative_path.extension() {
                            if ext != "jpg" && ext != "opf" {  // Ignore cover images and metadata files
                                book_paths.insert(parent.to_path_buf());
                            }
                        }
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
        let now = chrono::Local::now();
        let now_micro = now.format("%Y-%m-%d %H:%M:%S.%6f").to_string();
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
            "DELETE FROM kobo_bookmark WHERE kobo_reading_state_id IN (
                SELECT id FROM kobo_reading_state WHERE book_id NOT IN (?)
            )",
            params![valid_book_ids],
        )?;
        if deleted > 0 {
            println!(" -> Removed {} orphaned Kobo bookmark entries", deleted);
        }

        // Clean up Kobo statistics before reading state
        let deleted = tx.execute(
            "DELETE FROM kobo_statistics WHERE kobo_reading_state_id IN (
                SELECT id FROM kobo_reading_state WHERE book_id NOT IN (?)
            )",
            params![valid_book_ids],
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