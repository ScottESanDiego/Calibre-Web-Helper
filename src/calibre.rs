use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Transaction, OptionalExtension};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use crate::models::{BookMetadata, ExistingBookData, UpdateChanges, UpsertResult};
use crate::utils::{now_utc_micro, format_timestamp_micro, find_or_create_by_name, find_or_create_by_name_and_sort, find_or_create_language, calculate_file_hash, validate_id, validate_table_name, validate_column_name, get_valid_filename, title_sort as compute_title_sort, get_sorted_author, set_metadata_dirty, detect_book_format};

/// Retrieves existing book metadata for comparison
fn get_existing_book_data(tx: &Connection, book_id: i64) -> Result<ExistingBookData> {
    // Get basic book data
    let (pubdate_str, series_index): (Option<String>, f64) = tx.query_row(
        "SELECT pubdate, series_index FROM books WHERE id = ?1",
        params![book_id],
        |row| Ok((row.get(0)?, row.get(1)?))
    )?;
    
    // Parse pubdate if it exists
    let pubdate = pubdate_str.and_then(|s| {
        // Try parsing with timezone first
        if let Ok(dt) = DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.6f%z") {
            Some(dt.with_timezone(&Utc))
        } else if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.6f") {
            // If that fails, try parsing as naive datetime and assume UTC
            Some(DateTime::from_naive_utc_and_offset(naive, Utc))
        } else {
            None
        }
    });
    
    // Get publisher name
    let publisher: Option<String> = tx.query_row(
        "SELECT p.name FROM publishers p 
         JOIN books_publishers_link bpl ON p.id = bpl.publisher 
         WHERE bpl.book = ?1",
        params![book_id],
        |row| row.get(0)
    ).optional()?;
    
    // Get series name
    let series: Option<String> = tx.query_row(
        "SELECT s.name FROM series s 
         JOIN books_series_link bsl ON s.id = bsl.series 
         WHERE bsl.book = ?1",
        params![book_id],
        |row| row.get(0)
    ).optional()?;
    
    Ok(ExistingBookData {
        pubdate,
        series_index,
        publisher,
        series,
    })
}

/// Get the file path of an existing book in the library
fn get_existing_book_file_path(library_dir: &Path, book_path: &str) -> Result<Option<PathBuf>> {
    let book_dir = library_dir.join(book_path);
    if !book_dir.exists() {
        return Ok(None);
    }
    
    // Look for EPUB or KEPUB files in the book directory
    for entry in fs::read_dir(&book_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let path_str = path.to_string_lossy().to_lowercase();
            if path_str.ends_with(".epub") || path_str.ends_with(".kepub") {
                return Ok(Some(path));
            }
        }
    }
    
    Ok(None)
}

/// Compares new metadata with existing book data to determine what needs updating
fn determine_changes(existing: &ExistingBookData, new_metadata: &BookMetadata) -> UpdateChanges {
    let mut changes = UpdateChanges::default();
    
    // Compare pubdate
    if existing.pubdate != new_metadata.pubdate {
        changes.pubdate_changed = true;
    }
    
    // Compare series_index
    let new_series_index = new_metadata.series_index.unwrap_or(1.0);
    if (existing.series_index - new_series_index).abs() > f64::EPSILON {
        changes.series_index_changed = true;
    }
    
    // Compare publisher
    if existing.publisher != new_metadata.publisher {
        changes.publisher_changed = true;
    }
    
    // Compare series
    if existing.series != new_metadata.series {
        changes.series_changed = true;
    }
    
    changes
}

/// Handles the database transaction for adding or updating a book.
/// If a book with the same title and author exists, it updates it. Otherwise, it creates a new one.
pub(crate) fn add_book_to_db(
    conn: &mut Connection, 
    metadata: &BookMetadata, 
    library_dir: &Path, 
    new_epub_file: &Path,
    dry_run: bool
) -> Result<UpsertResult> {
    if metadata.title.trim().is_empty() {
        anyhow::bail!("Book title cannot be empty");
    }
    if metadata.author.trim().is_empty() {
        anyhow::bail!("Book author cannot be empty");
    }
    if !new_epub_file.exists() {
        anyhow::bail!("EPUB file does not exist: {:?}", new_epub_file);
    }

    let tx = conn.transaction()
        .context("Failed to start database transaction")?;

    let author_sort_name = get_sorted_author(&metadata.author);
    let existing_book: Option<(i64, String)> = tx.query_row(
        "SELECT id, path FROM books WHERE title = ?1 AND author_sort = ?2",
        params![&metadata.title, &author_sort_name],
        |row| Ok((row.get(0)?, row.get(1)?))
    ).optional()?;

    let result = if let Some((book_id, book_path)) = existing_book {
        update_book(&tx, book_id, &book_path, metadata, library_dir, new_epub_file, dry_run)?
    } else {
        create_book(&tx, metadata, dry_run)?
    };

    tx.commit()
        .context("Failed to commit book transaction")?;

    Ok(result)
}

/// Updates an existing book's metadata when the EPUB file or metadata has changed.
fn update_book(
    tx: &Transaction,
    book_id: i64,
    book_path: &str,
    metadata: &BookMetadata,
    library_dir: &Path,
    new_epub_file: &Path,
    dry_run: bool,
) -> Result<UpsertResult> {
    println!(" -> Found existing book with ID: {}. Checking file hash...", book_id);

    let new_file_hash = calculate_file_hash(new_epub_file)?;

    if let Some(existing_file_path) = get_existing_book_file_path(library_dir, book_path)? {
        if let Ok(existing_file_hash) = calculate_file_hash(&existing_file_path) {
            if new_file_hash == existing_file_hash {
                println!(" -> Files are identical (same hash). No changes needed.");
                if dry_run {
                    println!("   [DRY RUN] Would skip all operations");
                }
                return Ok(UpsertResult::NoChanges { book_id, book_path: book_path.to_string() });
            } else if dry_run {
                println!(" -> Files differ (different hash). Would check metadata changes...");
            } else {
                println!(" -> Files differ (different hash). Checking metadata changes...");
            }
        } else {
            println!(" -> Could not hash existing file. Proceeding with metadata comparison...");
        }
    } else {
        println!(" -> Existing file not found. Proceeding with update...");
    }

    let existing_data = get_existing_book_data(tx, book_id)?;
    let changes = determine_changes(&existing_data, metadata);

    if !changes.has_any_changes() {
        if dry_run {
            println!(" -> No metadata changes detected. Would skip database update.");
            println!("   [DRY RUN] Would skip all operations");
        } else {
            println!(" -> No metadata changes detected. Skipping database update.");
        }
        return Ok(UpsertResult::NoChanges { book_id, book_path: book_path.to_string() });
    }

    if dry_run {
        println!(" -> Metadata changes detected. Would update database...");
        println!("   [DRY RUN] Would update: pubdate={}, series_index={}, publisher={}, series={}",
            changes.pubdate_changed, changes.series_index_changed,
            changes.publisher_changed, changes.series_changed);
        return Ok(UpsertResult::Updated { book_id, book_path: book_path.to_string() });
    }

    println!(" -> Metadata changes detected. Updating database...");
    let now_str = now_utc_micro();

    let mut set_clauses: Vec<String> = vec!["last_modified = ?".to_string()];
    let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(now_str)];

    if changes.pubdate_changed
        && let Some(pubdate) = metadata.pubdate {
            set_clauses.push("pubdate = ?".to_string());
            param_values.push(Box::new(format_timestamp_micro(&pubdate)));
        }
    if changes.series_index_changed {
        set_clauses.push("series_index = ?".to_string());
        param_values.push(Box::new(metadata.series_index.unwrap_or(1.0)));
    }

    param_values.push(Box::new(book_id));
    let sql = format!(
        "UPDATE books SET {} WHERE id = ?",
        set_clauses.join(", ")
    );
    let param_refs: Vec<&dyn rusqlite::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
    tx.execute(&sql, &param_refs[..])?;

    if changes.publisher_changed {
        tx.execute(
            "DELETE FROM books_publishers_link WHERE book = ?1",
            params![book_id],
        ).with_context(|| format!("Failed to delete old publisher link for book {}", book_id))?;

        if let Some(publisher_name) = &metadata.publisher {
            let publisher_id = find_or_create_by_name(tx, "publishers", publisher_name)
                .with_context(|| format!("Failed to find or create publisher '{}'", publisher_name))?;
            tx.execute(
                "INSERT INTO books_publishers_link (book, publisher) VALUES (?1, ?2)",
                params![book_id, publisher_id],
            ).with_context(|| format!(
                "Failed to link book {} to publisher {}",
                book_id, publisher_id
            ))?;
        }
    }

    if changes.series_changed {
        tx.execute(
            "DELETE FROM books_series_link WHERE book = ?1",
            params![book_id],
        ).with_context(|| format!("Failed to delete old series link for book {}", book_id))?;

        if let Some(series_name) = &metadata.series {
            let series_sort = compute_title_sort(series_name);
            let series_id = find_or_create_by_name_and_sort(tx, "series", series_name, &series_sort)
                .with_context(|| format!("Failed to find or create series '{}'", series_name))?;
            tx.execute(
                "INSERT INTO books_series_link (book, series) VALUES (?1, ?2)",
                params![book_id, series_id],
            ).with_context(|| format!(
                "Failed to link book {} to series {}",
                book_id, series_id
            ))?;
        }
    }

    set_metadata_dirty(tx, book_id)?;

    Ok(UpsertResult::Updated { book_id, book_path: book_path.to_string() })
}

/// Creates a brand new book record with all associated metadata.
fn create_book(
    tx: &Transaction,
    metadata: &BookMetadata,
    dry_run: bool,
) -> Result<UpsertResult> {
    if dry_run {
        println!(" -> Would create new book with title: '{}'", metadata.title);
        println!(" -> Would assign author: '{}'", metadata.author);
        if let Some(publisher) = &metadata.publisher {
            println!(" -> Would set publisher: '{}'", publisher);
        }
        if let Some(series) = &metadata.series {
            println!(" -> Would add to series: '{}'", series);
        }
        println!("   [DRY RUN] Would create new database entry and copy files");
        let dry_author = get_valid_filename(&metadata.author, 96);
        let dry_title = get_valid_filename(&metadata.title, 96);
        return Ok(UpsertResult::Created { book_id: 0, book_path: format!("{}/{} (NEW)", dry_author, dry_title) });
    }

    let author_sort_name = get_sorted_author(&metadata.author);
    let author_id = find_or_create_by_name_and_sort(tx, "authors", &metadata.author, &author_sort_name)
        .with_context(|| format!("Failed to find or create author '{}'", metadata.author))?;

    let now = Utc::now();
    let now_str = format_timestamp_micro(&now);
    let pubdate_str = format_timestamp_micro(&metadata.pubdate.unwrap_or(now));
    let book_uuid = Uuid::new_v4().to_string();
    let title_sort = compute_title_sort(&metadata.title);

    tx.execute(
        "INSERT INTO books (title, sort, author_sort, timestamp, pubdate, last_modified, path, series_index, uuid)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, '', ?7, ?8)",
        params![
            &metadata.title,
            &title_sort,
            &author_sort_name,
            &now_str,
            &pubdate_str,
            &now_str,
            metadata.series_index.unwrap_or(1.0),
            &book_uuid,
        ],
    ).with_context(|| format!("Failed to insert book '{}' into database", metadata.title))?;
    let book_id = tx.last_insert_rowid();

    let author_dir = get_valid_filename(&metadata.author, 96);
    let title_dir = get_valid_filename(&metadata.title, 96);
    let book_path = format!("{}/{} ({})", author_dir, title_dir, book_id);

    tx.execute(
        "UPDATE books SET path = ?1 WHERE id = ?2",
        params![&book_path, book_id],
    ).with_context(|| format!("Failed to update path for book {}", book_id))?;

    tx.execute(
        "INSERT INTO books_authors_link (book, author) VALUES (?1, ?2)",
        params![book_id, author_id],
    ).with_context(|| format!("Failed to link book {} to author {}", book_id, author_id))?;

    let (book_format, _extension) = detect_book_format(&metadata.path)?;
    let data_name = format!("{} - {}", get_valid_filename(&metadata.title, 42), get_valid_filename(&metadata.author, 42));
    tx.execute(
        "INSERT INTO data (book, format, uncompressed_size, name) VALUES (?1, ?2, ?3, ?4)",
        params![book_id, book_format, metadata.file_size as i64, data_name],
    )?;

    let mut comment_parts = Vec::new();
    if let Some(subtitle) = &metadata.subtitle {
        comment_parts.push(format!("<h3>{}</h3>", subtitle));
    }
    if let Some(description) = &metadata.description {
        comment_parts.push(description.to_string());
    }
    if let Some(rights) = &metadata.rights {
        comment_parts.push(format!("<p>Rights: {}</p>", rights));
    }

    if !comment_parts.is_empty() {
        let comment_text = comment_parts.join("\n");
        tx.execute(
            "INSERT INTO comments (book, text) VALUES (?1, ?2)",
            params![book_id, comment_text],
        )?;
    }
    if let Some(language) = &metadata.language {
        let lang_id = find_or_create_language(tx, language)?;
        tx.execute(
            "INSERT INTO books_languages_link (book, lang_code) VALUES (?1, ?2)",
            params![book_id, lang_id],
        )?;
    }
    if let Some(isbn) = &metadata.isbn {
        tx.execute(
            "INSERT INTO identifiers (book, type, val) VALUES (?1, 'ISBN', ?2)",
            params![book_id, isbn],
        )?;
    }

    if let Some(publisher_name) = &metadata.publisher {
        let publisher_id = find_or_create_by_name(tx, "publishers", publisher_name)?;
        tx.execute(
            "INSERT INTO books_publishers_link (book, publisher) VALUES (?1, ?2)",
            params![book_id, publisher_id],
        )?;
    }

    if let Some(series_name) = &metadata.series {
        let series_sort = compute_title_sort(series_name);
        let series_id = find_or_create_by_name_and_sort(tx, "series", series_name, &series_sort)?;
        tx.execute(
            "INSERT INTO books_series_link (book, series) VALUES (?1, ?2)",
            params![book_id, series_id],
        )?;

        if let Some(index) = metadata.series_index {
            tx.execute(
                "UPDATE books SET series_index = ?1 WHERE id = ?2",
                params![index, book_id],
            )?;
        }
    }

    set_metadata_dirty(tx, book_id)?;

    Ok(UpsertResult::Created { book_id, book_path })
}


/// Lists all books with their attributes.
pub(crate) fn list_books(
    conn: &Connection,
    appdb_conn: Option<&Connection>,
    shelf_name: Option<&str>,
    unshelved: bool,
    verbose: bool,
) -> Result<()> {
    let book_ids_on_shelf = if unshelved {
        // Find books NOT on any shelf
        let appdb = appdb_conn.context("app.db connection is required to find unshelved books")?;
        
        // First get all book IDs from metadata.db
        let mut all_books_stmt = conn.prepare("SELECT id FROM books")?;
        let all_book_ids: Vec<i64> = all_books_stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<i64>, _>>()?;
        
        // Then get book IDs that ARE on shelves from app.db
        let mut shelved_stmt = appdb.prepare("SELECT DISTINCT book_id FROM book_shelf_link")?;
        let shelved_ids: HashSet<i64> = shelved_stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<i64>, _>>()?
            .into_iter().collect();
        
        // Find books that are NOT on any shelf
        let unshelved_ids: Vec<i64> = all_book_ids.into_iter()
            .filter(|id| !shelved_ids.contains(id))
            .collect();

        if unshelved_ids.is_empty() {
            println!("No unshelved books found. All books are on at least one shelf.");
            return Ok(());
        }
        Some(unshelved_ids)
    } else if let Some(shelf) = shelf_name {
        let appdb = appdb_conn.context("app.db connection is required to filter by shelf")?;
        let mut stmt = appdb.prepare(
            "SELECT bsl.book_id FROM book_shelf_link bsl
             JOIN shelf s ON s.id = bsl.shelf
             WHERE s.name = ?1",
        )?;
        let ids_iter = stmt.query_map(params![shelf], |row| row.get(0))?;
        let ids = ids_iter.collect::<Result<Vec<i64>, _>>()?;

        if ids.is_empty() {
            println!("No books found on shelf '{}'.", shelf);
            return Ok(());
        }
        Some(ids)
    } else {
        None
    };

    let sql = if let Some(ids) = &book_ids_on_shelf {
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        format!(
            "SELECT * FROM books WHERE id IN ({}) ORDER BY title",
            placeholders
        )
    } else {
        "SELECT * FROM books ORDER BY title".to_string()
    };

    let mut stmt = conn.prepare(&sql)?;

    let params_vec: Vec<&dyn rusqlite::ToSql> = if let Some(ids) = &book_ids_on_shelf {
        ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect()
    } else {
        vec![]
    };

    let mut rows = stmt.query(&params_vec[..])?;

    if unshelved {
        println!("📚 Listing books not on any shelf...\n");
    } else if let Some(shelf) = shelf_name {
        println!("📚 Listing books on shelf '{}'...\n", shelf);
    } else {
        println!("📚 Listing all books in the library...\n");
    }

    let mut shelf_stmt = appdb_conn
        .map(|db| {
            db.prepare(
                "SELECT s.name, u.name as username 
                 FROM shelf s 
                 JOIN book_shelf_link bsl ON s.id = bsl.shelf 
                 LEFT JOIN user u ON s.user_id = u.id 
                 WHERE bsl.book_id = ?1",
            )
        })
        .transpose()?;

    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        println!("{}", "─".repeat(80));
        let id: i64 = row.get("id")?;
        println!("ID:          {}", id);
        println!("Title:       {}", row.get::<_, String>("title")?);

        let authors = get_linked_items(conn, "authors", "books_authors_link", "author", id)?;
        println!("Authors:     {}", authors.join(" & "));

        if let Some(stmt) = &mut shelf_stmt {
            let shelves_iter = stmt.query_map(params![id], |row| {
                Ok((
                    row.get::<_, String>("name")?,
                    row.get::<_, Option<String>>("username")?,
                ))
            })?;
            let shelves: Vec<(String, Option<String>)> = shelves_iter.collect::<Result<Vec<_>, _>>()?;
            if !shelves.is_empty() {
                println!("Shelves:");
                for (shelf_name, username) in shelves {
                    let user_display = username.unwrap_or_else(|| "admin".to_string());
                    println!("            - {} (User: {})", shelf_name, user_display);
                }
            }
        }

        let series = get_linked_items(conn, "series", "books_series_link", "series", id)?;
        if !series.is_empty() {
            println!("Series:      {} (#{})", series.join(", "), row.get::<_, f64>("series_index")?);
        }

        let tags = get_linked_items(conn, "tags", "books_tags_link", "tag", id)?;
        if !tags.is_empty() {
            println!("Tags:        {}", tags.join(", "));
        }

        let publisher =
            get_linked_items(conn, "publishers", "books_publishers_link", "publisher", id)?;
        if !publisher.is_empty() {
            println!("Publisher:   {}", publisher.join(", "));
        }

        println!("Published:   {}", row.get::<_, DateTime<Utc>>("pubdate")?.format("%Y-%m-%d"));
        println!("Path:        {}", row.get::<_, String>("path")?);

        if verbose {
            println!("Sort:        {}", row.get::<_, String>("sort")?);
            println!("Author Sort: {}", row.get::<_, String>("author_sort")?);
            println!("Timestamp:   {}", row.get::<_, DateTime<Utc>>("timestamp")?);
            println!("Last Mod:    {}", row.get::<_, DateTime<Utc>>("last_modified")?);
            println!("UUID:        {}", row.get::<_, String>("uuid")?);
            println!("Has Cover:   {}", row.get::<_, bool>("has_cover")?);

            if let Some(language) = get_book_language(conn, id)? {
                println!("Language:    {}", language);
            }

            let identifiers = get_book_identifiers(conn, id)?;
            if !identifiers.is_empty() {
                println!("Identifiers:");
                for (id_type, id_val) in identifiers {
                    println!("  {}: {}", id_type, id_val);
                }
            }
        }
    }
    
    if count > 0 {
        println!("{}", "─".repeat(80));
    }

    Ok(())
}


/// Deletes a book from the database and filesystem.
pub(crate) fn delete_book(calibre_conn: &mut Connection, appdb_conn: Option<&Connection>, library_db_path: &Path, book_id: i64) -> Result<()> {
    // Validate book ID
    validate_id(book_id, "book")?;
    
    // Create backup before destructive operation
    crate::utils::backup_database(library_db_path, "delete_book")
        .context("Failed to create database backup before deletion")?;
    
    let book_info: Option<(String, String)> = calibre_conn.query_row(
            "SELECT title, path FROM books WHERE id = ?1",
            params![book_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .with_context(|| format!("Failed to query book with ID {}", book_id))?;

    let book_path_str = if let Some((title, path)) = book_info.as_ref() {
        println!("You are about to delete:");
        println!("  ID:    {}", book_id);
        println!("  Title: {}", title);
        path.clone()
    } else {
        println!("Warning: Book with ID {} not found in Calibre database. Attempting to clean up Calibre-Web shelves and filesystem.", book_id);
        String::new()
    };

    // Delete from DB. Triggers will handle linked tables.
    let tx = calibre_conn.transaction()
        .context("Failed to start deletion transaction")?;
    let affected = tx.execute("DELETE FROM books WHERE id = ?1", params![book_id])
        .with_context(|| format!("Failed to delete book {} from database", book_id))?;
    tx.commit()
        .context("Failed to commit deletion transaction")?;

    if affected == 0 && book_info.is_some() {
         anyhow::bail!("No book found with ID {} to delete.", book_id);
    }
    
    // Also delete from Calibre-Web shelves if app.db is provided
    if let Some(conn) = appdb_conn {
        let mut stmt = conn.prepare("SELECT shelf FROM book_shelf_link WHERE book_id = ?1")?;
        let shelf_ids: Vec<i64> = stmt.query_map(params![book_id], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;

        conn.execute("DELETE FROM book_shelf_link WHERE book_id = ?1", params![book_id])?;
        println!(" -> Removed book from all Calibre-Web shelves.");

        for shelf_id in shelf_ids {
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM book_shelf_link WHERE shelf = ?1", params![shelf_id], |row| row.get(0))?;
            if count == 0 {
                let shelf_name: String = conn.query_row("SELECT name FROM shelf WHERE id = ?1", params![shelf_id], |row| row.get(0))?;
                conn.execute("DELETE FROM shelf WHERE id = ?1", params![shelf_id])?;
                println!(" -> Removed empty shelf '{}'.", shelf_name);
            }
        }
    }
    
    println!(" -> Successfully deleted database entry for book ID {}", book_id);

    // Delete cover image and directory from filesystem
    if !book_path_str.is_empty() {
        let book_dir = library_db_path.parent().unwrap_or_else(|| Path::new(".")).join(book_path_str);
        // Delete cover image if it exists
        let cover_path = book_dir.join("cover.jpg");
        if cover_path.exists() {
            fs::remove_file(&cover_path)
                .with_context(|| format!("Failed to remove cover image: {:?}", cover_path))?;
            println!(" -> Cover image deleted.");
        }
        if book_dir.exists() {
            fs::remove_dir_all(&book_dir)
                .with_context(|| format!("Failed to delete book directory: {:?}", book_dir))?;
            println!(" -> Successfully deleted book directory: {:?}", book_dir);

            // Check if the parent author directory is now empty
            if let Some(author_dir) = book_dir.parent()
                && let Ok(mut entries) = fs::read_dir(author_dir)
                    && entries.next().is_none()
                        && fs::remove_dir(author_dir).is_ok() {
                            println!(" -> Successfully deleted empty author directory: {:?}", author_dir);
                        }
        } else {
            println!(
                " -> Book directory not found, skipping filesystem delete: {:?}",
                book_dir
            );
        }
    }

    println!("\n✅ Success! Book ID {} has been deleted.", book_id);
    Ok(())
}

/// Helper function to get linked items like authors, tags, etc. for a book.
fn get_linked_items(
    conn: &Connection,
    item_table: &str,
    link_table: &str,
    item_column: &str,
    book_id: i64,
) -> Result<Vec<String>> {
    // Validate table and column names to prevent SQL injection
    validate_table_name(item_table)
        .with_context(|| format!("Invalid item table name: {}", item_table))?;
    validate_table_name(link_table)
        .with_context(|| format!("Invalid link table name: {}", link_table))?;
    validate_column_name(item_column)
        .with_context(|| format!("Invalid column name: {}", item_column))?;
    validate_id(book_id, "book")?;
    
    let query = format!(
        "SELECT t.name FROM {} t JOIN {} lt ON t.id = lt.{} WHERE lt.book = ?1",
        item_table, link_table, item_column
    );
    let mut stmt = conn.prepare(&query)
        .with_context(|| format!("Failed to prepare query for {}", item_table))?;
    let items_iter = stmt.query_map(params![book_id], |row| row.get(0))?;
    items_iter.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Helper function to get the language of a book.
fn get_book_language(conn: &Connection, book_id: i64) -> Result<Option<String>> {
    conn.query_row(
        "SELECT l.lang_code FROM languages l JOIN books_languages_link bll ON l.id = bll.lang_code WHERE bll.book = ?1",
        params![book_id],
        |row| row.get(0),
    ).optional().map_err(Into::into)
}

/// Helper function to get the identifiers of a book.
fn get_book_identifiers(conn: &Connection, book_id: i64) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT type, val FROM identifiers WHERE book = ?1",
    )?;
    let identifiers_iter = stmt.query_map(params![book_id], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    identifiers_iter.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}
