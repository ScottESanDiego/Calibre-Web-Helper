use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{functions::FunctionFlags, params, Connection, OptionalExtension};
use std::path::Path;
use uuid::Uuid;
use crate::utils::{now_utc_micro, format_timestamp_micro, find_or_create_by_name, find_or_create_by_name_and_sort, find_or_create_language};

pub struct BookMetadata {
    pub title: String,
    pub author: String,
    pub path: std::path::PathBuf,
    pub description: Option<String>,
    pub language: Option<String>,
    pub isbn: Option<String>,
    pub rights: Option<String>,
    pub subtitle: Option<String>,
    pub series: Option<String>,
    pub series_index: Option<f64>,
    pub publisher: Option<String>,
    pub pubdate: Option<DateTime<Utc>>,
    pub file_size: u64,
}

pub enum UpsertResult {
    Created { book_id: i64, book_path: String },
    Updated { book_id: i64, book_path: String },
}

/// Handles the entire database transaction for adding a new book.
/// If a book with the same title and author exists, it updates it. Otherwise, it creates a new one.
pub fn add_book_to_db(conn: &mut Connection, metadata: &BookMetadata) -> Result<UpsertResult> {
    let tx = conn.transaction()?;

    // Check for an existing book by title and author sort key.
    let author_sort_name = get_author_sort(&metadata.author);
    let existing_book: Option<(i64, String)> = tx.query_row(
        "SELECT id, path FROM books WHERE title = ?1 AND author_sort = ?2",
        params![&metadata.title, &author_sort_name],
        |row| Ok((row.get(0)?, row.get(1)?))
    ).optional()?;

    if let Some((book_id, book_path)) = existing_book {
        // UPDATE PATH
        println!(" -> Found existing book with ID: {}. Updating.", book_id);
        let now_str = now_utc_micro();
        
        // Get the pubdate string
        let pubdate_str = metadata.pubdate.map(|dt| 
            format_timestamp_micro(&dt)
        );
        
        // Update the timestamps and pubdate if provided
        if let Some(pdate) = pubdate_str {
            tx.execute(
                "UPDATE books SET last_modified = ?1, pubdate = ?2 WHERE id = ?3",
                params![&now_str, &pdate, book_id],
            )?;
        } else {
            tx.execute(
                "UPDATE books SET last_modified = ?1 WHERE id = ?2",
                params![&now_str, book_id],
            )?;
        }

        // Update publisher information
        tx.execute(
            "DELETE FROM books_publishers_link WHERE book = ?1",
            params![book_id],
        )?;

        if let Some(publisher_name) = &metadata.publisher {
            // Get or create publisher entry
            let publisher_id = find_or_create_by_name(&tx, "publishers", publisher_name)?;

            // Link book to publisher
            tx.execute(
                "INSERT INTO books_publishers_link (book, publisher) VALUES (?1, ?2)",
                params![book_id, publisher_id],
            )?;
        }

        // Update series information
        if let Some(series_name) = &metadata.series {
            // Get or create series entry
            let series_id = find_or_create_by_name_and_sort(&tx, "series", series_name, series_name)?;

            // Remove any existing series links
            tx.execute(
                "DELETE FROM books_series_link WHERE book = ?1",
                params![book_id],
            )?;

            // Link book to series
            tx.execute(
                "INSERT INTO books_series_link (book, series) VALUES (?1, ?2)",
                params![book_id, series_id],
            )?;

            // Set series index in books table
            if let Some(index) = metadata.series_index {
                tx.execute(
                    "UPDATE books SET series_index = ?1 WHERE id = ?2",
                    params![index, book_id],
                )?;
            }
        } else {
            // If no series info provided, remove any existing series information
            tx.execute(
                "DELETE FROM books_series_link WHERE book = ?1",
                params![book_id],
            )?;
            tx.execute(
                "UPDATE books SET series_index = 1.0 WHERE id = ?1",
                params![book_id],
            )?;
        }

        tx.commit()?;
        return Ok(UpsertResult::Updated { book_id, book_path });
    }

    // CREATE PATH
    let author_sort_name = get_author_sort(&metadata.author);
    let author_id = find_or_create_by_name_and_sort(&tx, "authors", &metadata.author, &author_sort_name)?;

    // 2. Insert the book record (with a temporary path)
    // Calibre expects timestamps with microsecond precision.
    let now = Utc::now();
    let now_str = format_timestamp_micro(&now);
    let pubdate_str = format_timestamp_micro(&metadata.pubdate.unwrap_or(now));
        
    tx.execute(
        "INSERT INTO books (title, sort, author_sort, timestamp, pubdate, last_modified, path, series_index)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, '', 1.0)",
        params![
            &metadata.title,
            &metadata.title, // Using title for sort key for simplicity
            &author_sort_name,
            &now_str,
            &pubdate_str,
            &now_str,
        ],
    )?;
    let book_id = tx.last_insert_rowid();

    // 3. Construct the final path and update the book record with it
    let book_path = format!("{}/{} ({})", metadata.author, metadata.title, book_id);

    tx.execute(
        "UPDATE books SET path = ?1 WHERE id = ?2",
        params![&book_path, book_id],
    )?;

    // 4. Link the book and author
    tx.execute(
        "INSERT INTO books_authors_link (book, author) VALUES (?1, ?2)",
        params![book_id, author_id],
    )?;

    // 5. Add the file format information to the 'data' table
    // Determine format based on filename
    let path_str = metadata.path.to_string_lossy();
    let (format, filename) = if path_str.ends_with(".kepub.epub") || path_str.ends_with(".kepub") {
        ("KEPUB", format!("{} - {}", metadata.title, metadata.author))
    } else if path_str.ends_with(".epub") {
        ("EPUB", format!("{} - {}", metadata.title, metadata.author))
    } else {
        anyhow::bail!("Unsupported file extension. File must end in .epub, .kepub, or .kepub.epub")
    };    tx.execute(
        "INSERT INTO data (book, format, uncompressed_size, name) VALUES (?1, ?2, ?3, ?4)",
        params![book_id, format, metadata.file_size, filename],
    )?;

    // 6. Add other metadata
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
        let lang_id = find_or_create_language(&tx, language)?;

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

    // Handle publisher information
    if let Some(publisher_name) = &metadata.publisher {
        // Get or create publisher entry
        let publisher_id = find_or_create_by_name(&tx, "publishers", publisher_name)?;

        // Link book to publisher
        tx.execute(
            "INSERT INTO books_publishers_link (book, publisher) VALUES (?1, ?2)",
            params![book_id, publisher_id],
        )?;
    }

    // Handle series information
    if let Some(series_name) = &metadata.series {
        // Get or create series entry
        let series_id = find_or_create_by_name_and_sort(&tx, "series", series_name, series_name)?;

        // Link book to series
        tx.execute(
            "INSERT INTO books_series_link (book, series) VALUES (?1, ?2)",
            params![book_id, series_id],
        )?;

        // Set series index in books table
        if let Some(index) = metadata.series_index {
            tx.execute(
                "UPDATE books SET series_index = ?1 WHERE id = ?2",
                params![index, book_id],
            )?;
        }
    }

    tx.commit()?;

    Ok(UpsertResult::Created { book_id, book_path })
}


/// Lists all books with their attributes.
pub fn list_books(
    conn: &Connection,
    appdb_conn: Option<&Connection>,
    shelf_name: Option<&str>,
    verbose: bool,
) -> Result<()> {
    let book_ids_on_shelf = if let Some(shelf) = shelf_name {
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

    if let Some(shelf) = shelf_name {
        println!("📚 Listing books on shelf '{}'...
", shelf);
    } else {
        println!("📚 Listing all books in the library...
");
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
                    println!("            - {} (owned by {})", shelf_name, user_display);
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
pub fn delete_book(calibre_conn: &mut Connection, appdb_conn: Option<&Connection>, library_db_path: &Path, book_id: i64) -> Result<()> {
    let book_info: Option<(String, String)> = calibre_conn.query_row(
            "SELECT title, path FROM books WHERE id = ?1",
            params![book_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    let (_title, book_path_str) = if let Some((t, p)) = book_info.as_ref() {
        println!("You are about to delete:");
        println!("  ID:    {}", book_id);
        println!("  Title: {}", t);
        (t.clone(), p.clone())
    } else {
        println!("Warning: Book with ID {} not found in Calibre database. Attempting to clean up Calibre-Web shelves and filesystem.", book_id);
        ("(Unknown Title)".to_string(), "".to_string())
    };

    // Delete from DB. Triggers will handle linked tables.
    let tx = calibre_conn.transaction()?;
    let affected = tx.execute("DELETE FROM books WHERE id = ?1", params![book_id])?;
    tx.commit()?;

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
            std::fs::remove_file(&cover_path)
                .with_context(|| format!("Failed to remove cover image: {:?}", cover_path))?;
            println!(" -> Cover image deleted.");
        }
        if book_dir.exists() {
            std::fs::remove_dir_all(&book_dir)
                .with_context(|| format!("Failed to delete book directory: {:?}", book_dir))?;
            println!(" -> Successfully deleted book directory: {:?}", book_dir);

            // Check if the parent author directory is now empty
            if let Some(author_dir) = book_dir.parent() {
                if let Ok(mut entries) = std::fs::read_dir(author_dir) {
                    if entries.next().is_none() {
                        if std::fs::remove_dir(author_dir).is_ok() {
                            println!(" -> Successfully deleted empty author directory: {:?}", author_dir);
                        }
                    }
                }
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

/// Creates Calibre-specific custom SQL functions needed by the database triggers.
pub fn create_calibre_functions(conn: &Connection) -> Result<()> {
    // Calibre's triggers use a custom `title_sort` function. We need to provide one.
    // This is a simplified version for demonstration.
    conn.create_scalar_function(
        "title_sort",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| {
            let title = ctx.get::<String>(0)?;
            Ok(title_sort_logic(&title))
        },
    )?;

    // The book insert trigger also requires a uuid4 function.
    conn.create_scalar_function(
        "uuid4",
        0,
        FunctionFlags::SQLITE_UTF8,
        move |_ctx| {
            Ok(Uuid::new_v4().to_string())
        },
    )?;

    Ok(())
}

/// A simplified implementation of Calibre's title sorting logic.
/// It moves common English articles to the end.
fn title_sort_logic(title: &str) -> String {
    let articles = ["the ", "a ", "an ", "le ", "la ", "les ", "el ", "los ", "las "];
    let lower_title = title.to_lowercase();
    for article in &articles {
        if lower_title.starts_with(article) {
            let len = article.len();
            return format!("{}, {}", &title[len..], &title[..len - 1]);
        }
    }
    title.to_string()
}

/// Helper function to get linked items like authors, tags, etc. for a book.
fn get_linked_items(
    conn: &Connection,
    item_table: &str,
    link_table: &str,
    item_column: &str,
    book_id: i64,
) -> Result<Vec<String>> {
    let query = format!(
        "SELECT t.name FROM {} t JOIN {} lt ON t.id = lt.{} WHERE lt.book = ?1",
        item_table, link_table, item_column
    );
    let mut stmt = conn.prepare(&query)?;
    let items_iter = stmt.query_map(params![book_id], |row| row.get(0))?;
    items_iter.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Generates a sortable author name (e.g., "John Doe" -> "Doe, John").
/// This is a simplified version of Calibre's logic.
fn get_author_sort(author: &str) -> String {
    let parts: Vec<&str> = author.split_whitespace().collect();
    if parts.len() > 1 {
        let last = parts.last().unwrap_or(&"");
        let first = &parts[..parts.len() - 1].join(" ");
        format!("{}, {}", last, first)
    } else {
        author.to_string()
    }
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
