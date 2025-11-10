use chrono::{DateTime, Local, TimeZone, Utc};
use rusqlite::{params, Transaction, Error as SqliteError, Connection, OptionalExtension};
use anyhow::{Result, Context};
use sha1::{Sha1, Digest};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Format a timestamp with microsecond precision for database storage
/// This matches the format used by both Calibre and Calibre-Web
pub fn format_timestamp_micro<Tz: TimeZone>(dt: &DateTime<Tz>) -> String 
where
    Tz::Offset: std::fmt::Display,
{
    dt.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
}

/// Get current UTC timestamp formatted for database storage
pub fn now_utc_micro() -> String {
    format_timestamp_micro(&Utc::now())
}

/// Get current local timestamp formatted for database storage
pub fn now_local_micro() -> String {
    format_timestamp_micro(&Local::now())
}

/// Generic find-or-create pattern for database entities
/// 
/// This pattern is used extensively throughout the codebase for entities like
/// authors, publishers, series, etc. It tries to find an existing record,
/// and if not found, creates a new one.
///
/// # Arguments
/// * `tx` - Database transaction
/// * `find_query` - SQL query to find existing record (should return id)
/// * `find_params` - Parameters for the find query
/// * `insert_query` - SQL query to insert new record
/// * `insert_params` - Parameters for the insert query
///
/// # Returns
/// The id of the found or created record
pub fn find_or_create<P1, P2>(
    tx: &Transaction,
    find_query: &str,
    find_params: P1,
    insert_query: &str,
    insert_params: P2,
) -> Result<i64, SqliteError>
where
    P1: rusqlite::Params,
    P2: rusqlite::Params,
{
    match tx.query_row(find_query, find_params, |row| row.get::<_, i64>(0)) {
        Ok(id) => Ok(id),
        Err(SqliteError::QueryReturnedNoRows) => {
            tx.execute(insert_query, insert_params)?;
            Ok(tx.last_insert_rowid())
        }
        Err(e) => Err(e),
    }
}

/// Simplified find-or-create for cases where we just need to find by name
/// and insert with name (common pattern for publishers, simple entities)
pub fn find_or_create_by_name(
    tx: &Transaction,
    table_name: &str,
    name: &str,
) -> Result<i64, SqliteError> {
    let find_query = format!("SELECT id FROM {} WHERE name = ?1", table_name);
    let insert_query = format!("INSERT INTO {} (name) VALUES (?1)", table_name);
    
    find_or_create(
        tx,
        &find_query,
        params![name],
        &insert_query,
        params![name],
    )
}

/// Find-or-create for entities that have both name and sort fields
/// (common pattern for authors, series)
pub fn find_or_create_by_name_and_sort(
    tx: &Transaction,
    table_name: &str,
    name: &str,
    sort: &str,
) -> Result<i64, SqliteError> {
    let find_query = format!("SELECT id FROM {} WHERE name = ?1", table_name);
    let insert_query = format!("INSERT INTO {} (name, sort) VALUES (?1, ?2)", table_name);
    
    find_or_create(
        tx,
        &find_query,
        params![name],
        &insert_query,
        params![name, sort],
    )
}

/// Find-or-create for language codes (special case for languages table)
pub fn find_or_create_language(
    tx: &Transaction,
    lang_code: &str,
) -> Result<i64, SqliteError> {
    find_or_create(
        tx,
        "SELECT id FROM languages WHERE lang_code = ?1",
        params![lang_code],
        "INSERT INTO languages (lang_code) VALUES (?1)",
        params![lang_code],
    )
}

/// Verifies and repairs any NULL timestamp values in both databases.
/// This is run automatically when opening the databases to prevent NULL value errors.
pub fn verify_and_repair_timestamps(calibre_conn: &mut Connection, appdb_conn: Option<&mut Connection>) -> Result<()> {
    // Fix timestamps in Calibre database
    let tx = calibre_conn.transaction()?;
    
    // Get current timestamp with microsecond precision
    let now = now_utc_micro();

    // Fix NULL timestamps in books table
    let fixed = tx.execute(
        "UPDATE books SET timestamp = ?1 WHERE timestamp IS NULL",
        [&now],
    )?;
    if fixed > 0 {
        println!(" -> Fixed {} books with missing timestamp", fixed);
    }

    let fixed = tx.execute(
        "UPDATE books SET pubdate = ?1 WHERE pubdate IS NULL",
        [&now],
    )?;
    if fixed > 0 {
        println!(" -> Fixed {} books with missing pubdate", fixed);
    }

    let fixed = tx.execute(
        "UPDATE books SET last_modified = ?1 WHERE last_modified IS NULL",
        [&now],
    )?;
    if fixed > 0 {
        println!(" -> Fixed {} books with missing last_modified", fixed);
    }

    tx.commit()?;

    // Fix timestamps in Calibre-Web database if provided
    if let Some(conn) = appdb_conn {
        let tx = conn.transaction()?;
        let now_micro = now_local_micro();

        // Fix shelf timestamps
        let fixed = tx.execute(
            "UPDATE shelf SET created = ?1 WHERE created IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelves with missing created timestamp", fixed);
        }

        let fixed = tx.execute(
            "UPDATE shelf SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelves with missing last_modified timestamp", fixed);
        }

        // Fix book_shelf_link timestamps
        let fixed = tx.execute(
            "UPDATE book_shelf_link SET date_added = ?1 WHERE date_added IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelf links with missing date_added", fixed);
        }

        // Fix archived_book timestamps
        let fixed = tx.execute(
            "UPDATE archived_book SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} archived books with missing last_modified", fixed);
        }

        // Fix kobo_reading_state timestamps
        let fixed = tx.execute(
            "UPDATE kobo_reading_state SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo reading states with missing last_modified", fixed);
        }

        let fixed = tx.execute(
            "UPDATE kobo_reading_state SET priority_timestamp = ?1 WHERE priority_timestamp IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo reading states with missing priority_timestamp", fixed);
        }

        // Fix kobo_bookmark timestamps
        let fixed = tx.execute(
            "UPDATE kobo_bookmark SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now_micro],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo bookmarks with missing last_modified", fixed);
        }

        tx.commit()?;
    }

    Ok(())
}

/// Calculate SHA1 hash of a file
pub fn calculate_file_hash(file_path: &std::path::Path) -> Result<String> {
    let mut file = File::open(file_path)?;
    let mut hasher = Sha1::new();
    let mut buffer = [0; 8192]; // 8KB buffer for reading chunks
    
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    
    Ok(format!("{:x}", hasher.finalize()))
}

/// Validates that an ID is positive and within reasonable bounds
pub fn validate_id(id: i64, entity_type: &str) -> Result<()> {
    if id <= 0 {
        anyhow::bail!("Invalid {} ID: {}. ID must be positive.", entity_type, id);
    }
    if id > i64::MAX / 2 {
        anyhow::bail!("Invalid {} ID: {}. ID is unreasonably large.", entity_type, id);
    }
    Ok(())
}

/// Validates a table name to prevent SQL injection
/// Only allows alphanumeric characters and underscores
pub fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        anyhow::bail!("Table name cannot be empty");
    }
    
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!(
            "Invalid table name '{}'. Only alphanumeric characters and underscores allowed.",
            table_name
        );
    }
    
    // Check against known valid table names
    const VALID_TABLES: &[&str] = &[
        "books", "authors", "publishers", "tags", "series", "languages",
        "books_authors_link", "books_publishers_link", "books_tags_link",
        "books_series_link", "books_languages_link", "identifiers",
        "comments", "data", "shelf", "book_shelf_link", "user",
        "kobo_reading_state", "kobo_bookmark", "kobo_statistics",
        "kobo_synced_books", "book_read_link"
    ];
    
    if !VALID_TABLES.contains(&table_name) {
        anyhow::bail!(
            "Table name '{}' is not in the list of known valid tables",
            table_name
        );
    }
    
    Ok(())
}

/// Validates a column name to prevent SQL injection
pub fn validate_column_name(column_name: &str) -> Result<()> {
    if column_name.is_empty() {
        anyhow::bail!("Column name cannot be empty");
    }
    
    if !column_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        anyhow::bail!(
            "Invalid column name '{}'. Only alphanumeric characters and underscores allowed.",
            column_name
        );
    }
    
    Ok(())
}

/// Creates a backup of a database file
pub fn backup_database(db_path: &Path, operation_name: &str) -> Result<std::path::PathBuf> {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let backup_name = format!(
        "{}_backup_{}_{}.db",
        db_path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("database"),
        operation_name,
        timestamp
    );
    
    let backup_path = db_path.parent()
        .unwrap_or_else(|| Path::new("."))
        .join(backup_name);
    
    std::fs::copy(db_path, &backup_path)
        .with_context(|| format!(
            "Failed to create backup of {:?} to {:?}",
            db_path, backup_path
        ))?;
    
    println!(" -> Created database backup: {:?}", backup_path);
    Ok(backup_path)
}

/// Validates foreign key existence in a table
pub fn validate_foreign_key(
    conn: &Connection,
    table_name: &str,
    id: i64,
    entity_type: &str,
) -> Result<()> {
    validate_table_name(table_name)?;
    validate_id(id, entity_type)?;
    
    let query = format!("SELECT 1 FROM {} WHERE id = ?1", table_name);
    let exists: bool = conn
        .query_row(&query, params![id], |_| Ok(true))
        .optional()
        .with_context(|| format!(
            "Failed to validate {} with ID {} in table {}",
            entity_type, id, table_name
        ))?
        .is_some();
    
    if !exists {
        anyhow::bail!(
            "{} with ID {} does not exist in table {}",
            entity_type, id, table_name
        );
    }
    
    Ok(())
}