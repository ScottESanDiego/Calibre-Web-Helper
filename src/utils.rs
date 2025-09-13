use chrono::{DateTime, Local, TimeZone, Utc};
use rusqlite::{params, Transaction, Error as SqliteError, Connection};
use anyhow::Result;

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