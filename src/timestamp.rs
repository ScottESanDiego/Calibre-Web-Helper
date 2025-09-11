use anyhow::Result;
use rusqlite::Connection;
use chrono::Utc;

/// Verifies and repairs any NULL timestamp values in both databases.
/// This is run automatically when opening the databases to prevent NULL value errors.
pub fn verify_and_repair_timestamps(calibre_conn: &mut Connection, appdb_conn: Option<&mut Connection>) -> Result<()> {
    // Fix timestamps in Calibre database
    let tx = calibre_conn.transaction()?;
    
    // Get current timestamp with microsecond precision
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S.%6f").to_string();

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
        let now = chrono::Local::now().naive_local();

        // Fix shelf timestamps
        let fixed = tx.execute(
            "UPDATE shelf SET created = ?1 WHERE created IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelves with missing created timestamp", fixed);
        }

        let fixed = tx.execute(
            "UPDATE shelf SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelves with missing last_modified timestamp", fixed);
        }

        // Fix book_shelf_link timestamps
        let fixed = tx.execute(
            "UPDATE book_shelf_link SET date_added = ?1 WHERE date_added IS NULL",
            [&now.format("%Y-%m-%d %H:%M:%S.%6f").to_string()],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} shelf links with missing date_added", fixed);
        }

        // Fix archived_book timestamps
        let fixed = tx.execute(
            "UPDATE archived_book SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} archived books with missing last_modified", fixed);
        }

        // Fix kobo_reading_state timestamps
        let fixed = tx.execute(
            "UPDATE kobo_reading_state SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo reading states with missing last_modified", fixed);
        }

        let fixed = tx.execute(
            "UPDATE kobo_reading_state SET priority_timestamp = ?1 WHERE priority_timestamp IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo reading states with missing priority_timestamp", fixed);
        }

        // Fix kobo_bookmark timestamps
        let fixed = tx.execute(
            "UPDATE kobo_bookmark SET last_modified = ?1 WHERE last_modified IS NULL",
            [&now],
        )?;
        if fixed > 0 {
            println!(" -> Fixed {} Kobo bookmarks with missing last_modified", fixed);
        }

        tx.commit()?;
    }

    Ok(())
}