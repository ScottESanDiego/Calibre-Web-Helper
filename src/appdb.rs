use crate::utils::{format_timestamp_micro, now_utc_micro, validate_id};
use anyhow::{Context, Result};
use chrono::{Duration, NaiveDateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use uuid::Uuid;

/// Opens the app.db connection if a path is provided.
pub(crate) fn open_appdb(path: Option<&Path>) -> Result<Option<Connection>> {
    path.map(crate::db::open_appdb)
        .transpose()
}

/// Lists all unique shelves from the Calibre-Web app.db.
pub(crate) fn list_shelves(appdb_conn: Option<&Connection>) -> Result<()> {
    if let Some(conn) = appdb_conn {
        println!("📖 Finding available shelves from Calibre-Web...");

        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.kobo_sync, u.name as username, COUNT(bsl.book_id) as book_count
             FROM shelf s 
             LEFT JOIN user u ON s.user_id = u.id 
             LEFT JOIN book_shelf_link bsl ON s.id = bsl.shelf
             GROUP BY s.id, s.name, s.kobo_sync, u.name
             ORDER BY u.name, s.name"
        )?;
        
        let shelves_iter = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,           // shelf id
                row.get::<_, String>(1)?,       // shelf name
                row.get::<_, i64>(2)?,          // kobo_sync
                row.get::<_, Option<String>>(3)?, // username (may be NULL)
                row.get::<_, i64>(4)?           // book_count
            ))
        })?;
        
        let shelves: Vec<(i64, String, i64, Option<String>, i64)> = shelves_iter.collect::<Result<Vec<_>, _>>()?;

        if shelves.is_empty() {
            println!("\nNo shelves found in the Calibre-Web database.");
        } else {
            println!("\nAvailable shelves:");
            for (id, shelf_name, kobo_sync, username, book_count) in shelves {
                let user_display = username.unwrap_or_else(|| "Unknown".to_string());
                let kobo_indicator = if kobo_sync == 1 { " [Kobo]" } else { "" };
                let book_text = if book_count == 1 { "book" } else { "books" };
                println!("- {} (ID: {}) - User: {}{} - {} {}", 
                         shelf_name, id, user_display, kobo_indicator, book_count, book_text);
            }
        }
    } else {
        anyhow::bail!("The --appdb-file argument is required to list shelves.");
    }

    Ok(())
}

/// Resolves a username to user_id, defaulting to admin (id=1) if no username is provided
fn resolve_user_id(tx: &rusqlite::Transaction, username: Option<&str>) -> Result<i64> {
    if let Some(uname) = username {
        match tx.query_row(
            "SELECT id FROM user WHERE name = ?1",
            params![uname],
            |row| row.get::<_, i64>(0),
        ).optional()? {
            Some(id) => Ok(id),
            None => anyhow::bail!("User '{}' not found", uname),
        }
    } else {
        Ok(1) // Default admin user
    }
}

/// Finds or creates a shelf for the given user
fn find_or_create_shelf(tx: &rusqlite::Transaction, shelf_name: &str, user_id: i64, username: Option<&str>) -> Result<i64> {
    match tx.query_row(
        "SELECT id FROM shelf WHERE name = ?1 AND user_id = ?2",
        params![shelf_name, user_id],
        |row| row.get(0),
    ).optional()? {
        Some(id) => Ok(id),
        None => {
            // Shelf doesn't exist, create it for the specific user
            // Matches Calibre-Web: Shelf() uses datetime.now(timezone.utc) for created/last_modified
            let uuid = Uuid::new_v4().to_string();
            let now_micro = now_utc_micro();
            
            tx.execute(
                "INSERT INTO shelf (uuid, name, is_public, user_id, kobo_sync, created, last_modified) VALUES (?1, ?2, 0, ?3, 0, ?4, ?5)",
                params![uuid, shelf_name, user_id, now_micro, now_micro],
            )?;
            println!(" -> Created new shelf '{}' for user {}.", shelf_name, 
                    username.unwrap_or("admin"));
            Ok(tx.last_insert_rowid())
        }
    }
}

/// Returns a timestamp strictly newer than every parseable membership timestamp
/// for this user. This makes a large directory add safe for Calibre-Web's strict
/// Kobo cursor even when more than one page is added before the next sync.
fn next_shelf_link_timestamp(tx: &rusqlite::Transaction, user_id: i64) -> Result<String> {
    let latest = {
        let mut stmt = tx.prepare(
            "SELECT bsl.date_added
             FROM book_shelf_link bsl
             JOIN shelf s ON s.id = bsl.shelf
             WHERE s.user_id = ?1 AND typeof(bsl.date_added) = 'text'",
        )?;
        let timestamps = stmt.query_map([user_id], |row| row.get::<_, String>(0))?;
        let mut latest: Option<chrono::DateTime<Utc>> = None;
        for timestamp in timestamps {
            let timestamp = timestamp?;
            if let Ok(parsed) =
                NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%d %H:%M:%S%.f")
            {
                let parsed = parsed.and_utc();
                latest = Some(latest.map_or(parsed, |current| current.max(parsed)));
            }
        }
        latest
    };

    // Compare at the same precision that is stored. Otherwise two nanosecond-
    // distinct values can truncate to the same microsecond string.
    let now = NaiveDateTime::parse_from_str(
        &format_timestamp_micro(&Utc::now()),
        "%Y-%m-%d %H:%M:%S%.f",
    )?
    .and_utc();
    let timestamp = match latest {
        Some(latest) if latest >= now => latest + Duration::microseconds(1),
        _ => now,
    };
    Ok(format_timestamp_micro(&timestamp))
}

/// Core function to add a book to a shelf with duplicate handling control.
/// Matches Calibre-Web's `add_to_shelf()` behavior: insert BookShelf row,
/// update shelf.last_modified. No proactive Kobo sync record creation.
fn add_book_to_shelf_core(conn: &mut Connection, book_id: i64, shelf_name: &str, username: Option<&str>, allow_duplicates: bool) -> Result<bool> {
    validate_id(book_id, "book")
        .context("Invalid book ID for shelf operation")?;
    
    if shelf_name.trim().is_empty() {
        anyhow::bail!("Shelf name cannot be empty");
    }
    
    let tx = conn.transaction()
        .context("Failed to start shelf operation transaction")?;

    let user_id = resolve_user_id(&tx, username)
        .context("Failed to resolve user ID for shelf operation")?;
    let shelf_id = find_or_create_shelf(&tx, shelf_name, user_id, username)
        .with_context(|| format!("Failed to find or create shelf '{}'", shelf_name))?;

    // Check if the link already exists to prevent duplicates
    let link_exists: bool = tx.query_row(
        "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
        params![book_id, shelf_id],
        |_| Ok(true)
    ).optional()
        .with_context(|| format!(
            "Failed to check if book {} is already on shelf {}",
            book_id, shelf_id
        ))?
        .is_some();

    if link_exists {
        if allow_duplicates {
            println!(" -> Book is already on shelf '{}'.", shelf_name);
        } else {
            println!(" -> Book {} is already on shelf '{}'.", book_id, shelf_name);
        }
        tx.commit()?;
        return Ok(false);
    }

    // Get the next order value for this shelf (matches Calibre-Web's max(order) + 1 logic)
    let next_order: i64 = tx.query_row(
        "SELECT COALESCE(MAX(\"order\"), 0) + 1 FROM book_shelf_link WHERE shelf = ?1",
        params![shelf_id],
        |row| row.get(0)
    )?;

    // Insert the book-shelf link with UTC timestamp (matches Calibre-Web's datetime.now(timezone.utc))
    let now_micro = next_shelf_link_timestamp(&tx, user_id)?;
    
    tx.execute(
        "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added) VALUES (?1, ?2, ?3, ?4)",
        params![book_id, shelf_id, next_order, &now_micro]
    )?;

    // Update the shelf's last_modified timestamp (matches Calibre-Web's shelf.last_modified = datetime.now(timezone.utc))
    tx.execute(
        "UPDATE shelf SET last_modified = ?1 WHERE id = ?2",
        params![&now_micro, shelf_id],
    )?;

    tx.commit()
        .context("Failed to commit shelf link transaction")?;
    Ok(true)
}

/// Adds a book to a shelf in the Calibre-Web database. Creates the shelf if it doesn't exist.
pub(crate) fn add_book_to_shelf_in_appdb(conn: &mut Connection, book_id: i64, shelf_name: &str, username: Option<&str>) -> Result<()> {
    let was_added = add_book_to_shelf_core(conn, book_id, shelf_name, username, true)?;
    
    if was_added {
        println!(" -> Added book to shelf '{}'.", shelf_name);
    }
    
    Ok(())
}

/// Inspects the database contents, showing relationships between books and shelves
pub(crate) fn inspect_databases(appdb_conn: Option<&Connection>, calibre_conn: &Connection) -> Result<()> {
    println!("\n📚 Database Inspection Report");
    println!("═════════════════════════");

    // If we have an app.db connection, show shelf information
    if let Some(conn) = appdb_conn {
        println!("\n🔎 Shelves and Books:");
        println!("──────────────────");
        
        // Get all shelves with their user information
        let mut shelf_stmt = conn.prepare(
            "SELECT s.id, s.name, u.name as username, s.is_public 
             FROM shelf s 
             LEFT JOIN user u ON s.user_id = u.id 
             ORDER BY s.name"
        )?;
        
        let shelf_rows = shelf_stmt.query_map(params![], |row| {
            Ok((
                row.get::<_, i64>("id")?,
                row.get::<_, String>("name")?,
                row.get::<_, Option<String>>("username")?,
                row.get::<_, bool>("is_public")?,
            ))
        })?;

        for shelf_result in shelf_rows {
            let (shelf_id, shelf_name, username, is_public) = shelf_result?;
            println!("\nShelf: {} (ID: {})", shelf_name, shelf_id);
            println!("  Owner: {}", username.unwrap_or_else(|| "Unknown".to_string()));
            println!("  Public: {}", if is_public { "Yes" } else { "No" });

            // Get book IDs from this shelf
            let mut book_stmt = conn.prepare(
                "SELECT book_id FROM book_shelf_link WHERE shelf = ? ORDER BY book_id"
            )?;

            let book_ids: Vec<i64> = book_stmt.query_map(params![shelf_id], |row| {
                row.get::<_, i64>("book_id")
            })?.collect::<Result<Vec<_>, _>>()?;

            let mut book_count = 0;
            println!("  Books:");
            
            // Look up book details in the Calibre database
            if !book_ids.is_empty() {
                let placeholders = book_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let query = format!(
                    "SELECT id, title, author_sort FROM books WHERE id IN ({}) ORDER BY title",
                    placeholders
                );
                
                let mut cal_stmt = calibre_conn.prepare(&query)?;
                let params_vec: Vec<&dyn rusqlite::ToSql> = book_ids.iter()
                    .map(|id| id as &dyn rusqlite::ToSql)
                    .collect();
                
                let book_rows = cal_stmt.query_map(&params_vec[..], |row| {
                    Ok((
                        row.get::<_, i64>("id")?,
                        row.get::<_, String>("title")?,
                        row.get::<_, String>("author_sort")?,
                    ))
                })?;

                for book_result in book_rows {
                    let (book_id, title, author) = book_result?;
                    println!("   - {} by {} (ID: {})", title, author, book_id);
                    book_count += 1;
                }
            }
            if book_count == 0 {
                println!("   (No books on this shelf)");
            }
        }
    }

    // Show Calibre database information
    println!("\n📚 Calibre Library Statistics:");
    println!("─────────────────────────");

    let book_count: i64 = calibre_conn.query_row("SELECT COUNT(*) FROM books", params![], |row| row.get(0))?;
    let author_count: i64 = calibre_conn.query_row("SELECT COUNT(*) FROM authors", params![], |row| row.get(0))?;
    let series_count: i64 = calibre_conn.query_row("SELECT COUNT(*) FROM series", params![], |row| row.get(0))?;

    println!("Total Books: {}", book_count);
    println!("Total Authors: {}", author_count);
    println!("Total Series: {}", series_count);

    if book_count > 0 {
            println!("\nRecent Books:");
        let mut recent_stmt = calibre_conn.prepare(
            "SELECT title, author_sort, timestamp 
             FROM books 
             ORDER BY timestamp DESC 
             LIMIT 5"
        )?;
        
        let recent_rows = recent_stmt.query_map(params![], |row| {
            Ok((
                row.get::<_, String>("title")?,
                row.get::<_, String>("author_sort")?,
                row.get::<_, String>("timestamp")?,
            ))
        })?;

        for recent_result in recent_rows {
            let (title, author, timestamp) = recent_result?;
            println!(" - {} by {} (Added: {})", title, author, timestamp);
        }
    }

    // Check for any shelf links to non-existent books
    if let Some(conn) = appdb_conn {
        let mut orphaned_stmt = conn.prepare(
            "SELECT DISTINCT book_id FROM book_shelf_link ORDER BY book_id"
        )?;
        
        let orphaned_books: Vec<i64> = orphaned_stmt.query_map(params![], |row| {
            row.get::<_, i64>("book_id")
        })?.collect::<Result<Vec<_>, _>>()?;

        if !orphaned_books.is_empty() {
            let placeholders = orphaned_books.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!(
                "SELECT id FROM books WHERE id IN ({})",
                placeholders
            );
            
            let mut cal_stmt = calibre_conn.prepare(&query)?;
            let params_vec: Vec<&dyn rusqlite::ToSql> = orphaned_books.iter()
                .map(|id| id as &dyn rusqlite::ToSql)
                .collect();
            
            let existing_books: std::collections::HashSet<i64> = cal_stmt.query_map(&params_vec[..], |row| {
                row.get::<_, i64>("id")
            })?.collect::<Result<_, _>>()?;

            let missing_books: Vec<_> = orphaned_books.iter()
                .filter(|id| !existing_books.contains(id))
                .collect();

            if !missing_books.is_empty() {
                println!("\n⚠️  Warning: Found shelf links to non-existent books:");
                for book_id in missing_books {
                    println!("   - Book ID: {}", book_id);
                }
                println!("\nYou can use the 'clean-shelves' command to remove these orphaned links.");
            }
        }
    }

    println!("\n");
    Ok(())
}

pub(crate) fn clean_empty_shelves(appdb_conn: &mut Connection, calibre_conn: &Connection) -> Result<()> {
    println!("🧹 Cleaning empty shelves from Calibre-Web...");

    let mut calibre_check_stmt = calibre_conn.prepare("SELECT 1 FROM books WHERE id = ?1")
        .context("Failed to prepare book existence check query")?;

    // Collect shelf data up-front so the borrow on appdb_conn is released before the transaction
    let shelves: Vec<(i64, String)> = {
        let mut stmt = appdb_conn.prepare("SELECT id, name FROM shelf")
            .context("Failed to prepare shelf query")?;
        stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?.collect::<Result<Vec<_>, _>>()?
    };

    // Collect all orphaned link IDs and empty shelf IDs before mutating
    let mut orphan_link_ids: Vec<(i64, String)> = Vec::new();
    let mut empty_shelf_ids: Vec<(i64, String)> = Vec::new();

    for (shelf_id, shelf_name) in &shelves {
        let links: Vec<(i64, i64)> = {
            let mut link_stmt = appdb_conn.prepare("SELECT id, book_id FROM book_shelf_link WHERE shelf = ?1")?;
            link_stmt.query_map(params![shelf_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?.collect::<Result<Vec<_>, _>>()?
        };

        let mut orphaned_count = 0;
        for (link_id, book_id) in links {
            let exists: bool = calibre_check_stmt.query_row(params![book_id], |_| Ok(true)).optional()?.is_some();
            if !exists {
                orphan_link_ids.push((link_id, shelf_name.clone()));
                orphaned_count += 1;
            }
        }

        if orphaned_count > 0 {
            println!(" -> Found {} orphaned book links for shelf '{}'.", orphaned_count, shelf_name);
        }
    }

    // Now perform all deletes inside a single transaction
    let tx = appdb_conn.transaction()
        .context("Failed to start shelf cleanup transaction")?;

    for (link_id, _shelf_name) in &orphan_link_ids {
        tx.execute("DELETE FROM book_shelf_link WHERE id = ?1", params![link_id])?;
    }

    if !orphan_link_ids.is_empty() {
        println!(" -> Removed {} orphaned book links.", orphan_link_ids.len());
    }

    for (shelf_id, shelf_name) in &shelves {
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM book_shelf_link WHERE shelf = ?1",
            params![shelf_id],
            |row| row.get(0),
        )?;
        if count == 0 {
            tx.execute("DELETE FROM shelf WHERE id = ?1", params![shelf_id])?;
            empty_shelf_ids.push((*shelf_id, shelf_name.clone()));
        }
    }

    tx.commit()
        .context("Failed to commit shelf cleanup transaction")?;

    for (_id, name) in &empty_shelf_ids {
        println!(" -> Removed empty shelf '{}'.", name);
    }

    println!("✅ Shelf cleaning complete.");
    Ok(())
}

#[derive(Debug)]
struct ReadingStateRepair {
    id: i64,
    user_id: Option<i64>,
    book_id: Option<i64>,
    last_modified_is_null: bool,
    priority_timestamp_is_null: bool,
    statistics_count: i64,
    null_statistics_timestamps: i64,
    preferred_bookmark_id: Option<i64>,
    bookmark_count: i64,
    null_bookmark_timestamps: i64,
    current_bookmark_is_valid: bool,
    has_book_read_link: bool,
}

impl ReadingStateRepair {
    fn needs_repair(&self) -> bool {
        self.last_modified_is_null
            || self.priority_timestamp_is_null
            || self.statistics_count == 0
            || self.null_statistics_timestamps > 0
            || self.bookmark_count == 0
            || self.null_bookmark_timestamps > 0
            || !self.current_bookmark_is_valid
            || !self.has_book_read_link
    }
}

/// Repairs incomplete graphs rooted at existing Kobo reading states.
///
/// Shelf membership timestamps are Kobo pagination cursors, so this repair must
/// never rewrite them or create reading states for shelf members.
pub(crate) fn fix_kobo_sync_issues(appdb_conn: &mut Connection) -> Result<()> {
    println!("🔧 Repairing existing Kobo reading-state data...");

    ensure_current_bookmark_column(appdb_conn)?;
    appdb_conn
        .pragma_update(None, "foreign_keys", "ON")
        .context("Failed to enable foreign key enforcement before Kobo repair")?;

    let repair_result = repair_existing_kobo_state_graphs(appdb_conn);

    // A failed transaction is rolled back when it is dropped. Explicitly restore
    // and verify the connection setting before returning either result.
    appdb_conn
        .pragma_update(None, "foreign_keys", "ON")
        .context("Failed to leave foreign key enforcement enabled after Kobo repair")?;
    let foreign_keys_enabled: i64 = appdb_conn
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .context("Failed to verify foreign key enforcement after Kobo repair")?;
    if foreign_keys_enabled != 1 {
        anyhow::bail!("Foreign key enforcement is disabled after Kobo repair");
    }

    let (removed_duplicates, repaired_states) = repair_result?;
    if removed_duplicates == 0 && repaired_states == 0 {
        println!("✅ No Kobo reading-state repairs were needed.");
    } else {
        println!(
            "✅ Kobo repair complete: removed {} duplicate states and repaired {} existing states.",
            removed_duplicates, repaired_states
        );
    }
    println!(" -> Shelf membership timestamps were left unchanged.");

    Ok(())
}

fn ensure_current_bookmark_column(conn: &Connection) -> Result<()> {
    let has_current_bookmark = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(kobo_reading_state)")
            .context("Failed to inspect kobo_reading_state schema")?;
        let column_names = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        column_names.iter().any(|name| name == "current_bookmark")
    };

    if !has_current_bookmark {
        println!(" -> Adding nullable current_bookmark column");
        conn.execute(
            "ALTER TABLE kobo_reading_state ADD COLUMN current_bookmark INTEGER",
            [],
        )
        .context("Failed to add current_bookmark column")?;
    }

    Ok(())
}

fn repair_existing_kobo_state_graphs(conn: &mut Connection) -> Result<(usize, usize)> {
    let tx = conn
        .transaction()
        .context("Failed to start Kobo data repair transaction")?;

    let ranked_states = {
        let mut stmt = tx.prepare(
            "SELECT krs.id, krs.user_id, krs.book_id
             FROM kobo_reading_state krs
             ORDER BY
                 krs.user_id,
                 krs.book_id,
                 EXISTS(
                     SELECT 1 FROM kobo_bookmark kb
                     WHERE kb.id = krs.current_bookmark
                       AND kb.kobo_reading_state_id = krs.id
                 ) DESC,
                 EXISTS(
                     SELECT 1 FROM kobo_bookmark kb
                     WHERE kb.kobo_reading_state_id = krs.id
                 ) DESC,
                 (SELECT MAX(kb.last_modified) FROM kobo_bookmark kb
                  WHERE kb.kobo_reading_state_id = krs.id) DESC,
                 krs.last_modified DESC,
                 krs.id DESC",
        )?;
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    let mut canonical_by_key = HashMap::new();
    let mut duplicate_states = Vec::new();
    for (state_id, user_id, book_id) in ranked_states {
        let key = (user_id, book_id);
        if let Some(canonical_state_id) = canonical_by_key.get(&key) {
            duplicate_states.push((state_id, *canonical_state_id));
        } else {
            canonical_by_key.insert(key, state_id);
        }
    }

    let mut merged_state_ids = HashSet::new();
    for (state_id, canonical_state_id) in &duplicate_states {
        let canonical_current_is_valid: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM kobo_bookmark kb
                 JOIN kobo_reading_state krs ON krs.id = ?1
                 WHERE kb.id = krs.current_bookmark
                   AND kb.kobo_reading_state_id = krs.id
             )",
            params![canonical_state_id],
            |row| row.get(0),
        )?;
        if !canonical_current_is_valid {
            tx.execute(
                "UPDATE kobo_reading_state SET current_bookmark = NULL WHERE id = ?1",
                params![canonical_state_id],
            )?;
        }

        let latest_priority: Option<String> = tx.query_row(
            "SELECT MAX(priority_timestamp) FROM kobo_reading_state
             WHERE id IN (?1, ?2) AND priority_timestamp IS NOT NULL",
            params![canonical_state_id, state_id],
            |row| row.get(0),
        )?;
        if let Some(priority_timestamp) = latest_priority {
            tx.execute(
                "UPDATE kobo_reading_state SET priority_timestamp = ?1 WHERE id = ?2",
                params![priority_timestamp, canonical_state_id],
            )?;
        }

        let retained_statistics_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM kobo_statistics
                 WHERE kobo_reading_state_id IN (?1, ?2)
                 ORDER BY last_modified DESC, id DESC
                 LIMIT 1",
                params![canonical_state_id, state_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(statistics_id) = retained_statistics_id {
            tx.execute(
                "DELETE FROM kobo_statistics
                 WHERE kobo_reading_state_id IN (?1, ?2) AND id != ?3",
                params![canonical_state_id, state_id, statistics_id],
            )?;
            tx.execute(
                "UPDATE kobo_statistics SET kobo_reading_state_id = ?1 WHERE id = ?2",
                params![canonical_state_id, statistics_id],
            )?;
        }

        tx.execute(
            "UPDATE kobo_bookmark SET kobo_reading_state_id = ?1
             WHERE kobo_reading_state_id = ?2",
            params![canonical_state_id, state_id],
        )?;
        tx.execute(
            "DELETE FROM kobo_reading_state WHERE id = ?1",
            params![state_id],
        )?;
        merged_state_ids.insert(*canonical_state_id);
    }

    let states = {
        let mut stmt = tx.prepare(
            "SELECT
                krs.id,
                krs.user_id,
                krs.book_id,
                krs.last_modified IS NULL,
                krs.priority_timestamp IS NULL,
                (SELECT COUNT(*) FROM kobo_statistics ks
                 WHERE ks.kobo_reading_state_id = krs.id),
                (SELECT COUNT(*) FROM kobo_statistics ks
                 WHERE ks.kobo_reading_state_id = krs.id AND ks.last_modified IS NULL),
                (SELECT kb.id FROM kobo_bookmark kb
                 WHERE kb.kobo_reading_state_id = krs.id
                 ORDER BY kb.last_modified DESC, kb.id DESC LIMIT 1),
                (SELECT COUNT(*) FROM kobo_bookmark kb
                 WHERE kb.kobo_reading_state_id = krs.id),
                (SELECT COUNT(*) FROM kobo_bookmark kb
                 WHERE kb.kobo_reading_state_id = krs.id AND kb.last_modified IS NULL),
                EXISTS(
                    SELECT 1 FROM kobo_bookmark kb
                    WHERE kb.id = krs.current_bookmark
                      AND kb.kobo_reading_state_id = krs.id
                ),
                CASE
                    WHEN krs.user_id IS NULL OR krs.book_id IS NULL THEN 1
                    ELSE EXISTS(
                        SELECT 1 FROM book_read_link brl
                        WHERE brl.user_id = krs.user_id AND brl.book_id = krs.book_id
                    )
                END
             FROM kobo_reading_state krs
             ORDER BY krs.id",
        )?;
        stmt.query_map([], |row| {
            Ok(ReadingStateRepair {
                id: row.get(0)?,
                user_id: row.get(1)?,
                book_id: row.get(2)?,
                last_modified_is_null: row.get(3)?,
                priority_timestamp_is_null: row.get(4)?,
                statistics_count: row.get(5)?,
                null_statistics_timestamps: row.get(6)?,
                preferred_bookmark_id: row.get(7)?,
                bookmark_count: row.get(8)?,
                null_bookmark_timestamps: row.get(9)?,
                current_bookmark_is_valid: row.get(10)?,
                has_book_read_link: row.get(11)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?
    };

    let states_to_repair: Vec<_> = states
        .into_iter()
        .filter(|state| state.needs_repair() || merged_state_ids.contains(&state.id))
        .collect();
    let repair_started_at = Utc::now();

    for (sequence, state) in states_to_repair.iter().enumerate() {
        let repair_timestamp = crate::utils::format_timestamp_micro(
            &(repair_started_at + Duration::microseconds(sequence as i64)),
        );

        if state.statistics_count == 0 {
            tx.execute(
                "INSERT INTO kobo_statistics
                    (kobo_reading_state_id, last_modified, remaining_time_minutes, spent_reading_minutes)
                 VALUES (?1, ?2, NULL, NULL)",
                params![state.id, &repair_timestamp],
            )?;
        } else if state.null_statistics_timestamps > 0 {
            tx.execute(
                "UPDATE kobo_statistics SET last_modified = ?1
                 WHERE kobo_reading_state_id = ?2 AND last_modified IS NULL",
                params![&repair_timestamp, state.id],
            )?;
        }

        let preferred_bookmark_id = if state.bookmark_count == 0 {
            tx.execute(
                "INSERT INTO kobo_bookmark
                    (kobo_reading_state_id, last_modified, location_source, location_type,
                     location_value, progress_percent, content_source_progress_percent)
                 VALUES (?1, ?2, 'Unknown', 'Unknown', '', 0.0, 0.0)",
                params![state.id, &repair_timestamp],
            )?;
            tx.last_insert_rowid()
        } else {
            if state.null_bookmark_timestamps > 0 {
                tx.execute(
                    "UPDATE kobo_bookmark SET last_modified = ?1
                     WHERE kobo_reading_state_id = ?2 AND last_modified IS NULL",
                    params![&repair_timestamp, state.id],
                )?;
            }
            state
                .preferred_bookmark_id
                .context("Existing bookmark could not be selected")?
        };

        if !state.current_bookmark_is_valid {
            tx.execute(
                "UPDATE kobo_reading_state SET current_bookmark = ?1 WHERE id = ?2",
                params![preferred_bookmark_id, state.id],
            )?;
        }

        if !state.has_book_read_link
            && let (Some(user_id), Some(book_id)) = (state.user_id, state.book_id)
        {
            tx.execute(
                "INSERT INTO book_read_link
                    (book_id, user_id, read_status, last_modified,
                     last_time_started_reading, times_started_reading)
                 VALUES (?1, ?2, 0, ?3, NULL, 0)",
                params![book_id, user_id, &repair_timestamp],
            )?;
        }

        tx.execute(
            "UPDATE kobo_reading_state
             SET last_modified = ?1,
                 priority_timestamp = COALESCE(priority_timestamp, ?1)
             WHERE id = ?2",
            params![&repair_timestamp, state.id],
        )?;
    }

    tx.commit()
        .context("Failed to commit Kobo data repair transaction")?;

    Ok((duplicate_states.len(), states_to_repair.len()))
}

/// Provides detailed diagnostics for Kobo sync setup
pub(crate) fn diagnose_kobo_sync(appdb_path: &Path, metadata_path: &Path) -> Result<()> {
    let appdb_conn = crate::db::open_appdb(appdb_path)?;
    let calibre_conn = crate::db::open_calibre_db(metadata_path)?;
    println!("🔍 Kobo Sync Diagnostic Report");
    println!("═══════════════════════════════");
    
    // Check user Kobo settings
    println!("\n👤 Users with Kobo sync enabled:");
    let mut user_stmt = appdb_conn.prepare(
        "SELECT id, name, kobo_only_shelves_sync FROM user WHERE id IN (SELECT DISTINCT user_id FROM shelf WHERE kobo_sync = 1)"
    )?;
    
    let user_rows = user_stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>("id")?,
            row.get::<_, String>("name")?,
            row.get::<_, Option<i64>>("kobo_only_shelves_sync")?,
        ))
    })?;
    
    for user_result in user_rows {
        let (user_id, username, kobo_only) = user_result?;
        println!("  - {} (ID: {}) - Kobo only shelves: {}", 
                username, user_id, kobo_only.unwrap_or(0) == 1);
    }
    
    // Check Kobo sync shelves
    println!("\n📚 Kobo Sync Shelves:");
    let mut shelf_stmt = appdb_conn.prepare(
        "SELECT s.id, s.name, s.user_id, u.name as username, s.created, s.last_modified, 
                COUNT(bsl.book_id) as book_count
         FROM shelf s 
         LEFT JOIN user u ON s.user_id = u.id
         LEFT JOIN book_shelf_link bsl ON s.id = bsl.shelf
         WHERE s.kobo_sync = 1 
         GROUP BY s.id"
    )?;
    
    let shelf_rows = shelf_stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>("id")?,
            row.get::<_, String>("name")?,
            row.get::<_, Option<String>>("username")?,
            row.get::<_, String>("created")?,
            row.get::<_, String>("last_modified")?,
            row.get::<_, i64>("book_count")?,
        ))
    })?;
    
    for shelf_result in shelf_rows {
        let (shelf_id, shelf_name, username, created, last_modified, book_count) = shelf_result?;
        let username = username.unwrap_or_else(|| "Unknown".to_string());
        println!("  - {} (ID: {}) - Owner: {} - Books: {}", shelf_name, shelf_id, username, book_count);
        println!("    Created: {} | Last Modified: {}", created, last_modified);
        
        // Show books on this shelf
        let mut book_stmt = appdb_conn.prepare(
            "SELECT bsl.book_id, bsl.date_added, bsl.\"order\"
             FROM book_shelf_link bsl 
             WHERE bsl.shelf = ?1 
             ORDER BY bsl.\"order\""
        )?;
        
        let book_rows = book_stmt.query_map([shelf_id], |row| {
            Ok((
                row.get::<_, i64>("book_id")?,
                row.get::<_, String>("date_added")?,
                row.get::<_, i64>("order")?,
            ))
        })?;
        
        for book_result in book_rows {
            let (book_id, date_added, order) = book_result?;
            
            // Get book title from Calibre
            let book_title: String = calibre_conn.query_row(
                "SELECT title FROM books WHERE id = ?1",
                [book_id],
                |row| row.get(0)
            ).unwrap_or_else(|_| format!("Unknown (ID: {})", book_id));
            
            // Check sync status
            let in_sync_table: bool = appdb_conn.query_row(
                "SELECT 1 FROM kobo_synced_books WHERE book_id = ?1",
                [book_id],
                |_| Ok(true)
            ).optional()?.is_some();
            
            let has_reading_state: bool = appdb_conn.query_row(
                "SELECT 1 FROM kobo_reading_state WHERE book_id = ?1",
                [book_id],
                |_| Ok(true)
            ).optional()?.is_some();
            
            let sync_status = match (in_sync_table, has_reading_state) {
                (true, true) => "✅ Full sync setup",
                (true, false) => "⚠️  Missing reading state",
                (false, true) => "⚠️  Missing sync entry",
                (false, false) => "❌ No sync setup",
            };
            
            println!("    [{}] {} - {} (Added: {})", order, book_title, sync_status, date_added);
        }
    }
    
    println!("\n💡 Troubleshooting Tips:");
    println!("  1. Ensure the Kobo device is properly connected to Calibre-Web");
    println!("  2. Check that the user account on Kobo matches the shelf owner");
    println!("  3. Verify the book file exists in the Calibre library directory");
    println!("  4. Try disconnecting and reconnecting the Kobo device");
    println!("  5. Check Calibre-Web logs for sync errors during the sync process");
    
    Ok(())
}

/// Adds an existing book to a shelf in the Calibre-Web database (like Calibre-Web does).
/// This function only operates on app.db and assumes the book already exists in metadata.db.
pub(crate) fn add_existing_book_to_shelf(conn: &mut Connection, book_id: i64, shelf_name: &str, username: Option<&str>) -> Result<()> {
    // Validate book ID
    validate_id(book_id, "book")
        .context("Cannot add book to shelf: invalid book ID")?;
    
    // Note: We can't validate against metadata.db here since we only have app.db connection
    // The caller should ensure the book exists in the Calibre database
    
    let was_added = add_book_to_shelf_core(conn, book_id, shelf_name, username, false)?;
    
    if was_added {
        println!("✅ Successfully added book {} to shelf '{}'.", book_id, shelf_name);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value;
    use std::collections::HashSet;

    fn appdb_fixture(with_current_bookmark: bool) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        let current_bookmark_column = if with_current_bookmark {
            ", current_bookmark INTEGER"
        } else {
            ""
        };
        conn.execute_batch(&format!(
            "CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
             );
             CREATE TABLE shelf (
                id INTEGER PRIMARY KEY,
                uuid TEXT,
                name TEXT NOT NULL,
                is_public INTEGER NOT NULL,
                user_id INTEGER NOT NULL REFERENCES user(id),
                kobo_sync INTEGER NOT NULL,
                created TEXT,
                last_modified TEXT
             );
             CREATE TABLE book_shelf_link (
                id INTEGER PRIMARY KEY,
                book_id INTEGER NOT NULL,
                shelf INTEGER NOT NULL REFERENCES shelf(id),
                \"order\" INTEGER NOT NULL,
                date_added
             );
             CREATE TABLE kobo_reading_state (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                book_id INTEGER,
                last_modified TEXT,
                priority_timestamp TEXT
                {current_bookmark_column}
             );
             CREATE TABLE kobo_statistics (
                id INTEGER PRIMARY KEY,
                kobo_reading_state_id INTEGER NOT NULL REFERENCES kobo_reading_state(id),
                last_modified TEXT,
                remaining_time_minutes INTEGER,
                spent_reading_minutes INTEGER
             );
             CREATE TABLE kobo_bookmark (
                id INTEGER PRIMARY KEY,
                kobo_reading_state_id INTEGER NOT NULL REFERENCES kobo_reading_state(id),
                last_modified TEXT,
                location_source TEXT,
                location_type TEXT,
                location_value TEXT,
                progress_percent REAL,
                content_source_progress_percent REAL
             );
             CREATE TABLE book_read_link (
                id INTEGER PRIMARY KEY,
                book_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL REFERENCES user(id),
                read_status INTEGER NOT NULL,
                last_modified TEXT,
                last_time_started_reading TEXT,
                times_started_reading INTEGER NOT NULL,
                UNIQUE(book_id, user_id)
             );"
        ))
        .unwrap();
        conn.execute("INSERT INTO user (id, name) VALUES (1, 'reader')", [])
            .unwrap();
        conn
    }

    fn insert_state(conn: &Connection, id: i64, book_id: i64) {
        conn.execute(
            "INSERT INTO kobo_reading_state
                (id, user_id, book_id, last_modified, priority_timestamp, current_bookmark)
             VALUES (?1, 1, ?2, '2020-01-01 00:00:00.000000', ?3, NULL)",
            params![id, book_id, format!("priority-{id:04}")],
        )
        .unwrap();
    }

    fn insert_bookmark(conn: &Connection, state_id: i64, progress: f64) -> i64 {
        conn.execute(
            "INSERT INTO kobo_bookmark
                (kobo_reading_state_id, last_modified, location_source, location_type,
                 location_value, progress_percent, content_source_progress_percent)
             VALUES (?1, '2020-01-01 00:00:00.000000', 'Kobo', 'EPUB', 'location', ?2, ?2)",
            params![state_id, progress],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_statistics(conn: &Connection, state_id: i64) {
        conn.execute(
            "INSERT INTO kobo_statistics
                (kobo_reading_state_id, last_modified, remaining_time_minutes, spent_reading_minutes)
             VALUES (?1, '2020-01-01 00:00:00.000000', 12, 34)",
            params![state_id],
        )
        .unwrap();
    }

    fn insert_read_link(conn: &Connection, book_id: i64) {
        conn.execute(
            "INSERT INTO book_read_link
                (book_id, user_id, read_status, last_modified,
                 last_time_started_reading, times_started_reading)
             VALUES (?1, 1, 0, '2020-01-01 00:00:00.000000', NULL, 0)",
            params![book_id],
        )
        .unwrap();
    }

    fn data_snapshot(conn: &Connection) -> String {
        let queries = [
            "SELECT id, user_id, book_id, last_modified, priority_timestamp, current_bookmark FROM kobo_reading_state ORDER BY id",
            "SELECT id, kobo_reading_state_id, last_modified, remaining_time_minutes, spent_reading_minutes FROM kobo_statistics ORDER BY id",
            "SELECT id, kobo_reading_state_id, last_modified, location_source, location_type, location_value, progress_percent, content_source_progress_percent FROM kobo_bookmark ORDER BY id",
            "SELECT id, book_id, user_id, read_status, last_modified, last_time_started_reading, times_started_reading FROM book_read_link ORDER BY id",
        ];
        let mut snapshot = String::new();
        for query in queries {
            let mut stmt = conn.prepare(query).unwrap();
            let column_count = stmt.column_count();
            let rows = stmt
                .query_map([], |row| {
                    (0..column_count)
                        .map(|column| row.get::<_, Value>(column))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            snapshot.push_str(&format!("{rows:?}\n"));
        }
        snapshot
    }

    #[test]
    fn large_kobo_shelf_keeps_timestamps_and_does_not_create_states() {
        let mut conn = appdb_fixture(true);
        conn.execute(
            "INSERT INTO shelf
                (id, uuid, name, is_public, user_id, kobo_sync, created, last_modified)
             VALUES (1, 'shelf', 'Kobo', 0, 1, 1, 'created', 'shelf-modified')",
            [],
        )
        .unwrap();
        for book_id in 1..=224 {
            let timestamp = if book_id % 2 == 0 {
                Value::Text(format!("added-{book_id:03}"))
            } else {
                Value::Blob(format!("raw-{book_id:03}").into_bytes())
            };
            conn.execute(
                "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added)
                 VALUES (?1, 1, ?1, ?2)",
                params![book_id, timestamp],
            )
            .unwrap();
        }

        let before_links: Vec<(i64, Value)> = conn
            .prepare("SELECT book_id, date_added FROM book_shelf_link ORDER BY book_id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let before_shelf: Value = conn
            .query_row("SELECT last_modified FROM shelf WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();

        fix_kobo_sync_issues(&mut conn).unwrap();

        let after_links: Vec<(i64, Value)> = conn
            .prepare("SELECT book_id, date_added FROM book_shelf_link ORDER BY book_id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let after_shelf: Value = conn
            .query_row("SELECT last_modified FROM shelf WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let state_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM kobo_reading_state", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(before_links, after_links);
        assert_eq!(before_shelf, after_shelf);
        assert_eq!(state_count, 0);
    }

    #[test]
    fn repaired_states_page_once_with_strict_cursor_and_preserve_progress() {
        let mut conn = appdb_fixture(true);
        for state_id in 1..=125 {
            insert_state(&conn, state_id, state_id);
        }
        conn.execute(
            "UPDATE kobo_reading_state SET last_modified = NULL WHERE id = 2",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE kobo_reading_state SET priority_timestamp = NULL WHERE id = 3",
            [],
        )
        .unwrap();
        let bookmark_id = insert_bookmark(&conn, 1, 42.5);
        conn.execute(
            "UPDATE kobo_reading_state SET current_bookmark = ?1 WHERE id = 1",
            params![bookmark_id],
        )
        .unwrap();

        fix_kobo_sync_issues(&mut conn).unwrap();

        let timestamps: Vec<String> = conn
            .prepare("SELECT last_modified FROM kobo_reading_state ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(timestamps.len(), 125);
        assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(timestamps.iter().collect::<HashSet<_>>().len(), 125);
        let repaired_priority: String = conn
            .query_row(
                "SELECT priority_timestamp FROM kobo_reading_state WHERE id = 3",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(repaired_priority, timestamps[2]);

        let priority: String = conn
            .query_row(
                "SELECT priority_timestamp FROM kobo_reading_state WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let progress: f64 = conn
            .query_row(
                "SELECT progress_percent FROM kobo_bookmark WHERE id = ?1",
                params![bookmark_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(priority, "priority-0001");
        assert_eq!(progress, 42.5);

        for table in ["kobo_statistics", "kobo_bookmark", "book_read_link"] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 125, "unexpected count for {table}");
        }

        let mut cursor = "0001-01-01 00:00:00.000000".to_string();
        let mut seen = Vec::new();
        loop {
            let page: Vec<(i64, String)> = conn
                .prepare(
                    "SELECT id, last_modified FROM kobo_reading_state
                     WHERE last_modified > ?1 ORDER BY last_modified, id LIMIT 100",
                )
                .unwrap()
                .query_map(params![&cursor], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            if page.is_empty() {
                break;
            }
            cursor = page.last().unwrap().1.clone();
            seen.extend(page.into_iter().map(|(id, _)| id));
            assert!(seen.len() <= 125, "strict cursor repeated a repaired state");
        }
        assert_eq!(seen, (1..=125).collect::<Vec<_>>());
    }

    #[test]
    fn second_repair_is_a_data_noop_and_duplicates_are_cleaned() {
        let mut conn = appdb_fixture(true);
        insert_state(&conn, 1, 7);
        conn.execute(
            "UPDATE kobo_reading_state SET priority_timestamp = NULL WHERE id = 1",
            [],
        )
        .unwrap();
        insert_statistics(&conn, 1);
        let retained_bookmark_id = insert_bookmark(&conn, 1, 75.0);
        conn.execute(
            "UPDATE kobo_reading_state SET current_bookmark = ?1 WHERE id = 1",
            params![retained_bookmark_id],
        )
        .unwrap();
        insert_state(&conn, 2, 7);
        insert_statistics(&conn, 2);
        conn.execute(
            "UPDATE kobo_statistics
             SET last_modified = '2025-01-01 00:00:00.000000',
                 remaining_time_minutes = 99,
                 spent_reading_minutes = 88
             WHERE kobo_reading_state_id = 2",
            [],
        )
        .unwrap();
        let merged_bookmark_id = insert_bookmark(&conn, 2, 25.0);
        conn.execute(
            "UPDATE kobo_bookmark
             SET last_modified = '2024-01-01 00:00:00.000000'
             WHERE id = ?1",
            params![merged_bookmark_id],
        )
        .unwrap();

        fix_kobo_sync_issues(&mut conn).unwrap();
        let after_first = data_snapshot(&conn);
        fix_kobo_sync_issues(&mut conn).unwrap();
        let after_second = data_snapshot(&conn);

        assert_eq!(after_first, after_second);
        let state_ids: Vec<i64> = conn
            .prepare("SELECT id FROM kobo_reading_state ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(state_ids, vec![1]);
        let retained_priority: String = conn
            .query_row(
                "SELECT priority_timestamp FROM kobo_reading_state WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_priority, "priority-0002");
        let bookmarks: Vec<(i64, i64, f64)> = conn
            .prepare(
                "SELECT id, kobo_reading_state_id, progress_percent
                 FROM kobo_bookmark ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            bookmarks,
            vec![
                (retained_bookmark_id, 1, 75.0),
                (merged_bookmark_id, 1, 25.0)
            ]
        );
        let statistics: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), kobo_reading_state_id,
                        remaining_time_minutes, spent_reading_minutes
                 FROM kobo_statistics",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(statistics, (1, 1, 99, 88));
        let discarded_children: i64 = conn
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM kobo_statistics WHERE kobo_reading_state_id = 2) +
                    (SELECT COUNT(*) FROM kobo_bookmark WHERE kobo_reading_state_id = 2)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(discarded_children, 0);
    }

    #[test]
    fn child_insert_failure_rolls_back_data_and_keeps_foreign_keys_on() {
        let mut conn = appdb_fixture(true);
        insert_state(&conn, 1, 1);
        insert_statistics(&conn, 1);
        insert_read_link(&conn, 1);
        insert_state(&conn, 2, 2);
        let bookmark_id = insert_bookmark(&conn, 2, 5.0);
        conn.execute(
            "UPDATE kobo_reading_state SET current_bookmark = ?1 WHERE id = 2",
            params![bookmark_id],
        )
        .unwrap();
        insert_read_link(&conn, 2);
        conn.execute_batch(
            "CREATE TRIGGER fail_statistics_insert
             BEFORE INSERT ON kobo_statistics
             BEGIN
                SELECT RAISE(ABORT, 'injected statistics failure');
             END;",
        )
        .unwrap();
        let before = data_snapshot(&conn);

        let error = fix_kobo_sync_issues(&mut conn).unwrap_err();

        assert!(error.to_string().contains("injected statistics failure"));
        assert_eq!(data_snapshot(&conn), before);
        let foreign_keys: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn missing_current_bookmark_column_is_added_without_disabling_foreign_keys() {
        let mut conn = appdb_fixture(false);

        fix_kobo_sync_issues(&mut conn).unwrap();

        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(kobo_reading_state)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(columns.iter().any(|column| column == "current_bookmark"));
        let foreign_keys: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn duplicate_add_to_shelf_is_timestamp_preserving_and_creates_no_state() {
        let mut conn = appdb_fixture(true);
        conn.execute(
            "INSERT INTO shelf
                (id, uuid, name, is_public, user_id, kobo_sync, created, last_modified)
             VALUES (1, 'shelf', 'Kobo', 0, 1, 1, 'created', 'unchanged-shelf')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added)
             VALUES (9, 1, 1, 'unchanged-link')",
            [],
        )
        .unwrap();

        add_existing_book_to_shelf(&mut conn, 9, "Kobo", Some("reader")).unwrap();

        let values: (String, String) = conn
            .query_row(
                "SELECT s.last_modified, bsl.date_added
                 FROM shelf s JOIN book_shelf_link bsl ON bsl.shelf = s.id
                 WHERE s.id = 1 AND bsl.book_id = 9",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(values, ("unchanged-shelf".into(), "unchanged-link".into()));
        let state_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM kobo_reading_state", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_count, 0);
    }

    #[test]
    fn ordinary_batch_add_only_appends_memberships_with_strict_cursor_timestamps() {
        let mut conn = appdb_fixture(true);
        conn.execute(
            "INSERT INTO shelf
                (id, uuid, name, is_public, user_id, kobo_sync, created, last_modified)
             VALUES (1, 'shelf', 'KoboMelissa', 0, 1, 1,
                     '2020-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000')",
            [],
        )
        .unwrap();

        for book_id in 1..=224 {
            conn.execute(
                "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added)
                 VALUES (?1, 1, ?1, ?2)",
                params![
                    book_id,
                    format!("2020-01-01 00:00:00.{book_id:06}")
                ],
            )
            .unwrap();
        }
        for state_id in 1..=125 {
            conn.execute(
                "INSERT INTO kobo_reading_state
                    (id, user_id, book_id, last_modified, priority_timestamp, current_bookmark)
                 VALUES (?1, 1, ?1, NULL, NULL, NULL)",
                [state_id],
            )
            .unwrap();
        }

        let old_links: Vec<(i64, String)> = conn
            .prepare(
                "SELECT book_id, date_added FROM book_shelf_link
                 WHERE book_id <= 224 ORDER BY book_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for book_id in 225..=349 {
            add_book_to_shelf_in_appdb(
                &mut conn,
                book_id,
                "KoboMelissa",
                Some("reader"),
            )
            .unwrap();
        }

        let unchanged_links: Vec<(i64, String)> = conn
            .prepare(
                "SELECT book_id, date_added FROM book_shelf_link
                 WHERE book_id <= 224 ORDER BY book_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(unchanged_links, old_links);

        let added_timestamps: Vec<NaiveDateTime> = conn
            .prepare(
                "SELECT date_added FROM book_shelf_link
                 WHERE book_id >= 225 ORDER BY book_id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|timestamp| {
                NaiveDateTime::parse_from_str(
                    &timestamp.unwrap(),
                    "%Y-%m-%d %H:%M:%S%.f",
                )
                .unwrap()
            })
            .collect();
        assert_eq!(added_timestamps.len(), 125);
        assert!(added_timestamps.windows(2).all(|pair| pair[0] < pair[1]));

        let untouched_states: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM kobo_reading_state
                 WHERE last_modified IS NULL AND priority_timestamp IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(untouched_states, 125);
    }
}
