use crate::utils::{format_timestamp_micro, validate_id};
use anyhow::{Context, Result};
use chrono::{Duration, NaiveDateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShelfTarget {
    pub(crate) user_id: i64,
    pub(crate) shelf_id: i64,
    pub(crate) shelf_name: String,
    pub(crate) username: String,
    pub(crate) kobo_sync: bool,
}

pub(crate) fn revalidate_shelf_target(
    conn: &Connection,
    target: &ShelfTarget,
) -> Result<ShelfTarget> {
    let current = conn
        .query_row(
            "SELECT u.id, u.name, s.id, s.name, s.kobo_sync
             FROM user u JOIN shelf s ON s.user_id = u.id
             WHERE u.id = ?1 AND u.name = ?2
               AND s.id = ?3 AND s.user_id = ?1 AND s.name = ?4",
            params![
                target.user_id,
                target.username,
                target.shelf_id,
                target.shelf_name
            ],
            |row| {
                Ok(ShelfTarget {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    shelf_id: row.get(2)?,
                    shelf_name: row.get(3)?,
                    kobo_sync: row.get::<_, i64>(4)? == 1,
                })
            },
        )
        .optional()?
        .context("Stored shelf or user identity no longer matches Calibre-Web")?;
    Ok(current)
}

pub(crate) fn resolve_existing_shelf(
    conn: &Connection,
    shelf_name: &str,
    username: Option<&str>,
) -> Result<ShelfTarget> {
    if shelf_name.trim().is_empty() {
        anyhow::bail!("Shelf name cannot be empty");
    }
    let (user_id, resolved_username): (i64, String) = if let Some(name) = username {
        conn.query_row("SELECT id, name FROM user WHERE name = ?1", [name], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?
        .with_context(|| format!("User '{}' not found", name))?
    } else {
        conn.query_row("SELECT id, name FROM user WHERE id = 1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?
        .context("Default admin user with ID 1 not found")?
    };
    let (shelf_id, kobo_sync): (i64, i64) = conn
        .query_row(
            "SELECT id, kobo_sync FROM shelf WHERE name = ?1 AND user_id = ?2",
            params![shelf_name, user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .with_context(|| {
            format!(
                "Shelf '{}' for user '{}' does not exist; create it explicitly in Calibre-Web",
                shelf_name, resolved_username
            )
        })?;
    Ok(ShelfTarget {
        user_id,
        shelf_id,
        shelf_name: shelf_name.to_owned(),
        username: resolved_username,
        kobo_sync: kobo_sync == 1,
    })
}

pub(crate) fn shelf_has_book(
    conn: &Connection,
    target: &ShelfTarget,
    book_id: i64,
) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
            params![book_id, target.shelf_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

pub(crate) fn add_book_to_resolved_shelf(
    conn: &mut Connection,
    target: &ShelfTarget,
    book_id: i64,
) -> Result<bool> {
    validate_id(book_id, "book")?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .context("Failed to start shelf operation transaction")?;
    let current = revalidate_shelf_target(&tx, target)?;
    if current.kobo_sync != target.kobo_sync {
        anyhow::bail!("Resolved shelf Kobo status changed before it could be updated");
    }
    let exists = tx
        .query_row(
            "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
            params![book_id, target.shelf_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        tx.commit()?;
        return Ok(false);
    }
    let next_order: i64 = tx.query_row(
        "SELECT COALESCE(MAX(\"order\"), 0) + 1 FROM book_shelf_link WHERE shelf = ?1",
        [target.shelf_id],
        |row| row.get(0),
    )?;
    let timestamp = next_shelf_link_timestamp(&tx, target.user_id)?;
    tx.execute(
        "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added)
         VALUES (?1, ?2, ?3, ?4)",
        params![book_id, target.shelf_id, next_order, &timestamp],
    )?;
    let changed = tx.execute(
        "UPDATE shelf SET last_modified = ?1 WHERE id = ?2 AND user_id = ?3",
        params![&timestamp, target.shelf_id, target.user_id],
    )?;
    if changed != 1 {
        anyhow::bail!("Resolved shelf disappeared while adding book {book_id}");
    }
    tx.commit()?;
    Ok(true)
}

pub(crate) fn user_has_synced_book(conn: &Connection, user_id: i64, book_id: i64) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM kobo_synced_books WHERE user_id = ?1 AND book_id = ?2",
            params![user_id, book_id],
            |_| Ok(()),
        )
        .optional()
        .context("Calibre-Web kobo_synced_books schema is unsupported")?
        .is_some())
}

pub(crate) fn planned_existing_book_key(
    conn: &Connection,
    target: &ShelfTarget,
    book_id: i64,
) -> Result<Option<String>> {
    if shelf_has_book(conn, target, book_id)? {
        return Ok(None);
    }
    if target.kobo_sync && user_has_synced_book(conn, target.user_id, book_id)? {
        anyhow::bail!(
            "Book {} is already synced for user '{}' and cannot be newly added to Kobo shelf '{}' safely",
            book_id,
            target.username,
            target.shelf_name
        );
    }
    Ok(Some(format!("book:{book_id}")))
}

/// Opens the app.db connection if a path is provided.
pub(crate) fn open_appdb(path: Option<&Path>) -> Result<Option<Connection>> {
    path.map(crate::db::open_appdb).transpose()
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
             ORDER BY u.name, s.name",
        )?;

        let shelves_iter = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,            // shelf id
                row.get::<_, String>(1)?,         // shelf name
                row.get::<_, i64>(2)?,            // kobo_sync
                row.get::<_, Option<String>>(3)?, // username (may be NULL)
                row.get::<_, i64>(4)?,            // book_count
            ))
        })?;

        let shelves: Vec<(i64, String, i64, Option<String>, i64)> =
            shelves_iter.collect::<Result<Vec<_>, _>>()?;

        if shelves.is_empty() {
            println!("\nNo shelves found in the Calibre-Web database.");
        } else {
            println!("\nAvailable shelves:");
            for (id, shelf_name, kobo_sync, username, book_count) in shelves {
                let user_display = username.unwrap_or_else(|| "Unknown".to_string());
                let kobo_indicator = if kobo_sync == 1 { " [Kobo]" } else { "" };
                let book_text = if book_count == 1 { "book" } else { "books" };
                println!(
                    "- {} (ID: {}) - User: {}{} - {} {}",
                    shelf_name, id, user_display, kobo_indicator, book_count, book_text
                );
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
        match tx
            .query_row(
                "SELECT id FROM user WHERE name = ?1",
                params![uname],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            Some(id) => Ok(id),
            None => anyhow::bail!("User '{}' not found", uname),
        }
    } else {
        Ok(1) // Default admin user
    }
}

/// Finds an existing shelf for the given user. Ordinary add never creates shelves.
fn find_shelf(
    tx: &rusqlite::Transaction,
    shelf_name: &str,
    user_id: i64,
    username: Option<&str>,
) -> Result<i64> {
    match tx
        .query_row(
            "SELECT id FROM shelf WHERE name = ?1 AND user_id = ?2",
            params![shelf_name, user_id],
            |row| row.get(0),
        )
        .optional()?
    {
        Some(id) => Ok(id),
        None => anyhow::bail!(
            "Shelf '{}' for user '{}' does not exist; create it explicitly in Calibre-Web",
            shelf_name,
            username.unwrap_or("admin")
        ),
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
            if let Ok(parsed) = NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%d %H:%M:%S%.f") {
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
fn add_book_to_shelf_core(
    conn: &mut Connection,
    book_id: i64,
    shelf_name: &str,
    username: Option<&str>,
    allow_duplicates: bool,
) -> Result<bool> {
    validate_id(book_id, "book").context("Invalid book ID for shelf operation")?;

    if shelf_name.trim().is_empty() {
        anyhow::bail!("Shelf name cannot be empty");
    }

    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .context("Failed to start shelf operation transaction")?;

    let user_id =
        resolve_user_id(&tx, username).context("Failed to resolve user ID for shelf operation")?;
    let shelf_id = find_shelf(&tx, shelf_name, user_id, username)
        .with_context(|| format!("Failed to find shelf '{}'", shelf_name))?;

    // Check if the link already exists to prevent duplicates
    let link_exists: bool = tx
        .query_row(
            "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
            params![book_id, shelf_id],
            |_| Ok(true),
        )
        .optional()
        .with_context(|| {
            format!(
                "Failed to check if book {} is already on shelf {}",
                book_id, shelf_id
            )
        })?
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
        |row| row.get(0),
    )?;

    // Insert the book-shelf link with UTC timestamp (matches Calibre-Web's datetime.now(timezone.utc))
    let now_micro = next_shelf_link_timestamp(&tx, user_id)?;

    tx.execute(
        "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added) VALUES (?1, ?2, ?3, ?4)",
        params![book_id, shelf_id, next_order, &now_micro]
    )?;

    // Update the shelf's last_modified timestamp (matches Calibre-Web's shelf.last_modified = datetime.now(timezone.utc))
    let changed = tx.execute(
        "UPDATE shelf SET last_modified = ?1 WHERE id = ?2",
        params![&now_micro, shelf_id],
    )?;
    if changed != 1 {
        anyhow::bail!("Shelf {shelf_id} disappeared during update");
    }

    tx.commit()
        .context("Failed to commit shelf link transaction")?;
    Ok(true)
}

/// Inspects the database contents, showing relationships between books and shelves
pub(crate) fn inspect_databases(
    appdb_conn: Option<&Connection>,
    calibre_conn: &Connection,
) -> Result<()> {
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
             ORDER BY s.name",
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
            println!(
                "  Owner: {}",
                username.unwrap_or_else(|| "Unknown".to_string())
            );
            println!("  Public: {}", if is_public { "Yes" } else { "No" });

            // Get book IDs from this shelf
            let mut book_stmt = conn
                .prepare("SELECT book_id FROM book_shelf_link WHERE shelf = ? ORDER BY book_id")?;

            let book_ids: Vec<i64> = book_stmt
                .query_map(params![shelf_id], |row| row.get::<_, i64>("book_id"))?
                .collect::<Result<Vec<_>, _>>()?;

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
                let params_vec: Vec<&dyn rusqlite::ToSql> = book_ids
                    .iter()
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

    let book_count: i64 =
        calibre_conn.query_row("SELECT COUNT(*) FROM books", params![], |row| row.get(0))?;
    let author_count: i64 =
        calibre_conn.query_row("SELECT COUNT(*) FROM authors", params![], |row| row.get(0))?;
    let series_count: i64 =
        calibre_conn.query_row("SELECT COUNT(*) FROM series", params![], |row| row.get(0))?;

    println!("Total Books: {}", book_count);
    println!("Total Authors: {}", author_count);
    println!("Total Series: {}", series_count);

    if book_count > 0 {
        println!("\nRecent Books:");
        let mut recent_stmt = calibre_conn.prepare(
            "SELECT title, author_sort, timestamp 
             FROM books 
             ORDER BY timestamp DESC 
             LIMIT 5",
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
        let mut orphaned_stmt =
            conn.prepare("SELECT DISTINCT book_id FROM book_shelf_link ORDER BY book_id")?;

        let orphaned_books: Vec<i64> = orphaned_stmt
            .query_map(params![], |row| row.get::<_, i64>("book_id"))?
            .collect::<Result<Vec<_>, _>>()?;

        if !orphaned_books.is_empty() {
            let placeholders = orphaned_books
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");
            let query = format!("SELECT id FROM books WHERE id IN ({})", placeholders);

            let mut cal_stmt = calibre_conn.prepare(&query)?;
            let params_vec: Vec<&dyn rusqlite::ToSql> = orphaned_books
                .iter()
                .map(|id| id as &dyn rusqlite::ToSql)
                .collect();

            let existing_books: std::collections::HashSet<i64> = cal_stmt
                .query_map(&params_vec[..], |row| row.get::<_, i64>("id"))?
                .collect::<Result<_, _>>()?;

            let missing_books: Vec<_> = orphaned_books
                .iter()
                .filter(|id| !existing_books.contains(id))
                .collect();

            if !missing_books.is_empty() {
                println!("\n⚠️  Warning: Found shelf links to non-existent books:");
                for book_id in missing_books {
                    println!("   - Book ID: {}", book_id);
                }
                println!(
                    "\nYou can use the 'clean-shelves' command to remove these orphaned links."
                );
            }
        }
    }

    println!("\n");
    Ok(())
}

pub(crate) fn clean_empty_shelves(
    appdb_conn: &mut Connection,
    calibre_conn: &Connection,
) -> Result<()> {
    println!("🧹 Cleaning empty shelves from Calibre-Web...");

    let mut calibre_check_stmt = calibre_conn
        .prepare("SELECT 1 FROM books WHERE id = ?1")
        .context("Failed to prepare book existence check query")?;

    // Collect shelf data up-front so the borrow on appdb_conn is released before the transaction
    let shelves: Vec<(i64, String)> = {
        let mut stmt = appdb_conn
            .prepare("SELECT id, name FROM shelf")
            .context("Failed to prepare shelf query")?;
        stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };

    // Collect all orphaned link IDs and empty shelf IDs before mutating
    let mut orphan_link_ids: Vec<(i64, String)> = Vec::new();
    let mut empty_shelf_ids: Vec<(i64, String)> = Vec::new();

    for (shelf_id, shelf_name) in &shelves {
        let links: Vec<(i64, i64)> = {
            let mut link_stmt =
                appdb_conn.prepare("SELECT id, book_id FROM book_shelf_link WHERE shelf = ?1")?;
            link_stmt
                .query_map(params![shelf_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut orphaned_count = 0;
        for (link_id, book_id) in links {
            let exists: bool = calibre_check_stmt
                .query_row(params![book_id], |_| Ok(true))
                .optional()?
                .is_some();
            if !exists {
                orphan_link_ids.push((link_id, shelf_name.clone()));
                orphaned_count += 1;
            }
        }

        if orphaned_count > 0 {
            println!(
                " -> Found {} orphaned book links for shelf '{}'.",
                orphaned_count, shelf_name
            );
        }
    }

    // Now perform all deletes inside a single transaction
    let tx = appdb_conn
        .transaction()
        .context("Failed to start shelf cleanup transaction")?;

    for (link_id, _shelf_name) in &orphan_link_ids {
        tx.execute(
            "DELETE FROM book_shelf_link WHERE id = ?1",
            params![link_id],
        )?;
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
        println!(
            "  - {} (ID: {}) - Kobo only shelves: {}",
            username,
            user_id,
            kobo_only.unwrap_or(0) == 1
        );
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
         GROUP BY s.id",
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
        println!(
            "  - {} (ID: {}) - Owner: {} - Books: {}",
            shelf_name, shelf_id, username, book_count
        );
        println!(
            "    Created: {} | Last Modified: {}",
            created, last_modified
        );

        // Show books on this shelf
        let mut book_stmt = appdb_conn.prepare(
            "SELECT bsl.book_id, bsl.date_added, bsl.\"order\"
             FROM book_shelf_link bsl 
             WHERE bsl.shelf = ?1 
             ORDER BY bsl.\"order\"",
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
            let book_title: String = calibre_conn
                .query_row("SELECT title FROM books WHERE id = ?1", [book_id], |row| {
                    row.get(0)
                })
                .unwrap_or_else(|_| format!("Unknown (ID: {})", book_id));

            // Check sync status
            let in_sync_table: bool = appdb_conn
                .query_row(
                    "SELECT 1 FROM kobo_synced_books WHERE book_id = ?1",
                    [book_id],
                    |_| Ok(true),
                )
                .optional()?
                .is_some();

            let has_reading_state: bool = appdb_conn
                .query_row(
                    "SELECT 1 FROM kobo_reading_state WHERE book_id = ?1",
                    [book_id],
                    |_| Ok(true),
                )
                .optional()?
                .is_some();

            let sync_status = match (in_sync_table, has_reading_state) {
                (true, true) => "✅ Full sync setup",
                (true, false) => "⚠️  Missing reading state",
                (false, true) => "⚠️  Missing sync entry",
                (false, false) => "❌ No sync setup",
            };

            println!(
                "    [{}] {} - {} (Added: {})",
                order, book_title, sync_status, date_added
            );
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
pub(crate) fn add_existing_book_to_shelf(
    conn: &mut Connection,
    book_id: i64,
    shelf_name: &str,
    username: Option<&str>,
) -> Result<()> {
    // Validate book ID
    validate_id(book_id, "book").context("Cannot add book to shelf: invalid book ID")?;

    // Note: We can't validate against metadata.db here since we only have app.db connection
    // The caller should ensure the book exists in the Calibre database

    let was_added = add_book_to_shelf_core(conn, book_id, shelf_name, username, false)?;

    if was_added {
        println!(
            "✅ Successfully added book {} to shelf '{}'.",
            book_id, shelf_name
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
                params![book_id, format!("2020-01-01 00:00:00.{book_id:06}")],
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
            assert!(
                add_book_to_shelf_core(&mut conn, book_id, "KoboMelissa", Some("reader"), true,)
                    .unwrap()
            );
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
                NaiveDateTime::parse_from_str(&timestamp.unwrap(), "%Y-%m-%d %H:%M:%S%.f").unwrap()
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
