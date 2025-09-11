use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// Opens the app.db connection if a path is provided.
pub fn open_appdb(path: Option<&Path>) -> Result<Option<Connection>> {
    path.map(|appdb_path| {
        if !appdb_path.exists() {
            anyhow::bail!("The specified app.db file does not exist: {:?}", appdb_path);
        }
        Connection::open(appdb_path)
            .with_context(|| format!("Failed to open Calibre-Web database at {:?}", appdb_path))
    })
    .transpose()
}

/// Lists all unique shelves from the Calibre-Web app.db.
pub fn list_shelves(appdb_conn: Option<&Connection>) -> Result<()> {
    if let Some(conn) = appdb_conn {
        println!("📖 Finding available shelves from Calibre-Web...");

        let mut stmt = conn.prepare("SELECT id, name FROM shelf ORDER BY name")?;
        let shelves_iter = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?;
        let shelves: Vec<(i64, String)> = shelves_iter.collect::<Result<Vec<(i64, String)>, _>>()?;

        if shelves.is_empty() {
            println!("\nNo shelves found in the Calibre-Web database.");
        } else {
            println!("\nAvailable shelves:");
            for (id, shelf) in shelves {
                println!("- {} (ID: {})", shelf, id);
            }
        }
    } else {
        anyhow::bail!("The --appdb-file argument is required to list shelves.");
    }

    Ok(())
}

/// Adds a book to a shelf in the Calibre-Web database. Creates the shelf if it doesn't exist.
pub fn add_book_to_shelf_in_appdb(conn: &mut Connection, book_id: i64, shelf_name: &str, username: Option<&str>) -> Result<()> {
    let tx = conn.transaction()?;

    // Get the user_id, defaulting to admin (id=1) if no username is provided
    let user_id = if let Some(uname) = username {
        match tx.query_row(
            "SELECT id FROM user WHERE name = ?1",
            params![uname],
            |row| row.get::<_, i64>(0),
        ).optional()? {
            Some(id) => id,
            None => anyhow::bail!("User '{}' not found", uname),
        }
    } else {
        1 // Default admin user
    };

    // 1. Find or create the shelf
    let shelf_id: i64 = match tx.query_row(
        "SELECT id FROM shelf WHERE name = ?1 AND user_id = ?2",
        params![shelf_name, user_id],
        |row| row.get(0),
    ).optional()? {
        Some(id) => id,
        None => {
            // Shelf doesn't exist, create it for the specific user
            let now = chrono::Local::now().naive_local();
            tx.execute(
                "INSERT INTO shelf (name, is_public, user_id, created, last_modified) VALUES (?1, 0, ?2, ?3, ?4)",
                params![shelf_name, user_id, now, now],
            )?;
            println!(" -> Created new shelf '{}' for user {}.", shelf_name, 
                    username.unwrap_or("admin"));
            tx.last_insert_rowid()
        }
    };

    // 2. Check if the link already exists to prevent duplicates
    let link_exists: bool = tx.query_row(
        "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
        params![book_id, shelf_id],
        |_| Ok(true)
    ).optional()?.is_some();

    // 3. Get the next order value for this shelf
    let next_order: i64 = tx.query_row(
        "SELECT COALESCE(MAX(\"order\"), 0) + 1 FROM book_shelf_link WHERE shelf = ?1",
        params![shelf_id],
        |row| row.get(0)
    )?;

    // 4. Link the book to the shelf only if it doesn't already exist
    if !link_exists {
        // Format current time with microsecond precision
        let now = chrono::Local::now();
        
        tx.execute(
            "INSERT INTO book_shelf_link (book_id, shelf, \"order\", date_added) VALUES (?1, ?2, ?3, ?4)",
            params![book_id, shelf_id, next_order, &now.format("%Y-%m-%d %H:%M:%S.%6f").to_string()]
        )?;

        // Also update the shelf's last_modified timestamp
        tx.execute(
            "UPDATE shelf SET last_modified = ?1 WHERE id = ?2",
            params![now.naive_local(), shelf_id],
        )?;

        // Check if this is a Kobo sync shelf and add entries to Kobo tables
        let is_kobo_sync: bool = tx.query_row(
            "SELECT kobo_sync FROM shelf WHERE id = ?1",
            params![shelf_id],
            |row| row.get(0),
        ).unwrap_or(false);

        if is_kobo_sync {
            // Check if book is already in kobo_synced_books for this user
            let already_synced: bool = tx.query_row(
                "SELECT 1 FROM kobo_synced_books WHERE book_id = ?1 AND user_id = ?2",
                params![book_id, user_id],
                |_| Ok(true)
            ).optional()?.is_some();

            if !already_synced {
                // Add to kobo_synced_books
                tx.execute(
                    "INSERT INTO kobo_synced_books (user_id, book_id) VALUES (?1, ?2)",
                    params![user_id, book_id],
                )?;
                println!(" -> Added book to Kobo sync list for user.");
            }

            // Check if book has a kobo_reading_state entry for this user
            let has_reading_state: bool = tx.query_row(
                "SELECT 1 FROM kobo_reading_state WHERE book_id = ?1 AND user_id = ?2",
                params![book_id, user_id],
                |_| Ok(true)
            ).optional()?.is_some();

            if !has_reading_state {
                // Add initial reading state
                tx.execute(
                    "INSERT INTO kobo_reading_state (user_id, book_id, last_modified, priority_timestamp) VALUES (?1, ?2, ?3, ?4)",
                    params![user_id, book_id, now.naive_local(), now.naive_local()],
                )?;
                println!(" -> Created Kobo reading state for user.");
            }
        }

        println!(" -> Added book to shelf '{}'.", shelf_name);
    } else {
        println!(" -> Book is already on shelf '{}'.", shelf_name);
    }

    tx.commit()?;

    Ok(())
}

/// Inspects the database contents, showing relationships between books and shelves
pub fn inspect_databases(appdb_conn: Option<&Connection>, calibre_conn: &Connection) -> Result<()> {
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
            Ok(row.get::<_, i64>("book_id")?)
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
                Ok(row.get::<_, i64>("id")?)
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

pub fn clean_empty_shelves(appdb_conn: &Connection, calibre_conn: &Connection) -> Result<()> {
    println!("🧹 Cleaning empty shelves from Calibre-Web...");

    let mut calibre_check_stmt = calibre_conn.prepare("SELECT 1 FROM books WHERE id = ?1")?;

    let mut stmt = appdb_conn.prepare("SELECT id, name FROM shelf")?;
    let shelf_iter = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    for shelf_result in shelf_iter {
        let (shelf_id, shelf_name) = shelf_result?;

        // Select the link's primary key and the book_id
        let mut link_stmt = appdb_conn.prepare("SELECT id, book_id FROM book_shelf_link WHERE shelf = ?1")?;
        let book_link_iter = link_stmt.query_map(params![shelf_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;

        let mut orphaned_links_found = 0;
        for book_link_result in book_link_iter {
            let (link_id, book_id) = book_link_result?;
            
            let exists: bool = calibre_check_stmt.query_row(params![book_id], |_| Ok(true)).optional()?.is_some();
            
            if !exists {
                // Delete the specific orphan link by its own id
                appdb_conn.execute("DELETE FROM book_shelf_link WHERE id = ?1", params![link_id])?;
                orphaned_links_found += 1;
            }
        }

        if orphaned_links_found > 0 {
            println!(" -> Found and removed {} orphaned book links for shelf '{}'.", orphaned_links_found, shelf_name);
        }

        // After cleaning orphans, check if the shelf is now empty
        let count: i64 = appdb_conn.query_row("SELECT COUNT(*) FROM book_shelf_link WHERE shelf = ?1", params![shelf_id], |row| row.get(0))?;

        if count == 0 {
            appdb_conn.execute("DELETE FROM shelf WHERE id = ?1", params![shelf_id])?;
            println!(" -> Removed empty shelf '{}'.", shelf_name);
        }
    }

    println!("✅ Shelf cleaning complete.");
    Ok(())
}

/// Diagnoses and fixes Kobo sync issues for existing shelf links
pub fn fix_kobo_sync_issues(appdb_conn: &mut Connection) -> Result<()> {
    println!("🔧 Diagnosing and fixing Kobo sync issues...");
    
    let tx = appdb_conn.transaction()?;
    
    // Find all books on Kobo sync shelves that aren't properly set up for sync
    let mut stmt = tx.prepare(
        "SELECT DISTINCT bsl.book_id, s.id as shelf_id, s.user_id, u.name as username
         FROM book_shelf_link bsl
         JOIN shelf s ON bsl.shelf = s.id
         LEFT JOIN user u ON s.user_id = u.id
         WHERE s.kobo_sync = 1"
    )?;
    
    let books_on_kobo_shelves = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>("book_id")?,
            row.get::<_, i64>("shelf_id")?,
            row.get::<_, i64>("user_id")?,
            row.get::<_, Option<String>>("username")?,
        ))
    })?;
    
    // Collect results before dropping the statement
    let books_to_process: Vec<_> = books_on_kobo_shelves.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    
    let mut fixed_sync_entries = 0;
    let mut fixed_reading_state = 0;
    let mut fixed_timestamps = 0;
    
    for (book_id, shelf_id, user_id, username) in books_to_process {
        let username = username.unwrap_or_else(|| "unknown".to_string());
        
        // Check if book is in kobo_synced_books
        let in_sync_table: bool = tx.query_row(
            "SELECT 1 FROM kobo_synced_books WHERE book_id = ?1 AND user_id = ?2",
            params![book_id, user_id],
            |_| Ok(true)
        ).optional()?.is_some();
        
        if !in_sync_table {
            tx.execute(
                "INSERT INTO kobo_synced_books (user_id, book_id) VALUES (?1, ?2)",
                params![user_id, book_id],
            )?;
            println!(" -> Added book {} to Kobo sync list for user {}", book_id, username);
            fixed_sync_entries += 1;
        }
        
        // Check if book has reading state
        let reading_state_id: Option<i64> = tx.query_row(
            "SELECT id FROM kobo_reading_state WHERE book_id = ?1 AND user_id = ?2",
            params![book_id, user_id],
            |row| row.get(0)
        ).optional()?;
        
        let now = chrono::Local::now().naive_local();
        
        if let Some(state_id) = reading_state_id {
            // Check if timestamps need standardization
            let current_timestamp: String = tx.query_row(
                "SELECT last_modified FROM kobo_reading_state WHERE id = ?1",
                params![state_id],
                |row| row.get(0)
            )?;
            
            // If timestamp doesn't have proper microsecond precision, update it
            if !current_timestamp.contains('.') || current_timestamp.len() < 26 {
                tx.execute(
                    "UPDATE kobo_reading_state SET last_modified = ?1, priority_timestamp = ?2 WHERE id = ?3",
                    params![now, now, state_id],
                )?;
                println!(" -> Standardized timestamps for book {} reading state", book_id);
                fixed_timestamps += 1;
            }
        } else {
            // Create new reading state
            tx.execute(
                "INSERT INTO kobo_reading_state (user_id, book_id, last_modified, priority_timestamp) VALUES (?1, ?2, ?3, ?4)",
                params![user_id, book_id, now, now],
            )?;
            println!(" -> Created Kobo reading state for book {} for user {}", book_id, username);
            fixed_reading_state += 1;
        }
        
        // Update the shelf's last_modified timestamp to trigger sync detection
        tx.execute(
            "UPDATE shelf SET last_modified = ?1 WHERE id = ?2",
            params![now, shelf_id],
        )?;
    }
    
    // Also check and fix any kobo_reading_state entries that have inconsistent timestamps
    let orphaned_states = tx.execute(
        "UPDATE kobo_reading_state 
         SET last_modified = priority_timestamp 
         WHERE last_modified IS NULL AND priority_timestamp IS NOT NULL",
        [],
    )?;
    
    if orphaned_states > 0 {
        println!(" -> Fixed {} reading states with NULL last_modified", orphaned_states);
        fixed_timestamps += orphaned_states;
    }
    
    let orphaned_priorities = tx.execute(
        "UPDATE kobo_reading_state 
         SET priority_timestamp = last_modified 
         WHERE priority_timestamp IS NULL AND last_modified IS NOT NULL",
        [],
    )?;
    
    if orphaned_priorities > 0 {
        println!(" -> Fixed {} reading states with NULL priority_timestamp", orphaned_priorities);
        fixed_timestamps += orphaned_priorities;
    }
    
    tx.commit()?;
    
    if fixed_sync_entries > 0 || fixed_reading_state > 0 || fixed_timestamps > 0 {
        println!("✅ Fixed {} sync entries, {} reading states, and {} timestamps.", fixed_sync_entries, fixed_reading_state, fixed_timestamps);
        println!("🔄 Shelf timestamps updated - try a Kobo sync now.");
    } else {
        println!("✅ No Kobo sync issues found.");
    }
    
    Ok(())
}

/// Provides detailed diagnostics for Kobo sync setup
pub fn diagnose_kobo_sync(appdb_conn: &Connection, calibre_conn: &Connection) -> Result<()> {
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
            row.get::<_, String>("username")?,
            row.get::<_, String>("created")?,
            row.get::<_, String>("last_modified")?,
            row.get::<_, i64>("book_count")?,
        ))
    })?;
    
    for shelf_result in shelf_rows {
        let (shelf_id, shelf_name, username, created, last_modified, book_count) = shelf_result?;
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
