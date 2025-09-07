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
    let conn = appdb_conn.context("The --appdb-file argument is required to list shelves.")?;

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

    Ok(())
}

/// Adds a book to a shelf in the Calibre-Web database. Creates the shelf if it doesn't exist.
pub fn add_book_to_shelf_in_appdb(conn: &mut Connection, book_id: i64, shelf_name: &str) -> Result<()> {
    let tx = conn.transaction()?;

    // 1. Find or create the shelf
    let shelf_id: i64 = match tx.query_row(
        "SELECT id FROM shelf WHERE name = ?1",
        params![shelf_name],
        |row| row.get(0),
    ).optional()? {
        Some(id) => id,
        None => {
            // Shelf doesn't exist, create it.
            // Calibre-Web seems to require a user_id, we'll default to 1 (usually the admin).
            tx.execute(
                "INSERT INTO shelf (name, is_public, user_id) VALUES (?1, 0, 1)",
                params![shelf_name],
            )?;
            println!(" -> Created new shelf '{}'.", shelf_name);
            tx.last_insert_rowid()
        }
    };

    // 2. Check if the link already exists to prevent duplicates
    let link_exists: bool = tx.query_row(
        "SELECT 1 FROM book_shelf_link WHERE book_id = ?1 AND shelf = ?2",
        params![book_id, shelf_id],
        |_| Ok(true)
    ).optional()?.is_some();

    // 3. Link the book to the shelf only if it doesn't already exist
    if !link_exists {
        tx.execute("INSERT INTO book_shelf_link (book_id, shelf) VALUES (?1, ?2)", params![book_id, shelf_id])?;
        println!(" -> Added book to shelf '{}'.", shelf_name);
    } else {
        println!(" -> Book is already on shelf '{}'.", shelf_name);
    }

    tx.commit()?;

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