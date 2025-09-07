use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::{Connection, params};
use std::path::Path;

mod cli;
use cli::{Cli, Commands};
mod appdb;
mod epub;
mod calibre;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Validate library database file path for all commands
    if !cli.metadata_file.exists() {
        anyhow::bail!(
            "The specified library database file does not exist: {:?}",
            cli.metadata_file
        );
    }

    let mut calibre_conn = Connection::open(&cli.metadata_file)
        .with_context(|| format!("Failed to open Calibre database at {:?}", cli.metadata_file))?;

    // Add the custom title_sort function that Calibre's triggers need
    calibre::create_calibre_functions(&calibre_conn)?;

    match cli.command {
        Commands::Add { epub_file, appdb_file, shelf } => {
            let mut appdb_conn = appdb::open_appdb(appdb_file.as_deref())?;
            add_book_flow(&mut calibre_conn, appdb_conn.as_mut(), &cli.metadata_file, &epub_file, shelf.as_deref())?;
        }
        Commands::List { appdb_file } => {
            let appdb_conn = appdb::open_appdb(appdb_file.as_deref())?;
            calibre::list_books(&calibre_conn, appdb_conn.as_ref())?;
        }
        Commands::ListShelves { appdb_file } => {
            let appdb_conn = appdb::open_appdb(Some(&appdb_file))?;
            appdb::list_shelves(appdb_conn.as_ref())?;
        }
        Commands::Delete { book_id, appdb_file } => {
            let appdb_conn = appdb::open_appdb(appdb_file.as_deref())?;
            calibre::delete_book(&mut calibre_conn, appdb_conn.as_ref(), &cli.metadata_file, book_id)?;
        }
        Commands::CleanShelves { appdb_file } => {
            let appdb_conn = appdb::open_appdb(Some(&appdb_file))?;
            if let Some(conn) = appdb_conn {
                appdb::clean_empty_shelves(&conn, &calibre_conn)?;
            }
        }
        Commands::InspectDb => {
            let conn = Connection::open("myfiles/app.db")?;
            let mut stmt = conn.prepare("SELECT id, book_id, shelf FROM book_shelf_link")?;
            let rows = stmt.query_map(params![], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
            })?;

            println!("Content of book_shelf_link:");
            for row_result in rows {
                let (id, book_id, shelf) = row_result?;
                println!("id: {}, book_id: {}, shelf: {}", id, book_id, shelf);
            }
        }
    }

    Ok(())
}

/// Handles the flow for adding a new book.
fn add_book_flow(
    calibre_conn: &mut Connection,
    appdb_conn: Option<&mut Connection>,
    library_db_path: &Path,
    epub_file: &Path,
    shelf_name: Option<&str>,
) -> Result<()> {
    if !epub_file.exists() {
        anyhow::bail!("The specified EPUB file does not exist.");
    }

    println!("📚 Reading EPUB metadata...");
    let metadata = epub::get_epub_metadata(epub_file)?;
    println!(" -> Title: {}", metadata.title);
    println!(" -> Author: {}", metadata.author);

    println!("✒️ Writing to Calibre database...");
    let upsert_result = calibre::add_book_to_db(calibre_conn, &metadata)?;

    let (book_id, book_path, is_update) = match upsert_result {
        calibre::UpsertResult::Created { book_id, book_path } => {
            println!(
                " -> Successfully created database entry with Book ID: {}",
                book_id
            );
            (book_id, book_path, false)
        }
        calibre::UpsertResult::Updated { book_id, book_path } => {
            println!(
                " -> Successfully updated database entry for Book ID: {}",
                book_id
            );
            (book_id, book_path, true)
        }
    };

    // Clap's `requires` attribute ensures appdb_conn is Some if shelf_name is Some.
    if let (Some(name), Some(conn)) = (shelf_name, appdb_conn) {
    appdb::add_book_to_shelf_in_appdb(conn, book_id, name)?;
    }

    println!("🚚 Updating files in library...");
    let cover_saved = epub::update_book_files(library_db_path.parent().unwrap_or_else(|| Path::new(".")), epub_file, &book_path, is_update)?;
    println!(" -> File copied successfully.");

    if cover_saved {
        calibre_conn.execute("UPDATE books SET has_cover = 1 WHERE id = ?1", params![book_id])?;
        println!(" -> Updated database to reflect cover image.");
    }

    let action_str = if is_update { "updated in" } else { "added to" };
    println!("
✅ Success! '{}' has been {} your Calibre library.", metadata.title, action_str);

    println!("   Please restart Calibre to see the new book.");

    Ok(())
}