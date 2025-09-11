use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::{Connection, params};
use std::path::Path;

mod cli;
use cli::{Cli, Commands};
mod appdb;
mod epub;
mod calibre;
mod cleanup;
mod timestamp;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let metadata_file = cli.metadata_file.context("--metadata-file is required")?;

    // Validate library database file path for all commands
    if !metadata_file.exists() {
        anyhow::bail!(
            "The specified library database file does not exist: {:?}",
            metadata_file
        );
    }

    let mut calibre_conn = Connection::open(&metadata_file)
        .with_context(|| format!("Failed to open Calibre database at {:?}", metadata_file))?;

    // Add the custom title_sort function that Calibre's triggers need
    calibre::create_calibre_functions(&calibre_conn)?;

    let mut appdb_conn = appdb::open_appdb(cli.appdb_file.as_deref())?;

    // Verify and repair any NULL timestamps in both databases
    timestamp::verify_and_repair_timestamps(&mut calibre_conn, appdb_conn.as_mut())?;

    match cli.command {
        Commands::Add { shelf, username } => {
            if shelf.is_some() && cli.appdb_file.is_none() {
                anyhow::bail!("--appdb-file is required when specifying a shelf");
            }
            let epub_file = cli.epub_file.context("--epub-file is required for the add command")?;
            add_book_flow(&mut calibre_conn, appdb_conn.as_mut(), &metadata_file, &epub_file, shelf.as_deref(), username.as_deref())?;
        }
        Commands::List { shelf, verbose } => {
            calibre::list_books(&calibre_conn, appdb_conn.as_ref(), shelf.as_deref(), verbose)?;
        }
        Commands::ListShelves => {
            appdb::list_shelves(appdb_conn.as_ref())?;
        }
        Commands::Delete { book_id } => {
            calibre::delete_book(&mut calibre_conn, appdb_conn.as_ref(), &metadata_file, book_id)?;
        }
        Commands::CleanShelves => {
            if let Some(conn) = appdb_conn {
                appdb::clean_empty_shelves(&conn, &calibre_conn)?;
            }
        }
        Commands::InspectDb => {
            appdb::inspect_databases(appdb_conn.as_ref(), &calibre_conn)?;
        }
        Commands::CleanDb => {
            cleanup::cleanup_databases(&mut calibre_conn, appdb_conn.as_mut(), &metadata_file.parent().unwrap_or_else(|| Path::new(".")).to_path_buf())?;
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
    username: Option<&str>,
) -> Result<()> {
    if !epub_file.exists() {
        anyhow::bail!("The specified EPUB file does not exist.");
    }

    println!("📚 Reading EPUB metadata...");
    let metadata = epub::get_epub_metadata(epub_file)?;

    // Language code was already normalized in get_epub_metadata

    println!(" -> Title: {}", metadata.title);
    println!(" -> Author: {}", metadata.author);
    if let Some(series) = &metadata.series {
        println!(" -> Series: {} {}", series, 
            metadata.series_index.map_or(String::new(), |idx| format!("#{}", idx)));
    }
    if let Some(publisher) = &metadata.publisher {
        println!(" -> Publisher: {}", publisher);
    }
    if let Some(pubdate) = metadata.pubdate {
        println!(" -> Published: {}", pubdate.format("%Y-%m-%d"));
    }

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
        appdb::add_book_to_shelf_in_appdb(conn, book_id, name, username)?;
    }

    println!("🚚 Updating files in library...");
    let cover_saved = epub::update_book_files(library_db_path.parent().unwrap_or_else(|| Path::new(".")), epub_file, &book_path, is_update)?;
    println!(" -> File copied successfully.");

    if cover_saved {
        calibre_conn.execute("UPDATE books SET has_cover = 1 WHERE id = ?1", params![book_id])?;
        println!(" -> Updated database to reflect cover image.");
    }

    let action_str = if is_update { "updated in" } else { "added to" };
    // Check series status for feedback message
    let series_msg = if let Some(series) = &metadata.series {
        format!(" (part of series '{}'{})'", series,
            metadata.series_index.map_or(String::new(), |idx| format!(" #{}", idx)))
    } else {
        String::new()
    };

    println!("
✅ Success! '{}'{} has been {} your Calibre library.",
        metadata.title, series_msg, action_str);

    println!("   Please restart Calibre to see the new book.");

    Ok(())
}