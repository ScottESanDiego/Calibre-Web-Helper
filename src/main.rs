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

    // For some commands, metadata_file is not required
    let needs_metadata = !matches!(cli.command, Commands::FixKoboSync | Commands::AddToShelf { .. });
    
    let metadata_file = if needs_metadata {
        Some(cli.metadata_file.context("--metadata-file is required")?)
    } else {
        cli.metadata_file
    };

    // Validate library database file path for commands that need it
    if let Some(ref metadata_file) = metadata_file {
        if !metadata_file.exists() {
            anyhow::bail!(
                "The specified library database file does not exist: {:?}",
                metadata_file
            );
        }
    }

    let mut calibre_conn = if let Some(ref metadata_file) = metadata_file {
        Some(Connection::open(metadata_file)
            .with_context(|| format!("Failed to open Calibre database at {:?}", metadata_file))?)
    } else {
        None
    };

    // Add the custom title_sort function that Calibre's triggers need
    if let Some(ref conn) = calibre_conn {
        calibre::create_calibre_functions(conn)?;
    }

    let mut appdb_conn = appdb::open_appdb(cli.appdb_file.as_deref())?;

    // Verify and repair any NULL timestamps in both databases
    if let Some(ref mut conn) = calibre_conn {
        timestamp::verify_and_repair_timestamps(conn, appdb_conn.as_mut())?;
    }

    match cli.command {
        Commands::Add { shelf, username } => {
            let calibre_conn = calibre_conn.as_mut().context("--metadata-file is required for add command")?;
            let metadata_file = metadata_file.as_ref().unwrap();
            if shelf.is_some() && cli.appdb_file.is_none() {
                anyhow::bail!("--appdb-file is required when specifying a shelf");
            }
            let epub_file = cli.epub_file.context("--epub-file is required for the add command")?;
            add_book_flow(calibre_conn, appdb_conn.as_mut(), metadata_file, &epub_file, shelf.as_deref(), username.as_deref())?;
        }
        Commands::List { shelf, verbose } => {
            let calibre_conn = calibre_conn.as_ref().context("--metadata-file is required for list command")?;
            calibre::list_books(calibre_conn, appdb_conn.as_ref(), shelf.as_deref(), verbose)?;
        }
        Commands::ListShelves => {
            appdb::list_shelves(appdb_conn.as_ref())?;
        }
        Commands::Delete { book_id } => {
            let calibre_conn = calibre_conn.as_mut().context("--metadata-file is required for delete command")?;
            let metadata_file = metadata_file.as_ref().unwrap();
            calibre::delete_book(calibre_conn, appdb_conn.as_ref(), metadata_file, book_id)?;
        }
        Commands::CleanShelves => {
            let calibre_conn = calibre_conn.as_ref().context("--metadata-file is required for clean-shelves command")?;
            if let Some(conn) = appdb_conn {
                appdb::clean_empty_shelves(&conn, calibre_conn)?;
            }
        }
        Commands::InspectDb => {
            let calibre_conn = calibre_conn.as_ref().context("--metadata-file is required for inspect-db command")?;
            appdb::inspect_databases(appdb_conn.as_ref(), calibre_conn)?;
        }
        Commands::CleanDb => {
            let calibre_conn = calibre_conn.as_mut().context("--metadata-file is required for clean-db command")?;
            let metadata_file = metadata_file.as_ref().unwrap();
            cleanup::cleanup_databases(calibre_conn, appdb_conn.as_mut(), &metadata_file.parent().unwrap_or_else(|| Path::new(".")).to_path_buf())?;
        }
        Commands::FixKoboSync => {
            if let Some(mut conn) = appdb_conn {
                appdb::fix_kobo_sync_issues(&mut conn)?;
            } else {
                anyhow::bail!("--appdb-file is required for the fix-kobo-sync command");
            }
        }
        Commands::DiagnoseKoboSync => {
            let metadata_path = metadata_file.as_ref().context("metadata-file is required")?;
            let appdb_path = cli.appdb_file.as_ref().context("appdb-file is required")?;
            
            appdb::diagnose_kobo_sync(appdb_path.to_str().unwrap(), metadata_path.to_str().unwrap())
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
        Commands::AddToShelf { book_id, shelf, username } => {
            let appdb_path = cli.appdb_file.as_ref().context("appdb-file is required")?;
            let mut appdb_conn = appdb::open_appdb(Some(appdb_path))?.context("Failed to open app.db")?;
            
            appdb::add_existing_book_to_shelf(&mut appdb_conn, book_id, &shelf, username.as_deref())
                .map_err(|e| anyhow::anyhow!("{}", e))?;
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