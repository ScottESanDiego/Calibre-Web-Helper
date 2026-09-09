use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

mod cli;
use cli::{Cli, Commands, FixKoboSyncAction};
mod appdb;
mod calibre;
mod cleanup;
mod db;
mod epub;
mod models;
mod recovery;
mod safety;
mod utils;

fn library_dir(metadata_file: &Path) -> &Path {
    metadata_file.parent().unwrap_or_else(|| Path::new("."))
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // For some commands, metadata_file is not required
    let needs_metadata = !matches!(
        cli.command,
        Commands::FixKoboSync { .. } | Commands::AddToShelf { .. } | Commands::ListShelves
    );

    let metadata_file = if needs_metadata {
        Some(cli.metadata_file.context("--metadata-file is required")?)
    } else {
        cli.metadata_file
    };

    // Validate library database file path for commands that need it
    if let Some(ref metadata_file) = metadata_file
        && !metadata_file.exists()
    {
        anyhow::bail!(
            "The specified library database file does not exist: {:?}",
            metadata_file
        );
    }

    let mut calibre_conn = if let Some(ref metadata_file) = metadata_file {
        let conn = db::open_calibre_db(metadata_file)
            .with_context(|| format!("Failed to open Calibre database at {:?}", metadata_file))?;
        Some(conn)
    } else {
        None
    };

    let mut appdb_conn = if matches!(&cli.command, Commands::FixKoboSync { .. }) {
        None
    } else {
        appdb::open_appdb(cli.appdb_file.as_deref())?
    };

    match cli.command {
        Commands::Add {
            shelf,
            username,
            dry_run,
            allow_empty,
        } => {
            let calibre_conn = calibre_conn
                .as_mut()
                .context("--metadata-file is required for add command")?;
            let metadata_file = metadata_file.as_ref().unwrap();
            if shelf.is_some() && cli.appdb_file.is_none() {
                anyhow::bail!("--appdb-file is required when specifying a shelf");
            }

            if dry_run {
                println!("🧪 DRY RUN MODE: No changes will be made to databases or files\n");
            }

            // Validate that exactly one of epub_file or epub_dir is provided
            match (cli.epub_file, cli.epub_dir) {
                (Some(epub_file), None) => {
                    let paths = vec![epub_file];
                    let target = preflight_adds(
                        calibre_conn,
                        appdb_conn.as_ref(),
                        cli.appdb_file.as_deref(),
                        metadata_file,
                        &paths,
                        shelf.as_deref(),
                        username.as_deref(),
                        true,
                        true,
                    )?;
                    let lock_base = cli.appdb_file.as_deref().unwrap_or(metadata_file);
                    let _lock = if dry_run {
                        None
                    } else {
                        safety::acquire_add_lock(lock_base, false)?
                    };
                    let target = if dry_run {
                        target
                    } else {
                        preflight_adds(
                            calibre_conn,
                            appdb_conn.as_ref(),
                            cli.appdb_file.as_deref(),
                            metadata_file,
                            &paths,
                            shelf.as_deref(),
                            username.as_deref(),
                            true,
                            false,
                        )?
                    };
                    add_book_flow(
                        calibre_conn,
                        appdb_conn.as_mut(),
                        metadata_file,
                        lock_base,
                        &paths[0],
                        target.target.as_ref(),
                        dry_run,
                    )?;
                }
                (None, Some(epub_dir)) => {
                    add_directory_flow(
                        calibre_conn,
                        appdb_conn.as_mut(),
                        &epub_dir,
                        AddDirectoryOptions {
                            appdb_path: cli.appdb_file.as_deref(),
                            library_db_path: metadata_file,
                            shelf_name: shelf.as_deref(),
                            username: username.as_deref(),
                            dry_run,
                            allow_empty,
                        },
                    )?;
                }
                (Some(_), Some(_)) => {
                    anyhow::bail!(
                        "Cannot specify both --epub-file and --epub-dir. Please use one or the other."
                    );
                }
                (None, None) => {
                    anyhow::bail!(
                        "Either --epub-file or --epub-dir is required for the add command"
                    );
                }
            }
        }
        Commands::List {
            shelf,
            unshelved,
            verbose,
        } => {
            let calibre_conn = calibre_conn
                .as_ref()
                .context("--metadata-file is required for list command")?;
            calibre::list_books(
                calibre_conn,
                appdb_conn.as_ref(),
                shelf.as_deref(),
                unshelved,
                verbose,
            )?;
        }
        Commands::ListShelves => {
            appdb::list_shelves(appdb_conn.as_ref())?;
        }
        Commands::Delete { book_id } => {
            let calibre_conn = calibre_conn
                .as_mut()
                .context("--metadata-file is required for delete command")?;
            let metadata_file = metadata_file.as_ref().unwrap();
            calibre::delete_book(calibre_conn, appdb_conn.as_ref(), metadata_file, book_id)?;
        }
        Commands::CleanShelves => {
            let calibre_conn = calibre_conn
                .as_ref()
                .context("--metadata-file is required for clean-shelves command")?;
            if let Some(ref mut conn) = appdb_conn {
                if let Some(ref appdb_path) = cli.appdb_file {
                    println!("📦 Creating app.db backup before cleaning shelves...");
                    crate::utils::backup_database(appdb_path, "clean_shelves")
                        .context("Failed to backup app.db")?;
                }
                appdb::clean_empty_shelves(conn, calibre_conn)?;
            }
        }
        Commands::InspectDb => {
            let calibre_conn = calibre_conn
                .as_ref()
                .context("--metadata-file is required for inspect-db command")?;
            appdb::inspect_databases(appdb_conn.as_ref(), calibre_conn)?;
        }
        Commands::CleanDb => {
            let calibre_conn = calibre_conn
                .as_mut()
                .context("--metadata-file is required for clean-db command")?;
            let metadata_file = metadata_file.as_ref().unwrap();

            // Create backup before cleanup
            println!("📦 Creating database backups before cleanup...");
            crate::utils::backup_database(metadata_file, "clean_db")
                .context("Failed to backup metadata.db")?;

            if let Some(ref appdb_path) = cli.appdb_file {
                crate::utils::backup_database(appdb_path, "clean_db")
                    .context("Failed to backup app.db")?;
            }

            cleanup::cleanup_databases(
                calibre_conn,
                appdb_conn.as_mut(),
                &library_dir(metadata_file).to_path_buf(),
            )?;
        }
        Commands::FixKoboSync { action } => {
            let _appdb_path = cli
                .appdb_file
                .as_ref()
                .context("--appdb-file is required for fix-kobo-sync")?;
            match action {
                FixKoboSyncAction::Plan {
                    username,
                    after_sync_attempt,
                } => {
                    anyhow::bail!(
                        "Guarded Kobo plan construction is not implemented yet (username={username}, after-sync-attempt={after_sync_attempt:?})"
                    );
                }
                FixKoboSyncAction::Apply {
                    plan,
                    batch_size,
                    acknowledge_unknown_kobo_cursor,
                } => {
                    anyhow::bail!(
                        "Guarded Kobo apply is not implemented yet (plan={plan}, batch-size={batch_size}, unknown cursor accepted={acknowledge_unknown_kobo_cursor})"
                    );
                }
            }
        }
        Commands::DiagnoseKoboSync => {
            let metadata_path = metadata_file
                .as_ref()
                .context("metadata-file is required")?;
            let appdb_path = cli.appdb_file.as_ref().context("appdb-file is required")?;

            appdb::diagnose_kobo_sync(appdb_path, metadata_path)?;
        }
        Commands::AddToShelf {
            book_id,
            shelf,
            username,
        } => {
            let appdb_path = cli.appdb_file.as_ref().context("appdb-file is required")?;
            let mut appdb_conn =
                appdb::open_appdb(Some(appdb_path))?.context("Failed to open app.db")?;

            // Validate the book exists in metadata.db if available
            if let Some(ref _metadata_file) = metadata_file {
                let calibre_conn = calibre_conn
                    .as_ref()
                    .context("Failed to get Calibre connection")?;
                crate::utils::validate_foreign_key(calibre_conn, "books", book_id, "book")
                    .context("Book does not exist in Calibre library")?;
            }

            appdb::add_existing_book_to_shelf(
                &mut appdb_conn,
                book_id,
                &shelf,
                username.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
    }

    Ok(())
}

#[derive(Clone)]
struct PreflightBook {
    path: PathBuf,
    metadata: models::BookMetadata,
    existing: Option<(i64, String)>,
}

fn preflight_book(
    calibre_conn: &Connection,
    library_db_path: &Path,
    epub_file: &Path,
) -> Result<PreflightBook> {
    let metadata = epub::get_epub_metadata(epub_file)?;
    let existing = calibre::find_existing_book(calibre_conn, &metadata)?;
    calibre::validate_planned_destination(
        library_dir(library_db_path),
        &metadata,
        epub_file,
        existing.as_ref(),
    )?;
    Ok(PreflightBook {
        path: epub_file.to_path_buf(),
        metadata,
        existing,
    })
}

#[derive(Debug)]
struct AddPreflight {
    target: Option<appdb::ShelfTarget>,
    rejected: HashMap<PathBuf, String>,
}

#[allow(clippy::too_many_arguments)]
fn preflight_adds(
    calibre_conn: &Connection,
    appdb_conn: Option<&Connection>,
    recovery_base_path: Option<&Path>,
    library_db_path: &Path,
    paths: &[PathBuf],
    shelf_name: Option<&str>,
    username: Option<&str>,
    fail_item_error: bool,
    announce: bool,
) -> Result<AddPreflight> {
    let target = match shelf_name {
        Some(name) => Some(appdb::resolve_existing_shelf(
            appdb_conn.context("--appdb-file is required when specifying a shelf")?,
            name,
            username,
        )?),
        None => None,
    };

    let mut books = Vec::new();
    let mut rejected = HashMap::new();
    for path in paths {
        match preflight_book(calibre_conn, library_db_path, path) {
            Ok(book) => books.push(book),
            Err(error) if fail_item_error => return Err(error),
            Err(error) => {
                if announce {
                    println!("   ❌ Preflight failed for {}: {error}", path.display());
                }
                rejected.insert(path.clone(), error.to_string());
            }
        }
    }

    let Some(target) = target else {
        return Ok(AddPreflight {
            target: None,
            rejected,
        });
    };
    if !target.kobo_sync {
        if announce {
            println!(
                "ℹ️  Shelf '{}' for user '{}' is not Kobo-enabled; the Kobo backlog guard is not applicable.",
                target.shelf_name, target.username
            );
        }
        return Ok(AddPreflight {
            target: Some(target),
            rejected,
        });
    }

    let appdb = appdb_conn.context("app.db connection disappeared during preflight")?;
    let mut planned = HashSet::new();
    for book in books {
        let key = if let Some((book_id, _)) = book.existing {
            match appdb::planned_existing_book_key(appdb, &target, book_id) {
                Ok(Some(key)) => key,
                Ok(None) => continue,
                Err(error) if fail_item_error => return Err(error),
                Err(error) => {
                    if announce {
                        println!(
                            "   ❌ Preflight failed for {}: {error}",
                            book.path.display()
                        );
                    }
                    rejected.insert(book.path, error.to_string());
                    continue;
                }
            }
        } else {
            format!(
                "new:{}\u{0}{}",
                crate::utils::get_sorted_author(&book.metadata.author),
                book.metadata.title
            )
        };
        planned.insert(key);
    }
    let recovery_base_path = recovery_base_path
        .context("--appdb-file is required to inspect Kobo capacity reservations")?;
    planned.extend(recovery::active_reservation_keys_if_present(
        recovery_base_path,
        target.user_id,
    )?);
    let capacity = safety::capacity_keys(appdb, target.user_id, planned)?;
    if capacity.len() > 100 {
        anyhow::bail!(
            "Kobo sync backlog would contain {} distinct books; maximum safe size is 100",
            capacity.len()
        );
    }
    Ok(AddPreflight {
        target: Some(target),
        rejected,
    })
}

/// Handles the flow for adding a new book.
fn add_book_flow(
    calibre_conn: &mut Connection,
    appdb_conn: Option<&mut Connection>,
    library_db_path: &Path,
    recovery_base_path: &Path,
    epub_file: &Path,
    shelf_target: Option<&appdb::ShelfTarget>,
    dry_run: bool,
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
        println!(
            " -> Series: {} {}",
            series,
            metadata
                .series_index
                .map_or(String::new(), |idx| format!("#{}", idx))
        );
    }
    if let Some(publisher) = &metadata.publisher {
        println!(" -> Publisher: {}", publisher);
    }
    if let Some(pubdate) = metadata.pubdate {
        println!(" -> Published: {}", pubdate.format("%Y-%m-%d"));
    }

    println!("✒️ Writing to Calibre database...");
    let upsert_result = if dry_run {
        calibre::add_book_to_db(
            calibre_conn,
            &metadata,
            library_dir(library_db_path),
            epub_file,
            true,
        )?
    } else {
        recovery::durable_add(
            calibre_conn,
            appdb_conn,
            library_db_path,
            recovery_base_path,
            epub_file,
            &metadata,
            shelf_target,
        )?
    };

    let book_path = upsert_result.book_path().to_string();
    let is_update = upsert_result.is_update();
    let skip_file_operations = upsert_result.skip_file_operations();

    match &upsert_result {
        models::UpsertResult::Created { book_id, .. } => {
            println!(
                " -> Successfully created database entry with Book ID: {}",
                book_id
            );
        }
        models::UpsertResult::Updated { book_id, .. } => {
            println!(
                " -> Successfully updated database entry for Book ID: {}",
                book_id
            );
        }
        models::UpsertResult::NoChanges { book_id, .. } => {
            println!(" -> No changes needed for Book ID: {}", book_id);
        }
    }

    if let Some(target) = shelf_target
        && dry_run
    {
        println!("📚 Would add book to shelf '{}'", target.shelf_name);
        println!("   [DRY RUN] Would update app.db with shelf assignment");
    }

    if !skip_file_operations && dry_run {
        println!("� Would update files in library...");
        println!("   [DRY RUN] Would copy EPUB file to: {}", book_path);
        println!("   [DRY RUN] Would extract and resize cover image");
    } else {
        if dry_run {
            println!("📁 Would skip file operations (no changes needed).");
        } else {
            println!("�📁 Skipping file operations (no changes needed).");
        }
    }

    let action_str = if dry_run {
        if skip_file_operations {
            "would be already up to date in"
        } else if is_update {
            "would be updated in"
        } else {
            "would be added to"
        }
    } else if skip_file_operations {
        "already up to date in"
    } else if is_update {
        "updated in"
    } else {
        "added to"
    };
    // Check series status for feedback message
    let series_msg = if let Some(series) = &metadata.series {
        format!(
            " (part of series '{}'{})'",
            series,
            metadata
                .series_index
                .map_or(String::new(), |idx| format!(" #{}", idx))
        )
    } else {
        String::new()
    };

    let success_icon = if dry_run { "🧪" } else { "✅" };
    println!(
        "
{} Success! '{}'{} has been {} your Calibre library.",
        success_icon, metadata.title, series_msg, action_str
    );

    if !skip_file_operations && !dry_run {
        println!("   Please restart Calibre to see the new book.");
    } else if dry_run {
        println!("   [DRY RUN] No actual changes were made.");
    }

    Ok(())
}

/// Handles the flow for adding all EPUB files in a directory.
struct AddDirectoryOptions<'a> {
    appdb_path: Option<&'a Path>,
    library_db_path: &'a Path,
    shelf_name: Option<&'a str>,
    username: Option<&'a str>,
    dry_run: bool,
    allow_empty: bool,
}

fn add_directory_flow(
    calibre_conn: &mut Connection,
    mut appdb_conn: Option<&mut Connection>,
    epub_dir: &Path,
    options: AddDirectoryOptions<'_>,
) -> Result<()> {
    if !epub_dir.exists() {
        anyhow::bail!("The specified directory does not exist: {:?}", epub_dir);
    }

    if !epub_dir.is_dir() {
        anyhow::bail!("The specified path is not a directory: {:?}", epub_dir);
    }

    println!("📁 Scanning directory for EPUB files: {:?}", epub_dir);

    // Find all EPUB files in the directory
    let mut epub_files = Vec::new();
    for entry in fs::read_dir(epub_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file()
            && let Some(extension) = path.extension()
        {
            let ext_str = extension.to_string_lossy().to_lowercase();
            if ext_str == "epub" || ext_str == "kepub" {
                epub_files.push(path);
            }
        }
    }

    if epub_files.is_empty() {
        if options.allow_empty {
            println!("⚠️  No EPUB files found in directory: {:?}", epub_dir);
            return Ok(());
        }
        anyhow::bail!(
            "No EPUB files found in directory: {:?}; use --allow-empty to accept an empty batch",
            epub_dir
        );
    }

    // Sort files for consistent processing order
    epub_files.sort();

    let preflight = preflight_adds(
        calibre_conn,
        appdb_conn.as_deref(),
        options.appdb_path,
        options.library_db_path,
        &epub_files,
        options.shelf_name,
        options.username,
        false,
        true,
    )?;
    let lock_base = options.appdb_path.unwrap_or(options.library_db_path);
    let _lock = safety::acquire_add_lock(lock_base, options.dry_run)?;
    let preflight = if options.dry_run {
        preflight
    } else {
        preflight_adds(
            calibre_conn,
            appdb_conn.as_deref(),
            options.appdb_path,
            options.library_db_path,
            &epub_files,
            options.shelf_name,
            options.username,
            false,
            false,
        )?
    };

    println!("📚 Found {} EPUB file(s) to process:", epub_files.len());
    for file in &epub_files {
        println!(
            "   - {}",
            file.file_name().unwrap_or_default().to_string_lossy()
        );
    }

    let mut successful = 0;
    let mut failed = 0;

    println!("\n🚀 Starting batch processing...\n");

    for (index, epub_file) in epub_files.iter().enumerate() {
        println!(
            "📖 Processing ({}/{}) - {}",
            index + 1,
            epub_files.len(),
            epub_file.file_name().unwrap_or_default().to_string_lossy()
        );

        if let Some(error) = preflight.rejected.get(epub_file) {
            failed += 1;
            println!("   ❌ Failed preflight: {error}\n");
            continue;
        }

        match add_book_flow(
            calibre_conn,
            appdb_conn.as_deref_mut(),
            options.library_db_path,
            lock_base,
            epub_file,
            preflight.target.as_ref(),
            options.dry_run,
        ) {
            Ok(()) => {
                successful += 1;
                println!("   ✅ Success!\n");
            }
            Err(e) => {
                failed += 1;
                println!("   ❌ Failed: {}\n", e);
                // Continue processing other files even if one fails
            }
        }
    }

    // Summary
    println!("📊 Batch processing complete:");
    println!("   ✅ Successfully processed: {}", successful);
    if failed > 0 {
        println!("   ❌ Failed: {}", failed);
    }
    println!("   📚 Total files: {}", epub_files.len());

    if successful > 0 {
        println!("\n   Please restart Calibre to see the new books.");
    }

    if failed > 0 {
        anyhow::bail!(
            "Batch completed with {failed} failed item(s) out of {}",
            epub_files.len()
        );
    }

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
    use std::io::{Cursor, Write};
    use zip::CompressionMethod;
    use zip::write::SimpleFileOptions;

    struct Fixture {
        root: PathBuf,
        metadata_path: PathBuf,
        appdb_path: PathBuf,
        epub_path: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn write_epub(path: &Path) {
        write_epub_with_title(path, "Generated Book");
    }

    fn write_epub_with_title(path: &Path, title: &str) {
        let mut cover = Vec::new();
        DynamicImage::ImageRgba8(RgbaImage::from_pixel(8, 8, Rgba([20, 40, 60, 200])))
            .write_to(&mut Cursor::new(&mut cover), ImageFormat::Png)
            .unwrap();
        let opf = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
        <package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
          <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
            <dc:title>{title}</dc:title><dc:creator>Fixture Author</dc:creator>
            <dc:identifier id="id">urn:isbn:978030? &amp; no</dc:identifier><dc:language>en</dc:language>
          </metadata>
          <manifest><item id="cover" href="cover.png" media-type="image/png" properties="cover-image"/>
          <item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest>
          <spine><itemref idref="chapter"/></spine>
        </package>"#
        );
        let container = r#"<?xml version="1.0"?>
        <container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
          <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>
        </container>"#;
        let mut archive = zip::ZipWriter::new(fs::File::create(path).unwrap());
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, bytes) in [
            ("mimetype", b"application/epub+zip".as_slice()),
            ("META-INF/container.xml", container.as_bytes()),
            ("OEBPS/content.opf", opf.as_bytes()),
            (
                "OEBPS/chapter.xhtml",
                b"<html xmlns=\"http://www.w3.org/1999/xhtml\"><body/></html>".as_slice(),
            ),
            ("OEBPS/cover.png", cover.as_slice()),
        ] {
            archive.start_file(name, options).unwrap();
            archive.write_all(bytes).unwrap();
        }
        archive.finish().unwrap();
    }

    fn fixture(unsynced: i64) -> Fixture {
        let root =
            std::env::temp_dir().join(format!("cwh-add-integration-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let metadata_path = root.join("metadata.db");
        let appdb_path = root.join("app.db");
        let epub_path = root.join("generated.epub");
        write_epub(&epub_path);
        let metadata = Connection::open(&metadata_path).unwrap();
        metadata.execute_batch(
            "CREATE TABLE books(id INTEGER PRIMARY KEY, title TEXT, sort TEXT, author_sort TEXT,
                 timestamp TEXT, pubdate TEXT, last_modified TEXT, path TEXT, series_index REAL,
                 uuid TEXT, has_cover INTEGER NOT NULL DEFAULT 0);
             CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT UNIQUE, sort TEXT);
             CREATE TABLE books_authors_link(book INTEGER, author INTEGER);
             CREATE TABLE data(id INTEGER PRIMARY KEY, book INTEGER, format TEXT,
                 uncompressed_size INTEGER, name TEXT);
             CREATE TABLE comments(id INTEGER PRIMARY KEY, book INTEGER, text TEXT);
             CREATE TABLE languages(id INTEGER PRIMARY KEY, lang_code TEXT UNIQUE);
             CREATE TABLE books_languages_link(book INTEGER, lang_code INTEGER);
             CREATE TABLE identifiers(id INTEGER PRIMARY KEY, book INTEGER, type TEXT, val TEXT);
             CREATE TABLE publishers(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
             CREATE TABLE books_publishers_link(book INTEGER, publisher INTEGER);
             CREATE TABLE series(id INTEGER PRIMARY KEY, name TEXT UNIQUE, sort TEXT);
             CREATE TABLE books_series_link(book INTEGER, series INTEGER);
             CREATE TABLE metadata_dirtied(book INTEGER PRIMARY KEY);",
        ).unwrap();
        drop(metadata);
        let appdb = Connection::open(&appdb_path).unwrap();
        appdb.execute_batch(
            "CREATE TABLE user(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE shelf(id INTEGER PRIMARY KEY, name TEXT NOT NULL, user_id INTEGER NOT NULL,
                 kobo_sync INTEGER NOT NULL, last_modified TEXT);
             CREATE TABLE book_shelf_link(id INTEGER PRIMARY KEY, book_id INTEGER NOT NULL,
                 shelf INTEGER NOT NULL, \"order\" INTEGER NOT NULL, date_added TEXT,
                 UNIQUE(book_id, shelf));
             CREATE TABLE kobo_synced_books(user_id INTEGER NOT NULL, book_id INTEGER NOT NULL);
             INSERT INTO user VALUES(7, 'melissa');
             INSERT INTO shelf VALUES(9, 'KoboMelissa', 7, 1, '2020-01-01 00:00:00.000000');",
        ).unwrap();
        for offset in 0..unsynced {
            appdb
                .execute(
                    "INSERT INTO book_shelf_link(book_id, shelf, \"order\", date_added)
                 VALUES(?1, 9, ?2, '2020-01-01 00:00:00.000000')",
                    rusqlite::params![10_000 + offset, offset + 1],
                )
                .unwrap();
        }
        drop(appdb);
        Fixture {
            root,
            metadata_path,
            appdb_path,
            epub_path,
        }
    }

    #[test]
    fn real_add_at_99_unsynced_publishes_book_cover_shelf_and_cleans_recovery() {
        let fixture = fixture(99);
        let mut metadata = db::open_calibre_db(&fixture.metadata_path).unwrap();
        let mut appdb = db::open_appdb(&fixture.appdb_path).unwrap();
        let target = preflight_adds(
            &metadata,
            Some(&appdb),
            Some(&fixture.appdb_path),
            &fixture.metadata_path,
            std::slice::from_ref(&fixture.epub_path),
            Some("KoboMelissa"),
            Some("melissa"),
            true,
            false,
        )
        .unwrap()
        .target
        .unwrap();
        let _lock = safety::acquire_add_lock(&fixture.appdb_path, false).unwrap();
        add_book_flow(
            &mut metadata,
            Some(&mut appdb),
            &fixture.metadata_path,
            &fixture.appdb_path,
            &fixture.epub_path,
            Some(&target),
            false,
        )
        .unwrap();
        let (book_id, path, has_cover): (i64, String, bool) = metadata
            .query_row("SELECT id, path, has_cover FROM books", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert!(
            fixture
                .root
                .join(&path)
                .join("Generated Book - Fixture Author.epub")
                .is_file()
        );
        assert!(fixture.root.join(&path).join("cover.jpg").is_file());
        assert!(has_cover);
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link WHERE shelf = 9 AND book_id = ?1",
                    [book_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        let state = recovery::StateDb::open(&fixture.appdb_path).unwrap();
        for table in ["operations", "reservations", "artifacts"] {
            assert_eq!(
                state
                    .connection()
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn real_dry_run_is_byte_identical_and_creates_no_helper_artifacts() {
        let fixture = fixture(99);
        let before_metadata = fs::read(&fixture.metadata_path).unwrap();
        let before_appdb = fs::read(&fixture.appdb_path).unwrap();
        let before_entries = fs::read_dir(&fixture.root).unwrap().count();
        let mut metadata = db::open_calibre_db(&fixture.metadata_path).unwrap();
        let appdb = db::open_appdb(&fixture.appdb_path).unwrap();
        let target = appdb::resolve_existing_shelf(&appdb, "KoboMelissa", Some("melissa")).unwrap();
        assert!(
            safety::acquire_add_lock(&fixture.appdb_path, true)
                .unwrap()
                .is_none()
        );
        add_book_flow(
            &mut metadata,
            None,
            &fixture.metadata_path,
            &fixture.appdb_path,
            &fixture.epub_path,
            Some(&target),
            true,
        )
        .unwrap();
        drop(metadata);
        drop(appdb);
        assert_eq!(fs::read(&fixture.metadata_path).unwrap(), before_metadata);
        assert_eq!(fs::read(&fixture.appdb_path).unwrap(), before_appdb);
        assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), before_entries);
        assert!(!safety::lock_path(&fixture.appdb_path).exists());
        assert!(!recovery::state_path(&fixture.appdb_path).exists());
    }

    #[test]
    fn real_add_at_100_unsynced_refuses_101_before_mutation() {
        let fixture = fixture(100);
        let before_metadata = fs::read(&fixture.metadata_path).unwrap();
        let before_appdb = fs::read(&fixture.appdb_path).unwrap();
        let before_entries = fs::read_dir(&fixture.root).unwrap().count();
        let metadata = db::open_calibre_db(&fixture.metadata_path).unwrap();
        let appdb = db::open_appdb(&fixture.appdb_path).unwrap();

        let error = preflight_adds(
            &metadata,
            Some(&appdb),
            Some(&fixture.appdb_path),
            &fixture.metadata_path,
            std::slice::from_ref(&fixture.epub_path),
            Some("KoboMelissa"),
            Some("melissa"),
            true,
            false,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("101 distinct books"),
            "{error:#}"
        );

        drop(metadata);
        drop(appdb);
        assert_eq!(fs::read(&fixture.metadata_path).unwrap(), before_metadata);
        assert_eq!(fs::read(&fixture.appdb_path).unwrap(), before_appdb);
        assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), before_entries);
        assert!(!safety::lock_path(&fixture.appdb_path).exists());
        assert!(!recovery::state_path(&fixture.appdb_path).exists());
    }

    #[test]
    fn real_mixed_directory_batch_continues_valid_item_and_returns_error() {
        let fixture = fixture(0);
        let batch_dir = fixture.root.join("batch");
        fs::create_dir(&batch_dir).unwrap();
        fs::write(batch_dir.join("bad.epub"), b"not an epub archive").unwrap();
        fs::rename(&fixture.epub_path, batch_dir.join("generated.epub")).unwrap();
        let mut metadata = db::open_calibre_db(&fixture.metadata_path).unwrap();
        let mut appdb = db::open_appdb(&fixture.appdb_path).unwrap();

        let error = add_directory_flow(
            &mut metadata,
            Some(&mut appdb),
            &batch_dir,
            AddDirectoryOptions {
                appdb_path: Some(&fixture.appdb_path),
                library_db_path: &fixture.metadata_path,
                shelf_name: Some("KoboMelissa"),
                username: Some("melissa"),
                dry_run: false,
                allow_empty: false,
            },
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Batch completed with 1 failed item(s) out of 2"),
            "{error:#}"
        );
        assert_eq!(
            metadata
                .query_row("SELECT COUNT(*) FROM books", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link WHERE shelf = 9",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn real_mixed_synced_reshelving_is_skipped_while_new_book_is_added() {
        let fixture = fixture(0);
        let mut metadata = db::open_calibre_db(&fixture.metadata_path).unwrap();
        let mut appdb = db::open_appdb(&fixture.appdb_path).unwrap();
        {
            let _lock = safety::acquire_add_lock(&fixture.appdb_path, false).unwrap();
            add_book_flow(
                &mut metadata,
                None,
                &fixture.metadata_path,
                &fixture.appdb_path,
                &fixture.epub_path,
                None,
                false,
            )
            .unwrap();
        }
        let synced_book: i64 = metadata
            .query_row(
                "SELECT id FROM books WHERE title = 'Generated Book'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        appdb
            .execute(
                "INSERT INTO kobo_synced_books(user_id, book_id) VALUES(7, ?1)",
                [synced_book],
            )
            .unwrap();

        let batch_dir = fixture.root.join("synced-mixed-batch");
        fs::create_dir(&batch_dir).unwrap();
        fs::rename(&fixture.epub_path, batch_dir.join("already-synced.epub")).unwrap();
        write_epub_with_title(&batch_dir.join("fresh-new.epub"), "Fresh Book");

        let error = add_directory_flow(
            &mut metadata,
            Some(&mut appdb),
            &batch_dir,
            AddDirectoryOptions {
                appdb_path: Some(&fixture.appdb_path),
                library_db_path: &fixture.metadata_path,
                shelf_name: Some("KoboMelissa"),
                username: Some("melissa"),
                dry_run: false,
                allow_empty: false,
            },
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Batch completed with 1 failed item(s) out of 2"),
            "{error:#}"
        );
        let fresh_book: i64 = metadata
            .query_row(
                "SELECT id FROM books WHERE title = 'Fresh Book'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link
                     WHERE shelf = 9 AND book_id = ?1",
                    [fresh_book],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link
                     WHERE shelf = 9 AND book_id = ?1",
                    [synced_book],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }
}
