use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// A command-line tool to manage a Calibre library.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Path to the Calibre library database file (metadata.db).
    #[clap(long, value_parser, global = true)]
    pub metadata_file: Option<PathBuf>,

    /// Path to the Calibre-Web app.db file for shelf management.
    #[clap(long, global = true)]
    pub appdb_file: Option<PathBuf>,

    /// Path to the EPUB file to add.
    #[clap(long, value_parser, global = true)]
    pub epub_file: Option<PathBuf>,

    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Add an EPUB file to the library
    Add {
        /// The name of the shelf to add the book to.
        #[clap(long)]
        shelf: Option<String>,
    },
    /// List all books in the library with their attributes
    List,
    /// List all available shelves from the Calibre-Web database
    ListShelves,
    /// Delete a book from the library by its ID. Also removes it from Calibre-Web shelves.
    Delete {
        /// The ID of the book to delete.
        #[clap(value_parser)]
        book_id: i64,
    },
    /// Remove any shelves that don't have any books on them.
    CleanShelves,
    /// Inspect the app.db database
    InspectDb,
}