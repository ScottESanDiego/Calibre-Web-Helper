use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// A command-line tool to manage a Calibre library.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Path to the Calibre library database file (metadata.db).
    #[clap(short, long, value_parser)]
    pub metadata_file: PathBuf,

    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Add an EPUB file to the library
    Add {
        /// Path to the EPUB file to add.
        #[clap(short, long, value_parser)]
        epub_file: PathBuf,
        /// Path to the Calibre-Web app.db file for shelf management.
        #[clap(long)]
        appdb_file: Option<PathBuf>,
        /// The name of the shelf to add the book to (requires --appdb-path).
        #[clap(long, requires = "appdb_file")]
        shelf: Option<String>,
    },
    /// List all books in the library with their attributes
    List {
        /// Path to the Calibre-Web app.db file to show shelf info.
        #[clap(long)]
        appdb_file: Option<PathBuf>,
    },
    /// List all available shelves from the Calibre-Web database
    ListShelves {
        /// Path to the Calibre-Web app.db file.
        #[clap(long)]
        appdb_file: PathBuf,
    },
    /// Delete a book from the library by its ID. Also removes it from Calibre-Web shelves.
    Delete {
        /// Path to the Calibre-Web app.db file for shelf management.
        #[clap(long)]
        appdb_file: Option<PathBuf>,
        /// The ID of the book to delete.
        #[clap(value_parser)]
        book_id: i64,
    },
    /// Remove any shelves that don't have any books on them.
    CleanShelves {
        /// Path to the Calibre-Web app.db file.
        #[clap(long)]
        appdb_file: PathBuf,
    },
    /// Inspect the app.db database
    InspectDb,
}
