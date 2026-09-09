use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// A command-line tool to manage a Calibre library.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Path to the Calibre library database file (metadata.db).
    #[clap(long, value_parser, global = true)]
    pub metadata_file: Option<PathBuf>,

    /// Path to the Calibre-Web database file (app.db) for shelf management.
    #[clap(long, global = true)]
    pub appdb_file: Option<PathBuf>,

    /// Path to the EPUB file to add.
    #[clap(long, value_parser, global = true)]
    pub epub_file: Option<PathBuf>,

    /// Path to a directory containing EPUB files to add.
    #[clap(long, value_parser, global = true)]
    pub epub_dir: Option<PathBuf>,

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
        /// The username to associate the shelf with. If not provided, uses the default admin user.
        #[clap(
            long,
            help = "The username to associate the shelf with. If not provided, uses the default admin user."
        )]
        username: Option<String>,
        /// Show what would be done without making any changes
        #[clap(long)]
        dry_run: bool,
        /// Succeed when a directory contains no EPUB files.
        #[clap(long)]
        allow_empty: bool,
    },
    /// List all books in the library with their attributes
    List {
        /// The name of the shelf to filter by.
        #[clap(long)]
        shelf: Option<String>,
        /// Show only books that aren't on any shelf
        #[clap(long, conflicts_with = "shelf")]
        unshelved: bool,
        /// List all attributes for each book.
        #[clap(long)]
        verbose: bool,
    },
    /// Delete a book from the library by its ID. Also removes it from Calibre-Web shelves.
    Delete {
        /// The ID of the book to delete.
        #[clap(value_parser)]
        book_id: i64,
    },
    /// List all available shelves from the Calibre-Web database
    ListShelves,
    /// Remove any shelves that don't have any books on them.
    CleanShelves,
    /// Inspect the app.db database
    InspectDb,
    /// Clean up orphaned data in both databases
    CleanDb,
    /// Reserved Kobo repair workflow; currently unavailable.
    FixKoboSync {
        #[command(subcommand)]
        action: FixKoboSyncAction,
    },
    /// Diagnose Kobo sync setup and show detailed information
    DiagnoseKoboSync,
    /// Add an existing book to a shelf (like Calibre-Web does)
    AddToShelf {
        /// The ID of the book to add to the shelf
        #[clap(value_parser)]
        book_id: i64,
        /// The name of the shelf to add the book to
        #[clap(long)]
        shelf: String,
        /// The username to associate the shelf with. If not provided, uses the default admin user
        #[clap(long)]
        username: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum FixKoboSyncAction {
    /// Reserved planning action; currently unavailable.
    Plan {
        #[arg(long)]
        username: String,
        #[arg(long)]
        after_sync_attempt: Option<String>,
    },
    /// Reserved apply action; currently unavailable.
    Apply {
        #[arg(long)]
        plan: String,
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=100))]
        batch_size: u16,
        #[arg(long, required = true, action = clap::ArgAction::SetTrue)]
        acknowledge_unknown_kobo_cursor: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(arguments: &[&str]) -> bool {
        Cli::try_parse_from(arguments).is_ok()
    }

    #[test]
    fn fix_kobo_sync_accepts_exact_plan_and_apply_forms() {
        assert!(parses(&[
            "helper",
            "fix-kobo-sync",
            "plan",
            "--username",
            "melissa",
        ]));
        assert!(parses(&[
            "helper",
            "fix-kobo-sync",
            "plan",
            "--username",
            "melissa",
            "--after-sync-attempt",
            "previous-plan",
        ]));
        assert!(parses(&[
            "helper",
            "fix-kobo-sync",
            "apply",
            "--plan",
            "plan-id",
            "--batch-size",
            "100",
            "--acknowledge-unknown-kobo-cursor",
        ]));
    }

    #[test]
    fn fix_kobo_sync_rejects_missing_values_ack_and_out_of_range_batches() {
        assert!(!parses(&["helper", "fix-kobo-sync", "plan"]));
        assert!(!parses(&[
            "helper",
            "fix-kobo-sync",
            "apply",
            "--batch-size",
            "1",
            "--acknowledge-unknown-kobo-cursor",
        ]));
        assert!(!parses(&[
            "helper",
            "fix-kobo-sync",
            "apply",
            "--plan",
            "plan-id",
            "--batch-size",
            "1",
        ]));
        for invalid in ["0", "101"] {
            assert!(!parses(&[
                "helper",
                "fix-kobo-sync",
                "apply",
                "--plan",
                "plan-id",
                "--batch-size",
                invalid,
                "--acknowledge-unknown-kobo-cursor",
            ]));
        }
    }
}
