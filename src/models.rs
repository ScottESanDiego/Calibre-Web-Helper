use chrono::{DateTime, Utc};
use std::path::PathBuf;

/// Metadata extracted from an EPUB file
#[derive(Debug, Clone)]
pub struct BookMetadata {
    pub title: String,
    pub author: String,
    pub path: PathBuf,
    pub description: Option<String>,
    pub language: Option<String>,
    pub isbn: Option<String>,
    pub rights: Option<String>,
    pub subtitle: Option<String>,
    pub series: Option<String>,
    pub series_index: Option<f64>,
    pub publisher: Option<String>,
    pub pubdate: Option<DateTime<Utc>>,
    pub file_size: u64,
}

/// Existing book data from the database for comparison
#[derive(Debug)]
pub struct ExistingBookData {
    pub pubdate: Option<DateTime<Utc>>,
    pub series_index: f64,
    pub publisher: Option<String>,
    pub series: Option<String>,
}

/// Tracks what metadata fields have changed during an update
#[derive(Debug, Default)]
pub struct UpdateChanges {
    pub pubdate_changed: bool,
    pub series_index_changed: bool,
    pub publisher_changed: bool,
    pub series_changed: bool,
}

impl UpdateChanges {
    /// Returns true if any fields have changed
    pub fn has_any_changes(&self) -> bool {
        self.pubdate_changed || self.series_index_changed || self.publisher_changed || self.series_changed
    }
}

/// Result of upserting a book to the database
pub enum UpsertResult {
    /// A new book was created
    Created { book_id: i64, book_path: String },
    /// An existing book was updated
    Updated { book_id: i64, book_path: String },
    /// No changes were needed
    NoChanges { book_id: i64, book_path: String },
}

impl UpsertResult {
    /// Gets the book ID regardless of the result type
    pub fn book_id(&self) -> i64 {
        match self {
            UpsertResult::Created { book_id, .. } => *book_id,
            UpsertResult::Updated { book_id, .. } => *book_id,
            UpsertResult::NoChanges { book_id, .. } => *book_id,
        }
    }

    /// Gets the book path regardless of the result type
    pub fn book_path(&self) -> &str {
        match self {
            UpsertResult::Created { book_path, .. } => book_path,
            UpsertResult::Updated { book_path, .. } => book_path,
            UpsertResult::NoChanges { book_path, .. } => book_path,
        }
    }

    /// Returns true if this was an update operation
    pub fn is_update(&self) -> bool {
        matches!(self, UpsertResult::Updated { .. } | UpsertResult::NoChanges { .. })
    }

    /// Returns true if file operations should be skipped
    pub fn skip_file_operations(&self) -> bool {
        matches!(self, UpsertResult::NoChanges { .. })
    }
}

/// Type alias for application results
pub type AppResult<T> = anyhow::Result<T>;
