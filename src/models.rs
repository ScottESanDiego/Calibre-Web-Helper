use chrono::{DateTime, Utc};
use std::path::PathBuf;

/// Metadata extracted from an EPUB file
#[derive(Debug, Clone)]
pub(crate) struct BookMetadata {
    pub(crate) title: String,
    pub(crate) author: String,
    pub(crate) path: PathBuf,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) isbn: Option<String>,
    pub(crate) rights: Option<String>,
    pub(crate) subtitle: Option<String>,
    pub(crate) series: Option<String>,
    pub(crate) series_index: Option<f64>,
    pub(crate) publisher: Option<String>,
    pub(crate) pubdate: Option<DateTime<Utc>>,
    pub(crate) file_size: u64,
}

/// Existing book data from the database for comparison
#[derive(Debug)]
pub(crate) struct ExistingBookData {
    pub(crate) pubdate: Option<DateTime<Utc>>,
    pub(crate) series_index: f64,
    pub(crate) publisher: Option<String>,
    pub(crate) series: Option<String>,
}

/// Tracks what metadata fields have changed during an update
#[derive(Debug, Default)]
pub(crate) struct UpdateChanges {
    pub(crate) pubdate_changed: bool,
    pub(crate) series_index_changed: bool,
    pub(crate) publisher_changed: bool,
    pub(crate) series_changed: bool,
}

impl UpdateChanges {
    pub(crate) fn has_any_changes(&self) -> bool {
        self.pubdate_changed || self.series_index_changed || self.publisher_changed || self.series_changed
    }
}

/// Result of upserting a book to the database
pub(crate) enum UpsertResult {
    /// A new book was created
    Created { book_id: i64, book_path: String },
    /// An existing book was updated
    Updated { book_id: i64, book_path: String },
    /// No changes were needed
    NoChanges { book_id: i64, book_path: String },
}

impl UpsertResult {
    pub(crate) fn book_id(&self) -> i64 {
        match self {
            UpsertResult::Created { book_id, .. } => *book_id,
            UpsertResult::Updated { book_id, .. } => *book_id,
            UpsertResult::NoChanges { book_id, .. } => *book_id,
        }
    }

    pub(crate) fn book_path(&self) -> &str {
        match self {
            UpsertResult::Created { book_path, .. } => book_path,
            UpsertResult::Updated { book_path, .. } => book_path,
            UpsertResult::NoChanges { book_path, .. } => book_path,
        }
    }

    pub(crate) fn is_update(&self) -> bool {
        matches!(self, UpsertResult::Updated { .. } | UpsertResult::NoChanges { .. })
    }

    pub(crate) fn skip_file_operations(&self) -> bool {
        matches!(self, UpsertResult::NoChanges { .. })
    }
}
