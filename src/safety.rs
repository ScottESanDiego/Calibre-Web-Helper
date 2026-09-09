use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

pub(crate) struct HelperLock {
    _conn: Connection,
}

impl HelperLock {
    pub(crate) fn acquire(appdb_path: &Path) -> Result<Self> {
        let path = lock_path(appdb_path);
        let created = !path.exists();
        let conn = Connection::open(&path)
            .with_context(|| format!("Failed to open helper lock {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS helper_lock (id INTEGER PRIMARY KEY CHECK (id = 1));
             INSERT OR IGNORE INTO helper_lock(id) VALUES (1);
             BEGIN IMMEDIATE;",
        )
        .context("Another calibre-web-helper mutation is active")?;
        if created {
            sync_parent(&path)?;
        }
        Ok(Self { _conn: conn })
    }
}

impl Drop for HelperLock {
    fn drop(&mut self) {
        let _ = self._conn.execute_batch("ROLLBACK");
    }
}

pub(crate) fn lock_path(appdb_path: &Path) -> PathBuf {
    suffix_path(appdb_path, ".calibre-web-helper.lock")
}

pub(crate) fn acquire_add_lock(appdb_path: &Path, dry_run: bool) -> Result<Option<HelperLock>> {
    if dry_run {
        Ok(None)
    } else {
        HelperLock::acquire(appdb_path).map(Some)
    }
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

pub(crate) fn capacity_keys(
    appdb: &Connection,
    user_id: i64,
    planned: impl IntoIterator<Item = String>,
) -> Result<HashSet<String>> {
    let mut keys = HashSet::new();
    let mut stmt = appdb
        .prepare(
            "SELECT DISTINCT bsl.book_id
             FROM book_shelf_link bsl
             JOIN shelf s ON s.id = bsl.shelf
             WHERE s.user_id = ?1 AND s.kobo_sync = 1
               AND NOT EXISTS (
                   SELECT 1 FROM kobo_synced_books ksb
                   WHERE ksb.user_id = ?1 AND ksb.book_id = bsl.book_id
               )",
        )
        .context("Calibre-Web Kobo shelf schema is unsupported")?;
    for id in stmt.query_map([user_id], |row| row.get::<_, i64>(0))? {
        keys.insert(format!("book:{}", id?));
    }
    keys.extend(planned);
    Ok(keys)
}

pub(crate) fn validate_relative_destination(root: &Path, relative: &Path) -> Result<PathBuf> {
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        anyhow::bail!("Library-relative destination must be non-empty and relative");
    }
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            anyhow::bail!(
                "Unsafe library-relative destination: {}",
                relative.display()
            );
        }
    }
    let canonical_root = root
        .canonicalize()
        .with_context(|| format!("Cannot canonicalize library root {}", root.display()))?;
    let mut current = canonical_root.clone();
    for component in relative.components() {
        current.push(component.as_os_str());
        if current.exists() {
            let metadata = fs::symlink_metadata(&current)?;
            if metadata.file_type().is_symlink() {
                anyhow::bail!(
                    "Symlink in library destination is not allowed: {}",
                    current.display()
                );
            }
            let canonical = current.canonicalize()?;
            if !canonical.starts_with(&canonical_root) {
                anyhow::bail!("Library destination escapes root: {}", relative.display());
            }
        }
    }
    Ok(canonical_root.join(relative))
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn temp_root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cwh-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn destination_rejects_empty_absolute_and_traversal() {
        let root = temp_root("paths");
        assert!(validate_relative_destination(&root, Path::new("")).is_err());
        assert!(validate_relative_destination(&root, Path::new("/tmp/book")).is_err());
        assert!(validate_relative_destination(&root, Path::new("author/../book")).is_err());
        assert!(validate_relative_destination(&root, Path::new("./book")).is_err());
        fs::remove_dir(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn destination_rejects_symlink_component() {
        let root = temp_root("symlink-root");
        let outside = temp_root("symlink-outside");
        symlink(&outside, root.join("author")).unwrap();
        assert!(validate_relative_destination(&root, Path::new("author/book")).is_err());
        fs::remove_file(root.join("author")).unwrap();
        fs::remove_dir(root).unwrap();
        fs::remove_dir(outside).unwrap();
    }

    #[test]
    fn capacity_is_a_distinct_union() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE shelf(id INTEGER PRIMARY KEY, user_id INTEGER, kobo_sync INTEGER);
             CREATE TABLE book_shelf_link(book_id INTEGER, shelf INTEGER);
             CREATE TABLE kobo_synced_books(user_id INTEGER, book_id INTEGER);
             INSERT INTO shelf VALUES(1, 7, 1);
             INSERT INTO book_shelf_link VALUES(9, 1), (10, 1);
             INSERT INTO kobo_synced_books VALUES(7, 10);",
        )
        .unwrap();
        let keys = capacity_keys(&conn, 7, ["book:9".to_owned(), "new:a".to_owned()]).unwrap();
        assert_eq!(
            keys,
            HashSet::from(["book:9".to_owned(), "new:a".to_owned(),])
        );
    }

    #[test]
    fn capacity_boundary_is_100_distinct_keys() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE shelf(id INTEGER PRIMARY KEY, user_id INTEGER, kobo_sync INTEGER);
             CREATE TABLE book_shelf_link(book_id INTEGER, shelf INTEGER);
             CREATE TABLE kobo_synced_books(user_id INTEGER, book_id INTEGER);
             INSERT INTO shelf VALUES(1, 7, 1);",
        )
        .unwrap();
        for book_id in 1..=99 {
            conn.execute("INSERT INTO book_shelf_link VALUES(?1, 1)", [book_id])
                .unwrap();
        }
        assert_eq!(capacity_keys(&conn, 7, []).unwrap().len(), 99);
        assert_eq!(
            capacity_keys(&conn, 7, ["new:a".to_owned()]).unwrap().len(),
            100
        );
        assert_eq!(
            capacity_keys(&conn, 7, ["new:a".to_owned(), "new:b".to_owned()])
                .unwrap()
                .len(),
            101
        );
    }

    #[test]
    fn dry_run_does_not_create_lock_artifact() {
        let root = temp_root("dry-run-lock");
        let appdb = root.join("app.db");
        assert!(acquire_add_lock(&appdb, true).unwrap().is_none());
        assert!(!lock_path(&appdb).exists());
        fs::remove_dir(root).unwrap();
    }
}
