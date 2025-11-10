use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

/// Configuration for database connections
pub struct DatabaseConfig {
    pub enable_foreign_keys: bool,
    pub busy_timeout_ms: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enable_foreign_keys: true,
            busy_timeout_ms: 5000,
        }
    }
}

/// Opens a database connection with standard configuration
pub fn open_connection(path: &Path, config: &DatabaseConfig) -> Result<Connection> {
    if !path.exists() {
        anyhow::bail!("Database file does not exist: {:?}", path);
    }

    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open database at {:?}", path))?;

    if config.enable_foreign_keys {
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("Failed to enable foreign key constraints")?;
    }

    if config.busy_timeout_ms > 0 {
        conn.busy_timeout(std::time::Duration::from_millis(config.busy_timeout_ms as u64))
            .context("Failed to set busy timeout")?;
    }

    Ok(conn)
}

/// Opens the Calibre metadata.db connection
pub fn open_calibre_db(path: &Path) -> Result<Connection> {
    let config = DatabaseConfig::default();
    let conn = open_connection(path, &config)?;
    
    // Add custom functions required by Calibre
    create_calibre_functions(&conn)?;
    
    Ok(conn)
}

/// Opens the Calibre-Web app.db connection
pub fn open_appdb(path: &Path) -> Result<Connection> {
    let config = DatabaseConfig::default();
    open_connection(path, &config)
}

/// Creates Calibre-specific custom SQL functions needed by the database triggers
fn create_calibre_functions(conn: &Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;
    use uuid::Uuid;

    // Calibre's triggers use a custom `title_sort` function
    conn.create_scalar_function(
        "title_sort",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| {
            let title = ctx.get::<String>(0)?;
            Ok(title_sort_logic(&title))
        },
    )?;

    // The book insert trigger also requires a uuid4 function
    conn.create_scalar_function(
        "uuid4",
        0,
        FunctionFlags::SQLITE_UTF8,
        move |_ctx| Ok(Uuid::new_v4().to_string()),
    )?;

    Ok(())
}

/// A simplified implementation of Calibre's title sorting logic
/// Moves common English articles to the end
fn title_sort_logic(title: &str) -> String {
    let articles = ["the ", "a ", "an ", "le ", "la ", "les ", "el ", "los ", "las "];
    let lower_title = title.to_lowercase();
    
    for article in &articles {
        if lower_title.starts_with(article) {
            let len = article.len();
            return format!("{}, {}", &title[len..], &title[..len - 1]);
        }
    }
    
    title.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_title_sort_logic() {
        assert_eq!(title_sort_logic("The Great Gatsby"), "Great Gatsby, The");
        assert_eq!(title_sort_logic("A Tale of Two Cities"), "Tale of Two Cities, A");
        assert_eq!(title_sort_logic("An American Tragedy"), "American Tragedy, An");
        assert_eq!(title_sort_logic("Normal Title"), "Normal Title");
    }
}
