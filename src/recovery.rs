use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SCHEMA_VERSION: i64 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactKind {
    File,
    Directory,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ArtifactIntent {
    pub(crate) role: String,
    pub(crate) kind: ArtifactKind,
    pub(crate) path: PathBuf,
    pub(crate) old_hash: Option<String>,
    pub(crate) final_hash: Option<String>,
    pub(crate) final_existed: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ReservationIntent {
    pub(crate) user_id: i64,
    pub(crate) book_key: String,
}

#[derive(Clone, Debug)]
pub(crate) struct OperationIntent {
    pub(crate) id: String,
    pub(crate) metadata_path: PathBuf,
    pub(crate) library_path: PathBuf,
    pub(crate) source_path: PathBuf,
    pub(crate) user_id: Option<i64>,
    pub(crate) shelf_id: Option<i64>,
    pub(crate) shelf_name: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) kobo_sync: bool,
    pub(crate) reservation: Option<ReservationIntent>,
    pub(crate) artifacts: Vec<ArtifactIntent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Planned,
    Staging,
    Staged,
    FilesPublished,
    MetadataCommitted,
    ShelfPending,
    Complete,
    RolledBack,
    RecoveryBlocked,
}

impl Phase {
    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "planned" => Self::Planned,
            "staging" => Self::Staging,
            "staged" => Self::Staged,
            "files_published" => Self::FilesPublished,
            "metadata_committed" => Self::MetadataCommitted,
            "shelf_pending" => Self::ShelfPending,
            "complete" => Self::Complete,
            "rolled_back" => Self::RolledBack,
            "recovery_blocked" => Self::RecoveryBlocked,
            _ => anyhow::bail!("Unsupported recovery phase {value}"),
        })
    }
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Staging => "staging",
            Self::Staged => "staged",
            Self::FilesPublished => "files_published",
            Self::MetadataCommitted => "metadata_committed",
            Self::ShelfPending => "shelf_pending",
            Self::Complete => "complete",
            Self::RolledBack => "rolled_back",
            Self::RecoveryBlocked => "recovery_blocked",
        }
    }
}

struct CleanupFinished(());

#[derive(Clone, Debug)]
pub(crate) struct StoredArtifact {
    pub(crate) role: String,
    pub(crate) kind: ArtifactKind,
    pub(crate) path: PathBuf,
    pub(crate) status: String,
    pub(crate) hash: Option<String>,
    pub(crate) old_hash: Option<String>,
    pub(crate) final_hash: Option<String>,
    pub(crate) final_existed: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredOperation {
    pub(crate) id: String,
    pub(crate) phase: Phase,
    pub(crate) metadata_path: PathBuf,
    pub(crate) library_path: PathBuf,
    pub(crate) source_path: PathBuf,
    pub(crate) user_id: Option<i64>,
    pub(crate) shelf_id: Option<i64>,
    pub(crate) shelf_name: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) kobo_sync: bool,
    pub(crate) book_id: Option<i64>,
    pub(crate) old_db_facts: Option<String>,
    pub(crate) expected_db_facts: Option<String>,
    pub(crate) artifacts: Vec<StoredArtifact>,
}

pub(crate) struct StateDb {
    connection: Connection,
    path: PathBuf,
}

impl StateDb {
    pub(crate) fn open(base_path: &Path) -> Result<Self> {
        let path = state_path(base_path);
        let created = !path.exists();
        let connection = Connection::open(&path).with_context(|| {
            format!("Failed to open recovery state database {}", path.display())
        })?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "DELETE")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS schema_version (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 version INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO schema_version(singleton, version) VALUES (1, 1);
             CREATE TABLE IF NOT EXISTS operations (
                 id TEXT PRIMARY KEY,
                 kind TEXT NOT NULL CHECK (kind IN ('add')),
                 phase TEXT NOT NULL CHECK (phase IN (
                     'planned', 'staging', 'staged', 'files_published',
                     'metadata_committed', 'shelf_pending', 'complete',
                     'rolled_back', 'recovery_blocked'
                 )),
                 metadata_path BLOB NOT NULL,
                 library_path BLOB NOT NULL,
                 source_path BLOB NOT NULL,
                 user_id INTEGER,
                 shelf_id INTEGER,
                 shelf_name TEXT,
                 username TEXT,
                 kobo_sync INTEGER NOT NULL DEFAULT 0 CHECK (kobo_sync IN (0, 1)),
                 book_key TEXT,
                 book_id INTEGER,
                 book_path BLOB,
                 was_existing INTEGER CHECK (was_existing IN (0, 1)),
                 old_db_facts TEXT,
                 expected_db_facts TEXT,
                 error TEXT,
                 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
             );
             CREATE TABLE IF NOT EXISTS reservations (
                 operation_id TEXT PRIMARY KEY REFERENCES operations(id) ON DELETE CASCADE,
                 user_id INTEGER NOT NULL,
                 book_key TEXT NOT NULL,
                 UNIQUE(user_id, book_key)
             );
             CREATE TABLE IF NOT EXISTS artifacts (
                 id INTEGER PRIMARY KEY,
                 operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
                 role TEXT NOT NULL,
                 kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
                 path BLOB NOT NULL,
                 status TEXT NOT NULL CHECK (status IN ('planned', 'writing', 'ready')),
                 hash_algorithm TEXT CHECK (hash_algorithm IS NULL OR hash_algorithm = 'sha256'),
                 hash TEXT,
                 old_hash TEXT,
                 final_hash TEXT,
                 final_existed INTEGER NOT NULL DEFAULT 0 CHECK (final_existed IN (0, 1)),
                 UNIQUE(operation_id, role),
                 CHECK ((hash_algorithm IS NULL) = (hash IS NULL))
             );
             COMMIT;",
        )?;

        let mut version: i64 = connection.query_row(
            "SELECT version FROM schema_version WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if version == 1 {
            migrate_v1_to_v2(&connection)?;
            version = connection.query_row(
                "SELECT version FROM schema_version WHERE singleton = 1",
                [],
                |row| row.get(0),
            )?;
        }
        if version != SCHEMA_VERSION {
            anyhow::bail!(
                "Unsupported recovery state schema version {version}; expected {SCHEMA_VERSION}"
            );
        }
        if created {
            sync_parent(&path)?;
        }
        Ok(Self { connection, path })
    }

    pub(crate) fn begin_operation(&mut self, intent: &OperationIntent) -> Result<()> {
        if intent.id.is_empty() || intent.artifacts.is_empty() {
            anyhow::bail!("Operation ID and at least one artifact intent are required");
        }
        let artifact_paths = intent
            .artifacts
            .iter()
            .map(|artifact| path_bytes(&artifact.path))
            .collect::<Result<Vec<_>>>()?;
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let reservation_key = intent
            .reservation
            .as_ref()
            .map(|value| value.book_key.as_str());
        require_one(
            transaction.execute(
                "INSERT INTO operations (
                     id, kind, phase, metadata_path, library_path, source_path,
                     user_id, shelf_id, shelf_name, username, kobo_sync, book_key
                 ) VALUES (?1, 'add', 'planned', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    intent.id,
                    path_bytes(&intent.metadata_path)?,
                    path_bytes(&intent.library_path)?,
                    path_bytes(&intent.source_path)?,
                    intent.user_id,
                    intent.shelf_id,
                    intent.shelf_name,
                    intent.username,
                    intent.kobo_sync,
                    reservation_key,
                ],
            )?,
            "insert planned operation",
        )?;
        if let Some(reservation) = &intent.reservation {
            require_one(
                transaction.execute(
                    "INSERT INTO reservations(operation_id, user_id, book_key)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![intent.id, reservation.user_id, reservation.book_key],
                )?,
                "insert capacity reservation",
            )?;
        }
        for (artifact, path) in intent.artifacts.iter().zip(artifact_paths) {
            require_one(
                transaction.execute(
                    "INSERT INTO artifacts (
                         operation_id, role, kind, path, status, old_hash,
                         final_hash, final_existed
                     ) VALUES (?1, ?2, ?3, ?4, 'planned', ?5, ?6, ?7)",
                    rusqlite::params![
                        intent.id,
                        artifact.role,
                        artifact.kind.as_str(),
                        path,
                        artifact.old_hash,
                        artifact.final_hash,
                        artifact.final_existed,
                    ],
                )?,
                "insert artifact intent",
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn record_book_facts(
        &mut self,
        operation_id: &str,
        book_id: i64,
        book_path: &Path,
        was_existing: bool,
        old_db_facts: Option<&str>,
        expected_db_facts: &str,
    ) -> Result<()> {
        validate_sha256(expected_db_facts)?;
        if let Some(hash) = old_db_facts {
            validate_sha256(hash)?;
        }
        require_one(
            self.connection.execute(
                "UPDATE operations SET book_id = ?2, book_path = ?3, was_existing = ?4,
                     old_db_facts = ?5, expected_db_facts = ?6,
                     updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
                 WHERE id = ?1 AND phase = 'planned' AND book_id IS NULL",
                rusqlite::params![
                    operation_id,
                    book_id,
                    path_bytes(book_path)?,
                    was_existing,
                    old_db_facts,
                    expected_db_facts,
                ],
            )?,
            "record operation book facts",
        )
    }

    pub(crate) fn transition_phase(
        &mut self,
        operation_id: &str,
        from: Phase,
        to: Phase,
    ) -> Result<()> {
        if !valid_transition(from, to) {
            anyhow::bail!(
                "Invalid recovery phase transition {} -> {}",
                from.as_str(),
                to.as_str()
            );
        }
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_one(
            transaction.execute(
                "UPDATE operations SET phase = ?3,
                     updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
                 WHERE id = ?1 AND phase = ?2",
                rusqlite::params![operation_id, from.as_str(), to.as_str()],
            )?,
            "transition recovery phase",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_artifact_writing(&mut self, operation_id: &str, role: &str) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_one(
            transaction.execute(
                "UPDATE artifacts SET status = 'writing'
                 WHERE operation_id = ?1 AND role = ?2 AND status = 'planned'",
                rusqlite::params![operation_id, role],
            )?,
            "mark artifact writing",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_artifact_ready(
        &mut self,
        operation_id: &str,
        role: &str,
        sha256: &str,
    ) -> Result<()> {
        validate_sha256(sha256)?;
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_one(
            transaction.execute(
                "UPDATE artifacts
                 SET status = 'ready', hash_algorithm = 'sha256', hash = ?3
                 WHERE operation_id = ?1 AND role = ?2 AND status = 'writing'",
                rusqlite::params![operation_id, role, sha256],
            )?,
            "mark artifact ready",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_directory_ready(&mut self, operation_id: &str, role: &str) -> Result<()> {
        require_one(
            self.connection.execute(
                "UPDATE artifacts SET status = 'ready'
                 WHERE operation_id = ?1 AND role = ?2 AND kind = 'directory'
                   AND status = 'writing'",
                rusqlite::params![operation_id, role],
            )?,
            "mark directory artifact ready",
        )
    }

    pub(crate) fn assign_book_id_and_reservation(
        &mut self,
        operation_id: &str,
        expected_old_key: &str,
        book_id: i64,
    ) -> Result<()> {
        if book_id <= 0 {
            anyhow::bail!("Book ID must be positive");
        }
        let new_key = format!("book:{book_id}");
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_one(
            transaction.execute(
                "UPDATE operations SET book_key = ?4,
                     updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
                 WHERE id = ?1 AND phase = 'planned' AND book_key = ?2
                   AND book_id = ?3 AND expected_db_facts IS NOT NULL",
                rusqlite::params![operation_id, expected_old_key, book_id, new_key],
            )?,
            "rebind operation book reservation",
        )?;
        require_one(
            transaction.execute(
                "UPDATE reservations SET book_key = ?3
                 WHERE operation_id = ?1 AND book_key = ?2",
                rusqlite::params![operation_id, expected_old_key, new_key],
            )?,
            "replace provisional reservation",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn active_reservation_keys(&self, user_id: i64) -> Result<HashSet<String>> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT r.book_key
             FROM reservations r
             JOIN operations o ON o.id = r.operation_id
             WHERE r.user_id = ?1 AND o.phase IN (
                 'planned', 'staging', 'staged', 'files_published',
                 'metadata_committed', 'shelf_pending', 'recovery_blocked'
             )",
        )?;
        let keys = statement
            .query_map([user_id], |row| row.get(0))?
            .collect::<std::result::Result<HashSet<String>, _>>()?;
        Ok(keys)
    }

    fn ensure_book_reservation(
        &mut self,
        operation_id: &str,
        user_id: i64,
        book_id: i64,
    ) -> Result<()> {
        let book_key = format!("book:{book_id}");
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT user_id, book_key FROM reservations WHERE operation_id = ?1",
                [operation_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((existing_user, existing_key)) = existing {
            if existing_user != user_id || existing_key != book_key {
                anyhow::bail!("Pending shelf reservation does not match the live target");
            }
            transaction.commit()?;
            return Ok(());
        }
        require_one(
            transaction.execute(
                "UPDATE operations SET book_key = ?4,
                     updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
                 WHERE id = ?1 AND user_id = ?2 AND book_id = ?3
                   AND phase IN ('metadata_committed', 'shelf_pending')",
                rusqlite::params![operation_id, user_id, book_id, book_key],
            )?,
            "record pending Kobo book key",
        )?;
        require_one(
            transaction.execute(
                "INSERT INTO reservations(operation_id, user_id, book_key)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![operation_id, user_id, book_key],
            )?,
            "reserve pending Kobo membership",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn block_recovery(
        &mut self,
        operation_id: &str,
        expected_phase: Phase,
        error: &str,
    ) -> Result<()> {
        if expected_phase == Phase::RecoveryBlocked {
            anyhow::bail!("A recovery-blocked operation cannot be blocked again");
        }
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        require_one(
            transaction.execute(
                "UPDATE operations SET phase = 'recovery_blocked', error = ?3,
                     updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
                 WHERE id = ?1 AND phase = ?2",
                rusqlite::params![operation_id, expected_phase.as_str(), error],
            )?,
            "block recovery",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_complete(
        &mut self,
        operation_id: &str,
        expected_phase: Phase,
    ) -> Result<()> {
        if !matches!(
            expected_phase,
            Phase::MetadataCommitted | Phase::ShelfPending
        ) {
            anyhow::bail!("Complete is only valid after metadata commit or shelf success");
        }
        self.transition_phase(operation_id, expected_phase, Phase::Complete)
    }

    pub(crate) fn mark_rolled_back(
        &mut self,
        operation_id: &str,
        expected_phase: Phase,
    ) -> Result<()> {
        if !matches!(
            expected_phase,
            Phase::Planned | Phase::Staging | Phase::Staged | Phase::FilesPublished
        ) {
            anyhow::bail!("Rolled back is invalid from the requested phase");
        }
        self.transition_phase(operation_id, expected_phase, Phase::RolledBack)
    }

    fn mark_complete_and_delete(&mut self, operation_id: &str) -> Result<()> {
        self.delete_terminal(operation_id, Phase::Complete, CleanupFinished(()))
    }

    fn mark_rolled_back_and_delete(&mut self, operation_id: &str) -> Result<()> {
        self.delete_terminal(operation_id, Phase::RolledBack, CleanupFinished(()))
    }

    fn delete_terminal(
        &mut self,
        operation_id: &str,
        terminal: Phase,
        _cleanup: CleanupFinished,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM operations WHERE id = ?1 AND phase = ?2",
                rusqlite::params![operation_id, terminal.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            anyhow::bail!("Operation is not in the required cleanup-safe terminal phase");
        }
        transaction.execute(
            "DELETE FROM reservations WHERE operation_id = ?1",
            [operation_id],
        )?;
        require_one(
            transaction.execute("DELETE FROM operations WHERE id = ?1", [operation_id])?,
            "delete terminal operation",
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn active_operations(&self) -> Result<Vec<StoredOperation>> {
        let mut statement = self.connection.prepare(
            "SELECT id, phase, metadata_path, library_path, source_path,
                    user_id, shelf_id, shelf_name, username, kobo_sync,
                    book_id, old_db_facts, expected_db_facts
             FROM operations ORDER BY created_at, id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, bool>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
            ))
        })?;
        let mut operations = Vec::new();
        for row in rows {
            let (
                id,
                phase,
                metadata,
                library,
                source,
                user_id,
                shelf_id,
                shelf_name,
                username,
                kobo_sync,
                book_id,
                old_db_facts,
                expected_db_facts,
            ) = row?;
            let artifacts = load_artifacts(&self.connection, &id)?;
            operations.push(StoredOperation {
                id,
                phase: Phase::parse(&phase)?,
                metadata_path: path_from_bytes(metadata)?,
                library_path: path_from_bytes(library)?,
                source_path: path_from_bytes(source)?,
                user_id,
                shelf_id,
                shelf_name,
                username,
                kobo_sync,
                book_id,
                old_db_facts,
                expected_db_facts,
                artifacts,
            });
        }
        Ok(operations)
    }
}

fn migrate_v1_to_v2(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE kobo_sync_plans (
             plan_id TEXT PRIMARY KEY,
             format_version INTEGER NOT NULL,
             appdb_key TEXT NOT NULL,
             canonical_appdb_path BLOB NOT NULL,
             user_id INTEGER NOT NULL,
             username TEXT NOT NULL,
             created_at TEXT NOT NULL,
             parent_applied_plan_id TEXT,
             schema_fingerprint TEXT NOT NULL,
             source_fingerprint TEXT NOT NULL,
             plan_payload BLOB NOT NULL
         );
         CREATE TABLE kobo_sync_plan_graphs (
             plan_id TEXT NOT NULL REFERENCES kobo_sync_plans(plan_id),
             ordinal INTEGER NOT NULL,
             graph_key TEXT NOT NULL,
             user_id INTEGER NOT NULL,
             book_id INTEGER NOT NULL,
             canonical_state_id INTEGER NOT NULL,
             root_weight INTEGER NOT NULL,
             source_fingerprint TEXT NOT NULL,
             disposition TEXT NOT NULL,
             graph_payload BLOB NOT NULL,
             PRIMARY KEY(plan_id, ordinal),
             UNIQUE(plan_id, graph_key)
         );
         CREATE TABLE kobo_sync_plan_applications (
             plan_id TEXT PRIMARY KEY REFERENCES kobo_sync_plans(plan_id),
             operation_id TEXT NOT NULL UNIQUE,
             applied_at TEXT NOT NULL,
             batch_size INTEGER NOT NULL,
             selected_graph_count INTEGER NOT NULL,
             selected_root_weight INTEGER NOT NULL,
             expected_result_fingerprint TEXT NOT NULL
         );
         CREATE TABLE kobo_sync_workflow (
             appdb_key TEXT PRIMARY KEY,
             awaiting_sync_attempt_after_plan TEXT,
             set_at TEXT
         );
         CREATE TRIGGER kobo_sync_plans_immutable_update
         BEFORE UPDATE ON kobo_sync_plans
         BEGIN SELECT RAISE(ABORT, 'immutable Kobo plan'); END;
         CREATE TRIGGER kobo_sync_plans_immutable_delete
         BEFORE DELETE ON kobo_sync_plans
         BEGIN SELECT RAISE(ABORT, 'immutable Kobo plan'); END;
         CREATE TRIGGER kobo_sync_plan_graphs_immutable_update
         BEFORE UPDATE ON kobo_sync_plan_graphs
         BEGIN SELECT RAISE(ABORT, 'immutable Kobo plan'); END;
         CREATE TRIGGER kobo_sync_plan_graphs_immutable_delete
         BEFORE DELETE ON kobo_sync_plan_graphs
         BEGIN SELECT RAISE(ABORT, 'immutable Kobo plan'); END;
         UPDATE schema_version SET version = 2 WHERE singleton = 1 AND version = 1;
         COMMIT;",
    )?;
    Ok(())
}

fn load_artifacts(connection: &Connection, operation_id: &str) -> Result<Vec<StoredArtifact>> {
    let mut statement = connection.prepare(
        "SELECT role, kind, path, status, hash, old_hash, final_hash, final_existed
         FROM artifacts WHERE operation_id = ?1 ORDER BY id",
    )?;
    let rows = statement.query_map([operation_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, bool>(7)?,
        ))
    })?;
    let mut artifacts = Vec::new();
    for row in rows {
        let (role, kind, path, status, hash, old_hash, final_hash, final_existed) = row?;
        let kind = match kind.as_str() {
            "file" => ArtifactKind::File,
            "directory" => ArtifactKind::Directory,
            _ => anyhow::bail!("Unsupported artifact kind {kind}"),
        };
        artifacts.push(StoredArtifact {
            role,
            kind,
            path: path_from_bytes(path)?,
            status,
            hash,
            old_hash,
            final_hash,
            final_existed,
        });
    }
    Ok(artifacts)
}

pub(crate) fn state_path(base_path: &Path) -> PathBuf {
    let mut value = base_path.as_os_str().to_os_string();
    value.push(".calibre-web-helper-state.sqlite3");
    PathBuf::from(value)
}

pub(crate) fn active_reservation_keys_if_present(
    base_path: &Path,
    user_id: i64,
) -> Result<HashSet<String>> {
    let path = state_path(base_path);
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| {
            format!(
                "Failed to read recovery reservations from {}",
                path.display()
            )
        })?;
    let mut statement = connection.prepare(
        "SELECT DISTINCT r.book_key FROM reservations r
         JOIN operations o ON o.id = r.operation_id
         WHERE r.user_id = ?1 AND o.phase IN (
             'planned', 'staging', 'staged', 'files_published',
             'metadata_committed', 'shelf_pending', 'recovery_blocked'
         )",
    )?;
    let keys = statement
        .query_map([user_id], |row| row.get(0))?
        .collect::<std::result::Result<HashSet<String>, _>>()?;
    Ok(keys)
}

fn deterministic_operation_id(
    metadata_path: &Path,
    source_path: &Path,
    source_hash: &str,
) -> String {
    let material = format!(
        "{}\0{}\0{}",
        metadata_path.display(),
        source_path.display(),
        source_hash
    );
    let hash = crate::utils::calculate_bytes_hash(material.as_bytes());
    format!(
        "{}-{}-{}-{}-{}",
        &hash[0..8],
        &hash[8..12],
        &hash[12..16],
        &hash[16..20],
        &hash[20..32]
    )
}

fn verified_hash(path: &Path, expected: &str) -> Result<()> {
    let actual = crate::utils::calculate_file_hash(path)
        .with_context(|| format!("Failed to hash recovery artifact {}", path.display()))?;
    if actual != expected {
        anyhow::bail!(
            "Recovery hash conflict at {}: expected {}, found {}",
            path.display(),
            expected,
            actual
        );
    }
    Ok(())
}

fn validate_stored_path(operation: &StoredOperation, path: &Path) -> Result<PathBuf> {
    let root = operation.library_path.canonicalize().with_context(|| {
        format!(
            "Stored library root {} is unavailable",
            operation.library_path.display()
        )
    })?;
    let relative = path.strip_prefix(&root).with_context(|| {
        format!(
            "Stored artifact {} is outside library root {}",
            path.display(),
            root.display()
        )
    })?;
    let checked = crate::safety::validate_relative_destination(&root, relative)?;
    if checked != path {
        anyhow::bail!("Stored artifact path changed identity: {}", path.display());
    }
    Ok(checked)
}

fn sync_artifact_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("Recovery artifact has no parent")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn artifact<'a>(operation: &'a StoredOperation, role: &str) -> Option<&'a StoredArtifact> {
    operation.artifacts.iter().find(|value| value.role == role)
}

fn verify_ready_final(operation: &StoredOperation, role: &str) -> Result<()> {
    let value = artifact(operation, role).with_context(|| format!("Missing {role} intent"))?;
    if value.status != "ready" {
        anyhow::bail!("Artifact {role} is not durably ready");
    }
    let expected = value
        .final_hash
        .as_deref()
        .context("Final artifact has no expected hash")?;
    let path = validate_stored_path(operation, &value.path)?;
    verified_hash(&path, expected)
}

fn verify_all_ready_finals(operation: &StoredOperation) -> Result<()> {
    let finals = operation
        .artifacts
        .iter()
        .filter(|value| value.role.ends_with("-final"))
        .collect::<Vec<_>>();
    if finals.is_empty() {
        anyhow::bail!("Recovery operation has no recorded final artifacts");
    }
    for value in finals {
        verify_ready_final(operation, &value.role)?;
    }
    Ok(())
}

fn validate_operation_identity(operation: &StoredOperation, calibre: &Connection) -> Result<()> {
    let actual_metadata = Path::new(
        calibre
            .path()
            .context("Recovery requires a file-backed Calibre database")?,
    )
    .canonicalize()
    .context("Failed to canonicalize the active Calibre database")?;
    let stored_metadata = operation
        .metadata_path
        .canonicalize()
        .context("Failed to canonicalize the stored Calibre database")?;
    if actual_metadata != stored_metadata {
        anyhow::bail!(
            "Recovery operation {} belongs to {}, not {}",
            operation.id,
            stored_metadata.display(),
            actual_metadata.display()
        );
    }
    let stored_library = operation
        .library_path
        .canonicalize()
        .context("Failed to canonicalize the stored library root")?;
    if stored_metadata.parent() != Some(stored_library.as_path()) {
        anyhow::bail!(
            "Recovery metadata path {} is not directly inside stored library root {}",
            stored_metadata.display(),
            stored_library.display()
        );
    }
    if !operation.source_path.is_absolute() {
        anyhow::bail!(
            "Recovery source evidence path is not absolute: {}",
            operation.source_path.display()
        );
    }
    Ok(())
}

fn cleanup_terminal_files(operation: &StoredOperation) -> Result<()> {
    for value in &operation.artifacts {
        if value.kind == ArtifactKind::Directory {
            continue;
        }
        if !(value.role.ends_with("-stage") || value.role.ends_with("-backup")) {
            continue;
        }
        if value.path.exists() {
            let path = validate_stored_path(operation, &value.path)?;
            let expected = value
                .hash
                .as_deref()
                .context("Cleanup artifact has no ready hash")?;
            verified_hash(&path, expected)?;
            fs::remove_file(&path)?;
            sync_artifact_parent(&path)?;
        }
    }
    Ok(())
}

fn rollback_files(operation: &StoredOperation) -> Result<()> {
    if operation
        .artifacts
        .iter()
        .any(|value| value.status == "writing")
    {
        anyhow::bail!("A recovery artifact was interrupted while writing; evidence preserved");
    }

    for prefix in ["epub", "cover"] {
        let final_role = format!("{prefix}-final");
        let backup_role = format!("{prefix}-backup");
        let Some(final_artifact) = artifact(operation, &final_role) else {
            continue;
        };
        let final_path = validate_stored_path(operation, &final_artifact.path)?;
        let backup = artifact(operation, &backup_role);
        if let Some(backup) = backup {
            let backup_path = validate_stored_path(operation, &backup.path)?;
            if backup.status == "ready" {
                let old_hash = backup
                    .old_hash
                    .as_deref()
                    .context("Backup has no old hash")?;
                verified_hash(&backup_path, old_hash)?;
                if final_path.exists() {
                    let expected = final_artifact
                        .final_hash
                        .as_deref()
                        .context("Published final has no expected hash")?;
                    verified_hash(&final_path, expected)?;
                    fs::remove_file(&final_path)?;
                }
                fs::rename(&backup_path, &final_path)?;
                sync_artifact_parent(&final_path)?;
            } else if backup_path.exists() {
                anyhow::bail!(
                    "Unready backup exists at {}; evidence preserved",
                    backup.path.display()
                );
            }
        } else if !final_artifact.final_existed && final_path.exists() {
            let expected = final_artifact
                .final_hash
                .as_deref()
                .context("New final has no expected hash")?;
            verified_hash(&final_path, expected)?;
            fs::remove_file(&final_path)?;
            sync_artifact_parent(&final_path)?;
        }
    }

    for value in operation.artifacts.iter().rev() {
        if value.role.ends_with("-stage") && value.path.exists() {
            let path = validate_stored_path(operation, &value.path)?;
            let hash = value.hash.as_deref().context("Stage has no ready hash")?;
            verified_hash(&path, hash)?;
            fs::remove_file(&path)?;
            sync_artifact_parent(&path)?;
        }
        if value.kind == ArtifactKind::Directory && !value.final_existed && value.path.exists() {
            let path = validate_stored_path(operation, &value.path)?;
            if value.status != "ready" {
                anyhow::bail!(
                    "Unready recorded directory exists at {}; evidence preserved",
                    value.path.display()
                );
            }
            fs::remove_dir(&path).with_context(|| {
                format!("Unrecorded entry prevents removing {}", path.display())
            })?;
            sync_artifact_parent(&path)?;
        }
    }
    Ok(())
}

fn shelf_target(operation: &StoredOperation) -> Result<Option<crate::appdb::ShelfTarget>> {
    match (operation.user_id, operation.shelf_id, &operation.shelf_name) {
        (Some(user_id), Some(shelf_id), Some(shelf_name)) => Ok(Some(crate::appdb::ShelfTarget {
            user_id,
            shelf_id,
            shelf_name: shelf_name.clone(),
            username: operation
                .username
                .clone()
                .context("Stored shelf operation has no username")?,
            kobo_sync: operation.kobo_sync,
        })),
        (None, None, None) => Ok(None),
        _ => anyhow::bail!("Stored shelf identity is incomplete"),
    }
}

fn retry_shelf_assignment(
    state: &mut StateDb,
    appdb: &mut Connection,
    operation_id: &str,
    stored_target: &crate::appdb::ShelfTarget,
    book_id: i64,
) -> Result<bool> {
    let live_target = crate::appdb::revalidate_shelf_target(appdb, stored_target)?;
    if live_target.kobo_sync {
        state.ensure_book_reservation(operation_id, live_target.user_id, book_id)?;
        let capacity = crate::safety::capacity_keys(
            appdb,
            live_target.user_id,
            state.active_reservation_keys(live_target.user_id)?,
        )?;
        if capacity.len() > 100 {
            anyhow::bail!(
                "Kobo retry remains pending because the distinct backlog is {} (>100)",
                capacity.len()
            );
        }
    }
    crate::appdb::add_book_to_resolved_shelf(appdb, &live_target, book_id)
}

fn block_with_evidence(
    state: &mut StateDb,
    operation: &StoredOperation,
    error: anyhow::Error,
) -> Result<()> {
    if operation.phase != Phase::RecoveryBlocked {
        state.block_recovery(&operation.id, operation.phase, &format!("{error:#}"))?;
    }
    Err(error)
}

pub(crate) fn recover_pending_adds(
    state: &mut StateDb,
    calibre: &Connection,
    mut appdb: Option<&mut Connection>,
) -> Result<()> {
    for mut operation in state.active_operations()? {
        if operation.phase == Phase::RecoveryBlocked {
            anyhow::bail!(
                "Recovery is blocked for operation {}; inspect {} before any further mutation",
                operation.id,
                state.path().display()
            );
        }
        if operation.phase == Phase::Complete {
            cleanup_terminal_files(&operation)
                .or_else(|error| block_with_evidence(state, &operation, error))?;
            state.mark_complete_and_delete(&operation.id)?;
            continue;
        }
        if operation.phase == Phase::RolledBack {
            cleanup_terminal_files(&operation)
                .or_else(|error| block_with_evidence(state, &operation, error))?;
            state.mark_rolled_back_and_delete(&operation.id)?;
            continue;
        }
        if let Err(error) = validate_operation_identity(&operation, calibre) {
            return block_with_evidence(state, &operation, error);
        }
        let book_id = operation
            .book_id
            .context("Stored add operation has no book ID")?;
        let current = crate::calibre::book_facts_hash(calibre, book_id)?;
        if current == operation.expected_db_facts {
            if let Err(error) = verify_all_ready_finals(&operation) {
                return block_with_evidence(state, &operation, error);
            }
            while operation.phase != Phase::MetadataCommitted
                && operation.phase != Phase::ShelfPending
            {
                let next = match operation.phase {
                    Phase::Planned => Phase::Staging,
                    Phase::Staging => Phase::Staged,
                    Phase::Staged => Phase::FilesPublished,
                    Phase::FilesPublished => Phase::MetadataCommitted,
                    _ => anyhow::bail!("Unexpected recovery phase before metadata commit"),
                };
                state.transition_phase(&operation.id, operation.phase, next)?;
                operation.phase = next;
            }
            if let Some(target) = shelf_target(&operation)? {
                if operation.phase == Phase::MetadataCommitted {
                    state.transition_phase(
                        &operation.id,
                        Phase::MetadataCommitted,
                        Phase::ShelfPending,
                    )?;
                    operation.phase = Phase::ShelfPending;
                }
                let appdb = appdb
                    .as_deref_mut()
                    .context("app.db is required to retry shelf assignment")?;
                retry_shelf_assignment(state, appdb, &operation.id, &target, book_id)?;
                state.mark_complete(&operation.id, Phase::ShelfPending)?;
            } else {
                state.mark_complete(&operation.id, Phase::MetadataCommitted)?;
            }
            let refreshed = state
                .active_operations()?
                .into_iter()
                .find(|value| value.id == operation.id)
                .context("Completed recovery operation disappeared")?;
            cleanup_terminal_files(&refreshed)?;
            state.mark_complete_and_delete(&operation.id)?;
        } else if current == operation.old_db_facts {
            if let Err(error) = rollback_files(&operation) {
                return block_with_evidence(state, &operation, error);
            }
            state.mark_rolled_back(&operation.id, operation.phase)?;
            state.mark_rolled_back_and_delete(&operation.id)?;
        } else {
            return block_with_evidence(
                state,
                &operation,
                anyhow::anyhow!("Metadata facts match neither the recorded old nor expected state"),
            );
        }
    }
    Ok(())
}

fn add_file_intents(
    intents: &mut Vec<ArtifactIntent>,
    prefix: &str,
    final_path: PathBuf,
    stage_path: PathBuf,
    backup_path: PathBuf,
    final_hash: &str,
) -> Result<()> {
    let old_hash = if final_path.exists() {
        Some(crate::utils::calculate_file_hash(&final_path)?)
    } else {
        None
    };
    intents.push(ArtifactIntent {
        role: format!("{prefix}-stage"),
        kind: ArtifactKind::File,
        path: stage_path,
        old_hash: None,
        final_hash: Some(final_hash.to_owned()),
        final_existed: false,
    });
    if let Some(hash) = &old_hash {
        intents.push(ArtifactIntent {
            role: format!("{prefix}-backup"),
            kind: ArtifactKind::File,
            path: backup_path,
            old_hash: Some(hash.clone()),
            final_hash: None,
            final_existed: false,
        });
    }
    intents.push(ArtifactIntent {
        role: format!("{prefix}-final"),
        kind: ArtifactKind::File,
        path: final_path,
        old_hash,
        final_hash: Some(final_hash.to_owned()),
        final_existed: intents
            .iter()
            .any(|value| value.role == format!("{prefix}-backup")),
    });
    Ok(())
}

fn create_recorded_directories(
    state: &mut StateDb,
    operation_id: &str,
    operation: &StoredOperation,
) -> Result<()> {
    for value in operation
        .artifacts
        .iter()
        .filter(|value| value.kind == ArtifactKind::Directory)
    {
        let path = validate_stored_path(operation, &value.path)?;
        state.mark_artifact_writing(operation_id, &value.role)?;
        fs::create_dir(&path)
            .with_context(|| format!("Failed to create recorded directory {}", path.display()))?;
        sync_artifact_parent(&path)?;
        state.mark_directory_ready(operation_id, &value.role)?;
    }
    Ok(())
}

fn stage_bytes(
    state: &mut StateDb,
    operation_id: &str,
    role: &str,
    operation: &StoredOperation,
    bytes: &[u8],
) -> Result<()> {
    let value = artifact(operation, role).with_context(|| format!("Missing {role} intent"))?;
    let path = validate_stored_path(operation, &value.path)?;
    state.mark_artifact_writing(operation_id, role)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    let hash = crate::utils::calculate_file_hash(&path)?;
    state.mark_artifact_ready(operation_id, role, &hash)?;
    sync_artifact_parent(&path)
}

fn backup_and_publish(
    state: &mut StateDb,
    operation_id: &str,
    operation: &StoredOperation,
    prefix: &str,
) -> Result<()> {
    let stage = artifact(operation, &format!("{prefix}-stage")).context("Missing stage intent")?;
    let final_artifact =
        artifact(operation, &format!("{prefix}-final")).context("Missing final intent")?;
    let stage_path = validate_stored_path(operation, &stage.path)?;
    let final_path = validate_stored_path(operation, &final_artifact.path)?;
    if let Some(backup) = artifact(operation, &format!("{prefix}-backup")) {
        let backup_path = validate_stored_path(operation, &backup.path)?;
        state.mark_artifact_writing(operation_id, &backup.role)?;
        fs::rename(&final_path, &backup_path)?;
        sync_artifact_parent(&final_path)?;
        let hash = crate::utils::calculate_file_hash(&backup_path)?;
        let expected = backup
            .old_hash
            .as_deref()
            .context("Backup has no expected old hash")?;
        if hash != expected {
            anyhow::bail!("Backup hash changed while publishing {prefix}");
        }
        state.mark_artifact_ready(operation_id, &backup.role, &hash)?;
    }
    state.mark_artifact_writing(operation_id, &final_artifact.role)?;
    fs::rename(&stage_path, &final_path)?;
    sync_artifact_parent(&final_path)?;
    let hash = crate::utils::calculate_file_hash(&final_path)?;
    let expected = final_artifact
        .final_hash
        .as_deref()
        .context("Final intent has no expected hash")?;
    if hash != expected {
        anyhow::bail!("Published {prefix} hash does not match its intent");
    }
    state.mark_artifact_ready(operation_id, &final_artifact.role, &hash)
}

fn operation_by_id(state: &StateDb, operation_id: &str) -> Result<StoredOperation> {
    state
        .active_operations()?
        .into_iter()
        .find(|value| value.id == operation_id)
        .with_context(|| format!("Recovery operation {operation_id} disappeared"))
}

pub(crate) fn durable_add(
    calibre: &mut Connection,
    mut appdb: Option<&mut Connection>,
    metadata_path: &Path,
    state_base: &Path,
    source: &Path,
    metadata: &crate::models::BookMetadata,
    target: Option<&crate::appdb::ShelfTarget>,
) -> Result<crate::models::UpsertResult> {
    let library = metadata_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("Failed to canonicalize Calibre library root")?;
    let metadata_path = metadata_path
        .canonicalize()
        .context("Failed to canonicalize metadata.db")?;
    let mut state = StateDb::open(state_base)?;
    recover_pending_adds(&mut state, calibre, appdb.as_deref_mut())?;

    let source_path = source
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize source EPUB {}", source.display()))?;
    let source_bytes = fs::read(&source_path)?;
    let source_hash = crate::utils::calculate_bytes_hash(&source_bytes);
    let cover_bytes = crate::epub::extract_cover_bytes(source)?;
    let cover_hash = cover_bytes
        .as_deref()
        .map(crate::utils::calculate_bytes_hash);
    let existing = crate::calibre::find_existing_book(calibre, metadata)?;
    let existing_format_path = existing
        .as_ref()
        .map(|(_, path)| crate::calibre::get_existing_book_file_path(&library, path))
        .transpose()?
        .flatten();
    let old_facts = existing
        .as_ref()
        .map(|(book_id, _)| crate::calibre::book_facts_hash(calibre, *book_id))
        .transpose()?
        .flatten();
    let operation_id = deterministic_operation_id(&metadata_path, &source_path, &source_hash);

    let transaction = calibre
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .context("Failed to start durable metadata transaction")?;
    let result =
        crate::calibre::add_book_to_transaction(&transaction, metadata, &library, source, false)?;
    let book_id = result.book_id();
    let book_path = PathBuf::from(result.book_path());
    let destination_directory = crate::safety::validate_relative_destination(&library, &book_path)?;
    let filename = crate::epub::destination_filename(metadata, source)?;
    let final_epub = existing_format_path.unwrap_or(crate::safety::validate_relative_destination(
        &library,
        &book_path.join(filename),
    )?);
    let final_cover =
        crate::safety::validate_relative_destination(&library, &book_path.join("cover.jpg"))?;
    let cover_installed =
        cover_bytes.is_some() || crate::epub::is_valid_installed_jpeg(&final_cover);
    crate::calibre::set_book_cover(&transaction, book_id, cover_installed)?;
    let expected_facts = crate::calibre::book_facts_hash(&transaction, book_id)?
        .context("Prepared book has no expected metadata facts")?;

    let mut intents = Vec::new();
    let mut current = library.to_path_buf();
    for (index, component) in book_path.components().enumerate() {
        current.push(component.as_os_str());
        if !current.exists() {
            intents.push(ArtifactIntent {
                role: format!("directory-{index}"),
                kind: ArtifactKind::Directory,
                path: current.clone(),
                old_hash: None,
                final_hash: None,
                final_existed: false,
            });
        }
    }
    let epub_stage =
        destination_directory.join(format!(".calibre-web-helper-{operation_id}.epub.stage"));
    let epub_backup =
        destination_directory.join(format!(".calibre-web-helper-{operation_id}.epub.backup"));
    add_file_intents(
        &mut intents,
        "epub",
        final_epub,
        epub_stage,
        epub_backup,
        &source_hash,
    )?;
    if let Some(hash) = &cover_hash {
        let cover_stage =
            destination_directory.join(format!(".calibre-web-helper-{operation_id}.cover.stage"));
        let cover_backup =
            destination_directory.join(format!(".calibre-web-helper-{operation_id}.cover.backup"));
        add_file_intents(
            &mut intents,
            "cover",
            final_cover,
            cover_stage,
            cover_backup,
            hash,
        )?;
    }

    let reservation = if let Some(target) = target.filter(|value| value.kobo_sync) {
        let key = if existing.is_some() {
            crate::appdb::planned_existing_book_key(
                appdb.as_deref().context("Kobo shelf requires app.db")?,
                target,
                book_id,
            )?
        } else {
            Some(format!("new:{operation_id}"))
        };
        key.map(|book_key| ReservationIntent {
            user_id: target.user_id,
            book_key,
        })
    } else {
        None
    };
    state.begin_operation(&OperationIntent {
        id: operation_id.clone(),
        metadata_path,
        library_path: library,
        source_path,
        user_id: target.map(|value| value.user_id),
        shelf_id: target.map(|value| value.shelf_id),
        shelf_name: target.map(|value| value.shelf_name.clone()),
        username: target.map(|value| value.username.clone()),
        kobo_sync: target.is_some_and(|value| value.kobo_sync),
        reservation,
        artifacts: intents.clone(),
    })?;
    state.record_book_facts(
        &operation_id,
        book_id,
        &book_path,
        existing.is_some(),
        old_facts.as_deref(),
        &expected_facts,
    )?;
    if target.is_some_and(|value| value.kobo_sync) && existing.is_none() {
        state.assign_book_id_and_reservation(
            &operation_id,
            &format!("new:{operation_id}"),
            book_id,
        )?;
    }

    if let Some(target) = target.filter(|value| value.kobo_sync) {
        let capacity = crate::safety::capacity_keys(
            appdb.as_deref().context("Kobo shelf requires app.db")?,
            target.user_id,
            state.active_reservation_keys(target.user_id)?,
        )?;
        if capacity.len() > 100 {
            drop(transaction);
            recover_pending_adds(&mut state, calibre, appdb.as_deref_mut())?;
            anyhow::bail!(
                "Kobo sync backlog would contain {} distinct books; maximum safe size is 100",
                capacity.len()
            );
        }
    }

    let publish_result = (|| -> Result<()> {
        state.transition_phase(&operation_id, Phase::Planned, Phase::Staging)?;
        let operation = operation_by_id(&state, &operation_id)?;
        create_recorded_directories(&mut state, &operation_id, &operation)?;
        let operation = operation_by_id(&state, &operation_id)?;
        stage_bytes(
            &mut state,
            &operation_id,
            "epub-stage",
            &operation,
            &source_bytes,
        )?;
        if let Some(bytes) = &cover_bytes {
            let operation = operation_by_id(&state, &operation_id)?;
            stage_bytes(&mut state, &operation_id, "cover-stage", &operation, bytes)?;
        }
        state.transition_phase(&operation_id, Phase::Staging, Phase::Staged)?;
        let operation = operation_by_id(&state, &operation_id)?;
        backup_and_publish(&mut state, &operation_id, &operation, "epub")?;
        if cover_bytes.is_some() {
            let operation = operation_by_id(&state, &operation_id)?;
            backup_and_publish(&mut state, &operation_id, &operation, "cover")?;
        }
        state.transition_phase(&operation_id, Phase::Staged, Phase::FilesPublished)?;
        Ok(())
    })();
    if let Err(error) = publish_result {
        drop(transaction);
        if let Err(recovery_error) = recover_pending_adds(&mut state, calibre, appdb.as_deref_mut())
        {
            return Err(error.context(format!(
                "automatic rollback also failed: {recovery_error:#}"
            )));
        }
        return Err(error);
    }

    transaction
        .commit()
        .context("Failed to commit durable book metadata")?;
    state.transition_phase(
        &operation_id,
        Phase::FilesPublished,
        Phase::MetadataCommitted,
    )?;
    if let Some(target) = target {
        state.transition_phase(&operation_id, Phase::MetadataCommitted, Phase::ShelfPending)?;
        let appdb = appdb.context("Shelf assignment requires app.db")?;
        let shelf_result =
            retry_shelf_assignment(&mut state, appdb, &operation_id, target, book_id);
        if let Err(error) = shelf_result {
            anyhow::bail!(
                "Book metadata/files committed, but shelf assignment is pending: {error:#}. The next mutating add will retry it"
            );
        }
        state.mark_complete(&operation_id, Phase::ShelfPending)?;
    } else {
        state.mark_complete(&operation_id, Phase::MetadataCommitted)?;
    }
    let operation = operation_by_id(&state, &operation_id)?;
    cleanup_terminal_files(&operation)
        .or_else(|error| block_with_evidence(&mut state, &operation, error))?;
    state.mark_complete_and_delete(&operation_id)?;
    Ok(result)
}

fn valid_transition(from: Phase, to: Phase) -> bool {
    matches!(
        (from, to),
        (Phase::Planned, Phase::Staging)
            | (Phase::Staging, Phase::Staged)
            | (Phase::Staged, Phase::FilesPublished)
            | (Phase::FilesPublished, Phase::MetadataCommitted)
            | (Phase::MetadataCommitted, Phase::ShelfPending)
            | (Phase::MetadataCommitted, Phase::Complete)
            | (Phase::ShelfPending, Phase::Complete)
            | (Phase::Planned, Phase::RecoveryBlocked)
            | (Phase::Staging, Phase::RecoveryBlocked)
            | (Phase::Staged, Phase::RecoveryBlocked)
            | (Phase::FilesPublished, Phase::RecoveryBlocked)
            | (Phase::MetadataCommitted, Phase::RecoveryBlocked)
            | (Phase::ShelfPending, Phase::RecoveryBlocked)
            | (Phase::Complete, Phase::RecoveryBlocked)
            | (Phase::RolledBack, Phase::RecoveryBlocked)
            | (Phase::Planned, Phase::RolledBack)
            | (Phase::Staging, Phase::RolledBack)
            | (Phase::Staged, Phase::RolledBack)
            | (Phase::FilesPublished, Phase::RolledBack)
    )
}

fn require_one(changed: usize, action: &str) -> Result<()> {
    if changed != 1 {
        anyhow::bail!("Recovery invariant failed during {action}: changed {changed} rows");
    }
    Ok(())
}

fn validate_sha256(hash: &str) -> Result<()> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        anyhow::bail!("SHA-256 must contain exactly 64 lowercase hexadecimal characters");
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn path_bytes(path: &Path) -> Result<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    Ok(path.as_os_str().as_bytes().to_vec())
}

#[cfg(unix)]
pub(crate) fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

#[cfg(windows)]
pub(crate) fn path_bytes(path: &Path) -> Result<Vec<u8>> {
    use std::os::windows::ffi::OsStrExt;
    let mut bytes = Vec::new();
    for unit in path.as_os_str().encode_wide() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    Ok(bytes)
}

#[cfg(windows)]
pub(crate) fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    if !bytes.len().is_multiple_of(2) {
        anyhow::bail!("Invalid encoded Windows recovery path");
    }
    let units = bytes
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    Ok(PathBuf::from(std::ffi::OsString::from_wide(&units)))
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn path_bytes(path: &Path) -> Result<Vec<u8>> {
    path.as_os_str()
        .to_str()
        .map(|value| value.as_bytes().to_vec())
        .context("Recovery paths must be valid UTF-8 on this platform")
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf> {
    String::from_utf8(bytes)
        .map(PathBuf::from)
        .context("Recovery path is not valid UTF-8")
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)
            .with_context(|| format!("Failed to open recovery state parent {}", parent.display()))?
            .sync_all()
            .with_context(|| {
                format!("Failed to sync recovery state parent {}", parent.display())
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn temp_root(label: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("cwh-recovery-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        path
    }

    fn insert_operation(connection: &Connection, id: &str, phase: &str) -> rusqlite::Result<()> {
        connection.execute(
            "INSERT INTO operations (
                 id, kind, phase, metadata_path, library_path, source_path,
                 user_id, shelf_id, shelf_name, book_key
             ) VALUES (?1, 'add', ?2, ?3, ?4, ?5, 7, 8, 'Shelf', 'new:key')",
            params![id, phase, b"metadata.db", b"library", b"source.epub"],
        )?;
        Ok(())
    }

    #[test]
    fn state_path_uses_exact_fixed_suffix() {
        assert_eq!(
            state_path(Path::new("/library/app.db")),
            PathBuf::from("/library/app.db.calibre-web-helper-state.sqlite3")
        );
    }

    #[test]
    fn open_creates_schema_with_required_pragmas() {
        let root = temp_root("schema");
        let base = root.join("app.db");
        let state = StateDb::open(&base).unwrap();
        assert_eq!(state.path(), state_path(&base));
        assert_eq!(
            state
                .connection()
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "delete"
        );
        assert_eq!(
            state
                .connection()
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            state
                .connection()
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            state
                .connection()
                .query_row("SELECT version FROM schema_version", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        for table in ["operations", "reservations", "artifacts"] {
            let exists: i64 = state
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1);
        }
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn reopen_preserves_operation_reservation_and_artifact() {
        let root = temp_root("reopen");
        let base = root.join("app.db");
        {
            let state = StateDb::open(&base).unwrap();
            insert_operation(state.connection(), "op-1", "planned").unwrap();
            state.connection().execute(
                "INSERT INTO reservations(operation_id, user_id, book_key) VALUES ('op-1', 7, 'new:key')",
                [],
            ).unwrap();
            state
                .connection()
                .execute(
                    "INSERT INTO artifacts (
                     operation_id, role, kind, path, status, hash_algorithm, hash,
                     old_hash, final_hash, final_existed
                 ) VALUES ('op-1', 'epub-final', 'file', ?1, 'ready', 'sha256',
                           'content-hash', 'old-hash', 'final-hash', 1)",
                    [b"Author/Book/book.epub".as_slice()],
                )
                .unwrap();
        }
        let state = StateDb::open(&base).unwrap();
        for table in ["operations", "reservations", "artifacts"] {
            assert_eq!(
                state
                    .connection()
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row
                        .get::<_, i64>(0),)
                    .unwrap(),
                1
            );
        }
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn phase_and_artifact_status_constraints_reject_invalid_values() {
        let root = temp_root("constraints");
        let base = root.join("app.db");
        let state = StateDb::open(&base).unwrap();
        assert!(insert_operation(state.connection(), "bad", "unknown").is_err());
        insert_operation(state.connection(), "good", "planned").unwrap();
        assert!(
            state
                .connection()
                .execute(
                    "INSERT INTO artifacts(operation_id, role, kind, path, status)
             VALUES ('good', 'epub-stage', 'file', ?1, 'partial')",
                    [b"stage.epub".as_slice()],
                )
                .is_err()
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    fn operation_intent(id: &str) -> OperationIntent {
        OperationIntent {
            id: id.to_owned(),
            metadata_path: PathBuf::from("metadata.db"),
            library_path: PathBuf::from("library"),
            source_path: PathBuf::from("source.epub"),
            user_id: Some(7),
            shelf_id: Some(8),
            shelf_name: Some("Shelf".to_owned()),
            username: Some("reader".to_owned()),
            kobo_sync: true,
            reservation: Some(ReservationIntent {
                user_id: 7,
                book_key: "new:key".to_owned(),
            }),
            artifacts: vec![
                ArtifactIntent {
                    role: "epub-stage".to_owned(),
                    kind: ArtifactKind::File,
                    path: PathBuf::from("Author/Book/.stage.epub"),
                    old_hash: None,
                    final_hash: Some("a".repeat(64)),
                    final_existed: false,
                },
                ArtifactIntent {
                    role: "book-directory".to_owned(),
                    kind: ArtifactKind::Directory,
                    path: PathBuf::from("Author/Book"),
                    old_hash: None,
                    final_hash: None,
                    final_existed: false,
                },
            ],
        }
    }

    fn retry_appdb(unsynced: i64) -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE user(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                 CREATE TABLE shelf(id INTEGER PRIMARY KEY, name TEXT NOT NULL,
                     user_id INTEGER NOT NULL, kobo_sync INTEGER NOT NULL,
                     last_modified TEXT);
                 CREATE TABLE book_shelf_link(id INTEGER PRIMARY KEY,
                     book_id INTEGER NOT NULL, shelf INTEGER NOT NULL,
                     \"order\" INTEGER NOT NULL, date_added TEXT,
                     UNIQUE(book_id, shelf));
                 CREATE TABLE kobo_synced_books(user_id INTEGER NOT NULL,
                     book_id INTEGER NOT NULL);
                 INSERT OR REPLACE INTO user VALUES(7, 'reader');
                 INSERT OR REPLACE INTO shelf
                     VALUES(8, 'Shelf', 7, 0, '2020-01-01 00:00:00.000000');",
            )
            .unwrap();
        for offset in 0..unsynced {
            connection
                .execute(
                    "INSERT INTO book_shelf_link(book_id, shelf, \"order\", date_added)
                     VALUES(?1, 8, ?2, '2020-01-01 00:00:00.000000')",
                    rusqlite::params![10_000 + offset, offset + 1],
                )
                .unwrap();
        }
        connection
    }

    fn pending_non_kobo_operation(state: &mut StateDb, id: &str, book_id: i64) {
        let mut intent = operation_intent(id);
        intent.kobo_sync = false;
        intent.reservation = None;
        state.begin_operation(&intent).unwrap();
        state
            .record_book_facts(
                id,
                book_id,
                Path::new("Author/Book"),
                false,
                None,
                &"a".repeat(64),
            )
            .unwrap();
        advance_to_metadata_committed(state, id);
        state
            .transition_phase(id, Phase::MetadataCommitted, Phase::ShelfPending)
            .unwrap();
    }

    #[test]
    fn pending_non_kobo_to_kobo_flip_revalidates_capacity_and_retains_reservation() {
        let root = temp_root("retry-kobo-flip");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        pending_non_kobo_operation(&mut state, "op-1", 500);
        let mut appdb = retry_appdb(100);
        appdb
            .execute("UPDATE shelf SET kobo_sync = 1 WHERE id = 8", [])
            .unwrap();
        let stored = crate::appdb::ShelfTarget {
            user_id: 7,
            shelf_id: 8,
            shelf_name: "Shelf".to_owned(),
            username: "reader".to_owned(),
            kobo_sync: false,
        };

        let error =
            retry_shelf_assignment(&mut state, &mut appdb, "op-1", &stored, 500).unwrap_err();
        assert!(error.to_string().contains("101 (>100)"));
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link WHERE book_id = 500",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            state.active_reservation_keys(7).unwrap(),
            HashSet::from(["book:500".to_owned()])
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn pending_retry_rejects_user_or_shelf_rename_without_membership_mutation() {
        let root = temp_root("retry-renames");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        pending_non_kobo_operation(&mut state, "op-1", 500);
        let mut appdb = retry_appdb(0);
        let stored = crate::appdb::ShelfTarget {
            user_id: 7,
            shelf_id: 8,
            shelf_name: "Shelf".to_owned(),
            username: "reader".to_owned(),
            kobo_sync: false,
        };

        appdb
            .execute("UPDATE user SET name = 'renamed' WHERE id = 7", [])
            .unwrap();
        assert!(retry_shelf_assignment(&mut state, &mut appdb, "op-1", &stored, 500).is_err());
        appdb
            .execute("UPDATE user SET name = 'reader' WHERE id = 7", [])
            .unwrap();
        appdb
            .execute("UPDATE shelf SET name = 'renamed' WHERE id = 8", [])
            .unwrap();
        assert!(retry_shelf_assignment(&mut state, &mut appdb, "op-1", &stored, 500).is_err());
        assert_eq!(
            appdb
                .query_row(
                    "SELECT COUNT(*) FROM book_shelf_link WHERE book_id = 500",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    fn ready_file(
        role: &str,
        path: PathBuf,
        hash: &str,
        old_hash: Option<&str>,
        final_hash: Option<&str>,
        final_existed: bool,
    ) -> StoredArtifact {
        StoredArtifact {
            role: role.to_owned(),
            kind: ArtifactKind::File,
            path,
            status: "ready".to_owned(),
            hash: Some(hash.to_owned()),
            old_hash: old_hash.map(str::to_owned),
            final_hash: final_hash.map(str::to_owned),
            final_existed,
        }
    }

    fn stored_operation(library_path: PathBuf, artifacts: Vec<StoredArtifact>) -> StoredOperation {
        StoredOperation {
            id: "stored-op".to_owned(),
            phase: Phase::FilesPublished,
            metadata_path: library_path.join("metadata.db"),
            source_path: library_path.join("source.epub"),
            library_path,
            user_id: None,
            shelf_id: None,
            shelf_name: None,
            username: None,
            kobo_sync: false,
            book_id: Some(1),
            old_db_facts: None,
            expected_db_facts: None,
            artifacts,
        }
    }

    #[test]
    fn stored_out_of_root_final_is_rejected_without_touching_sentinel() {
        let root = temp_root("stored-outside");
        let library = root.join("library");
        let outside = root.join("outside");
        fs::create_dir(&library).unwrap();
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel.epub");
        let contents = b"outside sentinel";
        fs::write(&sentinel, contents).unwrap();
        let hash = crate::utils::calculate_bytes_hash(contents);
        let operation = stored_operation(
            library,
            vec![ready_file(
                "epub-final",
                sentinel.clone(),
                &hash,
                None,
                Some(&hash),
                false,
            )],
        );

        assert!(verify_all_ready_finals(&operation).is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), contents);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn stored_symlinked_final_is_rejected_without_touching_sentinel() {
        let root = temp_root("stored-symlink");
        let library = root.join("library");
        let outside = root.join("outside");
        fs::create_dir(&library).unwrap();
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel.epub");
        let contents = b"symlink sentinel";
        fs::write(&sentinel, contents).unwrap();
        symlink(&outside, library.join("escape")).unwrap();
        let hash = crate::utils::calculate_bytes_hash(contents);
        let operation = stored_operation(
            library,
            vec![ready_file(
                "epub-final",
                root.join("library/escape/sentinel.epub"),
                &hash,
                None,
                Some(&hash),
                false,
            )],
        );

        assert!(verify_all_ready_finals(&operation).is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), contents);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn all_recorded_finals_include_missing_and_mismatched_cover() {
        let root = temp_root("all-finals");
        let library = root.join("library");
        fs::create_dir(&library).unwrap();
        let epub = library.join("book.epub");
        let cover = library.join("cover.jpg");
        let epub_contents = b"published epub";
        let cover_contents = b"published cover";
        fs::write(&epub, epub_contents).unwrap();
        let epub_hash = crate::utils::calculate_bytes_hash(epub_contents);
        let cover_hash = crate::utils::calculate_bytes_hash(cover_contents);
        let operation = stored_operation(
            library,
            vec![
                ready_file(
                    "epub-final",
                    epub,
                    &epub_hash,
                    None,
                    Some(&epub_hash),
                    false,
                ),
                ready_file(
                    "cover-final",
                    cover.clone(),
                    &cover_hash,
                    None,
                    Some(&cover_hash),
                    false,
                ),
            ],
        );

        assert!(verify_all_ready_finals(&operation).is_err());
        fs::write(&cover, b"wrong cover").unwrap();
        assert!(verify_all_ready_finals(&operation).is_err());
        fs::write(&cover, cover_contents).unwrap();
        verify_all_ready_finals(&operation).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rollback_restores_verified_backup_and_removes_verified_new_final() {
        let root = temp_root("rollback-files");
        let library = root.join("library");
        fs::create_dir(&library).unwrap();
        let epub = library.join("book.epub");
        let backup = library.join("book.epub.backup");
        let cover = library.join("cover.jpg");
        let old_epub = b"old epub";
        let new_epub = b"new epub";
        let new_cover = b"new cover";
        fs::write(&epub, new_epub).unwrap();
        fs::write(&backup, old_epub).unwrap();
        fs::write(&cover, new_cover).unwrap();
        let old_hash = crate::utils::calculate_bytes_hash(old_epub);
        let new_hash = crate::utils::calculate_bytes_hash(new_epub);
        let cover_hash = crate::utils::calculate_bytes_hash(new_cover);
        let operation = stored_operation(
            library,
            vec![
                ready_file(
                    "epub-final",
                    epub.clone(),
                    &new_hash,
                    Some(&old_hash),
                    Some(&new_hash),
                    true,
                ),
                ready_file(
                    "epub-backup",
                    backup.clone(),
                    &old_hash,
                    Some(&old_hash),
                    None,
                    false,
                ),
                ready_file(
                    "cover-final",
                    cover.clone(),
                    &cover_hash,
                    None,
                    Some(&cover_hash),
                    false,
                ),
            ],
        );

        rollback_files(&operation).unwrap();
        assert_eq!(fs::read(&epub).unwrap(), old_epub);
        assert!(!backup.exists());
        assert!(!cover.exists());
        fs::remove_dir_all(root).unwrap();
    }

    fn advance_to_metadata_committed(state: &mut StateDb, operation_id: &str) {
        state
            .transition_phase(operation_id, Phase::Planned, Phase::Staging)
            .unwrap();
        state
            .transition_phase(operation_id, Phase::Staging, Phase::Staged)
            .unwrap();
        state
            .transition_phase(operation_id, Phase::Staged, Phase::FilesPublished)
            .unwrap();
        state
            .transition_phase(
                operation_id,
                Phase::FilesPublished,
                Phase::MetadataCommitted,
            )
            .unwrap();
    }

    #[test]
    fn begin_operation_commits_operation_reservation_and_all_intents_atomically() {
        let root = temp_root("typed-begin");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state.begin_operation(&operation_intent("op-1")).unwrap();
        assert_eq!(
            state
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM operations WHERE phase = 'planned'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            state
                .connection()
                .query_row("SELECT COUNT(*) FROM reservations", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            state
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM artifacts WHERE status = 'planned'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );

        let mut invalid = operation_intent("op-bad");
        invalid.artifacts[1].role = "epub-stage".to_owned();
        assert!(state.begin_operation(&invalid).is_err());
        assert_eq!(
            state
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM operations WHERE id = 'op-bad'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn transition_phase_enforces_graph_source_and_operation_identity() {
        let root = temp_root("typed-transition");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state.begin_operation(&operation_intent("op-1")).unwrap();
        state
            .transition_phase("op-1", Phase::Planned, Phase::Staging)
            .unwrap();
        state
            .transition_phase("op-1", Phase::Staging, Phase::Staged)
            .unwrap();
        assert!(
            state
                .transition_phase("op-1", Phase::Planned, Phase::Staging)
                .is_err()
        );
        assert!(
            state
                .transition_phase("missing", Phase::Planned, Phase::Staging)
                .is_err()
        );
        assert!(
            state
                .transition_phase("op-1", Phase::Staged, Phase::Complete)
                .is_err()
        );
        let phase: String = state
            .connection()
            .query_row(
                "SELECT phase FROM operations WHERE id = 'op-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(phase, "staged");
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn artifact_lifecycle_requires_exact_source_role_and_lowercase_sha256() {
        let root = temp_root("artifact-api");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state.begin_operation(&operation_intent("op-1")).unwrap();
        assert!(state.mark_artifact_writing("op-1", "missing").is_err());
        assert!(
            state
                .mark_artifact_ready("op-1", "epub-stage", &"a".repeat(64))
                .is_err()
        );
        state.mark_artifact_writing("op-1", "epub-stage").unwrap();
        assert!(
            state
                .mark_artifact_ready("op-1", "epub-stage", &"A".repeat(64))
                .is_err()
        );
        assert!(
            state
                .mark_artifact_ready("op-1", "epub-stage", "abc")
                .is_err()
        );
        state
            .mark_artifact_ready("op-1", "epub-stage", &"a".repeat(64))
            .unwrap();
        let stored: (String, String, String) = state
            .connection()
            .query_row(
                "SELECT status, hash_algorithm, hash FROM artifacts
             WHERE operation_id = 'op-1' AND role = 'epub-stage'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            stored,
            ("ready".to_owned(), "sha256".to_owned(), "a".repeat(64))
        );
        assert!(state.mark_artifact_writing("op-1", "epub-stage").is_err());
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn reservation_rebind_has_no_gap_and_rolls_back_on_collision() {
        let root = temp_root("reservation-rebind");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state.begin_operation(&operation_intent("op-1")).unwrap();
        let mut second = operation_intent("op-2");
        second.reservation.as_mut().unwrap().book_key = "book:9".to_owned();
        state.begin_operation(&second).unwrap();
        state
            .record_book_facts(
                "op-1",
                9,
                Path::new("Author/Book"),
                false,
                None,
                &"a".repeat(64),
            )
            .unwrap();

        assert!(
            state
                .assign_book_id_and_reservation("op-1", "new:key", 9)
                .is_err()
        );
        let unchanged: (Option<i64>, String, String) = state
            .connection()
            .query_row(
                "SELECT o.book_id, o.book_key, r.book_key
             FROM operations o JOIN reservations r ON r.operation_id = o.id
             WHERE o.id = 'op-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            unchanged,
            (Some(9), "new:key".to_owned(), "new:key".to_owned())
        );
        assert_eq!(
            state.active_reservation_keys(7).unwrap(),
            HashSet::from(["book:9".to_owned(), "new:key".to_owned()])
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn book_facts_survive_reopen_before_provisional_reservation_rebind() {
        let root = temp_root("facts-before-rebind");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state.begin_operation(&operation_intent("op-1")).unwrap();
        assert!(
            state
                .assign_book_id_and_reservation("op-1", "new:key", 10)
                .is_err()
        );
        state
            .record_book_facts(
                "op-1",
                10,
                Path::new("Author/Book"),
                false,
                None,
                &"a".repeat(64),
            )
            .unwrap();
        drop(state);

        let mut state = StateDb::open(&base).unwrap();
        let before_rebind: (i64, String, String, String) = state
            .connection()
            .query_row(
                "SELECT o.book_id, o.book_key, o.expected_db_facts, r.book_key
                 FROM operations o JOIN reservations r ON r.operation_id = o.id
                 WHERE o.id = 'op-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            before_rebind,
            (
                10,
                "new:key".to_owned(),
                "a".repeat(64),
                "new:key".to_owned()
            )
        );
        state
            .assign_book_id_and_reservation("op-1", "new:key", 10)
            .unwrap();
        assert_eq!(
            state.active_reservation_keys(7).unwrap(),
            HashSet::from(["book:10".to_owned()])
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn active_reservations_retain_pending_and_blocked_and_exclude_terminals() {
        let root = temp_root("active-reservations");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        for (id, key) in [
            ("pending", "new:pending"),
            ("blocked", "new:blocked"),
            ("complete", "new:complete"),
            ("rolled", "new:rolled"),
        ] {
            let mut operation = operation_intent(id);
            operation.reservation.as_mut().unwrap().book_key = key.to_owned();
            state.begin_operation(&operation).unwrap();
        }
        advance_to_metadata_committed(&mut state, "pending");
        state
            .transition_phase("pending", Phase::MetadataCommitted, Phase::ShelfPending)
            .unwrap();
        state
            .block_recovery("blocked", Phase::Planned, "hash mismatch")
            .unwrap();
        advance_to_metadata_committed(&mut state, "complete");
        state
            .mark_complete("complete", Phase::MetadataCommitted)
            .unwrap();
        state.mark_rolled_back("rolled", Phase::Planned).unwrap();
        assert_eq!(
            state.active_reservation_keys(7).unwrap(),
            HashSet::from(["new:pending".to_owned(), "new:blocked".to_owned()])
        );
        assert_eq!(
            state
                .connection()
                .query_row(
                    "SELECT book_key FROM reservations WHERE operation_id = 'blocked'",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "new:blocked"
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn terminal_cleanup_signal_releases_only_after_terminal_and_survives_reopen() {
        let root = temp_root("terminal-cleanup");
        let base = root.join("app.db");
        let mut state = StateDb::open(&base).unwrap();
        state
            .begin_operation(&operation_intent("complete"))
            .unwrap();
        assert!(state.mark_complete_and_delete("complete").is_err());
        assert_eq!(
            state.active_reservation_keys(7).unwrap(),
            HashSet::from(["new:key".to_owned()])
        );
        advance_to_metadata_committed(&mut state, "complete");
        state
            .mark_complete("complete", Phase::MetadataCommitted)
            .unwrap();
        state.mark_complete_and_delete("complete").unwrap();
        assert!(state.active_reservation_keys(7).unwrap().is_empty());

        let mut rolled = operation_intent("rolled");
        rolled.reservation.as_mut().unwrap().book_key = "new:rolled".to_owned();
        state.begin_operation(&rolled).unwrap();
        state.mark_rolled_back("rolled", Phase::Planned).unwrap();
        drop(state);
        let mut state = StateDb::open(&base).unwrap();
        let phase: String = state
            .connection()
            .query_row(
                "SELECT phase FROM operations WHERE id = 'rolled'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(phase, "rolled_back");
        assert_eq!(
            state
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM reservations WHERE operation_id = 'rolled'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        state.mark_rolled_back_and_delete("rolled").unwrap();
        assert_eq!(
            state
                .connection()
                .query_row("SELECT COUNT(*) FROM operations", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn v1_to_v2_migration_preserves_recovery_rows_and_is_reopen_idempotent() {
        let root = temp_root("v2-migration");
        let base = root.join("app.db");
        {
            let mut state = StateDb::open(&base).unwrap();
            state
                .begin_operation(&operation_intent("preserved-op"))
                .unwrap();
            state
                .connection()
                .execute_batch(
                    "DROP TRIGGER kobo_sync_plans_immutable_update;
                 DROP TRIGGER kobo_sync_plans_immutable_delete;
                 DROP TRIGGER kobo_sync_plan_graphs_immutable_update;
                 DROP TRIGGER kobo_sync_plan_graphs_immutable_delete;
                 DROP TABLE kobo_sync_plan_applications;
                 DROP TABLE kobo_sync_workflow;
                 DROP TABLE kobo_sync_plan_graphs;
                 DROP TABLE kobo_sync_plans;
                 UPDATE schema_version SET version = 1 WHERE singleton = 1;",
                )
                .unwrap();
        }

        for _ in 0..2 {
            let state = StateDb::open(&base).unwrap();
            assert_eq!(
                state
                    .connection()
                    .query_row(
                        "SELECT version FROM schema_version WHERE singleton = 1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                2
            );
            assert_eq!(
                state
                    .connection()
                    .query_row(
                        "SELECT COUNT(*) FROM operations WHERE id = 'preserved-op'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                1
            );
            for table in [
                "kobo_sync_plans",
                "kobo_sync_plan_graphs",
                "kobo_sync_plan_applications",
                "kobo_sync_workflow",
            ] {
                assert_eq!(
                    state
                        .connection()
                        .query_row(
                            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                            [table],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    1
                );
            }
        }
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn v2_plan_and_graph_rows_are_immutable() {
        let root = temp_root("v2-immutable");
        let base = root.join("app.db");
        let state = StateDb::open(&base).unwrap();
        state
            .connection()
            .execute(
                "INSERT INTO kobo_sync_plans (
                 plan_id, format_version, appdb_key, canonical_appdb_path,
                 user_id, username, created_at, schema_fingerprint,
                 source_fingerprint, plan_payload
             ) VALUES ('plan-1', 1, 'app-key', ?1, 7, 'melissa',
                       '2026-09-08 12:00:00.000000', 'schema', 'source', ?2)",
                rusqlite::params![b"/tmp/app.db", b"plan-payload"],
            )
            .unwrap();
        state
            .connection()
            .execute(
                "INSERT INTO kobo_sync_plan_graphs (
                 plan_id, ordinal, graph_key, user_id, book_id,
                 canonical_state_id, root_weight, source_fingerprint,
                 disposition, graph_payload
             ) VALUES ('plan-1', 0, '7:42', 7, 42, 101, 2,
                       'graph-source', 'applicable', ?1)",
                [b"graph-payload".as_slice()],
            )
            .unwrap();

        for sql in [
            "UPDATE kobo_sync_plans SET username = 'other' WHERE plan_id = 'plan-1'",
            "DELETE FROM kobo_sync_plans WHERE plan_id = 'plan-1'",
            "UPDATE kobo_sync_plan_graphs SET disposition = 'no_change' WHERE plan_id = 'plan-1'",
            "DELETE FROM kobo_sync_plan_graphs WHERE plan_id = 'plan-1'",
        ] {
            let error = state.connection().execute(sql, []).unwrap_err();
            assert!(error.to_string().contains("immutable Kobo plan"));
        }
        drop(state);
        fs::remove_file(state_path(&base)).unwrap();
        fs::remove_dir(root).unwrap();
    }
}
