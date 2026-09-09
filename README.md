# Calibre-Web-Helper

Calibre-Web-Helper adds EPUB and KEPUB files to an existing Calibre library from the command line. It can also place books on existing Calibre-Web shelves, so routine book imports do not require the web interface.

## Examples

### Add a book to an existing shelf

This command adds one EPUB to the library and places it on `KoboMelissa` for the user `melissa`:

```sh
calibre-web-helper \
  --metadata-file calibre-library/metadata.db \
  --appdb-file config/app.db \
  --epub-file /path/to/book.epub \
  add --shelf KoboMelissa --username melissa
```

The user and shelf must already exist. The command fails without changing the library if either name is wrong.

### Create a shelf in Calibre-Web

Calibre-Web-Helper does not create shelves. In Calibre-Web, create a shelf named `KoboMelissa` and assign it to `melissa`. Enable Kobo sync if you want the shelf to appear on that user's Kobo. You can then verify the shelf from the command line:

```sh
calibre-web-helper \
  --appdb-file config/app.db \
  list-shelves
```

## Adding books safely

Before an add, the helper finds the exact username and verifies that the user owns the named shelf. It never creates a user or shelf. For a non-Kobo shelf, it prints a notice and skips the Kobo backlog check.

For Kobo-enabled shelves, the helper counts distinct unsynced books, planned first-time memberships, and active reservations. It rejects an add if the total would exceed 100. An existing membership changes nothing, including its timestamp. The helper refuses to assign an already-synced book to a new Kobo shelf because Calibre-Web's pagination bug makes that operation unsafe.

Directory imports run in sorted order. An empty directory is an error unless you pass `--allow-empty`. The helper reports each invalid file but continues with valid files. Any failure makes the command exit nonzero. `--dry-run` validates the input but changes no database or library file and creates no helper lock.

Each write takes `<app.db>.calibre-web-helper.lock`. Without an app database, it takes `<metadata.db>.calibre-web-helper.lock` instead. The helper keeps and reuses this small lock file.

Before publishing files, the helper writes a recovery record to `<app.db>.calibre-web-helper-state.sqlite3`. The record includes database facts, the Kobo capacity reservation, and the paths and hashes for staged, final, and backup files.

After an interruption, the next write reads that record. It resumes only when the hashes and database state prove one safe outcome. Otherwise, it marks the operation `recovery_blocked` and stops for operator review. Recovery does not combine the two application databases into one SQLite transaction. Keep normal library backups.

## License

Calibre-Web-Helper uses the BSD 2-Clause License. See [LICENSE](LICENSE).
