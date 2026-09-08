# Calibe-Web-Helper
Note: This tool is built in Rust, but I don't really know Rust - it's basically just VScode and GitHub Copilot writing this based on my prompts.

Manipulate the Calibre and Calibre-Web databases from the CLI, specifically for adding new books.  The goal is not to have to interact with a WebUI to add and share new epub and kepub files via Calibre-Web.

`fix-kobo-sync` is an idempotent integrity repair for existing Kobo reading-state records. It fills missing state relationships and timestamps, but deliberately does not create state for every Kobo-shelf book, force a shelf-wide resync, or rewrite shelf membership timestamps. Those timestamp rewrites can trigger Calibre-Web's unfixed 100-item shelf-only pagination loop.

The ordinary `add --shelf ... --username ...` path is Kobo-safe by default: it never repairs or rewrites unrelated Calibre-Web rows, creates only the requested shelf membership, and gives successive directory additions strictly increasing membership timestamps for that user. This lets an unfixed Calibre-Web server advance its strict cursor through batches larger than 100 books.
