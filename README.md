# Zage

A lightweight Rust download manager for Linux with a terminal UI.

## Build and run

Install Rust with [rustup](https://rustup.rs/), then run:

```sh
. "$HOME/.cargo/env"
cargo build --release
cargo run
```

You can also open the queue with a download pre-added:

```sh
cargo run -- https://example.com/file.iso ./file.iso
```

The terminal UI shows a bordered, scrollable download list with progress bars,
per-download speed and priority, queue totals, and readable failure messages.
Press `a` to add a URL (optionally followed by an output path), `p` to pause,
`r` to resume or retry, `d` to remove a queue entry, `c` to clear completed
downloads, and `s` to set a global speed limit (for example `2M`, or `off`).
Use `+`/`-` to adjust queued priority, `?` or `F1` for the built-in help,
arrow keys or `j`/`k` to select a download, and `q` to quit. Up to three files
download at once; range-capable servers are downloaded with up to eight
segments per file. Queue state is stored in
`$XDG_STATE_HOME/zage/downloads.json` or `~/.local/state/zage/downloads.json`.

The manager resumes saved HTTP range segments when the server supports them
and validates the returned `Content-Range`. Servers without range support use
a single stream. The destination must not already exist. Interrupted data is
kept in adjacent `.part` files until the download can be assembled.

Torrent/magnet downloads, browser and media-link integration, scheduled
downloads, and desktop GUI support are not implemented yet.
