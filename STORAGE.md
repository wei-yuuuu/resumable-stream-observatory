# Durable storage

The demo deliberately has two durable stores:

```text
SQLite (server)                  IndexedDB (one browser profile)
streams + buffer_chunks          events + cursors
documents + documents_fts        local cache / durable client cursor
demo_streams                     demo-only stream labels and grouping
source of replay truth           UI projection + checkpoint
```

SQLite is the authoritative server-side log. IndexedDB lets one browser restore
its already-applied view immediately and tell the server where to resume.

## Retention and cleanup

This demo has no automatic retention policy yet, so cleanup is explicit:

| Data | Role | Safe cleanup rule |
| --- | --- | --- |
| SQLite `buffer_chunks` | Authoritative replay bytes for one stream. | Delete only when the stream no longer needs to be replayed or resumed. |
| SQLite `streams` | Stream lifecycle metadata and producer lease. | Delete together with that stream's `buffer_chunks`. |
| SQLite `demo_streams` | Demo-only dropdown labels/grouping. | Delete together with the matching `streams` row. |
| SQLite `documents` | Demo search corpus, not stream state. | Keep while using the search demo; delete only for a full demo reset. |
| SQLite `documents_fts` | Rebuildable FTS index over `documents`. | Can be rebuilt from `documents`; do not treat it as canonical data. |
| IndexedDB `events` | Browser-local UI projection/history. | Safe to delete if you are willing to redraw by replaying from the server. |
| IndexedDB `cursors` | Browser-local resume checkpoint. | Delete when you want that browser to replay from the start. |
| `localStorage` active stream ID | Reload/new-tab convenience pointer. | Safe to delete; it does not contain durable stream progress. |

The demo's **Delete stream** button performs the per-stream cleanup path:

1. Delete browser IndexedDB `events` and `cursors` for the selected stream.
2. Call `DELETE /streams/:id`.
3. The server deletes SQLite `buffer_chunks`, `streams`, and `demo_streams`
   rows for that stream.

For a full local reset, stop the server and delete `data/streams.sqlite` plus
its `-wal` and `-shm` sidecar files. That removes stream buffers, stream
metadata, demo labels, the search corpus, and the FTS index.

The browser-side `events` and `cursors` stores intentionally model two different
ideas:

```text
events   = local replay/projection data for the UI
cursors  = resume checkpoint for the next server request
```

In this maze demo, `events` can redraw the maze after reload, while `cursors`
answers the smaller question: "what should the next `?after=` value be?"

The server appends a `buffer_chunks` row and advances `streams.next_seq` in the same
SQLite transaction. It notifies live tailers only after that transaction commits.

This learning project intentionally has no schema migration system. A database
created by an earlier demo schema (for example, one with a `chunks` table or
without the `interrupted` status) is not supported by the current code; for a
clean local reset, stop the server and delete `data/streams.sqlite` plus its
`-wal` and `-shm` sidecar files.

## SQLite: `streams`

One row per logical stream. The demo file is `data/streams.sqlite`.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT
    id,
    status,
    next_seq,
    error,
    datetime(created_at / 1000, 'unixepoch', 'localtime') AS created_at,
    datetime(updated_at / 1000, 'unixepoch', 'localtime') AS updated_at
  FROM streams
  ORDER BY created_at DESC;
"
```

Columns:

- `id`: Stable UUID used by clients in `/streams/:id`.
- `status`: `streaming`, `completed`, `failed`, or `interrupted`. On hub
  startup, an old `streaming` row becomes `interrupted`: its previous producer
  process is gone, so no more chunks can arrive from that connection.
- `producer_lease`: Internal UUID proving which background producer owns this
  stream. Do not expose or modify it in normal application code.
- `next_seq`: Sequence number to allocate to the next persisted chunk. The most
  recently written chunk is normally `next_seq - 1`.
- `created_at`: Unix milliseconds when the stream was created.
- `updated_at`: Unix milliseconds of the latest chunk or terminal state.
- `error`: Failure or interruption reason; otherwise `NULL`.

## SQLite: `buffer_chunks`

Every row is one raw `Uint8Array` received from a `StreamSource`. The composite
primary key makes a sequence unique within a stream.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT
    stream_id,
    seq,
    length(data) AS byte_length,
    datetime(created_at / 1000, 'unixepoch', 'localtime') AS created_at
  FROM buffer_chunks
  WHERE stream_id = '<stream-id>'
  ORDER BY seq ASC;
"
```

Columns:

- `stream_id`: Parent `streams.id`.
- `seq`: Zero-based, monotonically increasing cursor for this stream.
- `data`: Raw chunk bytes (`BLOB`). The library does not assume text, JSON, or
  AI tokens.
- `created_at`: Unix milliseconds when SQLite accepted the chunk.

The maze demo happens to store UTF-8 JSON, so it can be inspected like this:

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT seq, CAST(data AS TEXT) AS maze_event
  FROM buffer_chunks
  WHERE stream_id = '<stream-id>'
  ORDER BY seq ASC;
"
```

Do not use `CAST(data AS TEXT)` for a general stream source: binary data may be
invalid text. For normal inspection, prefer `length(data)` and metadata.

## SQLite: `documents`

Demo document corpus used by the long-running search example. These rows are
not stream chunks; they are the source data that a search stream scans or
queries.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT public_id, title
  FROM documents
  ORDER BY rowid ASC
  LIMIT 10;
"
```

Columns:

- `rowid`: Integer primary key. SQLite FTS5 uses this as its content row ID.
- `public_id`: Stable demo document ID shown in search events.
- `title`: Searchable title.
- `body`: Searchable body text.
- `created_at`: Unix milliseconds when the demo corpus was seeded.

## SQLite: `documents_fts`

SQLite FTS5 virtual table for indexed document search. This is SQLite's
built-in inverted index, not a hand-written index in application code.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT d.public_id, d.title
  FROM documents_fts
  JOIN documents d ON d.rowid = documents_fts.rowid
  WHERE documents_fts MATCH 'sql*'
  LIMIT 10;
"
```

The important parts of the FTS query are:

- `CREATE VIRTUAL TABLE ... USING fts5(...)`: creates a special table backed by
  SQLite's full-text index. You query it like a table, but internally SQLite
  stores token → matching row mappings.
- `content='documents'`: tells FTS5 that `documents` is the canonical content
  table. The FTS table is the searchable index, not the app's main record.
- `content_rowid='rowid'`: makes `documents_fts.rowid` point at
  `documents.rowid`, so a search hit can be joined back to the full document.
- `WHERE documents_fts MATCH 'sql*'`: asks the FTS index for rows containing
  tokens with the given prefix. This avoids checking every document body one by
  one, and lets `sql` match `sqlite`.
- `snippet(documents_fts, 1, '', '', '…', 14)`: asks FTS5 for a short matching
  fragment. Column `1` means the `body` column because the FTS table columns
  are `title` then `body`.
- `bm25(documents_fts)`: asks FTS5 for a relevance score. In SQLite FTS5, lower
  scores are better, so the demo sorts ascending.

The search demo has two backends:

- `scan`: Reads `documents` one row at a time and emits progress/results.
- `fts5`: Turns each user term into a prefix token query such as `"sql"*`, uses
  `documents_fts MATCH ?` to get indexed candidates quickly, then streams
  result events. This is prefix search, not typo-fuzzy search.

## SQLite: `demo_streams`

Demo-only metadata used by the browser UI to keep Maze streams and Search
streams in separate dropdowns. This table is not part of the core `StreamHub`
library contract.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT stream_id, demo_type, label,
         datetime(created_at / 1000, 'unixepoch', 'localtime') AS created_at
  FROM demo_streams
  ORDER BY created_at DESC;
"
```

Columns:

- `stream_id`: Parent `streams.id`.
- `demo_type`: `maze` or `search`.
- `label`: User-facing dropdown label, such as a search query/backend.
- `created_at`: Unix milliseconds when the demo metadata was recorded.

## Browser IndexedDB: `resumable-stream-observatory`

The demo opens IndexedDB database `resumable-stream-observatory`, version `1`.
Inspect it with browser DevTools → Application/Storage → IndexedDB, or use the
console snippets below.

### `events`

One entry for each event the browser has persisted and applied.

- Key path: `[streamId, seq]`.
- `streamId`: Server stream UUID.
- `seq`: Server chunk sequence number.
- `event`: Decoded demo JSON. Maze streams store `maze-edge` events; search
  streams store `search-started`, `progress`, `result`, and `summary` events.
  A real app would store its decoded projection or a lossless local
  representation appropriate to its source.
- `storedAt`: Browser-side Unix milliseconds when the transaction committed.

This store is a browser-local projection of the stream. It is useful for UI
restore: reloads and new tabs can redraw the maze immediately from IndexedDB
before asking the server for newer chunks.

The demo also uses `events` for time travel. Clicking an event log row redraws
the maze from all saved events with `seq <= clickedSeq`. This changes only the
visible projection; the stored cursor and server-side stream keep moving
forward.

`events` is not the authoritative stream log. The authoritative server-side log
is SQLite `buffer_chunks`. If this browser deletes `events`, another browser or
the server still has its own state.

### `cursors`

One row per stream that this browser has seen.

- Key path: `streamId`.
- `streamId`: Server stream UUID.
- `seq`: Last event written to IndexedDB. This is the `after` value used on the
  next connection.

This store is the browser's checkpoint. It lets the reconnect code do:

```text
GET /streams/:streamId?after=<cursors.seq>
```

`cursors` does not contain enough data to redraw the maze. It only says how far
this browser safely got.

The browser writes `events` and `cursors` in **one read-write transaction**.
It never advances `cursors.seq` before the corresponding event is durable.

Example after applying three chunks:

```text
events:
  ["stream-1", 0] -> { event: { kind: "maze-edge", ... } }
  ["stream-1", 1] -> { event: { kind: "maze-edge", ... } }
  ["stream-1", 2] -> { event: { kind: "maze-edge", ... } }

cursors:
  "stream-1" -> { seq: 2 }
```

On reload:

1. Read `events` for `stream-1` and redraw chunks `0..2`.
2. Read `cursors["stream-1"].seq`.
3. Connect to `/streams/stream-1?after=2`.

Could this demo use only one store? Yes, with tradeoffs:

- Only `events`: derive the cursor from the largest saved `seq`. Simpler for
  this maze demo, but slower/less flexible once old UI events are compacted or
  deleted.
- Only `cursors`: resume still works, but reload cannot redraw the already
  applied maze because the local event data is gone.

Keeping both stores makes the separation explicit: `events` is local UI history,
`cursors` is resume progress.

```js
const db = await new Promise((resolve, reject) => {
  const request = indexedDB.open("resumable-stream-observatory");
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error);
});

const cursor = await new Promise((resolve, reject) => {
  const request = db.transaction("cursors").objectStore("cursors").get("<stream-id>");
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error);
});

console.log(cursor);
```

## Browser controls and localStorage

`resumable-stream-observatory:active-stream` stores the most recently opened
stream ID. It is only a convenience for reload/new-tab auto-resume; it is not
the durable cursor and it does not replace IndexedDB.

```js
localStorage.getItem("resumable-stream-observatory:active-stream");
```

The stream dropdown is populated from `GET /streams`, so it shows streams that
still exist in the server-side SQLite `streams` table.

Other browser profiles may still have their own IndexedDB cache after a stream
is deleted, but they can no longer replay that stream from this server buffer.
