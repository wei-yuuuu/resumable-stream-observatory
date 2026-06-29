# Durable storage

The demo deliberately has two durable stores:

```text
SQLite (server)                  IndexedDB (one browser profile)
streams + buffer_chunks          events + cursors
source of replay truth           local cache / durable client cursor
```

SQLite is the authoritative server-side log. IndexedDB lets one browser restore
its already-applied view immediately and tell the server where to resume.

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

## Browser IndexedDB: `resumable-stream-observatory`

The demo opens IndexedDB database `resumable-stream-observatory`, version `1`.
Inspect it with browser DevTools → Application/Storage → IndexedDB, or use the
console snippets below.

### `events`

One entry for each event the browser has persisted and applied.

- Key path: `[streamId, seq]`.
- `streamId`: Server stream UUID.
- `seq`: Server chunk sequence number.
- `event`: Decoded maze JSON for the demo. A real app would store its decoded
  projection or a lossless local representation appropriate to its source.
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

## Browser localStorage

`resumable-stream-observatory:active-stream` stores the most recently opened
stream ID. It is only a convenience for reload/new-tab auto-resume; it is not
the durable cursor and it does not replace IndexedDB.

```js
localStorage.getItem("resumable-stream-observatory:active-stream");
```

The **Stop auto-resume** button removes this key and clears the current UI. It
intentionally leaves the IndexedDB event cache alone, so reopening the same
stream ID later can still restore it.

The **Delete local cache** button deletes this browser's IndexedDB `events` and
`cursors` rows for the selected stream. It does not delete the server-side
SQLite `streams` or `buffer_chunks` rows.
