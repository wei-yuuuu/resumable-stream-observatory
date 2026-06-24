# Durable storage

The demo deliberately has two durable stores:

```text
SQLite (server)                  IndexedDB (one browser profile)
streams + chunks                 events + cursors
source of replay truth           local cache / durable client cursor
```

SQLite is the authoritative server-side log. IndexedDB lets one browser restore
its already-applied view immediately and tell the server where to resume.

The server appends a `chunks` row and advances `streams.next_seq` in the same
SQLite transaction. It notifies live tailers only after that transaction commits.

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
- `status`: `streaming`, `completed`, or `failed`.
- `producer_lease`: Internal UUID proving which background producer owns this
  stream. Do not expose or modify it in normal application code.
- `next_seq`: Sequence number to allocate to the next persisted chunk. The most
  recently written chunk is normally `next_seq - 1`.
- `created_at`: Unix milliseconds when the stream was created.
- `updated_at`: Unix milliseconds of the latest chunk or terminal state.
- `error`: Failure message when `status = 'failed'`; otherwise `NULL`.

## SQLite: `chunks`

Every row is one raw `Uint8Array` received from a `StreamSource`. The composite
primary key makes a sequence unique within a stream.

```sh
sqlite3 -header -column data/streams.sqlite "
  SELECT
    stream_id,
    seq,
    length(data) AS byte_length,
    datetime(created_at / 1000, 'unixepoch', 'localtime') AS created_at
  FROM chunks
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
  FROM chunks
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

### `cursors`

One row per stream that this browser has seen.

- Key path: `streamId`.
- `streamId`: Server stream UUID.
- `seq`: Last event written to IndexedDB. This is the `after` value used on the
  next connection.

The browser writes `events` and `cursors` in **one read-write transaction**.
It never advances `cursors.seq` before the corresponding event is durable.

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

The **Forget saved maze** button removes this key. It intentionally leaves the
IndexedDB event cache alone, so reopening the same stream ID later can still
restore it.
