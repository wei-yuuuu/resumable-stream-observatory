# Resumable Stream Observatory — plan

This is a small learning library for one idea: **the connection to a provider
must not be owned by the browser connection that happened to start it.**

The first version deliberately uses no framework and no third-party package:
Node 24.16, Web `ReadableStream`, `fetch`, `node:sqlite`, browser `fetch`,
SVG, and IndexedDB.

The package boundary is deliberately small:

```text
user's StreamSource factory ──> StreamHub ──> ReadableStream<TailEvent>
                                        └──> toRawByteStream() / toSse() adapters
```

`StreamHub` knows nothing about mazes, HTTP, or SSE. The maze source and the
HTTP server are demo code under `src/demo/`.

`StreamHub` also accepts an optional `keepAliveWhile(task)` adapter. A long-lived
Node process uses the default `void task()` behavior; a runtime that needs an
explicit background-work lease can inject its own lifecycle primitive. The
included `createKeepAliveWhile({ begin })` helper starts a host heartbeat/alarm
and always calls its release function when the drain settles, including errors.
This keeps the drain running after a response returns, but cannot preserve a
TCP connection across a hub process restart or redeploy.

## The shape

```text
client A ─┐
          ├─ GET /streams/:id?after=42 ─┐
client B ─┘                              │
                                         ▼
                              StreamBuffer(streamId)
                              - status / producer lease
                              - SQLite buffer_chunks(seq, bytes)
                              - tailFrom(streamId, cursor)
                              - notifications
                                         ▲
                                         │ background drain
                                   provider fetch stream
```

`StreamBuffer` is an in-process learning stand-in for the article's
never-redeployed buffer. In a production deployment it must be a separately
deployed, long-lived service or durable runtime. A SQLite file preserves what
was already received, but cannot keep a TCP connection alive after the process
that owns it has been killed.

## Rules of the protocol

1. `hub.create({ source })` creates a stable `streamId`, marks it `streaming`,
   acquires a producer lease, and invokes the source factory in a background
   task without awaiting it.
2. Every provider chunk is appended as raw bytes to
   `buffer_chunks(stream_id, seq)`.
   Only **after** SQLite accepts it are tailers notified.
3. A client reconnects with `GET /streams/:id?after=<cursor>`. The server
   replays every `seq > cursor`, then tails new rows.
4. The browser writes an event and its cursor to IndexedDB before considering
   the event applied. It sends that durable cursor next time.
5. `tailFrom()` is consumer-driven: its `pull()` reads SQLite in bounded
   batches, then waits for a notification only when caught up. Notifications
   are only wake-ups, never the source of truth. After every
   wake-up a tailer queries SQLite again, so coalesced notifications and replay
   races are harmless.
6. Terminal stream states are `completed`, `failed`, and `interrupted`.
   `tailFrom()` sends a final SSE `end` event once all rows are replayed.

On hub startup, any persisted `streaming` row is changed to `interrupted`: its
previous process is gone, so its provider connection cannot still be live. A
caller can replay the partial bytes and let the higher-level agent or workflow
decide how to recover. This is the same log used for browser reconnection; the
only difference is whether a live producer remains attached.

## Demo use cases

The provider is initially a fake perfect-maze generator. Each durable chunk is
a JSON passage between two maze-cell centres. The browser draws it as an SVG
path with `stroke-dasharray`; on reload it rebuilds all saved passages from
IndexedDB, then animates only newly tailed ones. The active stream ID is saved
in `localStorage`, so a reload or a new tab resumes without copying an ID.

The practical demo is long-running document search. The `scan` backend walks a
demo corpus one document at a time and streams progress/results. The `fts5`
backend uses SQLite FTS5, SQLite's built-in inverted index, to show the
difference between a slow resumable scan and an indexed search whose result
hydration can still be streamed.

To connect a real provider, replace the demo's `createMazeSource()` with a
source factory that gets a stream from `fetch`:

```ts
const source = async () => {
  const response = await fetch(providerRequest);
  if (!response.body) throw new Error("provider did not return a body");
  return response.body;
};

hub.create({ source });
```

The ownership rule remains the same: the drain belongs to `StreamHub`, not to
the HTTP response returned to the initiating client.

When replaying a provider stream, use `toRawByteStream(hub.tailFrom(...))` and
pass the bytes back through that provider's native parser. The library stores
raw bytes specifically so it does not need to understand each provider's SSE
wire format.

## Deliberately postponed

- Cross-process notification / multi-instance ownership (Redis, Postgres, or a
  durable-object runtime).
- Authentication and authorization for stream IDs.
- Retention policy: checkpoints plus TTL-based deletion of old chunks.
- Resuming a provider request after the stream-hub itself dies. That needs a
  provider-specific checkpoint/idempotency contract; replaying saved output is
  the separate, already-supported capability.
