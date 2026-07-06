# Resumable Stream Observatory

A no-dependency Node 24.16 + TypeScript library experiment in durable stream
replay. The repository includes a maze visualiser and a practical long-running
document search demo; both feed the same stream buffer.

```sh
npm run dev
```

Open <http://127.0.0.1:8787>. Start a maze or document search, then reload or
open a new tab. The browser remembers the active stream ID and its IndexedDB
cursor. Disconnect or go offline, let the producer continue, and reconnect
without copying anything.

The UI keeps the two demos separated: Maze streams and Search streams have
their own tabs, controls, and dropdowns, while sharing the same underlying
`StreamHub` replay mechanism.

Read [PLAN.md](./PLAN.md) for the architecture and the important distinction
between keeping a provider drain alive and replaying a durable stream. See
[STORAGE.md](./STORAGE.md) for the exact SQLite, IndexedDB, and localStorage
records.

## Use as a library

`StreamHub` owns the provider drain and SQLite buffer. Give it a source
**factory**, rather than an already-open connection. The hub invokes the
factory after the stream metadata has been written, in a background task that
is independent of the caller's HTTP response.

```ts
import { StreamHub, toRawByteStream } from "resumable-stream-observatory";

const hub = new StreamHub({ databasePath: "./data/streams.sqlite" });

const stream = hub.create({
  source: async () => {
    const response = await fetch("https://provider.example/stream");
    if (!response.body) throw new Error("provider did not return a body");
    return response.body;
  },
});

// Return the original provider bytes. Its native SDK/parser can consume this
// as though it were the original provider response.
const rawBody = toRawByteStream(hub.tailFrom(stream.id, -1));
```

`hub.tailFrom(streamId, cursor)` yields typed events. It uses stream `pull()`
and reads SQLite in small batches, so a slow consumer does not fill its stream
queue with an entire replay. `toRawByteStream()` preserves the provider's byte
stream, avoiding a custom parser for each provider SSE format. `toSse()` is a
separate application-envelope adapter; a user can also adapt typed events to a
WebSocket, a CLI renderer, or another transport.

### Stream lifecycle

The HTTP demo keeps the write side and read side separate without splitting the
stream identity:

```text
1. Trigger a producer
   POST /streams/maze
   POST /streams/search

2. Persist provider bytes
   StreamHub writes buffer_chunks(stream_id, seq, data)

3. Consume from a durable cursor
   GET /streams/:id?after=<seq>

4. Reconnect later
   Browser reads IndexedDB cursor and asks for seq > cursor
```

The typed create endpoints return as soon as the stream row exists and the
producer drain has been scheduled. The read endpoint never starts a provider
request; it only replays durable chunks and waits for new committed rows. This
keeps a browser tab, reload, or SSE connection from owning the provider
connection.

The core library exposes the same split:

```ts
const stream = hub.create({ source });     // trigger producer
const events = hub.tailFrom(stream.id, 7); // consume after durable cursor 7
```

There is no extra metadata event in the buffer today. Stream metadata lives in
`streams`, and consumers receive one terminal `end` event after all chunks have
been replayed. The transport adapter synthesizes that `end` event from stream
metadata, which keeps `buffer_chunks` focused on provider bytes.

### Runtime lifecycle

The default is suitable for a long-lived Node process: it starts the producer
without awaiting it. A runtime that might stop work after returning an HTTP
response should inject its lifecycle primitive:

```ts
const hub = new StreamHub({
  databasePath: "./data/streams.sqlite",
  keepAliveWhile: (task) => runtime.keepAliveWhile(task),
});
```

`keepAliveWhile` keeps the background **drain task** alive; it does not make a
provider TCP connection survive a process restart or redeployment. That still
requires running the hub in a separately durable runtime or using a
provider-specific resume/checkpoint protocol.

For the article's deploy-survival property, run the `StreamHub` in a separate,
long-lived buffer service or durable runtime. The included maze server is a
single-process learning demo, so it intentionally does not provide that
deployment boundary by itself.

The demo still wires `keepAliveWhile` with a visible heartbeat log so you can
see when the producer drain starts, stays alive, and releases. That heartbeat
belongs to the buffer's background producer task, not to a browser tab or SSE
connection.

If the runtime gives you primitive alarm operations rather than its own
`keepAliveWhile`, use `createKeepAliveWhile()`. Its lease is released on both
success and failure, so a completed or failed generation does not leak a
heartbeat:

```ts
import { createKeepAliveWhile } from "resumable-stream-observatory";

const keepAliveWhile = createKeepAliveWhile({
  begin() {
    const alarm = startHeartbeatAlarm();
    return () => stopHeartbeatAlarm(alarm);
  },
});
```

## Layout

```text
src/
  index.ts             public library exports
  types.ts             public contracts: source, lifecycle, stream, tail event
  stream-hub.ts        SQLite buffer, producer lease, replay/live tail
  keep-alive.ts        host heartbeat/alarm lease adapter
  raw.ts               provider-byte replay adapter
  sse.ts               optional SSE transport adapter
  demo/
    maze-source.ts     replaceable StreamSource implementation
    search-source.ts   slow scan + SQLite FTS5 search StreamSource
    server.ts          HTTP + static-file demo adapter
public/                browser SVG/search/IndexedDB visualiser
```

## HTTP surface

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/streams/maze` | Create a maze stream and start its background producer. |
| `POST` | `/streams/search` | Create a document search stream with `scan` or `fts5` backend. |
| `GET` | `/streams` | List recent stream metadata. |
| `GET` | `/streams?demoType=maze\|search` | List recent streams for one demo dropdown. |
| `GET` | `/streams/:id?after=<seq>` | SSE replay followed by live tail from a durable cursor. |
| `GET` | `/streams/:id/status` | Read stream metadata. |
| `DELETE` | `/streams/:id` | Delete server-side SQLite `buffer_chunks` and stream metadata. |

`GET /streams/:id` emits SSE messages whose `id` is the durable SQLite sequence
number. The browser demo stores that number in IndexedDB only after the
associated event is persisted.
