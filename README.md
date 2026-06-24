# Resumable Stream Observatory

A no-dependency Node 24.16 + TypeScript library experiment in durable stream
replay. The repository includes a maze visualiser, but the maze is only a demo
source; the library accepts any Web `ReadableStream<Uint8Array>`.

```sh
npm run dev
```

Open <http://127.0.0.1:8787>. Start a maze, then reload or open a new tab. The
browser remembers the active stream ID and its IndexedDB cursor. Disconnect or
go offline, let the producer continue, and reconnect without copying anything.

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
    server.ts          HTTP + static-file demo adapter
public/                browser SVG/IndexedDB visualiser
```

## HTTP surface

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/streams` | Create a stream and start its background producer. |
| `GET` | `/streams` | List stream metadata. |
| `GET` | `/streams/:id?after=<seq>` | SSE replay followed by live tail. |
| `GET` | `/streams/:id/status` | Read stream metadata. |

`GET /streams/:id` emits SSE messages whose `id` is the durable SQLite
sequence number. The browser demo stores that number in IndexedDB only after
the associated event is persisted.
