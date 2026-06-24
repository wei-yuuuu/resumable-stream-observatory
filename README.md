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
import { StreamHub, toSse } from "resumable-stream-observatory";

const hub = new StreamHub({ databasePath: "./data/streams.sqlite" });

const stream = hub.create({
  source: async () => {
    const response = await fetch("https://provider.example/stream");
    if (!response.body) throw new Error("provider did not return a body");
    return response.body;
  },
});

// An HTTP framework can return this as text/event-stream.
const sseBody = toSse(hub.tailFrom(stream.id, -1));
```

`hub.tailFrom(streamId, cursor)` yields typed events. `toSse()` is merely the
included Node/SSE adapter; a user can instead adapt the same events to a
WebSocket, a CLI renderer, or another transport.

## Layout

```text
src/
  index.ts             public library exports
  types.ts             public contracts: source, stream, tail event
  stream-hub.ts        SQLite buffer, producer lease, replay/live tail
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
