import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type {
  CreateStreamOptions,
  KeepAliveWhile,
  StreamHubOptions,
  StreamInfo,
  StreamSource,
  StreamStatus,
  TailEvent,
} from "./types.ts";

type StoredStream = {
  id: string;
  status: StreamStatus;
  producer_lease: string;
  next_seq: number;
  created_at: number;
  updated_at: number;
  error: string | null;
};

type StoredChunk = {
  seq: number;
  data: Uint8Array;
};

export class StreamHub {
  #db: DatabaseSync;
  #keepAliveWhile: KeepAliveWhile;
  #versions = new Map<string, number>();
  #waiters = new Map<string, Set<() => void>>();

  constructor({ databasePath, keepAliveWhile = nodeKeepAliveWhile }: StreamHubOptions) {
    this.#db = new DatabaseSync(databasePath);
    this.#keepAliveWhile = keepAliveWhile;
    this.#db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS streams (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('streaming', 'completed', 'failed')),
        producer_lease TEXT NOT NULL,
        next_seq INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        error TEXT
      ) STRICT;
      CREATE TABLE IF NOT EXISTS chunks (
        stream_id TEXT NOT NULL REFERENCES streams(id),
        seq INTEGER NOT NULL,
        data BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY(stream_id, seq)
      ) STRICT;
    `);
  }

  create({ source, streamId = randomUUID() }: CreateStreamOptions): StreamInfo {
    const producerLease = randomUUID();
    const now = Date.now();
    this.#db.prepare(`
      INSERT INTO streams (id, status, producer_lease, next_seq, created_at, updated_at)
      VALUES (?, 'streaming', ?, 0, ?, ?)
    `).run(streamId, producerLease, now, now);

    // The provider drain belongs to the hub, not to the endpoint response or
    // browser connection that created it. Hosts with a lifecycle API can keep
    // this task alive after that request has already returned.
    this.#keepAliveWhile(() => this.#startProducer(streamId, producerLease, source));
    return this.get(streamId)!;
  }

  get(streamId: string): StreamInfo | undefined {
    const stream = this.#db.prepare(`
      SELECT id, status, producer_lease, next_seq, created_at, updated_at, error
      FROM streams WHERE id = ?
    `).get(streamId) as StoredStream | undefined;
    return stream && toStreamInfo(stream);
  }

  list(): StreamInfo[] {
    const streams = this.#db.prepare(`
      SELECT id, status, producer_lease, next_seq, created_at, updated_at, error
      FROM streams ORDER BY created_at DESC LIMIT 30
    `).all() as StoredStream[];
    return streams.map(toStreamInfo);
  }

  tailFrom(streamId: string, after: number): ReadableStream<TailEvent> {
    const aborted = new AbortController();

    return new ReadableStream<TailEvent>({
      start: async (controller) => {
        let cursor = after;
        try {
          while (true) {
            const observedVersion = this.#version(streamId);
            const chunks = this.#chunksAfter(streamId, cursor);
            for (const chunk of chunks) {
              cursor = chunk.seq;
              controller.enqueue({ kind: "chunk", seq: chunk.seq, data: chunk.data });
            }

            const stream = this.get(streamId);
            if (!stream) {
              controller.enqueue({ kind: "error", message: "stream not found" });
              break;
            }
            if (stream.status !== "streaming") {
              controller.enqueue({ kind: "end", status: stream.status, error: stream.error });
              break;
            }
            await this.#waitForChange(streamId, observedVersion, aborted.signal);
            if (aborted.signal.aborted) break;
          }
        } finally {
          controller.close();
        }
      },
      cancel: () => aborted.abort(),
    });
  }

  close(): void {
    this.#db.close();
  }

  async #startProducer(streamId: string, lease: string, source: StreamSource): Promise<void> {
    try {
      const upstream = await source({ streamId });
      await this.#consume(streamId, lease, upstream.getReader());
    } catch (error) {
      this.#finish(streamId, lease, "failed", messageOf(error));
    }
  }

  async #consume(streamId: string, lease: string, reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) this.#append(streamId, lease, value);
      }
      this.#finish(streamId, lease, "completed");
    } catch (error) {
      this.#finish(streamId, lease, "failed", messageOf(error));
      // If a chunk cannot be made durable, continuing to drain would silently
      // discard provider output. Stop the upstream instead.
      await reader.cancel(error).catch(() => undefined);
    } finally {
      reader.releaseLock();
    }
  }

  #append(streamId: string, lease: string, data: Uint8Array): void {
    const stream = this.#storedStream(streamId);
    if (!stream || stream.status !== "streaming" || stream.producer_lease !== lease) return;

    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const now = Date.now();
      this.#db.prepare(`
        INSERT INTO chunks (stream_id, seq, data, created_at) VALUES (?, ?, ?, ?)
      `).run(streamId, stream.next_seq, data, now);
      const updated = this.#db.prepare(`
        UPDATE streams SET next_seq = ?, updated_at = ? WHERE id = ? AND producer_lease = ?
      `).run(stream.next_seq + 1, now, streamId, lease);
      if (updated.changes !== 1) throw new Error("producer lease was lost while appending a chunk");
      this.#db.exec("COMMIT");
    } catch (error) {
      if (this.#db.isTransaction) this.#db.exec("ROLLBACK");
      throw error;
    }
    this.#notify(streamId);
  }

  #finish(
    streamId: string,
    lease: string,
    status: Exclude<StreamStatus, "streaming">,
    error: string | null = null,
  ): void {
    const result = this.#db.prepare(`
      UPDATE streams SET status = ?, error = ?, updated_at = ?
      WHERE id = ? AND producer_lease = ? AND status = 'streaming'
    `).run(status, error, Date.now(), streamId, lease);
    if (result.changes > 0) this.#notify(streamId);
  }

  #storedStream(streamId: string): StoredStream | undefined {
    return this.#db.prepare(`
      SELECT id, status, producer_lease, next_seq, created_at, updated_at, error
      FROM streams WHERE id = ?
    `).get(streamId) as StoredStream | undefined;
  }

  #chunksAfter(streamId: string, after: number): StoredChunk[] {
    return this.#db.prepare(`
      SELECT seq, data FROM chunks
      WHERE stream_id = ? AND seq > ? ORDER BY seq ASC
    `).all(streamId, after) as StoredChunk[];
  }

  #version(streamId: string): number {
    return this.#versions.get(streamId) ?? 0;
  }

  #waitForChange(streamId: string, observedVersion: number, signal: AbortSignal): Promise<void> {
    if (this.#version(streamId) !== observedVersion || signal.aborted) return Promise.resolve();

    return new Promise((resolve) => {
      const waiters = this.#waiters.get(streamId) ?? new Set<() => void>();
      this.#waiters.set(streamId, waiters);
      const done = () => {
        waiters.delete(done);
        if (waiters.size === 0) this.#waiters.delete(streamId);
        signal.removeEventListener("abort", done);
        resolve();
      };

      waiters.add(done);
      signal.addEventListener("abort", done, { once: true });
      // The check after registration closes the read-then-subscribe race.
      if (this.#version(streamId) !== observedVersion) done();
    });
  }

  #notify(streamId: string): void {
    this.#versions.set(streamId, this.#version(streamId) + 1);
    for (const wake of this.#waiters.get(streamId) ?? []) wake();
  }
}

function toStreamInfo(stream: StoredStream): StreamInfo {
  return {
    id: stream.id,
    status: stream.status,
    nextSeq: stream.next_seq,
    createdAt: stream.created_at,
    updatedAt: stream.updated_at,
    error: stream.error,
  };
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function nodeKeepAliveWhile(task: () => Promise<void>): void {
  // A long-lived Node process stays alive while its active sockets, timers, or
  // other event-loop handles remain active. Serverless hosts should inject
  // their own keep-alive implementation instead.
  void task();
}
