import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import {
  createKeepAliveWhile,
  StreamHub,
  toRawByteStream,
  toSse,
  type KeepAliveWhile,
  type StreamStatus,
  type TailEvent,
} from "../src/index.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

test("StreamHub stores provider chunks and exposes them as durable tail events", async (t) => {
  const { hub } = createTestHub(t);

  const stream = hub.create({
    streamId: "chat-turn-1",
    source: () => byteStream(["hello", " ", "world"]),
  });

  assert.equal(stream.id, "chat-turn-1");
  await waitForStatus(hub, stream.id, "completed");

  const events = await collectTail(hub.tailFrom(stream.id, -1));
  assert.deepEqual(simplify(events), [
    { kind: "chunk", seq: 0, text: "hello" },
    { kind: "chunk", seq: 1, text: " " },
    { kind: "chunk", seq: 2, text: "world" },
    { kind: "end", status: "completed", error: null },
  ]);
  assert.equal(hub.get(stream.id)?.nextSeq, 3);
});

test("StreamHub delegates the producer drain to the injected keepAliveWhile", async (t) => {
  const databasePath = testDatabasePath(t);
  let capturedTask: (() => Promise<void>) | undefined;
  const keepAliveWhile: KeepAliveWhile = (task) => {
    capturedTask = task;
  };
  const hub = new StreamHub({ databasePath, keepAliveWhile });
  t.after(() => hub.close());

  const stream = hub.create({ source: () => byteStream(["leased"]) });

  assert.equal(hub.get(stream.id)?.status, "streaming");
  assert.equal(hub.get(stream.id)?.nextSeq, 0);
  assert.ok(capturedTask, "expected StreamHub to pass the producer drain to keepAliveWhile");

  await capturedTask();
  assert.equal(hub.get(stream.id)?.status, "completed");
  assert.equal(hub.get(stream.id)?.nextSeq, 1);
});

test("list returns stream metadata without reading buffered bytes", async (t) => {
  const { hub } = createTestHub(t);
  const stream = hub.create({ streamId: "listed-stream", source: () => byteStream(["metadata"]) });
  await waitForStatus(hub, stream.id, "completed");

  assert.deepEqual(hub.list().map(({ id, status, nextSeq, error }) => ({ id, status, nextSeq, error })), [
    { id: "listed-stream", status: "completed", nextSeq: 1, error: null },
  ]);
});

test("tailFrom resumes after the last durable cursor", async (t) => {
  const { hub } = createTestHub(t);
  const stream = hub.create({ source: () => byteStream(["a", "b", "c"]) });
  await waitForStatus(hub, stream.id, "completed");

  const events = await collectTail(hub.tailFrom(stream.id, 0));

  assert.deepEqual(simplify(events), [
    { kind: "chunk", seq: 1, text: "b" },
    { kind: "chunk", seq: 2, text: "c" },
    { kind: "end", status: "completed", error: null },
  ]);
});

test("tailFrom waits when a consumer catches up before the producer finishes", async (t) => {
  const { hub } = createTestHub(t);
  const source = controlledByteStream();
  const stream = hub.create({ source: source.stream });

  await source.ready;
  const reader = hub.tailFrom(stream.id, -1).getReader();
  t.after(() => reader.cancel().catch(() => undefined));

  const firstRead = reader.read();
  await Promise.resolve();
  source.controller.enqueue(encoder.encode("late chunk"));

  assert.deepEqual(simplifyOne(await firstRead), { kind: "chunk", seq: 0, text: "late chunk" });

  source.controller.close();
  assert.deepEqual(simplifyOne(await reader.read()), { kind: "end", status: "completed", error: null });
});

test("failed provider streams preserve the failure as the terminal tail event", async (t) => {
  const { hub } = createTestHub(t);
  let pulled = false;
  const stream = hub.create({
    source: () => new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!pulled) {
          pulled = true;
          controller.enqueue(encoder.encode("partial"));
          return;
        }
        controller.error(new Error("provider disconnected"));
      },
    }),
  });

  await waitForStatus(hub, stream.id, "failed");

  const events = await collectTail(hub.tailFrom(stream.id, -1));
  assert.deepEqual(simplify(events), [
    { kind: "chunk", seq: 0, text: "partial" },
    { kind: "end", status: "failed", error: "provider disconnected" },
  ]);
});

test("delete removes both stream metadata and buffered chunks", async (t) => {
  const { hub } = createTestHub(t);
  const stream = hub.create({ source: () => byteStream(["delete me"]) });
  await waitForStatus(hub, stream.id, "completed");

  assert.equal(hub.delete(stream.id), true);
  assert.equal(hub.get(stream.id), undefined);

  const events = await collectTail(hub.tailFrom(stream.id, -1));
  assert.deepEqual(events, [{ kind: "error", message: "stream not found" }]);
});

test("a new hub marks unfinished streams from a previous process as interrupted", async (t) => {
  const databasePath = testDatabasePath(t);
  const firstHub = new StreamHub({ databasePath });
  const source = controlledByteStream();
  const stream = firstHub.create({ source: source.stream });
  await source.ready;
  firstHub.close();

  const secondHub = new StreamHub({ databasePath });
  t.after(() => secondHub.close());

  assert.equal(secondHub.get(stream.id)?.status, "interrupted");
  const events = await collectTail(secondHub.tailFrom(stream.id, -1));
  assert.deepEqual(events, [{
    kind: "end",
    status: "interrupted",
    error: "stream hub restarted before the producer completed",
  }]);
});

test("createKeepAliveWhile starts and releases the host lease around a task", async () => {
  const calls: string[] = [];
  const keepAliveWhile = createKeepAliveWhile({
    begin() {
      calls.push("begin");
      return () => calls.push("release");
    },
  });

  keepAliveWhile(async () => {
    calls.push("task");
  });
  await waitFor(() => calls.includes("release"));

  assert.deepEqual(calls, ["begin", "task", "release"]);
});

test("toRawByteStream replays only provider bytes", async () => {
  const raw = toRawByteStream(tailEventStream([
    { kind: "chunk", seq: 0, data: encoder.encode("one") },
    { kind: "chunk", seq: 1, data: encoder.encode("two") },
    { kind: "end", status: "completed", error: null },
  ]));

  assert.equal(await readByteStream(raw), "onetwo");
});

test("toSse adapts typed tail events into resumable SSE frames", async () => {
  const sse = toSse(tailEventStream([
    { kind: "chunk", seq: 7, data: encoder.encode("hi") },
    { kind: "end", status: "completed", error: null },
  ]));

  assert.equal(await readByteStream(sse), [
    `id: 7`,
    `event: chunk`,
    `data: {"seq":7,"data":"aGk="}`,
    ``,
    `event: end`,
    `data: {"status":"completed","error":null}`,
    ``,
    ``,
  ].join("\n"));
});

function createTestHub(t: TestContext): { hub: StreamHub } {
  const hub = new StreamHub({ databasePath: testDatabasePath(t) });
  t.after(() => hub.close());
  return { hub };
}

function testDatabasePath(t: { after(callback: () => void): void }): string {
  const directory = mkdtempSync(join(tmpdir(), "resumable-stream-test-"));
  t.after(() => rmSync(directory, { force: true, recursive: true }));
  return join(directory, "streams.sqlite");
}

function byteStream(chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

function controlledByteStream(): {
  ready: Promise<void>;
  controller: ReadableStreamDefaultController<Uint8Array>;
  stream: () => ReadableStream<Uint8Array>;
} {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let markReady!: () => void;
  const ready = new Promise<void>((resolve) => {
    markReady = resolve;
  });

  return {
    ready,
    get controller() {
      return controller;
    },
    stream: () => new ReadableStream<Uint8Array>({
      start(nextController) {
        controller = nextController;
        markReady();
      },
    }),
  };
}

function tailEventStream(events: TailEvent[]): ReadableStream<TailEvent> {
  return new ReadableStream<TailEvent>({
    start(controller) {
      for (const event of events) controller.enqueue(event);
      controller.close();
    },
  });
}

async function collectTail(stream: ReadableStream<TailEvent>): Promise<TailEvent[]> {
  const reader = stream.getReader();
  const events: TailEvent[] = [];
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return events;
      events.push(value);
    }
  } finally {
    reader.releaseLock();
  }
}

async function readByteStream(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  let output = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return output;
      output += decoder.decode(value, { stream: true });
    }
  } finally {
    output += decoder.decode();
    reader.releaseLock();
  }
}

async function waitForStatus(
  hub: StreamHub,
  streamId: string,
  status: StreamStatus,
): Promise<void> {
  await waitFor(() => hub.get(streamId)?.status === status);
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (!predicate()) {
    assert.ok(Date.now() - startedAt < 1_000, "timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function simplify(events: TailEvent[]): unknown[] {
  return events.map(simplifyOne);
}

function simplifyOne(result: ReadableStreamReadResult<TailEvent>): unknown;
function simplifyOne(event: TailEvent): unknown;
function simplifyOne(value: ReadableStreamReadResult<TailEvent> | TailEvent): unknown {
  const event = "done" in value ? value.value : value;
  if (!event) return value;
  if (event.kind === "chunk") return { kind: "chunk", seq: event.seq, text: decoder.decode(event.data) };
  return event;
}
