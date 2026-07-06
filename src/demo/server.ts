import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { createKeepAliveWhile, StreamHub, toSse } from "../index.ts";
import { createMazeSource } from "./maze-source.ts";
import { createSearchSource, prepareSearchDemo, type SearchBackend } from "./search-source.ts";

const root = fileURLToPath(new URL("../..", import.meta.url));
const publicDir = join(root, "public");
const dataDir = join(root, "data");
mkdirSync(dataDir, { recursive: true });
const databasePath = join(dataDir, "streams.sqlite");

const searchDb = new DatabaseSync(databasePath);
prepareSearchDemo(searchDb);
prepareDemoStreams(searchDb);

const demoKeepAliveWhile = createKeepAliveWhile({
  begin() {
    // Demo-only: local Node does not need a host alarm to survive idle eviction.
    // This visible lease shows that keepAliveWhile belongs to the producer
    // drain, not to any browser SSE connection.
    console.log("[keepAliveWhile] producer drain started");
    const heartbeat = setInterval(() => {
      console.log("[keepAliveWhile] producer drain still alive");
    }, 5_000);

    return () => {
      clearInterval(heartbeat);
      console.log("[keepAliveWhile] producer drain released");
    };
  },
});

const hub = new StreamHub({
  databasePath,
  keepAliveWhile: demoKeepAliveWhile,
});

const server = createServer(async (req, res) => {
  const requestTarget = req.url ?? "/";
  const baseUrl = "http://localhost";
  if (!URL.canParse(requestTarget, baseUrl)) {
    return json(res, 400, { error: "invalid request URL" });
  }
  const url = new URL(requestTarget, baseUrl);

  try {
    if (req.method === "GET" && url.pathname === "/") return serveFile(res, "index.html", "text/html; charset=utf-8");
    if (req.method === "GET" && url.pathname === "/app.js") return serveFile(res, "app.js", "text/javascript; charset=utf-8");

    if (req.method === "GET" && url.pathname === "/streams") {
      const demoType = parseDemoType(url.searchParams.get("demoType"));
      if (url.searchParams.has("demoType") && !demoType) return json(res, 400, { error: "demoType must be maze or search" });
      return json(res, 200, listDemoStreams(demoType));
    }
    if (req.method === "POST" && url.pathname === "/streams/maze") {
      const options = await readJson<{ count?: number; intervalMs?: number }>(req);
      const stream = hub.create({ source: createMazeSource(options) });
      recordDemoStream(stream.id, "maze", `Maze ${new Date(stream.createdAt).toLocaleTimeString()}`);
      return json(res, 201, stream);
    }
    if (req.method === "POST" && url.pathname === "/streams/search") {
      const options = await readJson<{ query?: string; backend?: SearchBackend; scanDelayMs?: number }>(req);
      const stream = hub.create({ source: createSearchSource(searchDb, options) });
      const backend = options.backend === "fts5" ? "fts5" : "scan";
      const query = options.query?.trim() || "sqlite";
      recordDemoStream(stream.id, "search", `${backend} search "${query}"`);
      return json(res, 201, stream);
    }

    const match = url.pathname.match(/^\/streams\/([0-9a-f-]+)(\/status)?$/i);
    if (match) {
      const streamId = match[1];
      const stream = hub.get(streamId);
      if (!stream) return json(res, 404, { error: "stream not found" });
      if (match[2] === "/status") return json(res, 200, stream);
      if (req.method === "DELETE") {
        hub.delete(streamId);
        deleteDemoStream(streamId);
        return json(res, 200, { deleted: true, streamId });
      }
      if (req.method === "GET") return sendSse(req, res, streamId, stream.status, url.searchParams.get("after"));
    }

    return json(res, 404, { error: "not found" });
  } catch (error) {
    console.error(error);
    return json(res, 500, { error: error instanceof Error ? error.message : "internal error" });
  }
});

const port = Number.parseInt(process.env.PORT ?? "8787", 10);
server.listen(port, "127.0.0.1", () => {
  console.log(`Stream Observatory listening on http://127.0.0.1:${port}`);
});

async function sendSse(
  req: IncomingMessage,
  res: ServerResponse,
  streamId: string,
  status: string,
  afterParameter: string | null,
): Promise<void> {
  const after = parseCursor(afterParameter);
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Buffer-Status": status,
  });

  const reader = toSse(hub.tailFrom(streamId, after)).getReader();
  req.on("close", () => void reader.cancel());
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    res.write(value);
  }
  res.end();
}

function parseCursor(value: string | null): number {
  const parsed = Number.parseInt(value ?? "-1", 10);
  return Number.isFinite(parsed) ? Math.max(-1, parsed) : -1;
}

function serveFile(res: ServerResponse, file: string, contentType: string): void {
  res.writeHead(200, { "Content-Type": contentType, "Cache-Control": "no-store" });
  res.end(readFileSync(join(publicDir, file)));
}

function json(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

async function readJson<T>(req: IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(Buffer.from(chunk));
  if (chunks.length === 0) return {} as T;
  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T;
}

function prepareDemoStreams(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS demo_streams (
      stream_id TEXT PRIMARY KEY,
      demo_type TEXT NOT NULL CHECK(demo_type IN ('maze', 'search')),
      label TEXT NOT NULL,
      created_at INTEGER NOT NULL
    ) STRICT;
  `);
}

function recordDemoStream(streamId: string, demoType: "maze" | "search", label: string): void {
  searchDb.prepare(`
    INSERT OR REPLACE INTO demo_streams (stream_id, demo_type, label, created_at)
    VALUES (?, ?, ?, ?)
  `).run(streamId, demoType, label, Date.now());
}

function deleteDemoStream(streamId: string): void {
  searchDb.prepare("DELETE FROM demo_streams WHERE stream_id = ?").run(streamId);
}

function listDemoStreams(demoType?: "maze" | "search"): unknown[] {
  if (!demoType) return hub.list();

  const metadataRows = searchDb.prepare(`
    SELECT stream_id, demo_type, label
    FROM demo_streams
    WHERE demo_type = ?
    ORDER BY created_at DESC
    LIMIT 30
  `).all(demoType) as Array<{ stream_id: string; demo_type: "maze" | "search"; label: string }>;

  return metadataRows.flatMap((row) => {
    const stream = hub.get(row.stream_id);
    if (!stream) return [];
    return {
      ...stream,
      demoType: row.demo_type,
      label: row.label,
    };
  });
}

function parseDemoType(value: string | null): "maze" | "search" | undefined {
  return value === "maze" || value === "search" ? value : undefined;
}
