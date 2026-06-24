import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { StreamHub, toSse } from "../index.ts";
import { createMazeSource } from "./maze-source.ts";

const root = fileURLToPath(new URL("../..", import.meta.url));
const publicDir = join(root, "public");
const dataDir = join(root, "data");
mkdirSync(dataDir, { recursive: true });

const hub = new StreamHub({ databasePath: join(dataDir, "streams.sqlite") });

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

    if (req.method === "GET" && url.pathname === "/streams") return json(res, 200, hub.list());
    if (req.method === "POST" && url.pathname === "/streams") {
      const options = await readJson<{ count?: number; intervalMs?: number }>(req);
      const stream = hub.create({ source: createMazeSource(options) });
      return json(res, 201, stream);
    }

    const match = url.pathname.match(/^\/streams\/([0-9a-f-]+)(\/status)?$/i);
    if (match) {
      const streamId = match[1];
      const stream = hub.get(streamId);
      if (!stream) return json(res, 404, { error: "stream not found" });
      if (match[2] === "/status") return json(res, 200, stream);
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

  const reader = toSse(hub.tail(streamId, after)).getReader();
  req.on("close", () => void reader.cancel());
  for (;;) {
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
