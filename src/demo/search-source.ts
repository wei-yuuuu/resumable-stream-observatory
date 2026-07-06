import { DatabaseSync } from "node:sqlite";
import type { StreamSource } from "../types.ts";

export type SearchBackend = "scan" | "fts5";

export type SearchOptions = {
  query?: string;
  backend?: SearchBackend;
  scanDelayMs?: number;
};

type StoredDocument = {
  rowid: number;
  public_id: string;
  title: string;
  body: string;
};

type SearchResult = {
  public_id: string;
  title: string;
  snippet: string;
  score: number;
};

const encoder = new TextEncoder();
const defaultQuery = "sqlite";
const defaultScanDelayMs = 10;
const demoDocumentCount = 1_500;
const ftsResultLimit = 120;

export function prepareSearchDemo(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS documents (
      rowid INTEGER PRIMARY KEY,
      public_id TEXT UNIQUE NOT NULL,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at INTEGER NOT NULL
    ) STRICT;
    -- FTS5 is SQLite's built-in full-text index. The virtual table stores
    -- searchable title/body tokens and points back to documents.rowid.
    CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
      title,
      body,
      content='documents',
      content_rowid='rowid'
    );
  `);

  const count = db.prepare("SELECT COUNT(*) AS count FROM documents").get() as { count: number };
  if (count.count >= demoDocumentCount) {
    const ftsCount = db.prepare("SELECT COUNT(*) AS count FROM documents_fts").get() as { count: number };
    if (ftsCount.count !== count.count) rebuildFtsIndex(db);
    return;
  }

  const insertDocument = db.prepare(`
    INSERT INTO documents (public_id, title, body, created_at) VALUES (?, ?, ?, ?)
  `);
  const insertFts = db.prepare(`
    INSERT INTO documents_fts (rowid, title, body)
    SELECT rowid, title, body FROM documents WHERE public_id = ?
  `);

  db.exec("BEGIN IMMEDIATE");
  try {
    const now = Date.now();
    for (const document of createDemoDocuments(count.count, demoDocumentCount)) {
      insertDocument.run(document.id, document.title, document.body, now);
      insertFts.run(document.id);
    }
    db.exec("COMMIT");
  } catch (error) {
    if (db.isTransaction) db.exec("ROLLBACK");
    throw error;
  }
}

export function createSearchSource(db: DatabaseSync, options: SearchOptions = {}): StreamSource {
  const query = normalizeQuery(options.query);
  const backend = options.backend === "fts5" ? "fts5" : "scan";
  const scanDelayMs = clampNumber(options.scanDelayMs, defaultScanDelayMs, 0, 1_000);

  return () => new ReadableStream<Uint8Array>({
    async start(controller) {
      const startedAt = Date.now();
      let matched = 0;
      try {
        if (backend === "fts5") {
          const results = searchWithFts(db, query);
          emit(controller, { kind: "search-started", query, backend, total: results.length });
          for (const [index, result] of results.entries()) {
            emit(controller, { kind: "result", documentId: result.public_id, title: result.title, snippet: result.snippet, score: result.score });
            matched += 1;
            if ((index + 1) % 10 === 0) await sleep(8);
          }
          emit(controller, { kind: "summary", query, backend, scanned: null, matched, elapsedMs: Date.now() - startedAt });
          controller.close();
          return;
        }

        const documents = allDocuments(db);
        emit(controller, { kind: "search-started", query, backend, total: documents.length });
        for (const [index, document] of documents.entries()) {
          if (scanDelayMs > 0) await sleep(scanDelayMs);
          const score = scanScore(document, query);
          if (score > 0) {
            matched += 1;
            emit(controller, {
              kind: "result",
              documentId: document.public_id,
              title: document.title,
              snippet: makeSnippet(`${document.title} ${document.body}`, query),
              score,
            });
          }
          if ((index + 1) % 25 === 0 || index + 1 === documents.length) {
            emit(controller, { kind: "progress", scanned: index + 1, total: documents.length, matched });
          }
        }
        emit(controller, { kind: "summary", query, backend, scanned: documents.length, matched, elapsedMs: Date.now() - startedAt });
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
  });
}

function allDocuments(db: DatabaseSync): StoredDocument[] {
  return db.prepare(`
    SELECT rowid, public_id, title, body
    FROM documents
    ORDER BY rowid ASC
  `).all() as StoredDocument[];
}

function searchWithFts(db: DatabaseSync, query: string): SearchResult[] {
  return db.prepare(`
    SELECT
      d.public_id,
      d.title,
      -- snippet(table, column, before, after, ellipsis, tokens) asks FTS5 to
      -- return a short matching fragment from the body column. Column 1 is
      -- body because the virtual table columns are (title, body).
      snippet(documents_fts, 1, '', '', '…', 14) AS snippet,
      -- bm25() returns a relevance score from the FTS index. Lower is better
      -- in SQLite FTS5, so ORDER BY score ASC puts stronger matches first.
      bm25(documents_fts) AS score
    FROM documents_fts
    -- documents_fts is the inverted index. It stores searchable text, but the
    -- app-facing id/title live in documents. content_rowid='rowid' makes the
    -- FTS rowid match documents.rowid, so this join hydrates each hit.
    JOIN documents d ON d.rowid = documents_fts.rowid
    -- MATCH uses the FTS index instead of scanning every document row.
    WHERE documents_fts MATCH ?
    ORDER BY score ASC
    LIMIT ${ftsResultLimit}
  `).all(ftsQuery(query)) as SearchResult[];
}

function rebuildFtsIndex(db: DatabaseSync): void {
  // External-content FTS tables can be rebuilt from their canonical content
  // table. This keeps local demo databases healthy after seed logic changes.
  db.prepare("INSERT INTO documents_fts(documents_fts) VALUES ('rebuild')").run();
}

function scanScore(document: StoredDocument, query: string): number {
  const terms = queryTerms(query);
  const title = document.title.toLocaleLowerCase();
  const body = document.body.toLocaleLowerCase();
  let score = 0;
  for (const term of terms) {
    if (title.includes(term)) score += 3;
    if (body.includes(term)) score += 1;
  }
  return score;
}

function makeSnippet(text: string, query: string): string {
  const lower = text.toLocaleLowerCase();
  const term = queryTerms(query).find((candidate) => lower.includes(candidate));
  if (!term) return text.slice(0, 120);
  const index = lower.indexOf(term);
  const start = Math.max(0, index - 48);
  const end = Math.min(text.length, index + term.length + 72);
  return `${start > 0 ? "…" : ""}${text.slice(start, end)}${end < text.length ? "…" : ""}`;
}

function emit(controller: ReadableStreamDefaultController<Uint8Array>, event: unknown): void {
  controller.enqueue(encoder.encode(JSON.stringify(event)));
}

function normalizeQuery(value: string | undefined): string {
  const trimmed = value?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : defaultQuery;
}

function queryTerms(query: string): string[] {
  return query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
}

function ftsQuery(query: string): string {
  // FTS5 is token-based, so plain "sql" would not match the token "sqlite".
  // Appending * makes each user term a prefix query while still avoiding fuzzy
  // typo matching; "sql" can match "sqlite", but "sqllite" still will not.
  return queryTerms(query).map((term) => `"${term.replaceAll("\"", "\"\"")}"*`).join(" OR ");
}

function clampNumber(value: number | undefined, fallback: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(value!)));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createDemoDocuments(start: number, end: number): Array<{ id: string; title: string; body: string }> {
  const topics = [
    ["sqlite", "SQLite WAL keeps writes durable while readers keep tailing committed rows."],
    ["indexeddb", "IndexedDB stores a browser projection so a reload can redraw before reconnecting."],
    ["redis", "Redis streams can coordinate distributed consumers, but the browser still needs a cursor."],
    ["search", "Search jobs often scan many records and should show partial results as they arrive."],
    ["durable object", "A durable runtime can keep a producer drain alive through quiet stretches."],
    ["csv import", "CSV importers validate rows incrementally and report row errors without blocking."],
    ["audit log", "Audit log discovery scans older records and streams matching entries to admins."],
    ["support tickets", "Support teams search tickets, hydrate permissions, and render snippets."],
    ["product catalog", "Catalog searches may combine indexed candidates with slow inventory checks."],
    ["report builder", "Report builders stream step logs while charts and artifacts are generated."],
  ];
  const adjectives = ["durable", "resumable", "incremental", "observable", "buffered", "offline-ready"];
  const documents = [];
  for (let index = start; index < end; index += 1) {
    const topic = topics[index % topics.length];
    const adjective = adjectives[index % adjectives.length];
    documents.push({
      id: `doc-${String(index + 1).padStart(3, "0")}`,
      title: `${titleCase(adjective)} ${titleCase(topic[0])} Note ${index + 1}`,
      body: `${topic[1]} This document compares ${topic[0]} with ${topics[(index + 3) % topics.length][0]} and ${topics[(index + 6) % topics.length][0]}. It is part of a demo corpus for long-running search streams, progress events, result hydration, and cursor resume.`,
    });
  }
  return documents;
}

function titleCase(value: string): string {
  return value.replace(/\b\w/g, (letter) => letter.toLocaleUpperCase());
}
