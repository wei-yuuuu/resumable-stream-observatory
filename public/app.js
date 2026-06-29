const statusEl = document.querySelector("#status");
const eventsEl = document.querySelector("#events");
const mazeLines = document.querySelector("#maze-lines");
let connection;
let currentId = "";
let eventLog = [];
let edges = [];
let viewedSeq = null;
const activeStreamKey = "resumable-stream-observatory:active-stream";

const database = await openDatabase();

document.querySelector("#create").addEventListener("click", async () => {
  const response = await fetch("/streams", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ count: 335, intervalMs: 80 }) });
  const stream = await response.json();
  rememberStream(stream.id);
  await connect(stream.id);
});
document.querySelector("#reconnect").addEventListener("click", () => connect(savedStreamId()));
document.querySelector("#disconnect").addEventListener("click", () => disconnect("disconnected by user"));
document.querySelector("#live").addEventListener("click", () => showLive());
document.querySelector("#forget-active").addEventListener("click", () => {
  disconnect("auto-resume stopped; local cache is still kept");
  localStorage.removeItem(activeStreamKey);
  history.replaceState({}, "", "/");
  currentId = "";
  resetView();
});
document.querySelector("#delete-cache").addEventListener("click", async () => {
  const streamId = currentId || savedStreamId() || new URL(location.href).searchParams.get("stream");
  if (!streamId) return setStatus("No saved stream selected.");
  disconnect("deleting local cache");
  try {
    await deleteLocalCache(streamId);
    if (currentId === streamId || savedStreamId() === streamId) localStorage.removeItem(activeStreamKey);
    history.replaceState({}, "", "/");
    currentId = "";
    resetView();
    setStatus(`Deleted IndexedDB events and cursor for ${streamId}.`);
  } catch (error) {
    setStatus(`Failed to delete local cache: ${error.message}`);
  }
});
window.addEventListener("offline", () => disconnect("browser is offline — the producer is still running"));
window.addEventListener("online", () => {
  const streamId = currentId || savedStreamId();
  if (streamId) void connect(streamId);
});

const initialStreamId = new URL(location.href).searchParams.get("stream") || savedStreamId();
if (initialStreamId) void connect(initialStreamId);

async function connect(streamId) {
  if (!streamId) return setStatus("Start a fresh maze first.");
  disconnect("switching connection");
  const controller = new AbortController();
  connection = controller;
  currentId = streamId;
  rememberStream(streamId);
  const savedEvents = await loadEvents(streamId);
  if (controller.signal.aborted) return;
  eventLog = savedEvents;
  showLive();
  const cursor = await loadCursor(streamId);
  if (controller.signal.aborted) return;
  setStatus(`Restored ${savedEvents.length} browser events; connecting after durable cursor ${cursor}`);

  try {
    const response = await fetch(`/streams/${encodeURIComponent(streamId)}?after=${cursor}`, { signal: controller.signal });
    if (!response.ok || !response.body) throw new Error(await response.text());
    setStatus(`Live tail connected — server said: ${response.headers.get("X-Buffer-Status")}`);
    await consumeSse(response.body, streamId, controller.signal);
  } catch (error) {
    if (!controller.signal.aborted) setStatus(`Connection ended: ${error.message}`);
  }
}

function disconnect(reason) {
  connection?.abort();
  connection = undefined;
  if (reason) setStatus(reason);
}

async function consumeSse(body, streamId, signal) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  while (!signal.aborted) {
    const { done, value } = await reader.read();
    if (done) break;
    pending += decoder.decode(value, { stream: true });
    const messages = pending.split("\n\n");
    pending = messages.pop();
    for (const message of messages) await handleSse(message, streamId);
  }
}

async function handleSse(message, streamId) {
  const fields = Object.fromEntries(message.split("\n").map((line) => {
    const separator = line.indexOf(":");
    return separator < 0 ? [line, ""] : [line.slice(0, separator), line.slice(separator + 1).trimStart()];
  }));
  if (fields.event === "end") return setStatus(`Stream ended: ${fields.data}`);
  if (fields.event !== "chunk") return;

  const envelope = JSON.parse(fields.data);
  const decoded = new TextDecoder().decode(base64Bytes(envelope.data));
  const event = JSON.parse(decoded);
  // The transaction commits both the event and the cursor before UI application.
  await persistEvent(streamId, envelope.seq, event);
  const entry = { streamId, seq: envelope.seq, event, storedAt: Date.now() };
  appendEventEntry(entry);
  if (viewedSeq === null && event.kind === "maze-edge") {
    edges.push(event);
    appendEdge(event, true);
  }
  renderEventLog();
  if (viewedSeq === null) eventsEl.scrollTop = eventsEl.scrollHeight;
  const view = viewedSeq === null ? "live" : `time-travel view at ${viewedSeq}`;
  setStatus(`Applied durable cursor ${envelope.seq}; ${edges.length} visible passages (${view}).`);
}

function resetView() {
  eventLog = [];
  viewedSeq = null;
  edges = [];
  renderMaze();
  renderEventLog();
}

function showLive() {
  viewedSeq = null;
  edges = mazeEdgesThrough(Infinity);
  renderMaze();
  renderEventLog();
  eventsEl.scrollTop = eventsEl.scrollHeight;
  const latest = latestSeq();
  setStatus(latest < 0 ? "Live view; no browser events yet." : `Live view at latest durable cursor ${latest}.`);
}

function showAt(seq) {
  viewedSeq = seq;
  edges = mazeEdgesThrough(seq);
  renderMaze();
  renderEventLog();
  setStatus(`Time travel: showing durable cursor ${seq}; live resume checkpoint is still ${latestSeq()}.`);
}

function renderMaze() {
  mazeLines.replaceChildren();
  for (const edge of edges) appendEdge(edge, false);
}

function renderEventLog() {
  eventsEl.replaceChildren();
  for (const entry of eventLog) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "event-entry";
    if (entry.seq === viewedSeq) button.classList.add("selected");
    button.textContent = `${String(entry.seq).padStart(3, "0")}  ${JSON.stringify(entry.event)}`;
    button.addEventListener("click", () => showAt(entry.seq));
    eventsEl.append(button);
  }
}

function appendEventEntry(entry) {
  const index = eventLog.findIndex((item) => item.seq === entry.seq);
  if (index >= 0) eventLog[index] = entry;
  else eventLog.push(entry);
  eventLog.sort((left, right) => left.seq - right.seq);
}

function mazeEdgesThrough(seq) {
  return eventLog
    .filter((entry) => entry.seq <= seq && entry.event.kind === "maze-edge")
    .map((entry) => entry.event);
}

function latestSeq() {
  return eventLog.at(-1)?.seq ?? -1;
}

function appendEdge(edge, animate) {
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", `M ${edge.x1} ${edge.y1} L ${edge.x2} ${edge.y2}`);
  path.setAttribute("pathLength", "1");
  if (animate) path.classList.add("edge-live");
  mazeLines.append(path);
}

function setStatus(message) { statusEl.textContent = message; }

function savedStreamId() { return localStorage.getItem(activeStreamKey) ?? ""; }

function rememberStream(streamId) {
  localStorage.setItem(activeStreamKey, streamId);
  history.replaceState({}, "", `/?stream=${encodeURIComponent(streamId)}`);
}

function base64Bytes(text) {
  const binary = atob(text);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function openDatabase() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("resumable-stream-observatory", 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("cursors", { keyPath: "streamId" });
      request.result.createObjectStore("events", { keyPath: ["streamId", "seq"] });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function loadCursor(streamId) {
  return new Promise((resolve, reject) => {
    const request = database.transaction("cursors").objectStore("cursors").get(streamId);
    request.onsuccess = () => resolve(request.result?.seq ?? -1);
    request.onerror = () => reject(request.error);
  });
}

function persistEvent(streamId, seq, event) {
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(["events", "cursors"], "readwrite");
    transaction.objectStore("events").put({ streamId, seq, event, storedAt: Date.now() });
    transaction.objectStore("cursors").put({ streamId, seq });
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
  });
}

function loadEvents(streamId) {
  return new Promise((resolve, reject) => {
    const entries = [];
    const request = database.transaction("events").objectStore("events").openCursor(
      IDBKeyRange.bound([streamId, 0], [streamId, Number.MAX_SAFE_INTEGER]),
    );
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return resolve(entries);
      entries.push(cursor.value);
      cursor.continue();
    };
    request.onerror = () => reject(request.error);
  });
}

function deleteLocalCache(streamId) {
  return new Promise((resolve, reject) => {
    const transaction = database.transaction(["events", "cursors"], "readwrite");
    transaction.objectStore("cursors").delete(streamId);

    const request = transaction.objectStore("events").openCursor(
      IDBKeyRange.bound([streamId, 0], [streamId, Number.MAX_SAFE_INTEGER]),
    );
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return;
      cursor.delete();
      cursor.continue();
    };

    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error);
  });
}
