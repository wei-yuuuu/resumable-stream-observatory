const statusEl = document.querySelector("#status");
const eventsEl = document.querySelector("#events");
const mazeLines = document.querySelector("#maze-lines");
let connection;
let currentId = "";
let edges = [];
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
document.querySelector("#forget").addEventListener("click", () => {
  disconnect("saved maze forgotten");
  localStorage.removeItem(activeStreamKey);
  history.replaceState({}, "", "/");
  currentId = "";
  edges = [];
  renderMaze();
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
  edges = savedEvents.map((entry) => entry.event).filter((event) => event.kind === "maze-edge");
  eventsEl.textContent = savedEvents.map((entry) => `${String(entry.seq).padStart(3, "0")}  ${JSON.stringify(entry.event)}`).join("\n");
  renderMaze();
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
  if (event.kind === "maze-edge") {
    edges.push(event);
    appendEdge(event, true);
  }
  eventsEl.textContent += `${String(envelope.seq).padStart(3, "0")}  ${decoded}\n`;
  eventsEl.scrollTop = eventsEl.scrollHeight;
  setStatus(`Applied durable cursor ${envelope.seq}; ${edges.length} durable maze passages.`);
}

function renderMaze() {
  mazeLines.replaceChildren();
  for (const edge of edges) appendEdge(edge, false);
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
