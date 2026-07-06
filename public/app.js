const statusEl = document.querySelector("#status");
const eventsEl = document.querySelector("#events");
const mazeLines = document.querySelector("#maze-lines");
const streamSelectEls = {
  maze: document.querySelector("#maze-stream-select"),
  search: document.querySelector("#search-stream-select"),
};
const connectionButtonEls = {
  maze: document.querySelector("#maze-connection"),
  search: document.querySelector("#search-connection"),
};
const deleteButtonEls = {
  maze: document.querySelector("#maze-delete"),
  search: document.querySelector("#search-delete"),
};
const searchQueryEl = document.querySelector("#search-query");
const searchStatusEl = document.querySelector("#search-status");
const searchBarEl = document.querySelector("#search-bar");
const searchResultsEl = document.querySelector("#search-results");
const mazePanelEl = document.querySelector("#maze-panel");
const searchPanelEl = document.querySelector("#search-panel");
const tabMazeEl = document.querySelector("#tab-maze");
const tabSearchEl = document.querySelector("#tab-search");
let connection;
let currentId = "";
let eventLog = [];
let edges = [];
let viewedSeq = null;
let streamOptionsRefresh;
let streamOptionsById = new Map();
let viewVersion = 0;
let activeDemo = "maze";
const activeStreamKey = "resumable-stream-observatory:active-stream";

const database = await openDatabase();

document.querySelector("#create").addEventListener("click", async () => {
  setActiveDemo("maze");
  const response = await fetch("/streams/maze", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ count: 335, intervalMs: 80 }) });
  const stream = await response.json();
  rememberStream(stream.id);
  await refreshStreamOptions(stream.id);
  await connect(stream.id);
});
document.querySelector("#start-search").addEventListener("click", async () => {
  setActiveDemo("search");
  const response = await fetch("/streams/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: searchQueryEl.value, backend: selectedSearchBackend() }),
  });
  const stream = await response.json();
  rememberStream(stream.id);
  await refreshStreamOptions(stream.id);
  await connect(stream.id);
});
tabMazeEl.addEventListener("click", () => setActiveDemo("maze"));
tabSearchEl.addEventListener("click", () => setActiveDemo("search"));
for (const kind of ["maze", "search"]) {
  streamSelectEls[kind].addEventListener("focus", () => void refreshStreamOptions(selectedStreamId(kind)));
  streamSelectEls[kind].addEventListener("click", () => void refreshStreamOptions(selectedStreamId(kind)));
  streamSelectEls[kind].addEventListener("change", () => connect(streamSelectEls[kind].value));
  connectionButtonEls[kind].addEventListener("click", () => {
    const streamId = selectedStreamId(kind);
    if (connection && currentId === streamId) disconnect("disconnected by user");
    else void connect(streamId);
  });
  deleteButtonEls[kind].addEventListener("click", () => void deleteSelectedStream(kind));
}
document.querySelector("#live").addEventListener("click", () => showLive());
async function deleteSelectedStream(kind) {
  const streamId = selectedStreamId(kind);
  if (!streamId) return setStatus("No stream selected.");
  if (currentId === streamId) disconnect("deleting stream");
  try {
    await deleteLocalCache(streamId);
    await deleteServerBuffer(streamId);
    if (currentId === streamId) {
      currentId = "";
      resetView();
    }
    if (savedStreamId() === streamId) localStorage.removeItem(activeStreamKey);
    if (new URL(location.href).searchParams.get("stream") === streamId) history.replaceState({}, "", "/");
    await refreshStreamOptions();
    setStatus(`Deleted IndexedDB cache and server SQLite buffer for ${streamId}.`);
  } catch (error) {
    setStatus(`Failed to delete stream: ${error.message}`);
  }
}
window.addEventListener("offline", () => disconnect("browser is offline — the producer is still running"));
window.addEventListener("online", () => {
  const streamId = currentId || savedStreamId();
  if (streamId) void connect(streamId);
});

const initialStreamId = new URL(location.href).searchParams.get("stream") || savedStreamId();
await refreshStreamOptions(initialStreamId);
updateConnectionButtons();
if (initialStreamId) void connect(initialStreamId);

async function connect(streamId) {
  if (!streamId) return setStatus("Start a fresh maze first.");
  const version = viewVersion + 1;
  viewVersion = version;
  connection?.abort();
  connection = undefined;
  currentId = streamId;
  rememberStream(streamId);
  selectStream(streamId);
  updateConnectionButtons();
  const savedEvents = await loadEvents(streamId);
  if (version !== viewVersion) return;
  eventLog = savedEvents;
  setActiveDemo(inferDemoKind() ?? activeDemo);
  showLive();
  const cursor = await loadCursor(streamId);
  if (version !== viewVersion) return;

  const stream = streamOptionsById.get(streamId);
  if (stream && stream.status !== "streaming") {
    updateConnectionButtons();
    return setStatus(`Opened ${stream.status} stream from IndexedDB cache at durable cursor ${cursor}.`);
  }

  const controller = new AbortController();
  connection = controller;
  updateConnectionButtons();
  setStatus(`Restored ${savedEvents.length} browser events; connecting after durable cursor ${cursor}`);

  try {
    const response = await fetch(`/streams/${encodeURIComponent(streamId)}?after=${cursor}`, { signal: controller.signal });
    if (!response.ok || !response.body) throw new Error(await response.text());
    setStatus(`Live tail connected — server said: ${response.headers.get("X-Buffer-Status")}`);
    await consumeSse(response.body, streamId, controller.signal);
  } catch (error) {
    if (!controller.signal.aborted) setStatus(`Connection ended: ${error.message}`);
  } finally {
    if (connection === controller) {
      connection = undefined;
      updateConnectionButtons();
    }
  }
}

function disconnect(reason) {
  viewVersion += 1;
  connection?.abort();
  connection = undefined;
  updateConnectionButtons();
  if (reason) setStatus(reason);
}

function updateConnectionButtons() {
  for (const kind of ["maze", "search"]) {
    const button = connectionButtonEls[kind];
    const streamId = selectedStreamId(kind);
    const stream = streamOptionsById.get(streamId);
    const isConnectedStream = Boolean(connection && currentId === streamId);

    button.textContent = isConnectedStream || !stream || stream.status !== "streaming" ? "Disconnect" : "Reconnect";
    button.disabled = !isConnectedStream && (!stream || stream.status !== "streaming");
  }
}

function setActiveDemo(kind) {
  activeDemo = kind;
  mazePanelEl.hidden = kind !== "maze";
  searchPanelEl.hidden = kind !== "search";
  tabMazeEl.classList.toggle("active", kind === "maze");
  tabSearchEl.classList.toggle("active", kind === "search");
  updateConnectionButtons();
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
  if (fields.event === "end") {
    const end = JSON.parse(fields.data);
    setStreamStatus(streamId, end.status);
    updateConnectionButtons();
    return setStatus(`Stream ended: ${fields.data}`);
  }
  if (fields.event !== "chunk") return;

  const envelope = JSON.parse(fields.data);
  const decoded = new TextDecoder().decode(base64Bytes(envelope.data));
  const event = JSON.parse(decoded);
  if (event.kind === "maze-edge") setActiveDemo("maze");
  if (event.kind === "search-started") setActiveDemo("search");
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
  renderSearchProjection();
  renderEventLog();
}

function showLive() {
  viewedSeq = null;
  edges = mazeEdgesThrough(Infinity);
  renderMaze();
  renderSearchProjection();
  renderEventLog();
  eventsEl.scrollTop = eventsEl.scrollHeight;
  const latest = latestSeq();
  setStatus(latest < 0 ? "Live view; no browser events yet." : `Live view at latest durable cursor ${latest}.`);
}

function showAt(seq) {
  viewedSeq = seq;
  edges = mazeEdgesThrough(seq);
  renderMaze();
  renderSearchProjection();
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
  if (viewedSeq === null) renderSearchProjection();
}

function mazeEdgesThrough(seq) {
  return eventLog
    .filter((entry) => entry.seq <= seq && entry.event.kind === "maze-edge")
    .map((entry) => entry.event);
}

function latestSeq() {
  return eventLog.at(-1)?.seq ?? -1;
}

function inferDemoKind() {
  if (eventLog.some((entry) => entry.event.kind === "search-started")) return "search";
  if (eventLog.some((entry) => entry.event.kind === "maze-edge")) return "maze";
  return undefined;
}

function renderSearchProjection() {
  const state = searchStateThrough(viewedSeq ?? Infinity);
  searchBarEl.style.width = state.total ? `${Math.round((state.scanned / state.total) * 100)}%` : "0%";
  searchStatusEl.textContent = state.started
    ? `${state.backend} search "${state.query}": ${state.matched} matched${state.total ? `, ${state.scanned}/${state.total} scanned` : ""}${state.elapsedMs ? `, ${state.elapsedMs}ms` : ""}.`
    : "Start or select a search stream.";

  searchResultsEl.replaceChildren();
  for (const result of state.results) {
    const card = document.createElement("article");
    card.className = "result-card";
    const title = document.createElement("strong");
    appendHighlightedText(title, result.title, state.query);
    const snippet = document.createElement("p");
    appendHighlightedText(snippet, result.snippet, state.query);
    card.append(title, snippet);
    searchResultsEl.append(card);
  }
}

function appendHighlightedText(parent, text, query) {
  const terms = queryTerms(query);
  if (terms.length === 0) {
    parent.textContent = text;
    return;
  }

  // RegExp.escape keeps user-entered search terms literal instead of treating them as regex syntax.
  const pattern = new RegExp(`(${terms.map(RegExp.escape).join("|")})`, "gi");
  for (const part of text.split(pattern)) {
    if (part.length === 0) continue;
    if (terms.some((term) => part.toLocaleLowerCase() === term)) {
      const mark = document.createElement("mark");
      mark.textContent = part;
      parent.append(mark);
    } else {
      parent.append(document.createTextNode(part));
    }
  }
}

function searchStateThrough(seq) {
  const state = {
    started: false,
    query: "",
    backend: "",
    scanned: 0,
    total: 0,
    matched: 0,
    elapsedMs: 0,
    results: [],
  };

  for (const entry of eventLog) {
    if (entry.seq > seq) break;
    const event = entry.event;
    if (event.kind === "search-started") {
      state.started = true;
      state.query = event.query;
      state.backend = event.backend;
      state.total = event.total ?? 0;
      state.scanned = 0;
      state.matched = 0;
      state.elapsedMs = 0;
      state.results = [];
    }
    if (event.kind === "progress") {
      state.scanned = event.scanned;
      state.total = event.total;
      state.matched = event.matched;
    }
    if (event.kind === "result") {
      state.results.push(event);
      state.matched = Math.max(state.matched, state.results.length);
    }
    if (event.kind === "summary") {
      state.scanned = event.scanned ?? (state.total || event.matched);
      state.matched = event.matched;
      state.elapsedMs = event.elapsedMs;
      if (!state.total) state.total = state.scanned || state.matched;
    }
  }
  return state;
}

function appendEdge(edge, animate) {
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", `M ${edge.x1} ${edge.y1} L ${edge.x2} ${edge.y2}`);
  path.setAttribute("pathLength", "1");
  if (animate) path.classList.add("edge-live");
  mazeLines.append(path);
}

function setStatus(message) { statusEl.textContent = message; }

function selectedSearchBackend() {
  return document.querySelector("input[name='backend']:checked").value;
}

function queryTerms(query) {
  return [...new Set(query.toLocaleLowerCase().split(/\s+/).filter(Boolean))]
    .sort((left, right) => right.length - left.length);
}

function savedStreamId() { return localStorage.getItem(activeStreamKey) ?? ""; }

function rememberStream(streamId) {
  localStorage.setItem(activeStreamKey, streamId);
  history.replaceState({}, "", `/?stream=${encodeURIComponent(streamId)}`);
}

function selectedStreamId(kind = activeDemo) {
  return streamSelectEls[kind].value || "";
}

function selectStream(streamId) {
  if (!streamId) return;
  const stream = streamOptionsById.get(streamId);
  const kind = stream?.demoType ?? inferDemoKind();
  if (!kind || !streamSelectEls[kind]) return;
  if ([...streamSelectEls[kind].options].some((option) => option.value === streamId)) {
    streamSelectEls[kind].value = streamId;
  }
}

async function refreshStreamOptions(preferredStreamId = selectedStreamId(activeDemo)) {
  if (streamOptionsRefresh) return streamOptionsRefresh;
  streamOptionsRefresh = loadStreamOptions(preferredStreamId);
  try {
    await streamOptionsRefresh;
  } finally {
    streamOptionsRefresh = undefined;
  }
}

async function loadStreamOptions(preferredStreamId) {
  try {
    const streamsByKind = Object.fromEntries(await Promise.all(
      ["maze", "search"].map(async (kind) => {
        const response = await fetch(`/streams?demoType=${kind}`);
        if (!response.ok) throw new Error(await response.text());
        return [kind, await response.json()];
      }),
    ));
    const streams = Object.values(streamsByKind).flat();
    streamOptionsById = new Map(streams.map((stream) => [stream.id, stream]));

    for (const kind of ["maze", "search"]) {
      const select = streamSelectEls[kind];
      const filtered = streamsByKind[kind];
      select.replaceChildren();
      if (filtered.length === 0) {
        select.append(new Option(`No ${kind} streams`, ""));
        continue;
      }
      for (const stream of filtered) {
        select.append(new Option(formatStreamOption(stream), stream.id));
      }
    }
    selectStream(preferredStreamId);
    updateConnectionButtons();
  } catch (error) {
    streamOptionsById = new Map();
    for (const select of Object.values(streamSelectEls)) select.replaceChildren(new Option("Failed to load streams", ""));
    updateConnectionButtons();
    setStatus(`Failed to load server streams: ${error.message}`);
  }
}

function setStreamStatus(streamId, status) {
  const stream = streamOptionsById.get(streamId);
  if (!stream) return;
  stream.status = status;
  for (const select of Object.values(streamSelectEls)) {
    for (const option of select.options) {
      if (option.value === streamId) option.textContent = formatStreamOption(stream);
    }
  }
}

function formatStreamOption(stream) {
  return `${stream.label ?? stream.id} · ${stream.status} · next ${stream.nextSeq}`;
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

async function deleteServerBuffer(streamId) {
  const response = await fetch(`/streams/${encodeURIComponent(streamId)}`, { method: "DELETE" });
  if (!response.ok) throw new Error(await response.text());
}
