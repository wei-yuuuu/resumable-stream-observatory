export type StreamStatus = "streaming" | "completed" | "failed";

export type StreamInfo = {
  id: string;
  status: StreamStatus;
  nextSeq: number;
  createdAt: number;
  updatedAt: number;
  error: string | null;
};

export type StreamSourceContext = {
  streamId: string;
};

// A factory is intentional: the hub starts it in its own background task,
// after the stream row and producer lease have been persisted.
export type StreamSource = (
  context: StreamSourceContext,
) => ReadableStream<Uint8Array> | Promise<ReadableStream<Uint8Array>>;

export type CreateStreamOptions = {
  source: StreamSource;
  streamId?: string;
};

export type StreamHubOptions = {
  databasePath: string;
};

export type TailEvent =
  | { kind: "chunk"; seq: number; data: Uint8Array }
  | { kind: "end"; status: Exclude<StreamStatus, "streaming">; error: string | null }
  | { kind: "error"; message: string };
