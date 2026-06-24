import type { TailEvent } from "./types.ts";

const encoder = new TextEncoder();

// Optional transport adapter. StreamHub itself exposes typed events so callers
// may choose SSE, WebSocket, a CLI renderer, or any other consumer transport.
export function toSse(events: ReadableStream<TailEvent>): ReadableStream<Uint8Array> {
  return events.pipeThrough(new TransformStream<TailEvent, Uint8Array>({
    transform(event, controller) {
      if (event.kind === "chunk") {
        const data = Buffer.from(event.data).toString("base64");
        controller.enqueue(encoder.encode(`id: ${event.seq}\nevent: chunk\ndata: ${JSON.stringify({ seq: event.seq, data })}\n\n`));
        return;
      }
      if (event.kind === "end") {
        controller.enqueue(encoder.encode(`event: end\ndata: ${JSON.stringify({ status: event.status, error: event.error })}\n\n`));
        return;
      }
      controller.enqueue(encoder.encode(`event: error\ndata: ${JSON.stringify({ message: event.message })}\n\n`));
    },
  }));
}
