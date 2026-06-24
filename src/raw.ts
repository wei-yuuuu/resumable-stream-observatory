import type { TailEvent } from "./types.ts";

/**
 * Replays the exact bytes saved from the provider without adding an envelope.
 * Feed this to the provider's native parser instead of writing a second parser
 * for every provider-specific SSE format.
 */
export function toRawByteStream(events: ReadableStream<TailEvent>): ReadableStream<Uint8Array> {
  return events.pipeThrough(new TransformStream<TailEvent, Uint8Array>({
    transform(event, controller) {
      if (event.kind === "chunk") {
        controller.enqueue(event.data);
        return;
      }
      if (event.kind === "error") controller.error(new Error(event.message));
    },
  }));
}
