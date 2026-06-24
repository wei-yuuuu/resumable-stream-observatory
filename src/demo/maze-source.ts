import type { StreamSource } from "../types.ts";

const columns = 24;
const rows = 14;
const encoder = new TextEncoder();

type MazeOptions = {
  count?: number;
  intervalMs?: number;
};

type MazeEdge = {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
};

export function createMazeSource(options: MazeOptions = {}): StreamSource {
  const count = clampNumber(options.count, columns * rows - 1, 1, columns * rows - 1);
  const intervalMs = clampNumber(options.intervalMs, 80, 20, 2_000);

  return () => {
    const edges = createMazeEdges(columns, rows).slice(0, count);
    let timer: ReturnType<typeof setInterval> | undefined;
    let step = 0;

    return new ReadableStream<Uint8Array>({
      start(controller) {
        timer = setInterval(() => {
          const event = { kind: "maze-edge", step, ...edges[step] };
          controller.enqueue(encoder.encode(JSON.stringify(event)));
          step += 1;
          if (step >= edges.length) {
            if (timer) clearInterval(timer);
            controller.close();
          }
        }, intervalMs);
      },
      cancel() {
        if (timer) clearInterval(timer);
      },
    });
  };
}

function createMazeEdges(width: number, height: number): MazeEdge[] {
  const visited = new Set<string>(["0,0"]);
  const stack = [{ x: 0, y: 0 }];
  const edges: MazeEdge[] = [];
  const key = (x: number, y: number) => `${x},${y}`;

  while (stack.length > 0) {
    const current = stack[stack.length - 1];
    const candidates = shuffle([
      { x: current.x + 1, y: current.y },
      { x: current.x - 1, y: current.y },
      { x: current.x, y: current.y + 1 },
      { x: current.x, y: current.y - 1 },
    ]).filter((next) => (
      next.x >= 0
      && next.x < width
      && next.y >= 0
      && next.y < height
      && !visited.has(key(next.x, next.y))
    ));

    const next = candidates[0];
    if (!next) {
      stack.pop();
      continue;
    }
    visited.add(key(next.x, next.y));
    edges.push({ x1: current.x + 0.5, y1: current.y + 0.5, x2: next.x + 0.5, y2: next.y + 0.5 });
    stack.push(next);
  }
  return edges;
}

function shuffle<T>(items: T[]): T[] {
  for (let index = items.length - 1; index > 0; index -= 1) {
    const swap = Math.floor(Math.random() * (index + 1));
    [items[index], items[swap]] = [items[swap], items[index]];
  }
  return items;
}

function clampNumber(value: number | undefined, fallback: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(value!)));
}
