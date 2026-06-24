import type { KeepAliveLease, KeepAliveWhile } from "./types.ts";

/**
 * Wraps a host heartbeat/alarm in the keepAliveWhile contract. The release
 * function runs when the task settles, whether it completes or throws.
 */
export function createKeepAliveWhile({ begin }: KeepAliveLease): KeepAliveWhile {
  return (task) => {
    const release = begin();
    let result: Promise<void>;
    try {
      result = task();
    } catch {
      release();
      return;
    }
    void result
      .finally(release)
      // The task owner records its own failure; avoid an unhandled rejection
      // from this lifecycle wrapper after the lease has been released.
      .catch(() => undefined);
  };
}
