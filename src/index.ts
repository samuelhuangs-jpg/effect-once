import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import * as path from "node:path";

/**
 * effect-once — run a side effect at most once per key, even when your
 * scheduler fires the same job twice.
 *
 * Background: at-least-once schedulers (cron supervisors, queue redelivery,
 * lambda retries) can invoke the same logical job more than once. If that job
 * has a side effect — sending a notification, posting a message, writing a
 * report — the side effect is duplicated. The naive fix ("touch a marker file,
 * skip if it exists") has a well-known bug: if you write the marker *before*
 * the effect succeeds, a failed effect leaves a marker behind and the message
 * is silently swallowed forever.
 *
 * effect-once is the correct, crash-safe version of that pattern:
 *   - the effect runs exactly once on success across duplicate/concurrent runs,
 *   - a *failed* effect is retryable (never swallowed),
 *   - a crashed run is reclaimed after a lease window.
 */

export type MarkerStatus = "pending" | "done" | "failed";

export interface Marker {
  key: string;
  status: MarkerStatus;
  attempt: number;
  startedAt: string;
  updatedAt: string;
  error?: string;
}

export interface OnceResult<T> {
  /** Whether `fn` actually executed during this call. */
  ran: boolean;
  /** The value returned by `fn`, present only when `ran` is true. */
  value?: T;
  /** The marker status after this call settled. */
  status: MarkerStatus;
  /** Why the effect was skipped, when it was. */
  reason?: "already-done" | "held-by-other";
}

export interface OnceStoreOptions {
  /** Directory where marker files are stored. Created on demand. */
  dir: string;
  /**
   * How long a `pending`/locked run is trusted before another runner may
   * reclaim it (handles crashed runs). Default 15 minutes.
   */
  leaseMs?: number;
  /** Clock injection point, for tests. Returns epoch milliseconds. */
  now?: () => number;
}

export interface OnceOptions {
  /** Override the store's default lease for this key. */
  leaseMs?: number;
}

export interface WrapOptions<Args extends unknown[]> extends OnceOptions {
  /** Derive the per-call key suffix from the wrapped function arguments. */
  key?: (...args: Args) => string;
}

const DEFAULT_LEASE_MS = 15 * 60 * 1000;

export class OnceStore {
  private readonly dir: string;
  private readonly leaseMs: number;
  private readonly now: () => number;
  private tmpCounter = 0;

  constructor(opts: OnceStoreOptions) {
    if (!opts?.dir) throw new TypeError("OnceStore requires a `dir`");
    this.dir = opts.dir;
    this.leaseMs = opts.leaseMs ?? DEFAULT_LEASE_MS;
    this.now = opts.now ?? Date.now;
  }

  /**
   * Run `fn` at most once for `key`. Returns whether it ran this time.
   *
   * - If a `done` marker already exists, `fn` is skipped.
   * - If another runner holds a fresh lock, `fn` is skipped (no double-run).
   * - On success, a `done` marker is written *after* `fn` resolves.
   * - On failure, the marker is left `failed` so the next call retries.
   */
  async once<T>(
    key: string,
    fn: () => Promise<T> | T,
    opts: OnceOptions = {},
  ): Promise<OnceResult<T>> {
    const leaseMs = opts.leaseMs ?? this.leaseMs;
    const markerPath = this.markerPath(key);
    const lockPath = `${markerPath}.lock`;

    // Fast path: already done, no lock needed.
    const existing = await this.readMarker(markerPath);
    if (existing?.status === "done") {
      return { ran: false, status: "done", reason: "already-done" };
    }

    const acquired = await this.acquireLock(lockPath, leaseMs);
    if (!acquired) {
      const cur = await this.readMarker(markerPath);
      return { ran: false, status: cur?.status ?? "pending", reason: "held-by-other" };
    }

    try {
      // Re-check under the lock: another runner may have finished in the gap.
      const underLock = await this.readMarker(markerPath);
      if (underLock?.status === "done") {
        return { ran: false, status: "done", reason: "already-done" };
      }

      const attempt = (underLock?.attempt ?? 0) + 1;
      const startedAt = new Date(this.now()).toISOString();
      await this.writeMarker(markerPath, {
        key,
        status: "pending",
        attempt,
        startedAt,
        updatedAt: startedAt,
      });

      try {
        const value = await fn();
        await this.writeMarker(markerPath, {
          key,
          status: "done",
          attempt,
          startedAt,
          updatedAt: new Date(this.now()).toISOString(),
        });
        return { ran: true, value, status: "done" };
      } catch (err) {
        await this.writeMarker(markerPath, {
          key,
          status: "failed",
          attempt,
          startedAt,
          updatedAt: new Date(this.now()).toISOString(),
          error: err instanceof Error ? err.message : String(err),
        });
        throw err;
      }
    } finally {
      await this.releaseLock(lockPath);
    }
  }

  /**
   * Return a callable that routes every invocation through `once`.
   *
   * By default the key is `${prefix}:${stableJson(args)}`; provide `key` when
   * the logical unit of work is more precise than the full argument list.
   */
  wrap<Args extends unknown[], T>(
    prefix: string,
    fn: (...args: Args) => Promise<T> | T,
    opts: WrapOptions<Args> = {},
  ): (...args: Args) => Promise<OnceResult<T>> {
    const { key: keyFn, ...onceOpts } = opts;
    return async (...args: Args) => {
      const suffix = keyFn ? keyFn(...args) : stableJson(args);
      return this.once(`${prefix}:${suffix}`, () => fn(...args), onceOpts);
    };
  }

  /** Current status of a key, or `"absent"` if never seen. */
  async status(key: string): Promise<MarkerStatus | "absent"> {
    const marker = await this.readMarker(this.markerPath(key));
    return marker?.status ?? "absent";
  }

  /** Forget a key entirely (marker + any stale lock). */
  async reset(key: string): Promise<void> {
    const markerPath = this.markerPath(key);
    await this.unlinkQuiet(markerPath);
    await this.unlinkQuiet(`${markerPath}.lock`);
  }

  /** Delete `done` markers older than `maxAgeMs`, to bound directory growth. */
  async sweep(maxAgeMs: number): Promise<number> {
    let removed = 0;
    let entries: string[];
    try {
      entries = await fs.readdir(this.dir);
    } catch {
      return 0;
    }
    const cutoff = this.now() - maxAgeMs;
    for (const name of entries) {
      if (!name.endsWith(".json")) continue;
      const full = path.join(this.dir, name);
      const marker = await this.readMarker(full);
      if (marker?.status === "done" && Date.parse(marker.updatedAt) < cutoff) {
        await this.unlinkQuiet(full);
        removed += 1;
      }
    }
    return removed;
  }

  private markerPath(key: string): string {
    const hash = createHash("sha256").update(key).digest("hex").slice(0, 16);
    const slug = key.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 60);
    return path.join(this.dir, `${slug}.${hash}.json`);
  }

  private async acquireLock(lockPath: string, leaseMs: number): Promise<boolean> {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        await fs.mkdir(path.dirname(lockPath), { recursive: true });
        const handle = await fs.open(lockPath, "wx");
        await handle.writeFile(
          JSON.stringify({ pid: process.pid, acquiredAt: this.now() }),
        );
        await handle.close();
        return true;
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
        // Lock exists — reclaim only if it is older than the lease.
        const age = await this.lockAge(lockPath);
        if (age === null || age < leaseMs) return false;
        await this.unlinkQuiet(lockPath);
        // loop once more to try to claim the freed lock
      }
    }
    return false;
  }

  private async lockAge(lockPath: string): Promise<number | null> {
    try {
      const raw = await fs.readFile(lockPath, "utf8");
      const parsed = JSON.parse(raw) as { acquiredAt?: number };
      if (typeof parsed.acquiredAt !== "number") {
        const stat = await fs.stat(lockPath);
        return this.now() - stat.mtimeMs;
      }
      return this.now() - parsed.acquiredAt;
    } catch {
      return null;
    }
  }

  private async releaseLock(lockPath: string): Promise<void> {
    await this.unlinkQuiet(lockPath);
  }

  private async readMarker(markerPath: string): Promise<Marker | null> {
    try {
      const raw = await fs.readFile(markerPath, "utf8");
      return JSON.parse(raw) as Marker;
    } catch {
      return null;
    }
  }

  private async writeMarker(markerPath: string, marker: Marker): Promise<void> {
    await fs.mkdir(path.dirname(markerPath), { recursive: true });
    const tmp = `${markerPath}.tmp.${process.pid}.${this.tmpCounter++}`;
    await fs.writeFile(tmp, JSON.stringify(marker, null, 2));
    await fs.rename(tmp, markerPath); // atomic on the same filesystem
  }

  private async unlinkQuiet(target: string): Promise<void> {
    try {
      await fs.unlink(target);
    } catch {
      /* already gone */
    }
  }
}

/** Convenience factory. */
export function createOnceStore(opts: OnceStoreOptions): OnceStore {
  return new OnceStore(opts);
}

function stableJson(value: unknown): string {
  return JSON.stringify(sortJsonValue(value, new WeakSet()));
}

function sortJsonValue(value: unknown, seen: WeakSet<object>): unknown {
  if (Array.isArray(value)) return value.map((item) => sortJsonValue(item, seen));
  if (!value || typeof value !== "object") return value;

  const toJSON = (value as { toJSON?: unknown }).toJSON;
  if (typeof toJSON === "function") {
    return sortJsonValue(toJSON.call(value), seen);
  }

  if (seen.has(value)) {
    throw new TypeError(
      "Cannot derive an effect-once key from circular arguments; provide a custom key",
    );
  }

  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    throw new TypeError(
      "Cannot derive an effect-once key from non-plain arguments; provide a custom key",
    );
  }

  seen.add(value);
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = sortJsonValue((value as Record<string, unknown>)[key], seen);
  }
  seen.delete(value);
  return sorted;
}
