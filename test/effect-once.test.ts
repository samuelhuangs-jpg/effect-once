import { createHash } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createOnceStore } from "../src/index.js";

const keyHash = (key: string) =>
  createHash("sha256").update(key).digest("hex").slice(0, 16);

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(path.join(tmpdir(), "effect-once-"));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("OnceStore", () => {
  it("runs the effect once and returns its value", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const res = await store.once("k", async () => {
      calls += 1;
      return 42;
    });
    expect(res.ran).toBe(true);
    expect(res.value).toBe(42);
    expect(res.status).toBe("done");
    expect(calls).toBe(1);
  });

  it("skips a second call for the same key (the duplicate-fire case)", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    await store.once("dup", async () => void calls++);
    const second = await store.once("dup", async () => void calls++);
    expect(calls).toBe(1);
    expect(second.ran).toBe(false);
    expect(second.reason).toBe("already-done");
  });

  it("runs exactly once under concurrent calls", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const fn = async () => {
      calls += 1;
      await new Promise((r) => setTimeout(r, 5));
    };
    const results = await Promise.all(
      Array.from({ length: 8 }, () => store.once("race", fn)),
    );
    expect(calls).toBe(1);
    expect(results.filter((r) => r.ran)).toHaveLength(1);
  });

  it("does NOT swallow the effect when it fails — the next call retries", async () => {
    // This is the regression guard for the classic 'write marker before send'
    // bug: a failed effect must remain retryable, never permanently skipped.
    const store = createOnceStore({ dir });
    let calls = 0;
    await expect(
      store.once("flaky", async () => {
        calls += 1;
        throw new Error("send failed");
      }),
    ).rejects.toThrow("send failed");
    expect(await store.status("flaky")).toBe("failed");

    const retry = await store.once("flaky", async () => {
      calls += 1;
      return "ok";
    });
    expect(calls).toBe(2);
    expect(retry.ran).toBe(true);
    expect(retry.status).toBe("done");
  });

  it("reclaims a stale lock from a crashed run", async () => {
    const store = createOnceStore({ dir, leaseMs: 1000, now: () => 100_000 });
    // Simulate a crashed run that left a lock behind, older than the lease.
    const lockPath = path.join(
      dir,
      // mirrors OnceStore.markerPath naming so we hit the same key
      `crashed.${keyHash("crashed")}.json.lock`,
    );
    await writeFile(lockPath, JSON.stringify({ pid: 999, acquiredAt: 0 }));
    let calls = 0;
    const res = await store.once("crashed", async () => void calls++);
    expect(calls).toBe(1);
    expect(res.ran).toBe(true);
  });

  it("sweeps old done markers", async () => {
    let clock = 1_000_000;
    const store = createOnceStore({ dir, now: () => clock });
    await store.once("old", async () => 1);
    clock += 10_000;
    const removed = await store.sweep(5_000);
    expect(removed).toBe(1);
    expect(await store.status("old")).toBe("absent");
  });

  it("wraps a function and deduplicates by derived argument key", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const sendOnce = store.wrap("daily-digest", async (day: string) => {
      calls += 1;
      return `sent:${day}`;
    });

    const first = await sendOnce("2026-05-31");
    const duplicate = await sendOnce("2026-05-31");
    const nextDay = await sendOnce("2026-06-01");

    expect(first).toMatchObject({ ran: true, value: "sent:2026-05-31" });
    expect(duplicate).toMatchObject({ ran: false, reason: "already-done" });
    expect(nextDay.ran).toBe(true);
    expect(calls).toBe(2);
  });

  it("wraps a function with a custom key function", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const sendOnce = store.wrap(
      "notify",
      async (message: { id: string; text: string }) => {
        calls += 1;
        return message.text;
      },
      { key: (message) => message.id },
    );

    await sendOnce({ id: "m1", text: "hello" });
    const duplicate = await sendOnce({ id: "m1", text: "hello again" });

    expect(duplicate.ran).toBe(false);
    expect(calls).toBe(1);
  });

  it("derives stable default keys for reordered plain object arguments", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const sendOnce = store.wrap("payload", async (_message: { id: string; text: string }) => {
      calls += 1;
      return calls;
    });

    const first = await sendOnce({ id: "m1", text: "hello" });
    const duplicate = await sendOnce({ text: "hello", id: "m1" });

    expect(first.ran).toBe(true);
    expect(duplicate).toMatchObject({ ran: false, reason: "already-done" });
    expect(calls).toBe(1);
  });

  it("uses toJSON values when deriving default keys", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const sendOnce = store.wrap("dated", async (_day: Date) => {
      calls += 1;
      return calls;
    });

    await sendOnce(new Date("2026-05-31T00:00:00.000Z"));
    const next = await sendOnce(new Date("2026-06-01T00:00:00.000Z"));

    expect(next.ran).toBe(true);
    expect(calls).toBe(2);
  });

  it("requires a custom key for non-plain default-key arguments", async () => {
    const store = createOnceStore({ dir });
    const sendOnce = store.wrap("map", async (_value: Map<string, string>) => "sent");

    await expect(sendOnce(new Map([["id", "m1"]]))).rejects.toThrow(
      "provide a custom key",
    );
  });

  it("keeps wrapped failures retryable", async () => {
    const store = createOnceStore({ dir });
    let calls = 0;
    const flakyOnce = store.wrap("flaky", async (id: string) => {
      calls += 1;
      if (calls === 1) throw new Error("boom");
      return id;
    });

    await expect(flakyOnce("a")).rejects.toThrow("boom");
    const retry = await flakyOnce("a");

    expect(retry).toMatchObject({ ran: true, value: "a", status: "done" });
    expect(calls).toBe(2);
  });
});
