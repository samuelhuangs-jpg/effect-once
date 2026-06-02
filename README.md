# effect-once

[![npm version](https://img.shields.io/npm/v/effect-once.svg)](https://www.npmjs.com/package/effect-once)
[![CI](https://github.com/samuelhuangs-jpg/effect-once/actions/workflows/ci.yml/badge.svg)](https://github.com/samuelhuangs-jpg/effect-once/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

Run a side effect **at most once per key** — even when your scheduler fires the
same job twice. A tiny (zero-dependency) crash-safe idempotency guard for
**at-least-once** environments: cron supervisors that double-trigger, queue
redelivery, lambda/step retries, agent runtimes that re-run a turn.

```ts
import { createOnceStore } from "effect-once";

const once = createOnceStore({ dir: "~/.cache/myapp/once" });

await once.once(`daily-digest:${today}`, async () => {
  await sendTelegram("Here is your daily digest…");
});
// Fires once. A duplicate run on the same key is a no-op.
```

## The problem

A surprising number of schedulers are **at-least-once**, not exactly-once. If
the same logical job runs twice and it has a side effect — sends a
notification, posts to a channel, writes a report, charges a card — that side
effect is **duplicated**. Users see two messages; downstream state drifts.

This is not hypothetical. It is the failure mode behind, e.g.,
[openclaw/openclaw#84976](https://github.com/openclaw/openclaw/issues/84976)
(*“Scheduled … cron runs get re-executed … duplicating side effects”*,
labelled `impact:message-loss` — delivery can be *duplicated*). While the
orchestration-layer fix is pending upstream, the durable mitigation lives in
**userland**: make the side effect itself idempotent.

## Why not just `touch` a marker file?

The naive version looks fine and is subtly broken:

```ts
// ❌ DON'T — swallows the message on failure
if (await exists(marker)) return;
await writeFile(marker, "");   // marker written BEFORE the effect
await sendTelegram(msg);       // if this throws, the marker is already there
                               // → the message is never sent, and never retried
```

The bug: writing the marker *before* the effect succeeds means a **failed**
effect is permanently skipped on every future run. The message is swallowed.

`effect-once` gets the ordering and the states right:

| Situation                          | Behaviour                                  |
| ---------------------------------- | ------------------------------------------ |
| First run                          | effect runs; `done` written **after** it resolves |
| Duplicate / concurrent run         | skipped — effect runs **exactly once**     |
| Effect throws                      | marked `failed`, **error re-thrown, retryable** — never swallowed |
| Process crashes mid-run            | lock is **reclaimed after the lease**, run retries |

## Install

```sh
npm install effect-once
```

Requires Node ≥ 18. Ships ESM + CJS + types.

## API

### `createOnceStore({ dir, leaseMs?, now? })`

- `dir` — directory for marker files (created on demand).
- `leaseMs` — how long a `pending`/locked run is trusted before another runner
  may reclaim it (crash recovery). Default 15 min.
- `now` — clock injection for tests.

### `store.once(key, fn, { leaseMs? }): Promise<OnceResult>`

Runs `fn` at most once for `key`. Returns `{ ran, value?, status, reason? }`.
Re-throws if `fn` throws (and leaves the key retryable).

### `store.status(key): Promise<"pending" | "done" | "failed" | "absent">`

### `store.wrap(prefix, fn, { key?, leaseMs? })`

Returns a callable that runs `fn` through `store.once` each time it is invoked.
By default, the per-call key is derived from `prefix` plus a stable JSON string
of the arguments; pass `key` to choose the logical unit of work explicitly.

```ts
const sendOnce = once.wrap(
  "daily-digest",
  async (day: string) => sendDigest(day),
  { key: (day) => day },
);

await sendOnce(today); // uses key `daily-digest:${today}`
```

### `store.reset(key): Promise<void>`

Forget a key (marker + any stale lock).

### `store.sweep(maxAgeMs): Promise<number>`

Delete `done` markers older than `maxAgeMs` to bound directory growth. Returns
the count removed.

## Recipe: idempotent OpenClaw cron job

For a `mode:none` cron task that sends its own notification, wrap the send so a
double-trigger can't double-send:

```ts
import { createOnceStore } from "effect-once";

const once = createOnceStore({ dir: `${process.env.HOME}/.cache/cron-once` });

export async function run() {
  const day = new Date().toISOString().slice(0, 10);
  const res = await once.once(`announce-scan:${day}`, async () => {
    const report = await buildReport();
    await sendTelegram(report);
  });
  if (!res.ran) console.log(`already sent (${res.reason}), skipping`);
}
```

Key choice matters: derive the key from the **logical unit of work** (task +
date, or `chat.id + message_id` for inbound dedup), not from the run id.

## License

MIT
