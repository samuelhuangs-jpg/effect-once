# Failure semantics

This document specifies exactly what `effect-once` guarantees, how it behaves
under every failure it is designed for, and — just as important — what it does
**not** guarantee. If you are deciding whether to trust it with a side effect,
read this page.

## The one-line guarantee

> For a given `key` and store directory, a side effect runs **at most once on
> success**. A side effect that *fails* stays retryable; it is never silently
> swallowed.

"At most once on success" is the useful guarantee in an **at-least-once** world.
Your scheduler promises only that the job runs *one or more* times; `effect-once`
collapses those duplicate runs down to a single successful effect.

## Why not just `touch` a marker file?

The pattern everyone reaches for first is a marker file: "if the marker exists,
skip; otherwise do the work and write the marker." It has a subtle, data-losing
bug that depends entirely on *ordering*.

```ts
// ❌ Bug A — marker written BEFORE the effect: swallows on failure
if (await exists(marker)) return;
await writeFile(marker, "");   // marker now exists...
await send(msg);               // ...but if this throws, the marker is a tombstone
                               // → every future run sees the marker and skips.
                               //   The message is never sent and never retried.
```

Flip the order and you trade one bug for another:

```ts
// ❌ Bug B — marker written AFTER the effect: double-sends on a crash or race
if (await exists(marker)) return;
await send(msg);               // succeeds...
await writeFile(marker, "");   // ...but a crash here (or a concurrent run that
                               //   already passed the exists() check) sends twice.
```

There is no ordering of a single `exists` + `write` that is both crash-safe and
duplicate-safe, because the check and the write are not atomic and the effect
sits between them. `effect-once` fixes this with **three states and a lock**,
not one boolean file.

## The state machine

Every key moves through these states. The state lives in a JSON **marker file**;
mutual exclusion lives in a separate **lock file**.

```
                        acquire lock
   absent ──────────────────────────────────▶ pending
     ▲                                          │
     │ reset()                          fn()    │
     │                            ┌─────────────┴─────────────┐
     │                    resolves│                           │throws
     │                           ▼                            ▼
     │ sweep()                 done ◀── (terminal,        failed ──┐
     └──────────  (after maxAge) │       skip forever)        │    │ next call
                                 │                            └────┘ retries
                                 ▼
                               absent
```

| Marker status | Meaning                                            | Next `once()` call does…                  |
| ------------- | -------------------------------------------------- | ----------------------------------------- |
| *absent*      | key never seen                                     | acquires lock, runs `fn`                  |
| `pending`     | a run is in progress (lock held, lease live)       | skips with `reason: "held-by-other"`      |
| `done`        | effect succeeded — **terminal**                    | skips with `reason: "already-done"`       |
| `failed`      | effect threw — error was re-thrown to the caller   | acquires lock, **retries** `fn`           |

`once()` returns `{ ran, value?, status, reason? }` so the caller can tell what
happened without re-reading the marker:

- `ran: true` → your effect executed this call; `value` holds its return.
- `ran: false, reason: "already-done"` → a previous run already succeeded.
- `ran: false, reason: "held-by-other"` → another runner holds a live lock.

## What happens in each failure

| Situation                              | Behaviour                                                                 |
| -------------------------------------- | ------------------------------------------------------------------------- |
| First run                              | `fn` runs; `done` is written **after** it resolves.                       |
| Duplicate fire (sequential)            | second call sees `done`, skips. Effect ran **exactly once**.              |
| Concurrent fires (same host)           | one call wins the lock and runs; the rest skip `held-by-other`.           |
| `fn` throws                            | marker set to `failed`, **the error is re-thrown**, key stays retryable.  |
| Process crashes *during* `fn`          | lock goes stale; reclaimed after the **lease** → next run retries.        |
| Process crashes *after* `fn`, before `done` is written | the effect already happened, but the marker is still `pending`; the lease lets a later run re-execute. **This is the one at-most-once gap — see below.** |
| Marker file corrupted / unreadable     | treated as *absent* — the run proceeds (fail-open toward delivery).       |

### The honest gap: crash between effect and marker

There is exactly one window `effect-once` cannot close on its own: your effect
*succeeds*, then the process dies **before** the `done` marker is written. The
marker is still `pending`; once the lease expires, a later run will execute the
effect a second time.

This is fundamental — it is the [two-generals / dual-write
problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem), and no library
that stores its bookkeeping separately from the effect's own system can fully
eliminate it. `effect-once` deliberately chooses **at-most-once on success with
a narrow re-run window on crash** over the alternative (mark-before-send), which
turns the same window into *permanent message loss*. A duplicate is recoverable;
a swallowed message is not.

If you need to close even this window, make the effect's own target idempotent
(e.g. send with a provider-side idempotency key derived from the same `key`), or
use a backend that commits the marker and the effect in one transaction (see
[issue #1](https://github.com/samuelhuangs-jpg/effect-once/issues/1)).

## Crash recovery: the lease

A `pending` marker could mean "a run is healthy and in progress" or "a run
crashed and abandoned the lock." You cannot tell these apart instantly, so
`effect-once` uses a **lease**: a lock is trusted until it is `leaseMs`
milliseconds old (default **15 minutes**). After that, another runner may
reclaim it and retry.

Tune the lease to your effect's worst-case duration:

- **Too short** → a slow-but-healthy run gets its lock stolen, and the effect
  can run twice (the lock no longer protects it).
- **Too long** → after a real crash, the key is stuck `pending` and the retry is
  delayed by up to the full lease.

Rule of thumb: `leaseMs` ≈ a few × the p99 runtime of `fn`. Set it per store
(`createOnceStore({ leaseMs })`) or per call (`once(key, fn, { leaseMs })`).

## Concurrency & atomicity

`effect-once` relies on two filesystem primitives, both atomic on a local
POSIX filesystem:

1. **Lock acquisition** uses `open(path, "wx")` (`O_CREAT | O_EXCL`). Exactly one
   caller can create the lock file; everyone else gets `EEXIST` and backs off.
   This is what serialises concurrent runs on the same host.
2. **Marker writes** are written to a temp file and `rename()`d into place.
   `rename` is atomic, so a reader never observes a half-written marker — it sees
   either the old state or the new one.

## Scope & limits (read before you ship)

`effect-once` is a **single-host, local-filesystem** guard. Be explicit about
where that holds:

- ✅ **Same machine, multiple processes/timers** — fully covered. This is the
  common cron-double-fire / queue-redelivery case.
- ⚠️ **Multiple hosts sharing a directory over NFS/SMB** — `O_EXCL` and `rename`
  atomicity are **not reliable** across many network filesystems. Do not rely on
  the lock for cross-host mutual exclusion.
- ❌ **Distributed, no shared filesystem** — out of scope. Use a shared backend
  (Redis `SET NX` / a database unique constraint) instead. Pluggable backends
  are tracked in [issue #1](https://github.com/samuelhuangs-jpg/effect-once/issues/1).

Other limits worth stating plainly:

- The `done` marker is **terminal and permanent** until you `reset(key)` or it is
  removed by `sweep(maxAgeMs)`. Choose keys that are meant to fire once
  *forever* under that identity (see below), and call `sweep` periodically so the
  directory does not grow without bound.
- A corrupted marker is treated as *absent* (fail-open). The bias is toward
  delivering the effect rather than skipping it on bad bookkeeping.

## Choosing a key

The key **is** the idempotency identity. Derive it from the *logical unit of
work*, not from a per-run id (a fresh run id every fire defeats the whole point).

```ts
// ✅ logical identity — one digest per day, no matter how many times cron fires
await once.once(`daily-digest:${today}`, () => send(report));

// ❌ run id — unique every fire, so the guard never matches
await once.once(`run:${process.env.RUN_ID}`, () => send(report));
```

Good keys: `task + date` for scheduled jobs, `chat.id + message_id` for inbound
message dedup, `order id` for a one-time charge.

## `wrap`: keys derived from arguments

`store.wrap(prefix, fn, opts?)` returns a callable that routes every invocation
through `once`. By default the key is `` `${prefix}:${stableJson(args)}` `` —
arguments are serialised with **sorted object keys**, so `{a:1,b:2}` and
`{b:2,a:1}` map to the same key.

```ts
const sendOnce = store.wrap("welcome-email", (user) => sendEmail(user.email));
await sendOnce({ id: 1, email: "a@x.com" }); // runs
await sendOnce({ id: 1, email: "a@x.com" }); // skipped — same key
```

Default key derivation is intentionally strict and will **throw** rather than
guess when it cannot produce a stable key:

- **Circular arguments** → throws; the key would be ill-defined.
- **Non-plain arguments** (class instances, etc.) → throws, because property
  order and hidden state make serialisation unreliable. Objects exposing
  `toJSON()` are serialised via that hook.

When the logical identity is narrower than the full argument list (or arguments
are not cleanly serialisable), pass an explicit `key`:

```ts
const charge = store.wrap("charge", (order) => chargeCard(order), {
  key: (order) => order.id, // identity is the order id, not the whole object
});
```

## See also

- [`README.md`](../README.md) — quick start and API summary.
- [issue #1](https://github.com/samuelhuangs-jpg/effect-once/issues/1) —
  pluggable Redis/SQLite backends for cross-host idempotency.
- [openclaw/openclaw#84976](https://github.com/openclaw/openclaw/issues/84976) —
  the real-world at-least-once cron bug this library mitigates in userland.
