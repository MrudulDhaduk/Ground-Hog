# Rung 3 — how naive replication breaks

> **Read this one critically.** The code was written by Claude at your direction (spec
> §5's "let Claude draft it and point your fault injector at it"), and so was this
> writeup. Everything below is a *reproducible claim* — every seed here replays. Run
> them, disagree where you disagree, and rewrite this file in your own words, because
> this is the file an interviewer probes and borrowed sentences do not survive contact
> with a follow-up question.

---

## The setup

- Replicator: [`groundhog/naive/replicator.py`](../groundhog/naive/replicator.py)
- One primary (node 1), two backups. Fire-and-forget replication. The client is told
  "saved" before anything has left the machine.
- Reproduce anything below with:
  `groundhog naive --seed N --faults PROFILE --trace out.jsonl`

The four decisions the implementation made:

| | Choice | Consequence |
|---|---|---|
| 1. Ack before or after `sync()`? | **Before** — sync is batched every 4 writes | there is always a window where the client has been told yes and nothing is durable |
| 2. Log first or apply first? | **Log first** | costs nothing here; there is no way to reject a command |
| 3. Do backups persist? | **Yes**, same batched path | crashes lose a tail, not everything |
| 4. Does a restarted primary re-send? | **No** | nothing in the design ever discovers a backup is behind |

None of those is a bug. Each is defensible on its own. The system built from them loses
acknowledged data anyway — which is the point.

## The baseline

```
groundhog naive --scan 0:500 --faults perfect      →   0 / 500 broke
groundhog naive --scan 0:500 --faults quiet        →  29 / 500 broke
groundhog naive --scan 0:500 --faults aggressive   → 488 / 500 broke
```

`perfect` and `quiet` differ in exactly one way: `perfect` has a **fixed** 5 ms latency,
`quiet` has a **variable** 1–10 ms latency. No drops, no partitions, no crashes, no disk
faults in either. That one difference is worth 29 broken universes in 500.

---

## Failure 1 — reordering, with nothing else wrong

**Seed:** 11 **Profile:** `quiet`

**What the client was promised:** `k2=v38` (acknowledged at tick 104382, the last write
to that key; `k2=v35` was acknowledged at tick 98850, before it)

**What the three copies actually held:**

```
node 1: {'k0': 'v39', 'k1': 'v37', 'k2': 'v38'}
node 2: {'k0': 'v39', 'k1': 'v37', 'k2': 'v38'}
node 3: {'k0': 'v39', 'k1': 'v37', 'k2': 'v35'}   ← two writes stale, forever
```

**What happened, in order:**

1. Primary applies `k2=v35`, sends `Replicate(k2=v35)` to both backups, acks the client.
2. ~5 ms later the primary applies `k2=v38`, sends `Replicate(k2=v38)`, acks the client.
3. Each message draws its own delivery latency from 1–10 ms. For node 3 on this seed,
   the second message drew a small delay and the first drew a large one.
4. Node 3 applies `v38`, then applies `v35` on top of it.
5. Nothing ever notices. There is no version, no index, no ordering — a `put` is a `put`.

**Which Raft rule fixes it:** the **AppendEntries consistency check (§5.3)**. Every entry
carries an index, and a follower refuses any batch whose `prevLogIndex`/`prevLogTerm`
does not match what it already has. A delayed message cannot be applied out of order
because the follower will reject it and the leader will resend from the right point. The
log supplies a total order that a bare key-value map does not have.

The deeper point: the bug is not that messages were reordered. Reordering is normal and
permanent. The bug is that this design had no way to *notice*.

---

## Failure 2 — a backup falls behind and nothing ever tells it

**Seed:** 1 **Profile:** `aggressive`

**What the client was promised:** `k0=v39`, `k1=v37`, `k2=v38` — all 40 writes acked.

**What the three copies actually held:**

```
node 1: {'k0': 'v39', 'k1': 'v37', 'k2': 'v38'}
node 2: {}                                        ← crashed, came back empty, never caught up
node 3: {'k0': 'v36', 'k2': 'v35'}                ← missing k1 entirely, others stale
```

**What happened, in order:**

1. 8% of messages are dropped. Node 3 missed several `Replicate` messages.
2. Node 2 crashed, lost its unsynced tail, restarted from its log — and restarted
   *behind*.
3. From that moment the primary sent both backups only *new* writes. Neither ever
   received what it had missed.
4. The gap is permanent. There is no mechanism in the design that could close it.

**Which Raft rule fixes it:** **`nextIndex` / `matchIndex` and the retry loop (§5.3).**
A leader keeps a per-follower guess of where that follower's log ends, walks it backwards
whenever an AppendEntries is refused, and keeps re-sending until the follower matches.
Fire-and-forget has no memory of what anyone is missing; Raft's leader has nothing *but*
that memory.

---

## Failure 3 — an acknowledged write that exists nowhere

**Seed:** 7 **Profile:** `aggressive` — *and the fault schedule is empty. No partitions,
no scheduled crashes. This is a disk fault and message loss alone.*

**What the client was promised:** `k0=v33`, acknowledged at tick 89917.

**What the three copies actually held:**

```
node 1: {}                                        ← the primary
node 2: {'k0': 'v30', 'k1': 'v31', 'k2': 'v29'}
node 3: {}
```

`v33` is not on any machine. The client was told it was saved.

**What happened, in order:**

```
tick 89917  client: k0=v33  →  primary applies it, sends to backups, ACKS THE CLIENT
                               (not synced yet — sync is batched every 4 writes)
tick 94604  primary's next disk write fails partway:
            "write failed after 15 of 17 bytes"
tick 94604  primary crashes, fail-stop — it cannot know how much of that write landed
            ... and nothing restarts it. Its unsynced tail, including v33, is gone.
tick 94604+ every subsequent write is refused; 6 of 40 never got an ack at all
```

Node 2 was three writes behind when the primary died (`k0=v30`); node 3 had lost its own
disk earlier. The most recent copy of `k0` anywhere is `v30`.

**Which Raft rule fixes it:** the **commit rule (§5.3, §5.4)** — a leader replies to the
client only once the entry is on a **majority** of servers, durably. Two consequences,
both fatal to this failure:

- The client would never have been told `v33` was saved, because at the moment of the
  ack exactly one machine had it and that machine had not synced.
- Even if the primary then died, a majority would hold the entry, and the election
  restriction (§5.4.1) guarantees the next leader is one of the machines that has it.

---

## The one-sentence version

> **Done when:** you can name a seed that silently loses an acknowledged write, replay
> it, and explain in one sentence why Raft would not have.

**Seed:** `groundhog naive --seed 7 --faults aggressive`

**Sentence:** The primary told the client `k0=v33` was saved at a moment when exactly one
machine had it and that machine had not yet synced, so when its disk failed four writes
later the value was gone from every copy — whereas Raft would not have acknowledged it
until a majority held it durably, and would therefore have had it somewhere to recover
from.

---

## What this bought

Every rule in Figure 2 that had looked like ceremony now has a failure attached to it:

| Raft rule | The thing it is preventing |
|---|---|
| log index + consistency check (§5.3) | failure 1 — applying writes in the wrong order |
| `nextIndex` / `matchIndex` + retry (§5.3) | failure 2 — a replica silently stuck in the past |
| commit on a majority before replying (§5.3) | failure 3 — acknowledging data that one disk can lose |
| election restriction (§5.4.1) | the follow-on to failure 3: the machine that has the data must be the one that wins |

M6 measured the last of those directly: with the election restriction removed,
`leader_completeness` fires on **114 of 200** aggressive seeds — and on **0 of 60**
merely-slow ones. The rule only matters when a partition or a crash is in play, which is
exactly why a test suite that does not simulate them will never find it missing.
