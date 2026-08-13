# GroundHog — Build Plan

A Raft KV store + deterministic simulation harness, in Python.
Same day, over and over, until the bugs run out.

Spec: [raft-dst-project.md](raft-dst-project.md)

---

## How to read this file

Every milestone is a **working program**. Nothing here is "scaffolding you can't run".
Do not start the next milestone until the current one's **Done when** line is true.

**Owner tags:**

| Tag | Meaning |
|---|---|
| `[C]` | Claude writes it. Plumbing, no consensus insight. |
| `[Y]` | **You** write it by hand, from the Raft paper. Claude does not touch these files. |
| `[C→Y]` | Claude writes the stub, signature, docstring and the failing test. You fill the body. |

The `[Y]` list is short on purpose. It is also the entire point of the project.
Per §5 of the spec: if Claude writes `RequestVote` and you can't defend the election
restriction in an interview, the project has negative value.

---

## Python-specific decisions (read once, then obey)

Go's version of this project gets determinism partly for free. Python does not.
These are the rules that make `seed in → run out` actually hold.

### The determinism rules

1. **One RNG object, passed explicitly.** Never `import random` and call
   `random.randint()`. The module-level RNG is global mutable state shared with every
   library you import. All randomness goes through `sim.rng.Rng`, which wraps a single
   `random.Random(seed)`.
   *Why it's safe:* Mersenne Twister is specified — `Random(847392).random()` returns
   the identical float on Windows, Linux, CPython 3.9 and 3.13.

2. **`PYTHONHASHSEED=0`, enforced in code.** Python randomizes `hash(str)` per process.
   That makes **set iteration order vary between runs** — the exact thing that destroys
   replay. The CLI checks the env var at startup and re-execs itself if it's unset.

3. **Never iterate a `set`. Ever.** Even with a fixed hash seed, this is a landmine.
   Use `dict` (insertion-ordered since 3.7, guaranteed) or `sorted()` with an explicit
   `key=`. If you need set semantics, use a `dict[K, None]`.

4. **Time is `int` microseconds.** No `float`, no `datetime`, no `timedelta`. Floats
   accumulate rounding differences; ints do not. Type alias: `Tick = int`.

5. **Banned inside a simulation run:** `time.time`, `time.monotonic`, `time.sleep`,
   `asyncio`, `threading`, `multiprocessing`, `socket`, `os.urandom`, `uuid4`, `id()`
   in any ordering decision, `__del__` finalizers.
   Enforced by a test that greps the package (`test_no_forbidden_imports`).

6. **Parallelism only across seeds, never inside one.** A single run is one thread, one
   loop. The sweep runner forks one process per seed.

7. **Total order in the event queue.** `heapq` entries are
   `(time, sequence_number, event)`. The `sequence_number` is a monotonically increasing
   int that guarantees ties break identically every run. **Never let the heap compare
   payload objects** — two events at the same tick would then be ordered by object
   contents or memory address.

### Architecture calls already made

- **Injected interfaces, not effect-lists.** The Raft node holds `Clock`, `Network` and
  `Storage` handles and calls them directly, as in the spec's Go sketch. (The
  alternative — `handle(msg) -> list[Effect]` — is marginally more testable but adds an
  abstraction layer you'd be fighting while learning Raft. Not worth it here.)
- **`typing.Protocol`, not `abc.ABC`.** Structural typing = Go interfaces. No
  inheritance, no registration.
- **Stdlib only in `groundhog/`.** No runtime dependencies at all. Every dependency is a
  new determinism risk and this project's whole premise is controlling the environment.
  Dev-only: `pytest`, `ruff`, `mypy`.
- **Hand-rolled binary codec** (`struct`, length-prefixed records). Not `pickle` (not
  version-stable, and it would hide the WAL framing work that is rung 2's whole lesson),
  not JSON (can't model a torn write at a byte offset).

---

## M0 — Skeleton and guardrails

- [x] `[C]` `pyproject.toml`, package `groundhog/`, `tests/`, ruff + mypy strict config
- [x] `[C]` `groundhog/types.py` — `NodeId`, `Term`, `Index`, `Tick` aliases
- [x] `[C]` `PYTHONHASHSEED` guard + re-exec shim in `groundhog/__main__.py`
- [x] `[C]` `tests/test_determinism_guards.py` — fails if any banned import appears in the package
- [x] `[C]` `pytest` runs green on an empty suite

**Done when:** `python -m groundhog --version` prints, `pytest` passes, `mypy --strict` is clean.

---

## M1 — The deterministic core

The heart. Everything else hangs off this.

- [x] `[C]` `sim/rng.py` — `Rng`: `below(n)`, `chance(pct)`, `pick(seq)`, `between(a,b)`.
      Tracks a call counter for trace output.
- [x] `[C]` `sim/event.py` — `Event`, `EventQueue` on `heapq` with `(tick, seq)` ordering
- [x] `[C]` `clock.py` — `Clock` Protocol; `SimClock` (advances only when the queue pops);
      timer registration + cancellation
- [x] `[C]` `sim/world.py` — `Simulator`: owns rng, clock, queue, node registry; `run(max_ticks)`
- [x] `[C]` `sim/trace.py` — JSONL event trace, one line per event, replayable and diffable
- [x] `[C]` Demo: two toy nodes ping-ponging with random delays (`groundhog demo`)
- [x] `[C]` `tests/test_replay.py` — **run seed 4471 twice, assert the two traces are byte-identical**

**Done when:** the same seed produces a byte-identical trace file across two runs, two
processes, and a reboot. If this is ever false, stop everything and fix it — every later
milestone is worthless without it.

---

## M2 — Storage, WAL, crash recovery *(spec rung 2)*

- [x] `[C]` `storage.py` — `Storage` Protocol: `append(entries)`, `sync()`, `read_all()`, `truncate_from(index)`
- [x] `[C]` `codec.py` — length-prefixed records: `[u32 len][u32 crc32][payload]`
- [x] `[C]` `FileStorage` — the real one, `os.fsync` on `sync()`
- [x] `[C]` `sim/disk.py` — `SimStorage` with the full fault menu:
  - [x] data written but not `sync()`ed is **lost on crash** (the important one)
  - [x] torn write: last record truncated mid-payload
  - [x] write fails with an error
  - [x] `sync()` is slow (costs virtual ticks — billed via `take_owed_ticks()`)
- [x] `[C]` Recovery: scan the WAL on restart, discard the trailing record if its CRC fails
- [x] `[C]` Tests: crash at every byte offset of a write; recovery must always yield a valid prefix

**Done when:** crashing mid-`append` at any offset, then restarting, always produces a
valid log that is a prefix of what was `sync()`ed.

---

## M3 — The fake network

- [x] `[C]` `network.py` — `Network` Protocol: `send(to, msg)`
- [x] `[C]` `sim/net.py` — `SimNetwork`, schedules delivery as a future event
- [x] `[C]` `sim/faults.py` — the full menu from spec §3:
  - [x] drop (probability)
  - [x] delay (random within a range)
  - [x] duplicate
  - [x] reorder (falls out of independent random delays — assert it actually happens)
  - [x] **partition**: a symmetric or one-way split for a duration
  - [x] node crash / restart (wipes volatile state, keeps synced disk)
  - [x] node hang (stops processing but stays "up")
- [x] `[C]` Fault schedule is generated from the rng up front and printed in the trace header
- [x] `[C]` `tests/test_faults.py` — each fault kind provably fires under some seed

**Done when:** `--seed N --faults aggressive` shows drops, partitions and crashes in the
trace, and re-running seed N reproduces the identical fault sequence.

---

## M4 — Break replication on purpose *(spec rung 3 — do not skip)*

> "Until you have personally caused three copies of data to diverge, Raft is just
> vocabulary."

- [ ] `[C]` `kv.py` — the state machine: `put/get/delete` applied from a committed log
- [ ] `[Y]` `naive/replicator.py` — **you write this.** One primary, two backups,
      fire-and-forget replication, ack the client immediately. No terms, no elections,
      no quorum.
- [ ] `[C]` `invariants/divergence.py` — a checker that just compares the three KV maps
- [ ] `[Y]` Point the simulator at it and find seeds where the three copies disagree
- [ ] `[Y]` **Write down, in `notes/rung3.md`, the three concrete ways you broke it**
      and which Raft rule fixes each one

**Done when:** you can name a seed that silently loses an acknowledged write, replay it,
and explain in one sentence why Raft would not have.

---

## M5 — Raft

The split from spec §5 applies here and nowhere else more strongly.

### Scaffolding
- [ ] `[C]` `messages.py` — `RequestVote`, `RequestVoteReply`, `AppendEntries`,
      `AppendEntriesReply`, `ClientRequest`, `ClientReply` as frozen dataclasses
- [ ] `[C]` `log.py` — `LogEntry(term, index, command)`, `RaftLog` with
      `last_index()`, `term_at(i)`, `slice_from(i)`, `truncate_from(i)`, `append(...)`
- [ ] `[C]` `raft/node.py` skeleton — role enum, persistent vs volatile state fields,
      the message dispatch switch, timer wiring, persistence calls
- [ ] `[C]` `raft/figure2.md` — Figure 2 of the paper transcribed as a checklist, each
      rule tagged with the function that must enforce it

### The core — `[Y]`, by hand, from the paper
- [ ] `[C→Y]` `on_request_vote()` — including the **election restriction**
      (§5.4.1: reject if the candidate's log is not at least as up-to-date)
- [ ] `[C→Y]` `on_request_vote_reply()` — vote counting, term stepping-down
- [ ] `[C→Y]` `on_append_entries()` — the consistency check, conflict truncation,
      `commitIndex` advance on the follower
- [ ] `[C→Y]` `on_append_entries_reply()` — `nextIndex`/`matchIndex`, backtracking on failure
- [ ] `[C→Y]` `advance_commit_index()` — majority `matchIndex`, **and the rule that a
      leader may only commit an entry from its own current term** (§5.4.2)
- [ ] `[C→Y]` `on_election_timeout()` — term increment, self-vote, randomized timeout
- [ ] `[C→Y]` Persistence ordering: `currentTerm`, `votedFor` and the log must be
      `sync()`ed **before** any reply that depends on them leaves the node

**Done when:** a 3-node cluster elects a leader, replicates writes, survives leader
crash, and recovers — under a clean network first, then under faults.

---

## M6 — The invariants

> "The invariants are the test. Fault injection only applies pressure."

Each runs after every event. Each must have a test that **deliberately breaks Raft and
proves the checker fires**. An invariant that has never caught anything is not known to work.

- [ ] `[C]` `invariants/base.py` — checker Protocol, registry, violation report w/ seed + tick
- [ ] `[C]` **Election safety** — at most one leader per term
- [ ] `[C]` **Log matching** — same (index, term) ⟹ all preceding entries identical
- [ ] `[C]` **Leader completeness** — a committed entry is in every future leader's log
- [ ] `[C]` **State machine safety** — an acked value is never lost or changed
- [ ] `[C]` Bonus: `commitIndex` never decreases; `lastApplied ≤ commitIndex`
- [ ] `[C]` `invariants/history.py` — client history log, for the state-machine check
- [ ] `[C]` `tests/test_invariants_catch_bugs.py` — one mutant per checker
      (e.g. delete the election restriction → leader completeness must fire)

**Done when:** commenting out the election restriction makes a sweep fail within a few
thousand seeds, and the reported seed replays the failure on demand.

---

## M7 — Seed sweeps and bug hunting *(spec rung 6 — the actual learning)*

- [ ] `[C]` `cli.py`:
  - [ ] `run --seed N [--trace out.jsonl]`
  - [ ] `sweep --from A --to B --workers 16`
  - [ ] `replay --seed N --verbose`
- [ ] `[C]` `multiprocessing.Pool`, one seed per worker, fail-fast + failure summary
- [ ] `[C]` Progress output: seeds/sec, elapsed, failures found
- [ ] `[C]` **Shrinking**: given a failing seed, minimize the fault schedule to the
      smallest set of faults that still reproduces it. Turns a 400-event trace into
      "these 3 things happened". Highest debugging leverage in the whole project.
- [ ] `[Y]` Run the multi-million-seed sweep. Fix what it finds. Keep a bug log.
- [ ] `[Y]` `notes/bugs.md` — one entry per real bug: seed, symptom, root cause, fix.
      **This file is the interview material.** Write it as you go; you will not
      reconstruct it later.

**Done when:** an overnight sweep runs clean over ≥1M seeds, and `notes/bugs.md` has at
least three genuine consensus bugs in it.

---

## M8 — Debug tooling

- [ ] `[C]` Human-readable trace renderer (timeline per node, colourized)
- [ ] `[C]` `--dump-state-at TICK` for stepping into a replayed failure
- [ ] `[C]` Graphviz/mermaid export of a message sequence around a violation
- [ ] `[C]` `--break-on-violation` → drop into `pdb` at the failing tick

---

## M9 — Prove the interfaces weren't a lie

If the simulated implementations are the *only* implementations, the abstraction was
never tested. This milestone is short but it is the one that makes the design claim true.

- [ ] `[C]` `RealClock`, `RealNetwork` (TCP), `FileStorage` wired into a real 3-process cluster
- [ ] `[C]` Same Raft code, unmodified, runs in both worlds
- [ ] `[C]` A smoke test: 3 real processes, kill one with `SIGKILL`, cluster survives

**Done when:** `groundhog serve --node 1` × 3 in separate terminals works, and
`git diff` shows zero changes to `raft/node.py`.

---

## M10 — The deliverable *(spec rung 7)*

- [ ] `[Y]` `DESIGN.md` — the fault model, the invariants, and **what this does not test**
      (spec §3 "Honest limitations" — state these out loud; it's the credibility marker)
- [ ] `[Y]` `README.md` — what it is, how to run a sweep, how to replay a failure
- [ ] `[C]` Reproduction instructions: exact commands for each bug in `notes/bugs.md`
- [ ] `[Y]` A short writeup of one bug, start to finish

---

## Explicitly out of scope

Snapshots, log compaction, membership changes, sharding, a web UI, benchmarking against
etcd, `asyncio` anywhere, Docker, any cloud. Spec §4. If a plan calls for these, it drifted.

---

## Progress

| Milestone | Status |
|---|---|
| M0 Skeleton | **done** |
| M1 Deterministic core | **done** |
| M2 Storage + recovery | **done** |
| M3 Fake network | **done** |
| M4 Break replication | not started |
| M5 Raft | not started |
| M6 Invariants | not started |
| M7 Sweeps | not started |
| M8 Debug tooling | not started |
| M9 Real world | not started |
| M10 Writeup | not started |
