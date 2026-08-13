# Raft KV Store with Deterministic Simulation Testing

A distributed key-value store built on the Raft consensus algorithm, tested by a
custom deterministic simulation harness that can reproduce any failure from a
single seed number.

---

## 1. What this project is, in plain words

Two things get built:

1. **A small replicated database.** You send it `put(x, 5)` and `get(x)`. It keeps
   the data on 3 machines instead of 1, so it survives a machine dying. The
   machines stay in agreement using Raft.

2. **A fake world to torture it in.** Instead of running the 3 machines over a
   real network, they run inside one single-threaded program, with a fake clock,
   a fake network, and a fake disk that you control completely. You order the
   fake world to delay messages, drop them, reorder them, kill nodes mid-write,
   and fail disk writes halfway.

The second part is the actual project. Raft implementations are common. The
simulator is not.

---

## 2. The problem being solved

### The real-world problem

If your bank balance lives on one server and that server's disk dies, your money
is gone. So it lives on 3 or 5 servers. But now a harder problem appears: those
machines must agree on what the data is, while messages get lost, delayed,
duplicated and reordered, and while machines crash halfway through writing.

If they disagree, the database silently corrupts. Raft's job is to make N
machines behave, from the outside, like one machine that never dies and never
lies. This is not academic — etcd (Raft-based) stores the entire state of every
Kubernetes cluster in the world. CockroachDB, Consul, and TiDB all depend on it.

### What a real bug actually looks like

Three nodes: A (leader), B, C.

1. Client sends `put(x=5)` to A.
2. A writes `x=5` to its own log.
3. A sends "add x=5" to B and C.
4. B writes it and replies OK. **The message to C is still stuck in the network.**
5. A has 2 of 3 copies — a majority — so it commits and tells the client
   **"saved successfully."**
6. **A crashes.**
7. C, having heard nothing, starts an election and asks B for a vote.
8. C's log does not contain `x=5`. B's does.

The correct Raft rule: B must **refuse** to vote for C, because C's log is behind.
That single rule is what keeps `x=5` alive.

Forget that rule, and C wins the election with a log that never had `x=5`, forces
B to delete it, and the value the client was promised is gone.

**The bug is a missing `if` statement.** The code looks fine. It passes every
normal test. It only breaks when a message is delayed *and* the leader crashes in
that exact window *and* the wrong node times out first — three coincidences at
once.

### Why normal testing cannot catch this

Those coincidences happen by luck. Maybe 1 run in 50,000. And when it finally
happens, you cannot reproduce it — thread scheduling, network jitter and disk
timing differ every run, so the failure evaporates the moment it occurs.

The industry-standard experience: lose data once every eight months, put three
engineers on it for two weeks, fail to reproduce it, add log lines, ship, hope.

---

## 3. How deterministic simulation solves it

### The core mechanism

In a normal program, four things come from outside and differ every run:

| Source | Controlled by |
|---|---|
| Thread scheduling | the OS |
| Message arrival | the network |
| `time.Now()` | the clock |
| Disk write timing | the disk |

Delete all four and write your own:

```
rng    = new Random(seed = 847392)
now    = 0
events = priority queue ordered by time

function send(from, to, msg):
    if rng.next() % 100 < 5:
        return                          // drop the message
    delay = rng.next() % 50
    events.add(now + delay, deliver msg to `to`)

loop:
    e   = events.take_earliest()
    now = e.time                        // time is just a number we advance
    deliver(e)

    if rng.next() % 1000 == 0:
        crash(node[rng.next() % 3])

    check_invariants()
```

Everything runs in **one loop, one thread**. Nothing is truly concurrent, so
events have a definite order. Every "random" choice comes from `rng`.

The key fact: **a seeded PRNG is not random.** Seed 847392 always produces the
same number sequence on any machine, any day. It only looks random.

So the whole program collapses into a pure function: `seed in → run out`. Feed in
847392 and you get a byte-for-byte identical run, forever.

### What that buys you

```
for seed in 1 .. 1,000,000:
    run(seed)
```

Each seed is a different universe of disasters. Somewhere around seed 847,392 the
three coincidences line up, `check_invariants()` fires, and the harness prints
the seed.

Now you type `run(847392)` and replay that exact disaster on demand, in a
debugger, as many times as you like.

**You are not solving one bug. You are covering a space.**

### Why simulation transfers to the real world

The common objection: *"we don't control the real network, so what's the point?"*

The network was never the thing that was broken. Delays are normal and permanent.
The bug was **your code losing data when a delay happens at the wrong moment**.

The reason this works is that the world's menu of misbehaviour is finite:

| Component | Everything it can do |
|---|---|
| Network | deliver, delay, drop, duplicate, reorder |
| Disk | succeed, fail, write only part of it |
| Node | run, crash, restart, hang |

About a dozen behaviours. Infinite *instances* (a delay can be 3ms or 3000ms) but
a fixed list of *kinds*. Production does not invent a thirteenth.

So you are not predicting the specific disaster that will hit you. You are
covering the space of disasters that exist — the same logic as crash-testing a
car. Nobody controls real accidents; you crash it 10,000 times in a lab and build
a chassis that survives the whole space.

**The only thing that changes is who finds the bug first: you, or your users.**

### Honest limitations

- The simulator only tests your **model** of the world. If real disks lie about
  having flushed data (some do) and your fake disk never lies, that class of bug
  is invisible to you. Testing quality = fault-model quality.
- **The invariants are the test.** If you never wrote the assertion "a committed
  value must never disappear," the harness will run a million seeds, report
  success, and your data will still vanish. The fault injection only applies
  pressure; the invariants are what detect the damage.

Stating these limits out loud is what separates someone who has used the
technique from someone who has read about it.

---

## 4. Scope

### In scope for v1

- 3-node cluster
- Leader election (terms, votes, heartbeats, election restriction)
- Log replication and commit rules
- Persistence and crash recovery
- Deterministic simulator: seeded PRNG, virtual clock, event queue
- Fault injection: partitions, drops, delays, reordering, node crash/restart,
  partial disk writes
- 4 invariant checkers
- Seed sweep runner + failure replay from a seed

### Explicitly out of scope

Snapshots, log compaction, cluster membership changes, a real network layer,
sharding, a web UI, benchmarking against etcd.

Every one of these adds weeks and adds nothing to the signal. Cut them.

### Invariants to check

1. **Election safety** — at most one leader per term.
2. **Log matching** — if two logs have an entry with the same index and term,
   all preceding entries are identical.
3. **Leader completeness** — a committed entry is present in the log of every
   future leader.
4. **State machine safety** — a value reported as committed to a client is never
   lost or changed.

---

## 5. Build plan

Do not start at Raft. Each rung below is a working program.

| Rung | What you build | What you learn | Time |
|---|---|---|---|
| 0 | Go basics | syntax, goroutines, interfaces | 1 week |
| 1 | Single-node KV store over TCP | sockets | weekend |
| 2 | Write-ahead log + restart recovery | durability, `fsync` | weekend |
| 3 | Naive replication to 2 backups — **then break it on purpose** | *why Raft exists* | 1 week |
| 4 | Deterministic simulator around that small broken system | the technique, on easy mode | 1 week |
| 5 | Raft, with the harness already in place | consensus | 4-5 weeks |
| 6 | Seed sweeps, bug hunting, fixes | the actual learning | 2 weeks |
| 7 | Design doc + README + reproduction instructions | the deliverable | 1 week |

**Rung 3 is the important one.** Until you have personally caused three copies of
data to diverge, Raft is just vocabulary. After you have, every rule in the paper
reads as "oh, that's fixing the thing that bit me."

**Total: roughly 120-160 hours.** At 10-12 h/week that is 10-12 weeks. At 20
h/week, 5-6 weeks.

**Checkpoint:** by the end of rung 4 you should be able to run `--seed 4471`,
watch a leader get elected, kill it mid-run, and watch a new one take over —
identically, every single time. If that does not work, stop and fix the
foundation before touching Raft.

### The one design decision that cannot be retrofitted

Hide time, network, and disk behind interfaces **from day one**:

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}

type Network interface {
    Send(to NodeID, msg Message) error
}

type Storage interface {
    Append(entries []Entry) error
    Sync() error
}
```

Two implementations of each: a real one and a simulated one. If you write
`time.Now()` or `net.Dial()` directly inside your Raft code, you cannot add the
simulator later — you rewrite from scratch.

### Using Claude Code without hollowing out the project

If Claude writes your Raft and an interviewer asks *"why does the election
restriction guarantee committed entries survive?"* and you cannot answer, the
project actively hurts you. Split the work:

- **Let Claude write:** event loop, network simulator, serialization, CLI, test
  harness, invariant plumbing, logging and trace output. Roughly 60% of the code
  and none of the insight.
- **Write yourself, by hand, from the paper:** `RequestVote`, `AppendEntries`,
  commit index rules, the election restriction.

Useful trick: once the simulator works, deliberately let Claude draft a Raft
implementation and point your own fault injector at it. It will produce code that
looks correct and fails on subtle orderings — exactly as humans do. Debugging a
wrong Raft with a tool that hands you a reproducible seed is the fastest possible
way to understand the algorithm, and it makes a better writeup.

---

## 6. Hardware requirements

The whole point of this design is that it runs on one machine. No cluster, no
cloud account, no GPU.

**Minimum**

| Item | Requirement |
|---|---|
| CPU | Any dual-core x86-64 or ARM |
| RAM | 4 GB |
| Disk | 5 GB free |
| Network | Only to download Go and dependencies |

**Recommended**

| Item | Requirement | Why |
|---|---|---|
| CPU | 4+ cores | seed sweeps are embarrassingly parallel — one seed per core |
| RAM | 8 GB+ | ~50-200 MB per simulation run |
| Disk | SSD, 20 GB free | trace output during debugging |

**On your current laptop:** the Ryzen 7 8840HS gives you 8 cores / 16 threads,
and with 24 GB of RAM you can comfortably run 16 seeds in parallel. That is a
multi-million-seed sweep overnight, on battery, with no cloud spend. You are
substantially over-provisioned for this project — no hardware purchase is needed
at any point.

**What you do not need:** GPU, multiple machines, Docker, Kubernetes, AWS/GCP
credits, a home lab. If a plan calls for any of these, the design has drifted.

---

## 7. Software requirements

### Required

| Tool | Version | Notes |
|---|---|---|
| Go | 1.22 or newer | install the latest stable release |
| Git | any recent | version control |
| An editor | — | VS Code + the official Go extension, or GoLand |
| Claude Code | latest | scaffolding, harness, plumbing |

### Recommended

| Tool | Purpose |
|---|---|
| `delve` (`dlv`) | step through a replayed failing seed |
| `go test -race` | catches accidental concurrency in the real-network build |
| `pprof` | built into Go; profile the sweep if it gets slow |
| `graphviz` | render event traces into readable diagrams |

### Not needed

Docker, Kubernetes, a message broker, a database, any cloud account, any paid
service. The entire project is `go run` on one machine.

### Notes on setup

- **OS:** Windows, Linux, and macOS all work. Go compiles natively on Windows —
  WSL2 is optional, not required.
- **Why Go:** excellent concurrency primitives you can deliberately bypass, a
  strong standard library for networking and encoding, fast compile-test loops,
  and it is the language etcd and CockroachDB are written in — which is exactly
  the audience this project targets. Rust is a valid alternative if you already
  know it; do not learn Rust and distributed systems simultaneously.
- **Student licence:** GoLand is free via the JetBrains student pack with a
  university email address, if you prefer it to VS Code.

---

## 8. Reading list

Read these in order. Do not skip the first one.

1. **"In Search of an Understandable Consensus Algorithm (Extended Version)"** —
   Ongaro & Ousterhout. The Raft paper. 18 pages. Read it twice: once for shape,
   once for the exact rules in Figure 2.
2. **raft.github.io** — the visualisation. Watch an election happen before you
   write any code.
3. **FoundationDB's testing writeups and talks** — the canonical source for this
   simulation technique.
4. **TigerBeetle's simulator (VOPR)** — a modern, readable implementation of the
   same idea.

Your CS G623 syllabus covers time in distributed systems, agreement and
consistency, coordination algorithms, and fault tolerance — this project sits
directly on top of those units, and CS G623 has a 30% project component.

---

## 9. What this project is and is not

**It is:** a training exercise that produces a serious artifact. It builds the
mental model FAANG infra interviews test for — reasoning about failure,
concurrency and correctness when parts of the system are broken — which cannot be
crammed the week before an interview.

**It is not:** a product. Nobody will use your KV store; etcd exists and is
better. This will earn ₹0.

It is entirely a resume/skill play. Judge it on that basis.
