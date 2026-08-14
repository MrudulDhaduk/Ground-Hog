# Figure 2, as a checklist

Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended
Version)*, Figure 2 — transcribed, with the function in [node.py](node.py) that has to
enforce each rule.

Read the paper first. This is a checklist, not a substitute: it tells you *what* the
rules are and *where* they go, and says nothing about why they work. The why is the
part an interviewer asks about.

Rules marked **★** are the ones whose absence produces a system that passes every
ordinary test and loses data under a rare interleaving. They are why this project has a
simulator.

---

## State

### Persistent on all servers
*Updated on stable storage **before responding to RPCs**.*

| Field | Meaning | Where |
|---|---|---|
| `current_term` | latest term seen; starts at 0, only increases | `HardState` |
| `voted_for` | who got this node's vote this term, or `None` | `HardState` |
| `log` | entries, each with a command and the term it was created in; **first index 1** | `RaftLog` |

- [ ] **★ Nothing that depends on these leaves the node before `sync()` returns.**
  → every handler that replies. Grant a vote, crash, come back having forgotten it,
  grant it again — and now one term has two leaders.

### Volatile on all servers

| Field | Meaning |
|---|---|
| `commit_index` | highest entry known committed; starts 0, only increases |
| `last_applied` | highest entry applied to the state machine; starts 0, only increases |

### Volatile on leaders
*Reinitialised after every election.*

| Field | Initial value |
|---|---|
| `next_index[peer]` | leader's `last_index() + 1` |
| `match_index[peer]` | 0 |

`next_index` is a *guess* that gets walked back until it is right. `match_index` is
*knowledge* and never goes backwards.

---

## AppendEntries RPC

Leader → followers. Also the heartbeat, with `entries` empty.

**Receiver — `on_append_entries()`**

- [ ] 1. Reply false if `term < current_term`. (§5.1)
- [ ] 2. **★** Reply false if the log has no entry at `prev_log_index` whose term is
  `prev_log_term`. (§5.3) — *this is the consistency check; `has_entry()` and
  `term_at()` exist for it.*
- [ ] 3. **★** If an existing entry conflicts with a new one (same index, different
  term), delete that entry **and every entry after it**. (§5.3) — *and only then. An
  entry that matches must not be deleted; truncating on every AppendEntries throws away
  committed data.*
- [ ] 4. Append any new entries not already in the log.
- [ ] 5. If `leader_commit > commit_index`, set
  `commit_index = min(leader_commit, index of last new entry)`. (§5.3)

**Sender — `on_append_entries_reply()`**

- [ ] On success: update `next_index` and `match_index` for that follower. (§5.3)
- [ ] **★** `match_index` must never move backwards — a late or duplicated reply carries
  a stale index.
- [ ] On failure caused by log inconsistency: decrement `next_index` and retry. (§5.3)
- [ ] Distinguish "failed because the log disagrees" from "failed because my term is
  stale". They need opposite responses: retry lower, versus stop being leader.

---

## RequestVote RPC

Candidate → everyone.

**Receiver — `on_request_vote()`**

- [ ] 1. Reply false if `term < current_term`. (§5.1)
- [ ] 2. If `voted_for` is `None` or already this candidate, **and** the candidate's log
  is at least as up to date as ours, grant the vote. (§5.2, §5.4)

### ★ The election restriction (§5.4.1)

> Raft determines which of two logs is more up-to-date by comparing the index and term
> of the last entries in the logs. If the logs have last entries with different terms,
> then the log with the later term is more up-to-date. If the logs end with the same
> term, then whichever log is longer is more up-to-date.

- [ ] Compare **term first**, then length. Not length first. A longer log with an older
  last term is *behind*, not ahead.

This is the rule from spec §2's worked example. Delete it and everything still works
until a leader dies in a particular 5-millisecond window, at which point an
acknowledged write vanishes. M6 makes leader completeness fire when it is missing.

**Candidate — `on_request_vote_reply()`**

- [ ] Count grants. On a majority, become leader.
- [ ] Ignore replies from an older term — they answer a question you already stopped
  asking.
- [ ] Count each voter once. A duplicated reply is not a second vote.

---

## Rules for Servers

### All servers

- [ ] If `commit_index > last_applied`: increment `last_applied` and apply
  `log[last_applied]` to the state machine. (§5.3) → `_apply_committed()`, provided.
- [ ] **★ If any RPC request or response carries term `T > current_term`: set
  `current_term = T`, clear `voted_for`, become a follower.** (§5.1)
  → **every one of the four handlers.** It is deliberately not done for you in the
  dispatcher: forgetting it in exactly one handler is a classic Raft bug, and it should
  be findable here rather than in production.

### Followers (§5.2)

- [ ] Respond to RPCs from candidates and leaders.
- [ ] If the election timeout elapses with no AppendEntries from the current leader and
  no vote granted to a candidate: become a candidate. → `on_election_timeout()`

### Candidates (§5.2)

- [ ] On becoming a candidate, start an election:
  - [ ] increment `current_term`
  - [ ] vote for self
  - [ ] reset the election timer
  - [ ] send `RequestVote` to every other server
- [ ] Majority of votes → leader.
- [ ] AppendEntries from a *new leader* → become a follower.
- [ ] Election timeout elapses again → start another election.

- [ ] **★ Randomise the election timeout.** Fixed timeouts mean every follower wakes at
  once, splits the vote, and repeats — possibly forever. Randomisation is the whole
  liveness argument (§5.2). `_arm_election_timer()` draws it for you; know why.

### Leaders

- [ ] On election: send empty AppendEntries (heartbeats) to everyone, and keep sending
  during idle periods so nobody times out. (§5.2)
- [ ] On a client command: append to the local log, and **reply only once the entry is
  applied**. (§5.3) — *not on receipt. That is rung 3's bug.*
- [ ] If `last_index() >= next_index[peer]`: send AppendEntries starting at
  `next_index[peer]`.
- [ ] **★ If there is an `N` such that `N > commit_index`, a majority of `match_index`
  are `>= N`, **and** `log[N].term == current_term`: set `commit_index = N`.** (§5.4.2)
  → `advance_commit_index()`

### ★ Why a leader may only commit entries from its own term (§5.4.2)

Figure 8 in the paper. A new leader can hold an entry from an old term that is stored on
a majority of servers and is *still not committed* — a later leader can overwrite it.
Counting replicas alone is not enough to make it safe.

The rule: commit an old entry only *indirectly*, by committing an entry from your
current term that sits after it. Get this wrong and the system commits an entry that is
subsequently overwritten — which violates state machine safety while every individual
step looks correct.

---

## Where each function fits

| Function | Owner | Rules |
|---|---|---|
| `on_request_vote()` | **you** | RequestVote receiver 1–2, election restriction §5.4.1, term rule |
| `on_request_vote_reply()` | **you** | vote counting, majority, term rule |
| `on_append_entries()` | **you** | AppendEntries receiver 1–5, term rule |
| `on_append_entries_reply()` | **you** | next/match index, backtracking, term rule |
| `advance_commit_index()` | **you** | §5.4.2 |
| `on_election_timeout()` | **you** | candidate conversion, randomised timeout |
| `_apply_committed()` | provided | all-servers apply rule |
| `become_follower/candidate/leader()` | provided | role transitions, leader state reinit |
| `_arm_election_timer()` / `_arm_heartbeat_timer()` | provided | timer plumbing |
| `_persist()` | provided | writes hard state; **you decide when to `sync()`** |

---

## What is deliberately not here

Snapshots, log compaction, membership changes, the `conflict_index` fast-backtracking
optimisation (§5.3's last paragraph), pre-vote, leadership transfer, read-only query
optimisations. Spec §4 cuts all of them. The fast-backtracking optimisation is the only
one you might miss, and only because a naive `next_index -= 1` takes many round trips to
repair a long divergence — which costs simulated time and nothing else.
