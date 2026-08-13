# Rung 3 — how I broke replication

> **This file is yours (M4 `[Y]`).** Claude wrote the headings and nothing under them.
>
> Write it as you go. You will not reconstruct it later, and per spec §5 this is the
> difference between a project that helps you in an interview and one that hurts you.

---

## The setup

- Replicator: `groundhog/naive/replicator.py`
- Command to reproduce anything below: `groundhog naive --seed N --faults PROFILE --trace out.jsonl`

Decisions I made while writing it (from the stub's list):

1. Ack before or after `sync()`? →
2. Log first or apply first? →
3. Do backups persist? →
4. Does a restarted primary re-send anything? →

---

## Failure 1

**Seed:**
**Profile:**
**What the client was promised:**
**What the three copies actually held:**

**What happened, in order:**

**Which Raft rule fixes it, and how:**

---

## Failure 2

**Seed:**
**Profile:**
**What the client was promised:**
**What the three copies actually held:**

**What happened, in order:**

**Which Raft rule fixes it, and how:**

---

## Failure 3

**Seed:**
**Profile:**
**What the client was promised:**
**What the three copies actually held:**

**What happened, in order:**

**Which Raft rule fixes it, and how:**

---

## The one-sentence version

> **Done when:** you can name a seed that silently loses an acknowledged write, replay
> it, and explain in one sentence why Raft would not have.

Seed:

Sentence:
