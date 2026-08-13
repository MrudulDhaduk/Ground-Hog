"""Rung 3: replication done wrong, on purpose.

    "Until you have personally caused three copies of data to diverge, Raft is just
    vocabulary. After you have, every rule in the paper reads as 'oh, that's fixing
    the thing that bit me.'"  -- spec §5

Everything in here except `replicator.py` is plumbing. `replicator.py` is the exercise.
"""
