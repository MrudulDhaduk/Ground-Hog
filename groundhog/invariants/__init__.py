"""The checkers.

Spec §3: *the invariants are the test.* Fault injection only applies pressure -- if
nobody ever wrote down "a committed value must never disappear", a million seeds will
run, report success, and the data will still be gone.

M6 adds the registry and the four Raft invariants. M4 starts with the one that needs no
consensus vocabulary at all: three copies of the same data should be the same.
"""
