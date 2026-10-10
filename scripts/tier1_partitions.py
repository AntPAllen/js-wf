"""Assign the complete compiled inventory to deterministic disjoint processes."""
import re


def partition_tests(names, seeded, count):
    if not 2 <= count <= 32 or len(names) != len(set(names)) or not names:
        raise ValueError("invalid test inventory or process count")
    if any(not re.fullmatch(r"Test\w+", name) for name in names):
        raise ValueError("invalid compiled test name")
    if not seeded or len(seeded) != len(set(seeded)) or not set(seeded).issubset(names) or len(seeded) < count:
        raise ValueError("every process needs a source-inventoried seeded test")
    groups = [[] for _ in range(count)]
    # Place seeded loops first so each process emits real aggregate coverage.
    # Selection does not change Go's compiled execution order within a process.
    seeded_names = set(seeded)
    ordered = [n for n in names if n in seeded_names] + [n for n in names if n not in seeded_names]
    for index, name in enumerate(ordered):
        groups[index % count].append(name)
    return groups


def selector(names):
    return "^(" + "|".join(names) + ")$"
