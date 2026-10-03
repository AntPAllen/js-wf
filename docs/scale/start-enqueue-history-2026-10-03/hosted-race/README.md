# Complete hosted checker-source Tier1 race qualification

Run37120760618 at9de1e726f6633e29a52f8b7b45e44b4ae1ac08c4 passes the complete
default1k race suite in2108.534s:168 top-level passes, only the two documented
trace-only skips,269 pins and121,000 bodies across121 seeded workloads.

Every980 before/after source hash matches exact Git. Retained binary SHA256,
race build metadata and actual simulator working directory are checked.
Independent regeneration from raw Go events and recorded inventories produces
the uploaded result byte-for-byte. Every archived member SHA256 is verified
by reopening the temporary archive before atomic rename.

The archive contains raw events, retained binary, source/inventory hashes,
command working directories, terminal job metadata and both verifier results.
This includes the corrected Start checker and its two fixed regression pins.
Later real-fixture additions,100k, real matrices and full release retain their
separate qualification requirements.
