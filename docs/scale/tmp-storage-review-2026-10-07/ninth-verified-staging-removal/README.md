# Additional archive staging cleanup

Removed four redundant local `.tar.gz` staging files: **469.3 MiB** allocated (492,101,632 bytes). Each local file matched committed archive metadata; a fresh full S3 readback verified archive bytes, every member, and the committed inventory before unlinking. Process, descriptor, Docker, mount, and loop-device checks were performed for each path; limitations of process visibility are recorded in the report.

Original fixture roots, retained causal evidence, live tests, both full 400k donors, and the million-timer primary remain local. See [removal report](removal.json) and [executed cleanup](executed-removal.py). No test qualification is changed by this cleanup.
