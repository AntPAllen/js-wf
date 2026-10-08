# Initial worker graph integration verification — failed

Frozen19a8242 passes native worker normal R1/R3 (3.360s), six focused journal groups normal (6.258s), client package normal (0.007s), and10,000 worker schedules/all503 pins (126.681s). The native worker race run fails R3 at the terminal client's pin cleanup with `blob publication CAS conflict`; R1 passes. No data-race detector report is present. The runner stops immediately after that failed command; later journal/client/simulation race commands remain unexecuted.

All1,398 selected tracked inputs matched Git before execution and remained unchanged afterward. This is a failed component verification attempt and is not accepted. The client pin release contends with the worker's release on the same logical root head. The intended fix retries only definite original-head CAS rejection after a new authority read; ambiguous mutations remain unretied. Full runtime migration and all broad release gates remain open.
