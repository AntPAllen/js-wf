# Seeded production-worker fresh timer wakeup regression

The workload starts with an unshifted old run timestamp,then uses a scheduling
leader and TimerNow at ±60s. Subsequent timer wakeups carry that scheduling
leader's timestamp. It executes the real worker,SDK,lease,CAS journal and
immutable outcome paths through modeled ports. Scheduling/delivery assumptions
are explicit; this is not a full NATS implementation.

Eight sequential waits cover all combinations of Sleep,Await,SelectSignal or
Select,positive250ms or1s duration,and both source offsets (16 cases). Every
fresh wait must suspend,schedule once and withhold its wakeup until the virtual
duration elapses. All eight must finish in exactly eight durations,nine handler
replays,one immutable terminal42,zero queued or physically retained runs and a
passing retained-state invariant audit. Completed due waits replay without
rescheduling. Four behind/250ms traces pin the real failure relationship.

1,000 seeds pass1.507s. Combined race execution of this workload and all184
pinned traces passes30.886s. General trace replay/minimization supports the new
workload. Overlaying the original SDK implementation fails at seed1 in0.004s:
all eight sleeps complete at virtual time0,with18 journal entries and zero
scheduled timers. The original failing trace,output and tested source hashes
are retained. This negative control isolates a production SDK decision from
modeled NATS scheduling behavior.

The old absolute-deadline source-transition characterization remains valid
and open; this regression does not certify arbitrary in-flight clock changes
or the full release gates. Full source55bd33e simulation proof predates these
four additional pins and is retained separately.
